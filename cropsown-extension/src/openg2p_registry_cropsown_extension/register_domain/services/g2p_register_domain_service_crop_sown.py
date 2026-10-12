import logging
import re
from datetime import date

from sqlalchemy import func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from openg2p_registry_core.models import G2PRegisterChangeRequest
from openg2p_registry_core.services import G2PRegisterDomainService

from .domain_validation_utils import as_float, as_int, is_blank, validation_error
from .farmer_registry_enrichment import compose_farmer_name, fetch_farmer_record, to_farmer_fields

_logger = logging.getLogger("g2p-register-domain-service")


_MOBILE_NUMBER_PATTERN = re.compile(r"^(\+251[79]\d{8}|0[79]\d{8})$")

# Farmer ids are issued by the farmer registry as FR- followed by ten digits.
_FARMER_ID_PATTERN = re.compile(r"^FR-[0-9]{10}$")

_SECTION_LIFECYCLE_STAGE_MAP = {
    "cs_planning_details": "PLANNING_APPROVED",
    "cs_cluster_details": "PLANNING_APPROVED",
    "cs_cultivation_details": "CULTIVATION_APPROVED",
    "cs_cultivation_cluster_details": "CULTIVATION_APPROVED",
    "cs_sowing_details": "SOWING_APPROVED",
    "cs_production_details": "SOWING_APPROVED",
    "cs_infestation_details": "SOWING_APPROVED",
    "cs_harvest_details": "HARVESTING_APPROVED",
}

class G2PRegisterDomainServiceCropSown(G2PRegisterDomainService):
    async def validate_domain_attributes(self, records: list[dict], session=None, **kwargs):
        for record in records:
            # On submit, the Farmer Registry's copy of this farmer replaces the
            # typed identity (matched on BOTH Farmer ID + Fayda). Fails open.
            await self._enrich_from_farmer_registry(record)

            from .domain_validation_utils import validate_alphabetical_name, validate_mobile_number
            validate_alphabetical_name(record.get("farmer_name"), "Farmer Name")
            self._validate_crop_year(record)
            self._validate_farmer_id(record)
            self._validate_fayda_fan_id(record)

            if session:
                await self._validate_fayda_season_and_year(record, session)

    async def _enrich_from_farmer_registry(self, record: dict) -> None:
        """Replace the typed farmer identity with the Farmer Registry's record,
        matched on BOTH Farmer ID and Fayda FAN ID (both-required) via a signed,
        consent-carrying DCI search. The registry's data wins over the operator's.
        Fails open: leaves the record as typed when enrichment is off/misconfigured,
        both ids are blank, or there is no single match (see
        farmer_registry_enrichment.fetch_farmer_record)."""
        farmer_id = record.get("farmer_id")
        fayda = record.get("fayda_fan_id") or record.get("fayda_id")
        if is_blank(farmer_id) and is_blank(fayda):
            return
        personal = await fetch_farmer_record(farmer_id, fayda)
        if not personal:
            return
        fields = to_farmer_fields(personal)
        name = compose_farmer_name(fields.get("first_name"), fields.get("middle_name"), fields.get("last_name"))
        if name:
            record["farmer_name"] = name
        if not is_blank(fields.get("fayda_fan_id")) and is_blank(record.get("fayda_fan_id")):
            record["fayda_fan_id"] = fields["fayda_fan_id"]
        _logger.info("Enriched crop_sown farmer %s from Farmer Registry: name=%s", farmer_id, name)

    async def _validate_fayda_season_and_year(self, record: dict, session) -> None:
        fayda_fan_id = record.get("fayda_fan_id") or record.get("fayda_id")
        crop_year = record.get("crop_year")
        prod_season = record.get("production_season")

        if not fayda_fan_id or not str(fayda_fan_id).strip():
            return

        from sqlalchemy import text
        query_reg = (
            "SELECT crop_year, production_season "
            "FROM g2p_register_crop_sowns "
            "WHERE fayda_fan_id = :fayda AND record_status = 'ACTIVE'"
        )
        params_reg = {"fayda": str(fayda_fan_id).strip()}
        if crop_year:
            query_reg += " AND crop_year = :c_year"
            params_reg["c_year"] = str(crop_year).strip()
        query_reg += " ORDER BY created_at DESC"

        res_reg = await session.execute(text(query_reg), params_reg)
        row_reg = res_reg.fetchone()
        if row_reg:
            reg_year, reg_season = row_reg[0], row_reg[1]
            p_clean = str(prod_season or "").replace("CROP_SEASON_", "").strip().upper()
            r_clean = str(reg_season or "").replace("CROP_SEASON_", "").strip().upper()
            if p_clean and r_clean and p_clean != r_clean:
                validation_error(
                    f"Production Season '{prod_season}' in Farmer Identity does not match registered Crop Season '{reg_season}' for Fayda ID '{fayda_fan_id}'."
                )
            if crop_year and reg_year and str(crop_year).strip() != str(reg_year).strip():
                validation_error(
                    f"Crop Year '{crop_year}' in Farmer Identity does not match registered Crop Year '{reg_year}' for Fayda ID '{fayda_fan_id}'."
                )

    def _validate_crop_year(self, record: dict) -> None:
        year = as_int(record.get("crop_year"))
        if year is not None and year > date.today().year:
            validation_error("crop_year must not be in the future")


    def _validate_farmer_id(self, record: dict) -> None:
        """Farmer ids come from the farmer registry as FR- plus ten digits."""
        value = record.get("farmer_id")
        if value is None or str(value).strip() == "":
            return
        if not _FARMER_ID_PATTERN.match(str(value).strip()):
            validation_error(
                f"farmer_id must be FR- followed by 10 digits (got '{value}')"
            )

    def _validate_fayda_fan_id(self, record: dict) -> None:
        value = record.get("fayda_fan_id")
        if value is None or str(value).strip() == "":
            return
        # Fayda FAN / UIN as held by the Farmer Registry's foundational_id: numeric digits.
        fyda_pattern = r'^\d{6,20}$'
        if not re.match(fyda_pattern, str(value).strip()):
            validation_error("Fayda ID (UIN) must be numeric digits, e.g. 4733616459")

    def _validate_mobile_number(self, record: dict, field: str) -> None:
        value = record.get(field)
        if value is None or str(value).strip() == "":
            return
        if not _MOBILE_NUMBER_PATTERN.match(str(value).strip()):
            validation_error(f"{field} is not a valid mobile number")

    def construct_search_text(self, payload: dict, extra: list[str] = None) -> str:
        _logger.info("Constructing search text for crop sown record")

        keys = [
            "functional_record_id",
            "farmer_name",
            "farmer_id",
            "fayda_fan_id",
            "status",
            "crop_year",
            "production_season",
            "lifecycle_stage",
            "region",
            "zone",
            "woreda",
            "kebele",
            "latitude",
            "longitude",
            "address_line_1",
            "address_line_2",
            "postal_code",
            "country_code",
        ]
        search_text = []
        if extra:
            search_text.extend(str(item).strip() for item in extra if str(item).strip())
        search_text.extend(
            str(payload.get(key) or "").strip()
            for key in keys
            if str(payload.get(key) or "").strip()
        )

        return " ".join(search_text).strip()

    def construct_record_name(self, payload: dict, extra: list[str] = None) -> str:
        _logger.info("Constructing record name for crop sown record")

        keys = ["farmer_id"]
        record_name = []
        if extra:
            record_name.extend(str(item).strip() for item in extra if str(item).strip())
        record_name.extend(
            str(payload.get(key) or "").strip()
            for key in keys
            if str(payload.get(key) or "").strip()
        )

        return " ".join(record_name).strip()

    # ── Cross-record rules ──────────────────────────────────────────────────
    # `validate_domain_attributes` only sees the records being written, so rules
    # that span a record and its siblings cannot live there. `pre_approve` runs
    # on approval with a live session, which is the first point both the record
    # and the database are available — so the Odoo @api.constrains that query
    # other rows are enforced here.

    async def pre_approve(self, change_request: G2PRegisterChangeRequest, session: AsyncSession):
        await self._check_unique_farmer_per_year(change_request, session)
        await self._check_crop_area_within_plot(change_request, session)
        await self._check_matching_land_id_across_sections(change_request, session)
        await self._check_matching_season_across_sections(change_request, session)
        await self._check_section_dates_validity(change_request, session)
        await self._recompute_ec_dates_across_sections(change_request, session)
        await self._refresh_admin_names(change_request, session)
        await self._adopt_uploaded_photo(change_request, session)

    async def _check_matching_season_across_sections(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Check that Season in Planning, Cultivation, Cluster, Sowing, and Production match Farmer Identity production_season."""
        from sqlalchemy import text
        from ..models import G2PRegisterCropSown

        rec_id = change_request.internal_record_id
        if not rec_id:
            return

        record = await session.get(G2PRegisterCropSown, rec_id)
        header_season = getattr(record, "production_season", None) if record else None
        sub_id = getattr(change_request, "submission_id", None)
        if not header_season and sub_id:
            res_sub = await session.execute(
                text("SELECT production_season FROM g2p_intake_form_crop_sowns WHERE submission_id = :sub_id AND production_season IS NOT NULL"),
                {"sub_id": sub_id}
            )
            row_sub = res_sub.fetchone()
            if row_sub:
                header_season = row_sub[0]

        if not header_season:
            return

        header_clean = str(header_season).replace("CROP_SEASON_", "").strip().upper()

        section_tables = [
            ("g2p_register_plannings", "g2p_intake_form_plannings", "Crop Planning"),
            ("g2p_register_cultivations", "g2p_intake_form_cultivations", "Cultivation / Land Preparation"),
            ("g2p_register_cultivation_clusters", "g2p_intake_form_cultivation_clusters", "Cultivation Cluster"),
            ("g2p_register_clusters", "g2p_intake_form_clusters", "Cluster Information"),
            ("g2p_register_sowings", "g2p_intake_form_sowings", "Sowing"),
            ("g2p_register_productions", "g2p_intake_form_productions", "Production"),
        ]

        for reg_tbl, intake_tbl, section_name in section_tables:
            season_col = "COALESCE(season, cluster_season)" if "sowing" in reg_tbl else "season"
            res_reg = await session.execute(
                text(f"SELECT {season_col} FROM {reg_tbl} WHERE link_internal_record_id = :rec_id AND record_status = 'ACTIVE' AND {season_col} IS NOT NULL"),
                {"rec_id": rec_id}
            )
            for r in res_reg.fetchall():
                if r[0]:
                    r_clean = str(r[0]).replace("CROP_SEASON_", "").strip().upper()
                    if r_clean != header_clean:
                        validation_error(
                            f"Season '{r[0]}' in {section_name} does not match the Crop Season '{header_season}' specified in Farmer Identity."
                        )

            if sub_id:
                intake_season_col = "COALESCE(season, cluster_season)" if "sowing" in intake_tbl else "season"
                res_int = await session.execute(
                    text(f"SELECT {intake_season_col} FROM {intake_tbl} WHERE submission_id = :sub_id AND {intake_season_col} IS NOT NULL"),
                    {"sub_id": sub_id}
                )
                for r in res_int.fetchall():
                    if r[0]:
                        r_clean = str(r[0]).replace("CROP_SEASON_", "").strip().upper()
                        if r_clean != header_clean:
                            validation_error(
                                f"Season '{r[0]}' in {section_name} does not match the Crop Season '{header_season}' specified in Farmer Identity."
                            )

    async def _check_section_dates_validity(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Enforce date-in-season and stage chronology constraints before approval."""
        from .g2p_register_domain_service_cultivation import G2PRegisterDomainServiceCultivation
        from .g2p_register_domain_service_sowing import G2PRegisterDomainServiceSowing
        from .g2p_register_domain_service_harvest import G2PRegisterDomainServiceHarvest

        rec_id = change_request.internal_record_id
        if not rec_id:
            return

        from sqlalchemy import text

        # Load pending CR payloads if available
        cr_payload_items = []
        try:
            from openg2p_registry_core.services.g2p_register_change_request_service import G2PRegisterChangeRequestService
            cr_service = G2PRegisterChangeRequestService.get_component() or G2PRegisterChangeRequestService()
            payload_obj = await cr_service._get_change_request_payload(change_request.change_request_id, session)
            if payload_obj and payload_obj.change_payload:
                cr_payload_items = payload_obj.change_payload
        except Exception as e:
            _logger.debug("Failed loading CR payload in _check_section_dates_validity: %s", e)

        # Cultivation check
        res_c = await session.execute(
            text("SELECT land_id, actual_cultivation_date, start_gc, end_gc, season, commodity, actual_crop_area FROM g2p_register_cultivations WHERE link_internal_record_id = :rec_id AND record_status = 'ACTIVE'"),
            {"rec_id": rec_id}
        )
        c_records = [
            {
                "link_internal_record_id": rec_id,
                "land_id": row[0],
                "actual_cultivation_date": row[1],
                "start_gc": row[2],
                "end_gc": row[3],
                "season": row[4],
                "commodity": row[5],
                "actual_crop_area": row[6],
            }
            for row in res_c.fetchall()
        ]
        if change_request.section_id == 'cropsown_cultivation_details_section_01' and cr_payload_items:
            for item in cr_payload_items:
                if isinstance(item, dict):
                    rec = dict(item)
                    rec["link_internal_record_id"] = rec_id
                    c_records.append(rec)
        if c_records:
            await G2PRegisterDomainServiceCultivation().validate_domain_attributes(c_records, session=session)

        # Sowing check
        res_s = await session.execute(
            text("SELECT land_id, sowing_date, NULL AS start_gc, NULL AS end_gc, COALESCE(season, cluster_season) AS season, commodity, COALESCE(area_sown, cluster_area_sown) AS area_sown FROM g2p_register_sowings WHERE link_internal_record_id = :rec_id AND record_status = 'ACTIVE'"),
            {"rec_id": rec_id}
        )
        s_records = [
            {
                "link_internal_record_id": rec_id,
                "land_id": row[0],
                "sowing_date": row[1],
                "start_gc": row[2],
                "end_gc": row[3],
                "season": row[4],
                "commodity": row[5],
                "area_sown": row[6],
            }
            for row in res_s.fetchall()
        ]
        if change_request.section_id == 'cropsown_sowing_details_section_01' and cr_payload_items:
            for item in cr_payload_items:
                if isinstance(item, dict):
                    rec = dict(item)
                    rec["link_internal_record_id"] = rec_id
                    s_records.append(rec)
        if s_records:
            await G2PRegisterDomainServiceSowing().validate_domain_attributes(s_records, session=session)

        # Harvest check
        res_h = await session.execute(
            text("SELECT land_id, harvest_date, NULL AS start_gc, NULL AS end_gc, NULL AS season, NULL AS commodity, area_harvested FROM g2p_register_harvests WHERE link_internal_record_id = :rec_id AND record_status = 'ACTIVE'"),
            {"rec_id": rec_id}
        )
        h_records = [
            {
                "link_internal_record_id": rec_id,
                "land_id": row[0],
                "harvest_date": row[1],
                "start_gc": row[2],
                "end_gc": row[3],
                "season": row[4],
                "commodity": row[5],
                "area_harvested": row[6],
            }
            for row in res_h.fetchall()
        ]
        if change_request.section_id == 'cropsown_harvest_details_section_01' and cr_payload_items:
            for item in cr_payload_items:
                if isinstance(item, dict):
                    rec = dict(item)
                    rec["link_internal_record_id"] = rec_id
                    h_records.append(rec)
        if h_records:
            await G2PRegisterDomainServiceHarvest().validate_domain_attributes(h_records, session=session)

    async def _adopt_uploaded_photo(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Promote a photo uploaded on the form to the record's avatar.

        A file widget stores its upload as a *section document* — the submit
        flow posts `documents: [{document_id, label}]` and never touches
        `record_image_document_id`, which is the field the profile widget and
        the record tree read. So an uploaded photo is saved but invisible.

        This bridges the two: on approval, if the record has no avatar yet, the
        earliest image attached to it becomes one. Records that already have an
        avatar are left alone, so re-approving cannot swap someone's photo.
        """
        from openg2p_registry_core.models import (
            G2PRegisterSectionDocument,
            G2PRegistryDocument,
        )

        from ..models import G2PRegisterCropSown

        record = await session.get(G2PRegisterCropSown, change_request.internal_record_id)
        if record is None or record.record_image_document_id:
            return

        row = (
            await session.execute(
                select(G2PRegistryDocument)
                .join(
                    G2PRegisterSectionDocument,
                    G2PRegisterSectionDocument.document_id
                    == G2PRegistryDocument.document_id,
                )
                .where(
                    G2PRegisterSectionDocument.internal_record_id
                    == change_request.internal_record_id
                )
                .where(G2PRegistryDocument.bucket == "documents")
                .order_by(G2PRegistryDocument.created_at)
            )
        ).scalars().first()

        if row is not None and self._is_image(row.source_filename):
            record.record_image_document_id = row.document_id

    @staticmethod
    def _is_image(filename: str | None) -> bool:
        return str(filename or "").lower().endswith(
            (".jpg", ".jpeg", ".png", ".webp", ".gif")
        )

    async def _refresh_admin_names(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Copy the admin unit display names onto the record.

        The register search returns stored values verbatim, so a tree column
        bound to `region` shows REGION_ET11. These denormalised names give the
        tree something readable while the coded value stays authoritative.
        """
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from openg2p_registry_core.engine import get_engines
        from openg2p_registry_core.models import G2PAttributeValue

        from ..models import G2PRegisterCropSown

        record = await session.get(G2PRegisterCropSown, change_request.internal_record_id)
        if record is None:
            return
        for field, name_field in (("region", "region_name"), ("zone", "zone_name"),
                                  ("woreda", "woreda_name"), ("kebele", "kebele_name")):
            value_id = getattr(record, field, None)
            if not value_id:
                setattr(record, name_field, None)
                continue
            
            # 1. Try local attributes table first
            row = (
                await session.execute(
                    select(G2PAttributeValue).where(G2PAttributeValue.value_id == value_id)
                )
            ).scalars().first()
            display_name = getattr(row, "value_display", None) if row else None
            
            # 2. Fall back to master-data geo hierarchy values
            if not display_name:
                master_data_engine = get_engines().get("db_engine_master_data")
                if master_data_engine:
                    parts = value_id.split("_", 1)
                    geo_id = f"{parts[0].lower()}-{parts[1]}" if len(parts) > 1 else value_id
                    session_maker = async_sessionmaker(master_data_engine, expire_on_commit=False)
                    async with session_maker() as md_session:
                        md_row = (await md_session.execute(
                            text("SELECT display_name FROM g2p_geo_level_values WHERE level_value_id = :val_id OR level_value_id = :orig_id"),
                            {"val_id": geo_id, "orig_id": value_id}
                        )).fetchone()
                        if md_row:
                            display_name = md_row[0]
            
            setattr(record, name_field, display_name or value_id)


    async def _check_unique_farmer_per_year(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Odoo: `_check_unique_farmer_id` — one registration per farmer per
        crop year."""
        from ..models import G2PRegisterCropSown

        record = await session.get(G2PRegisterCropSown, change_request.internal_record_id)
        if record is None or not record.farmer_id or not record.crop_year:
            return

        exclude_ids = [record.internal_record_id]
        if getattr(change_request, "subject_internal_record_id", None):
            exclude_ids.append(change_request.subject_internal_record_id)

        clash = (
            await session.execute(
                select(func.count())
                .select_from(G2PRegisterCropSown)
                .where(
                    G2PRegisterCropSown.farmer_id == record.farmer_id,
                    G2PRegisterCropSown.crop_year == record.crop_year,
                    G2PRegisterCropSown.internal_record_id.notin_(exclude_ids),
                    G2PRegisterCropSown.record_status == "ACTIVE",
                )
            )
        ).scalar_one()
        if clash:
            validation_error(
                f"Farmer {record.farmer_id} already has a crop sown record for "
                f"{record.crop_year}"
            )

    async def _check_crop_area_within_plot(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Odoo: `_check_land_area_allocation` / `_check_actual_crop_area_limits`.

        The crop area planned or worked on a plot may not exceed that plot's
        total area. Each line carries its own land_id and land_area, so the
        check groups a record's lines by plot and compares the sum against the
        area declared for it.
        """
        from ..models import (
            G2PRegisterCultivation, G2PRegisterPlanning, G2PRegisterSowing,
        )

        checks = [
            (G2PRegisterPlanning, "planned_area", "planned"),
            (G2PRegisterCultivation, "actual_crop_area", "cultivated"),
            (G2PRegisterSowing, "area_sown", "sown"),
        ]
        for model, area_field, label in checks:
            rows = (
                await session.execute(
                    select(model).where(
                        model.link_internal_record_id == change_request.internal_record_id,
                        model.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()

            by_plot: dict[str, list] = {}
            for row in rows:
                key = getattr(row, "land_id", None)
                if key:
                    by_plot.setdefault(key, []).append(row)

            for plot, plot_rows in by_plot.items():
                total = sum(as_float(getattr(r, area_field, None)) or 0.0 for r in plot_rows)
                plot_area = next(
                    (as_float(getattr(r, "land_area", None)) for r in plot_rows if getattr(r, "land_area", None)), None
                )
                if plot_area and total > plot_area + 1e-6:
                    validation_error(
                        f"Total {label} area on plot {plot} is {total:g} ha, which exceeds "
                        f"its registered area of {plot_area:g} ha"
                    )

    async def _check_matching_land_id_across_sections(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Check that Land IDs in Cluster Information sections match a Land ID specified in Crop Planning."""
        from ..models import (
            G2PRegisterPlanning, G2PRegisterCluster, G2PRegisterCultivationCluster, G2PRegisterCultivation,
            G2PIntakeFormPlanning, G2PIntakeFormCluster, G2PIntakeFormCultivationCluster, G2PIntakeFormCultivation,
            G2PRegisterSowing, G2PIntakeFormSowing, G2PRegisterInfestation, G2PIntakeFormInfestation,
            G2PRegisterHarvest, G2PIntakeFormHarvest,
        )

        planning_land_ids = set()
        cultivation_land_ids = set()

        planning_rows = (
            await session.execute(
                select(G2PRegisterPlanning).where(
                    G2PRegisterPlanning.link_internal_record_id == change_request.internal_record_id,
                    G2PRegisterPlanning.record_status == "ACTIVE",
                )
            )
        ).scalars().all()
        for r in planning_rows:
            lid = getattr(r, "land_id", None)
            if lid and str(lid).strip():
                planning_land_ids.add(str(lid).strip())

        cultivation_rows = (
            await session.execute(
                select(G2PRegisterCultivation).where(
                    G2PRegisterCultivation.link_internal_record_id == change_request.internal_record_id,
                    G2PRegisterCultivation.record_status == "ACTIVE",
                )
            )
        ).scalars().all()
        for r in cultivation_rows:
            lid = getattr(r, "land_id", None)
            if lid and str(lid).strip():
                cultivation_land_ids.add(str(lid).strip())

        sub_id = getattr(change_request, "submission_id", None)
        if sub_id:
            intake_planning = (
                await session.execute(
                    select(G2PIntakeFormPlanning).where(
                        G2PIntakeFormPlanning.submission_id == sub_id
                    )
                )
            ).scalars().all()
            for ip in intake_planning:
                lid = getattr(ip, "land_id", None)
                if lid and str(lid).strip():
                    planning_land_ids.add(str(lid).strip())

            intake_cultivation = (
                await session.execute(
                    select(G2PIntakeFormCultivation).where(
                        G2PIntakeFormCultivation.submission_id == sub_id
                    )
                )
            ).scalars().all()
            for ip in intake_cultivation:
                lid = getattr(ip, "land_id", None)
                if lid and str(lid).strip():
                    cultivation_land_ids.add(str(lid).strip())

        if planning_land_ids:
            cluster_rows = []
            c_rows = (
                await session.execute(
                    select(G2PRegisterCluster).where(
                        G2PRegisterCluster.link_internal_record_id == change_request.internal_record_id,
                        G2PRegisterCluster.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()
            cluster_rows.extend(c_rows)

            if sub_id:
                ic_rows = (
                    await session.execute(
                        select(G2PIntakeFormCluster).where(
                            G2PIntakeFormCluster.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                cluster_rows.extend(ic_rows)

            for c in cluster_rows:
                lid = getattr(c, "land_id", None)
                if lid and str(lid).strip() and str(lid).strip() not in planning_land_ids:
                    validation_error(
                        f"Land ID '{lid}' in Cluster Information does not match any Land ID specified in Crop Planning."
                    )

        if cultivation_land_ids:
            cultivation_cluster_rows = []
            c_rows = (
                await session.execute(
                    select(G2PRegisterCultivationCluster).where(
                        G2PRegisterCultivationCluster.link_internal_record_id == change_request.internal_record_id,
                        G2PRegisterCultivationCluster.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()
            cultivation_cluster_rows.extend(c_rows)

            if sub_id:
                ic_rows = (
                    await session.execute(
                        select(G2PIntakeFormCultivationCluster).where(
                            G2PIntakeFormCultivationCluster.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                cultivation_cluster_rows.extend(ic_rows)

            for c in cultivation_cluster_rows:
                lid = getattr(c, "land_id", None)
                if lid and str(lid).strip() and str(lid).strip() not in cultivation_land_ids:
                    validation_error(
                        f"Land ID '{lid}' in Cultivation Cluster does not match any Land ID specified in Cultivation/Land Preparation."
                    )

        if planning_land_ids:
            sowing_rows = []
            s_rows = (
                await session.execute(
                    select(G2PRegisterSowing).where(
                        G2PRegisterSowing.link_internal_record_id == change_request.internal_record_id,
                        G2PRegisterSowing.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()
            sowing_rows.extend(s_rows)

            if sub_id:
                is_rows = (
                    await session.execute(
                        select(G2PIntakeFormSowing).where(
                            G2PIntakeFormSowing.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                sowing_rows.extend(is_rows)

            sowing_land_ids = set()
            for c in sowing_rows:
                lid = getattr(c, "land_id", None)
                if lid and str(lid).strip():
                    sowing_land_ids.add(str(lid).strip())
                    if str(lid).strip() not in planning_land_ids:
                        validation_error(
                            f"Land ID '{lid}' in Sowing does not match any Land ID specified in Crop Planning."
                        )

            infestation_rows = []
            i_rows = (
                await session.execute(
                    select(G2PRegisterInfestation).where(
                        G2PRegisterInfestation.link_internal_record_id == change_request.internal_record_id,
                        G2PRegisterInfestation.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()
            infestation_rows.extend(i_rows)

            if sub_id:
                ii_rows = (
                    await session.execute(
                        select(G2PIntakeFormInfestation).where(
                            G2PIntakeFormInfestation.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                infestation_rows.extend(ii_rows)

            # 3. Validate Infestation Land IDs against the collected Sowing Land IDs
            for c in infestation_rows:
                lid = getattr(c, "land_id", None)
                if lid and str(lid).strip() and str(lid).strip() not in sowing_land_ids:
                    validation_error(
                        f"Land ID '{lid}' in Pest/Disease Infestation does not match any Land ID specified in Sowing."
                    )

            harvest_rows = []
            h_rows = (
                await session.execute(
                    select(G2PRegisterHarvest).where(
                        G2PRegisterHarvest.link_internal_record_id == change_request.internal_record_id,
                        G2PRegisterHarvest.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()
            harvest_rows.extend(h_rows)

            if sub_id:
                ih_rows = (
                    await session.execute(
                        select(G2PIntakeFormHarvest).where(
                            G2PIntakeFormHarvest.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                harvest_rows.extend(ih_rows)

            valid_harvest_land_ids = sowing_land_ids or planning_land_ids
            for c in harvest_rows:
                lid = getattr(c, "land_id", None)
                if lid and str(lid).strip() and str(lid).strip() not in valid_harvest_land_ids:
                    validation_error(
                        f"Land ID '{lid}' in Harvest does not match any Land ID specified in Sowing or Crop Planning."
                    )

    async def _recompute_ec_dates_across_sections(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> None:
        """Ensure all GC dates on active and intake records carry their computed EC date."""
        from .domain_compute_utils import compute_ec_date
        from ..models import (
            G2PRegisterPlanning, G2PIntakeFormPlanning,
            G2PRegisterCultivation, G2PIntakeFormCultivation,
            G2PRegisterSowing, G2PIntakeFormSowing,
            G2PRegisterInfestation, G2PIntakeFormInfestation,
            G2PRegisterHarvest, G2PIntakeFormHarvest,
        )

        mappings = [
            (G2PRegisterPlanning, G2PIntakeFormPlanning, "planned_date", "planned_date_ec"),
            (G2PRegisterCultivation, G2PIntakeFormCultivation, "actual_cultivation_date", "actual_cultivation_date_ec"),
            (G2PRegisterSowing, G2PIntakeFormSowing, "sowing_date", "sowing_date_ec"),
            (G2PRegisterInfestation, G2PIntakeFormInfestation, "observation_date", "observation_date_ec"),
            (G2PRegisterHarvest, G2PIntakeFormHarvest, "harvest_date", "harvest_date_ec"),
        ]

        sub_id = getattr(change_request, "submission_id", None)

        for reg_model, intake_model, gc_field, ec_field in mappings:
            rows = (
                await session.execute(
                    select(reg_model).where(
                        or_(
                            reg_model.link_internal_record_id == change_request.internal_record_id,
                            reg_model.internal_record_id == change_request.internal_record_id,
                        ),
                        reg_model.record_status == "ACTIVE",
                    )
                )
            ).scalars().all()

            if sub_id:
                i_rows = (
                    await session.execute(
                        select(intake_model).where(
                            intake_model.submission_id == sub_id
                        )
                    )
                ).scalars().all()
                rows.extend(i_rows)

            for row in rows:
                compute_ec_date(row, gc_field, ec_field)

    # ── Lifecycle ───────────────────────────────────────────────────────────
    # AWE owns the approval decision. `post_approve` runs once a change request
    # is approved and updates the lifecycle_stage on the root record.

    async def post_approve(self, change_request: G2PRegisterChangeRequest, session: AsyncSession):
        from ..models import G2PRegisterCropSown

        record = await session.get(G2PRegisterCropSown, change_request.internal_record_id)
        if record is None:
            return

        section_mnemonic = await self._resolve_section_mnemonic(change_request, session)
        new_stage = _SECTION_LIFECYCLE_STAGE_MAP.get(section_mnemonic)
        if new_stage:
            record.lifecycle_stage = new_stage
        else:
            _logger.warning(
                "crop sown %s: section %s approved but has no lifecycle_stage mapping — "
                "lifecycle_stage unchanged (%s)",
                record.internal_record_id, section_mnemonic, record.lifecycle_stage,
            )
        record.rejection_reason = None
        await self._recompute_ec_dates_across_sections(change_request, session)
        _logger.info(
            "crop sown %s: section %s approved, lifecycle_stage -> %s",
            record.internal_record_id, section_mnemonic, record.lifecycle_stage,
        )

    async def _resolve_section_mnemonic(
        self, change_request: G2PRegisterChangeRequest, session: AsyncSession
    ) -> str:
        from openg2p_registry_core.models import G2PRegisterSection

        if not change_request.section_id:
            return ""
        section = await session.get(G2PRegisterSection, change_request.section_id)
        return getattr(section, "section_mnemonic", "") or ""
