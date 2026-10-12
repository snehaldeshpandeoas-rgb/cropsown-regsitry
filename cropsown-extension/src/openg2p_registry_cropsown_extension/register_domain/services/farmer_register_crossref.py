"""Crop-Sown -> Farmer register cross-reference ("two change-requests").

A single Crop-Sown intake is about a farmer who is the system-of-record of the
Farmer Registry, not of this registry. When such an intake is finalised we want
the approver to see TWO approval items:

1. the Crop-Sown intake itself (raised by the platform's normal finalize), and
2. a Farmer **register** change-request that, on approval, materialises the
   farmer as a first-class row in this registry's Farmer register
   (``g2p_register_farmers``) — enriched from the Farmer Registry, not typed.

This module is the clean, env-driven replacement for the old in-container
live-hook: it carries NO secrets and opens NO hand-built database connections.
Farmer identity comes from :mod:`farmer_registry_enrichment` (the DCI partner
API + Consent Manager, both-ID required), and every database access reuses the
application's own async engine via ``dbengine`` — exactly as the core services
do — so it works unchanged in the staff-api and in the celery worker.

Behaviour, preserved from the proven flow:
  * existing-farmer skip — if the Farmer ID already has an ACTIVE row in this
    registry's Farmer register, no second change-request is raised (no dup).
  * a hidden DRAFT shell (``record_status='INACTIVE'``) is pre-created so the
    REGISTER-section change-request is viewable/approvable before the subject
    row is visible; approval fills the enriched values and flips it ACTIVE.
  * approval of a Farmer-register section is create-or-update: a REGISTER
    section CR cannot normally ADD, so the patched ``approve_register`` creates
    the row from the shell if absent and updates it otherwise.

Fails open: when disabled or anything goes wrong, this logs and returns without
blocking the finalize/approval it is attached to.

Config (env; the farmer identity settings live in farmer_registry_enrichment):
  CROPSOWN_TWO_CR_ENABLED        enable flag (default false)
  CROPSOWN_FARMER_REGISTER_ID    target Farmer register id
  CROPSOWN_FARMER_TAB_ID         Farmer tab id      (default cropsown_farmer_tab)
  CROPSOWN_FARMER_SECTION_ID     Farmer section id  (default cropsown_farmer_identity_section_01)
  CROPSOWN_TWO_CR_INTAKE_TABLE   intake table to read farmer ids from
                                 (default g2p_intake_form_crop_sowns)
  CROPSOWN_TWO_CR_SOURCE_PARTNER source_partner_id on the CR (default staff-portal)
  CROPSOWN_TWO_CR_CREATED_BY     created_by/change_request_source actor
                                 (default cropsown-two-cr)
"""

import logging
import os
import uuid
from datetime import date, datetime, timezone

from openg2p_fastapi_common.context import dbengine
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from .farmer_registry_enrichment import (
    compose_farmer_name,
    fetch_farmer_record,
    to_farmer_fields,
)

_logger = logging.getLogger("g2p-cropsown-two-cr")

_DEFAULT_REGISTER_ID = "fa120001-0000-4000-8000-000000000001"
_DEFAULT_TAB_ID = "cropsown_farmer_tab"
_DEFAULT_SECTION_ID = "cropsown_farmer_identity_section_01"
_DEFAULT_INTAKE_TABLE = "g2p_intake_form_crop_sowns"


def _setting(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _enabled() -> bool:
    return _setting("CROPSOWN_TWO_CR_ENABLED", "false").lower() in {"true", "1", "yes"}


def _config() -> dict:
    return {
        "register_id": _setting("CROPSOWN_FARMER_REGISTER_ID", _DEFAULT_REGISTER_ID),
        "tab_id": _setting("CROPSOWN_FARMER_TAB_ID", _DEFAULT_TAB_ID),
        "section_id": _setting("CROPSOWN_FARMER_SECTION_ID", _DEFAULT_SECTION_ID),
        "intake_table": _setting("CROPSOWN_TWO_CR_INTAKE_TABLE", _DEFAULT_INTAKE_TABLE),
        "source_partner_id": _setting("CROPSOWN_TWO_CR_SOURCE_PARTNER", "staff-portal"),
        "created_by": _setting("CROPSOWN_TWO_CR_CREATED_BY", "cropsown-two-cr"),
    }


def _sessionmaker():
    return async_sessionmaker(dbengine.get(), expire_on_commit=False)


async def _intake_farmer_ids(submission_id: str, cfg: dict) -> list[dict]:
    """Distinct (farmer_id, fayda_fan_id) carried by this submission's intake rows."""
    table = cfg["intake_table"]
    if not table.replace("_", "").isalnum():  # defensive: table name is config, keep it an identifier
        _logger.warning("two-CR: refusing non-identifier intake table %r", table)
        return []
    async with _sessionmaker()() as session:
        result = await session.execute(
            text(
                f"SELECT DISTINCT farmer_id, fayda_fan_id FROM {table} "
                "WHERE submission_id = :sid AND farmer_id IS NOT NULL AND farmer_id <> ''"
            ),
            {"sid": submission_id},
        )
        return [dict(row._mapping) for row in result]


async def _farmer_exists_active(farmer_id: str, cfg: dict) -> bool:
    """True when this Farmer ID already has an ACTIVE row in the Farmer register."""
    async with _sessionmaker()() as session:
        found = await session.execute(
            text(
                "SELECT 1 FROM g2p_register_farmers "
                "WHERE functional_record_id = :fid AND record_status = 'ACTIVE' LIMIT 1"
            ),
            {"fid": farmer_id},
        )
        return found.first() is not None


async def _precreate_draft_shell(internal_record_id: str, farmer_id: str, farmer_name: str, fayda: str | None) -> None:
    """A hidden (INACTIVE) shell so the REGISTER-section CR is viewable/approvable."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    search_text = " ".join(x for x in (farmer_id, farmer_name, fayda or "") if x).strip()
    async with _sessionmaker()() as session:
        await session.execute(
            text(
                "INSERT INTO g2p_register_farmers "
                "(internal_record_id, functional_record_id, farmer_id, record_name, search_text, "
                " record_status, status, created_by, created_at, last_approved_at, last_approved_by) "
                "VALUES (:irid, :fid, :fid, :name, :st, 'INACTIVE', 'APPROVAL_STATUS_DRAFT', "
                "        'cropsown-two-cr', :now, :now, 'system') "
                "ON CONFLICT (internal_record_id) DO NOTHING"
            ),
            {"irid": internal_record_id, "fid": farmer_id, "name": farmer_name, "st": search_text, "now": now},
        )
        await session.commit()


def _cr_fields(farmer_id: str, fields: dict, farmer_name: str) -> dict:
    """The Farmer-register change-payload: enriched identity, DRAFT status."""
    dob = fields.get("date_of_birth")
    return {
        "farmer_id": farmer_id,
        "fayda_fan_id": fields.get("fayda_fan_id"),
        "farmer_name": farmer_name,
        "first_name": fields.get("first_name"),
        "middle_name": fields.get("middle_name"),
        "last_name": fields.get("last_name"),
        "gender": fields.get("gender"),
        "date_of_birth": dob.isoformat() if isinstance(dob, date) else dob,
        "mobile_number": fields.get("mobile_number"),
        "status": "APPROVAL_STATUS_DRAFT",
        "record_name": farmer_name,
    }


async def raise_farmer_change_request(submission_id: str, bearer_token: str | None, requester_sub: str | None) -> None:
    """For each farmer in the submission, raise a Farmer-register change-request
    (unless the farmer already exists), enriched from the Farmer Registry."""
    if not _enabled():
        return
    cfg = _config()
    try:
        rows = await _intake_farmer_ids(submission_id, cfg)
    except Exception as exc:
        _logger.warning("two-CR: could not read intake farmer ids for %s: %r", submission_id, exc)
        return
    if not rows:
        _logger.info("two-CR: submission %s carries no farmer_id — nothing to raise", submission_id)
        return

    # Imported lazily: these are the caller's (core) schemas/services; importing
    # them at module load would couple extension import order to the core.
    from openg2p_registry_core.schemas.change_request import ChangePayload, ChangeRequestRequestPayload
    from openg2p_registry_core.services.g2p_register_change_request_service import (
        G2PRegisterChangeRequestService,
    )

    svc = G2PRegisterChangeRequestService.get_component()
    for row in rows:
        farmer_id = (row.get("farmer_id") or "").strip()
        fayda = (row.get("fayda_fan_id") or "").strip()
        if not farmer_id:
            continue
        try:
            if await _farmer_exists_active(farmer_id, cfg):
                _logger.info("two-CR: farmer %s already ACTIVE in Farmer register — skipping duplicate CR", farmer_id)
                continue

            personal = await fetch_farmer_record(farmer_id, fayda or None)
            if not personal:
                _logger.warning("two-CR: farmer %s / fayda=%s not resolved (both-ID required) — skipping", farmer_id, fayda or "-")
                continue
            fields = to_farmer_fields(personal)
            farmer_name = compose_farmer_name(fields.get("first_name"), fields.get("middle_name"), fields.get("last_name"))

            internal_record_id = uuid.uuid4().hex
            await _precreate_draft_shell(internal_record_id, farmer_id, farmer_name, fields.get("fayda_fan_id"))

            change = ChangePayload(internal_record_id=internal_record_id, edit_action="ADD")
            payload_fields = _cr_fields(farmer_id, fields, farmer_name)
            try:
                change.__pydantic_extra__ = dict(getattr(change, "__pydantic_extra__", None) or {})
                for key, value in payload_fields.items():
                    change.__pydantic_extra__[key] = value
            except Exception:
                for key, value in payload_fields.items():
                    setattr(change, key, value)

            cr_payload = ChangeRequestRequestPayload(
                register_id=cfg["register_id"],
                tab_id=cfg["tab_id"],
                section_id=cfg["section_id"],
                section_register_id=cfg["register_id"],
                internal_record_id=internal_record_id,
                change_payload=[change],
            )
            cr = await svc.create_change_request(
                change_request_request_payload=cr_payload,
                source_partner_id=cfg["source_partner_id"],
                created_by=cfg["created_by"],
                change_request_source="cropsown-two-cr",
                bearer_token=bearer_token,
                requester_sub=requester_sub,
            )
            _logger.info(
                "two-CR: raised Farmer change_request_id=%s for farmer=%s (%s) — approver task created",
                getattr(cr, "change_request_id", None), farmer_name, farmer_id,
            )
        except Exception as exc:
            _logger.exception("two-CR: failed to raise Farmer CR for %s: %r", farmer_id, exc)


def install(intake_service_cls) -> None:
    """Wrap ``finalize_submission_with_session`` to raise the Farmer CR after the
    platform's own finalize, and patch ``approve_register`` to create-or-update
    the Farmer register row. Idempotent and feature-flagged."""
    if not _enabled():
        return
    if getattr(intake_service_cls, "_cropsown_two_cr_installed", False):
        return

    original_finalize = intake_service_cls.finalize_submission_with_session

    async def finalize_with_two_cr(self, submission_id, session, finalized_by=None, bearer_token=None, requester_sub=None):
        result = await original_finalize(
            self, submission_id, session, finalized_by=finalized_by,
            bearer_token=bearer_token, requester_sub=requester_sub,
        )
        try:
            await raise_farmer_change_request(submission_id, bearer_token, requester_sub)
        except Exception as exc:  # never break the intake on the cross-reference
            _logger.exception("two-CR: finalize hook error (intake unaffected): %r", exc)
        return result

    intake_service_cls.finalize_submission_with_session = finalize_with_two_cr
    _install_approve_register(_config()["register_id"])
    intake_service_cls._cropsown_two_cr_installed = True
    _logger.info("CropSown two-CR hooks installed (finalize + approve_register create-or-update)")


def _install_approve_register(farmer_register_id: str) -> None:
    """Patch G2PRegisterChangeRequestService.approve_register so a Farmer-register
    section CR creates the row from the DRAFT shell when absent, updates otherwise,
    and flips it ACTIVE. Other registers are untouched."""
    from openg2p_registry_core.schemas.change_request import ChangeActionEnum
    from openg2p_registry_core.services.g2p_register_change_request_service import (
        G2PRegisterChangeRequestService,
    )
    from sqlalchemy.orm.attributes import flag_modified

    if getattr(G2PRegisterChangeRequestService, "_cropsown_approve_coru_installed", False):
        return
    original_approve_register = G2PRegisterChangeRequestService.approve_register

    async def approve_register_create_or_update(self, change_request, section, session):
        if getattr(section, "section_register_id", None) != farmer_register_id:
            return await original_approve_register(self, change_request, section, session)

        _reg_def, register_class, schema_class = await self._get_register_class_and_schema(
            section.section_register_id, session)
        payload = await self._get_change_request_payload(change_request.change_request_id, session)
        existing = await self._get_existing_record(register_class, change_request.internal_record_id, session)
        changed = False
        for change_payload in (payload.change_payload or []):
            action = change_payload.get("edit_action", ChangeActionEnum.UPDATE.value)
            if action == ChangeActionEnum.NO_CHANGE.value:
                continue
            change_payload["internal_record_id"] = change_request.internal_record_id
            schema_instance = schema_class(**(change_payload or {}))
            if existing is None:
                record_data = self._build_record_data(schema_instance.dict(), change_payload, register_class)
                record_data["internal_record_id"] = change_request.internal_record_id
                fid = change_payload.get("farmer_id") or record_data.get("farmer_id")
                self._set_data_if_column(record_data, register_class, "functional_record_id", fid)
                self._set_data_if_column(record_data, register_class, "created_by", change_request.created_by)
                self._set_data_if_column(record_data, register_class, "created_at", change_request.created_at)
                if not record_data.get("record_name"):
                    self._set_data_if_column(record_data, register_class, "record_name", change_payload.get("farmer_name"))
                existing = register_class(**record_data)
                session.add(existing)
                _logger.info("two-CR approve: created Farmer row internal_id=%s name=%s",
                             change_request.internal_record_id, change_payload.get("farmer_name"))
            else:
                self._update_existing_record(existing, schema_instance.dict(), change_payload, register_class)
                _logger.info("two-CR approve: updated Farmer row internal_id=%s name=%s",
                             change_request.internal_record_id, change_payload.get("farmer_name"))
            self._set_if_column(existing, "record_status", "ACTIVE")
            self._set_if_column(existing, "status", "APPROVAL_STATUS_APPROVED")
            self._set_if_column(existing, "last_approved_at", change_request.approved_at)
            self._set_if_column(existing, "last_approved_by", change_request.approved_by or "system")
            changed = True
        if changed:
            await session.flush()
            flag_modified(payload, "change_payload")

    G2PRegisterChangeRequestService.approve_register = approve_register_create_or_update
    G2PRegisterChangeRequestService._cropsown_approve_coru_installed = True
    _logger.info("CropSown two-CR: approve_register create-or-update patch installed")
