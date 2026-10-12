import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict

import httpx
from openg2p_registry_core.interfaces import G2PPayloadEnricherInterface
from sqlalchemy.orm import Session

_logger = logging.getLogger('g2p-payload-enricher-service')

# --- Farmer Registry DCI federated-fetch config (env-driven) ---------------
# Base URL of the Farmer Registry partner-api (DCI endpoint lives at
# {base}/dci/registry/sync/search). In a deployment both registries are
# reachable over the cluster network; locally set it on the celery-worker.
FARMER_REGISTRY_DCI_URL = os.environ.get(
    "CROPSOWN_FARMER_DCI_URL",
    "http://farmer-registry-partner-api:8000/dci/registry/sync/search",
)
# Our onboarded Partner Management identity (sender_id + signing kid).
FARMER_DCI_SENDER_ID = os.environ.get("CROPSOWN_DCI_SENDER_ID", "cropsown")
FARMER_DCI_TIMEOUT = float(os.environ.get("CROPSOWN_DCI_TIMEOUT", "15"))
FARMER_DCI_SUBJECT_ID_TYPE = os.environ.get("CROPSOWN_DCI_SUBJECT_ID_TYPE", "fayda_fan_id")
# Optional: a pre-minted compact consent JWS to embed. If empty, we mint and
# sign one on the fly from the CM signing config below (auto-approved partner
# consent — no human/OTP; CM /validate permits on signature + policy).
FARMER_DCI_CONSENT_JWS = os.environ.get("CROPSOWN_DCI_CONSENT_JWS", "")

# --- Consent Manager partner-signed consent config -------------------------
# The DCI `consent_jws` is a partner-signed compact JWS. We sign it with our
# PM-registered private key (kid); CM /validate verifies the signature against
# the public key PM serves, checks policy, and permits — minting the artefact.
CM_SIGNING_KEY_PEM = os.environ.get("CROPSOWN_CM_SIGNING_KEY_PEM", "")
CM_SIGNING_KID = os.environ.get("CROPSOWN_CM_SIGNING_KID", "")
CM_PARTNER_AUD = os.environ.get("CROPSOWN_CM_PARTNER_AUD", "")
CM_CONTROLLER_ID = os.environ.get("CROPSOWN_CM_CONTROLLER_ID", "")
CM_DATA_SCOPES = [s for s in os.environ.get("CROPSOWN_CM_SCOPES", "farmer_profile.basic").split(",") if s]
CM_SIGNING_ALG = os.environ.get("CROPSOWN_CM_ALG", "EdDSA")
CM_CONSENT_PURPOSE = os.environ.get("CROPSOWN_CM_PURPOSE", "share_farm_profile")
CM_CONSENT_VALIDITY_DAYS = int(os.environ.get("CROPSOWN_CM_VALIDITY_DAYS", "1"))


def _canonical_bytes(obj: Dict) -> bytes:
    """Canonical JSON — MUST match the Consent Manager's canonicalisation
    (stable key order, compact separators) so the signature round-trips."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")


def _mint_consent_jws(farmer_id: str) -> str:
    """Auto-approved consent for the DCI fetch: a partner-signed consent object.

    No human/OTP step — the partner self-asserts consent by signing the object;
    CM /validate permits it on signature + policy. Returns "" when no signing
    key is configured (consent-enforcement-off deployments)."""
    if not (CM_SIGNING_KEY_PEM and CM_SIGNING_KID):
        return ""
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from jwt.api_jws import PyJWS

    now = datetime.now(timezone.utc)
    claims = {
        "@context": "https://openg2p.org/contexts/consent_object.jsonld",
        "@type": "ConsentObject",
        "jti": uuid.uuid4().hex,
        "subject_id": {"type": FARMER_DCI_SUBJECT_ID_TYPE, "value": farmer_id},
        "data_controller": CM_CONTROLLER_ID,
        "aud": CM_PARTNER_AUD,
        "purpose": {"code": CM_CONSENT_PURPOSE, "text": "crop sown enrichment"},
        "data_scopes": CM_DATA_SCOPES,
        "fetch_type": "oneshot",
        "validity": {
            "valid_from": now.isoformat(),
            "valid_until": (now + timedelta(days=CM_CONSENT_VALIDITY_DAYS)).isoformat(),
        },
        "issued_at": now.isoformat(),
    }
    priv = load_pem_private_key(CM_SIGNING_KEY_PEM.encode("utf-8"), password=None)
    jws = PyJWS().encode(_canonical_bytes(claims), priv, algorithm=CM_SIGNING_ALG,
                         headers={"kid": CM_SIGNING_KID})
    _logger.info("Minted partner-signed consent JWS for farmer=%s (jti=%s, scopes=%s)",
                 farmer_id, claims["jti"], CM_DATA_SCOPES)
    return jws


def _build_dci_search_envelope(farmer_id: str) -> Dict:
    """Build a DCI sync-search envelope that looks a Farmer up by its id."""
    now = datetime.now(timezone.utc).isoformat()
    search_criteria: Dict = {
        "version": "1.0.0",
        "reg_type": "Farmer",
        "reg_record_type": "Farmer",
        "query_type": "idtype-value",
        # The registry resolves this to a search_text match; the Fayda/UIN is what
        # lives in the farmer record's search_text, so search by that value.
        "query": {"type": "idtype-value", "value": {"id_type": FARMER_DCI_SUBJECT_ID_TYPE, "id_value": farmer_id}},
    }
    consent_jws = FARMER_DCI_CONSENT_JWS or _mint_consent_jws(farmer_id)
    if consent_jws:
        # PEP reads the partner-signed consent from here and forwards it to CM.
        search_criteria["authorize"] = {"consent_jws": consent_jws}
    return {
        "signature": "cropsown-dci",
        "header": {
            "version": "1.0.0",
            "message_id": uuid.uuid4().hex,
            "message_ts": now,
            "action": "search",
            "sender_id": FARMER_DCI_SENDER_ID,
            "receiver_id": "farmer-registry",
            "total_count": 1,
            "is_msg_encrypted": False,
        },
        "message": {
            "transaction_id": uuid.uuid4().hex,
            "search_request": [
                {
                    "reference_id": "cropsown-enrich-1",
                    "timestamp": now,
                    "locale": "eng",
                    "search_criteria": search_criteria,
                }
            ],
        },
    }


def _fetch_farmer_from_registry(farmer_id: str) -> Dict:
    """Call the Farmer Registry DCI endpoint and return the first reg_record.

    SYNC on purpose — the enricher runs inside a sync celery task with a sync
    SQLAlchemy session, so an async client cannot be awaited here.
    """
    envelope = _build_dci_search_envelope(farmer_id)
    with httpx.Client(timeout=FARMER_DCI_TIMEOUT) as client:
        resp = client.post(FARMER_REGISTRY_DCI_URL, json=envelope)
        resp.raise_for_status()
        body = resp.json()
    status = ((body.get("header") or {}).get("status"))
    if status == "rjct":
        reason = (body.get("header") or {}).get("status_reason_message")
        raise ValueError(f"Farmer Registry DCI rejected the search: {reason}")
    search_response = (body.get("message") or {}).get("search_response") or []
    records = (search_response[0].get("data") or {}).get("reg_records") if search_response else []
    return (records or [{}])[0]


def _map_farmer_to_cropsown(dci_record: Dict) -> Dict:
    """Map a DCI Farmer record (farmer_personal_details scope) onto the crop
    sown record's farmer fields."""
    scope = dci_record.get("farmer_personal_details") or dci_record
    demo = (scope.get("demographic_info") or {})
    name = (demo.get("name") or {})
    ident = ((scope.get("member_identifier") or [{}])[0]) or (demo.get("identifier") or [{}])[0]
    full_name = " ".join(
        p for p in [name.get("given_name"), name.get("second_name"), name.get("surname")] if p
    ).strip()
    out: Dict = {}
    if full_name:
        out["farmer_name"] = full_name
    if scope.get("farmer_id"):
        out["farmer_id"] = scope.get("farmer_id")
    if scope.get("farmer_uuid"):
        out["farmer_uuid"] = scope.get("farmer_uuid")
    if ident.get("identifier_value"):
        out["fayda_fan_id"] = ident.get("identifier_value")
    if demo.get("sex"):
        out["gender"] = demo.get("sex")
    if demo.get("birth_date"):
        out["birth_date"] = demo.get("birth_date")
    return out

# DCI Payload Enrichers
class G2PDciCropSownCreateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        """Federated enrichment: intake carries only a Farmer ID / Fayda ID;
        resolve the full Farmer record from the Farmer Registry (DCI Partner
        API) and merge it into the crop sown payload before transformation.
        """
        _logger.info("Processing G2PDciCropSownCreateEnricherService")
        farmer_id = data.get("fayda_fan_id") or data.get("farmer_id")
        if not farmer_id:
            _logger.info("No farmer_id/fayda_fan_id in payload — nothing to enrich")
            return data

        _logger.info("Enriching crop sown record via Farmer Registry for id=%s", farmer_id)
        try:
            dci_record = _fetch_farmer_from_registry(str(farmer_id).strip())
        except Exception as exc:  # fail-closed: don't let a bad fetch corrupt ingest
            _logger.error("Farmer Registry enrichment failed for id=%s: %s", farmer_id, exc)
            raise

        farmer_fields = _map_farmer_to_cropsown(dci_record)
        if not farmer_fields:
            raise ValueError(f"Farmer '{farmer_id}' not found in Farmer Registry")

        # Merge resolved farmer data into the crop sown payload (do not clobber
        # any value the intake already provided).
        for key, value in farmer_fields.items():
            if not data.get(key):
                data[key] = value
        _logger.info("Enriched with farmer fields: %s", list(farmer_fields.keys()))
        return data

class G2PDciCropSownUpdateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PDciCropSownUpdateEnricherService")
        return data

class G2PDciCropSownDeleteEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PDciCropSownDeleteEnricherService")
        return data

# SPDCI Payload Enrichers
class G2PSpdciCropSownCreateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PSpdciCropSownCreateEnricherService")
        return data

class G2PSpdciCropSownUpdateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PSpdciCropSownUpdateEnricherService")
        return data

class G2PSpdciCropSownDeleteEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PSpdciCropSownDeleteEnricherService")
        return data

# UNDP Payload Enrichers
class G2PUndpCropSownCreateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PUndpCropSownCreateEnricherService")
        return data

class G2PUndpCropSownUpdateEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PUndpCropSownUpdateEnricherService")
        return data

class G2PUndpCropSownDeleteEnricherService(G2PPayloadEnricherInterface):
    def enrich(self, data: Dict, session: Session) -> Dict:
        _logger.info("Processing G2PUndpCropSownDeleteEnricherService")
        return data
