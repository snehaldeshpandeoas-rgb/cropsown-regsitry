from typing import Optional

from openg2p_registry_core.schemas import (
    G2PRegisterBaseSchema,
    G2PGeoSchema,
    G2PRegisterHistorySchema,
    G2PGeoHistorySchema,
    G2PIntakeFormSchemaBase,
)


class G2PSchemaFarmer:
    farmer_id: Optional[str] = None
    fayda_fan_id: Optional[str] = None
    farmer_name: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    gender: Optional[str] = None
    date_of_birth: Optional[str] = None
    mobile_number: Optional[str] = None
    registration_date: Optional[str] = None
    status: Optional[str] = None
    region: Optional[str] = None
    zone: Optional[str] = None
    woreda: Optional[str] = None
    kebele: Optional[str] = None


class G2PRegisterSchemaFarmer(G2PRegisterBaseSchema, G2PGeoSchema, G2PSchemaFarmer):
    """Farmer register schema."""


class G2PRegisterHistorySchemaFarmer(G2PRegisterHistorySchema, G2PGeoHistorySchema, G2PSchemaFarmer):
    """Farmer register-history schema."""


class G2PIntakeFormSchemaFarmer(G2PIntakeFormSchemaBase, G2PRegisterBaseSchema, G2PGeoSchema, G2PSchemaFarmer):
    """Farmer intake-form schema."""
