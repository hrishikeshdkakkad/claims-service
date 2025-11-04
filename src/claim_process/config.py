"""
Configuration for metadata-driven claims processing system.
This allows easy extension without code changes.
"""
from typing import Dict, List, Optional
from enum import Enum
from pydantic import BaseModel


class FieldType(str, Enum):
    """Supported field types for validation"""
    STRING = "string"
    NUMBER = "number"
    DATE = "date"
    CURRENCY = "currency"
    NPI = "npi"  # Special type for 10-digit NPI validation
    PROCEDURE_CODE = "procedure_code"  # Special type for procedure codes


class ValidationRule(BaseModel):
    """Defines validation rules for fields"""
    field_type: FieldType
    required: bool = True
    pattern: Optional[str] = None  # Regex pattern
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    starts_with: Optional[str] = None
    custom_validator: Optional[str] = None  # Name of custom validation function


class FieldMapping(BaseModel):
    """Maps various field name variations to canonical names"""
    canonical_name: str
    variations: List[str]  # All possible variations (case-insensitive)
    validation: ValidationRule
    description: str = ""


class CalculationFormula(BaseModel):
    """Defines calculation formulas in metadata"""
    name: str
    formula: str  # Expression that can be evaluated
    fields_required: List[str]  # Canonical field names needed
    description: str


# METADATA CONFIGURATION
# This can be loaded from a database, JSON file, or environment

FIELD_MAPPINGS: Dict[str, FieldMapping] = {
    "service_date": FieldMapping(
        canonical_name="service_date",
        variations=["service date", "Service Date", "SERVICE_DATE", "serviceDate", "service_dt"],
        validation=ValidationRule(
            field_type=FieldType.DATE,
            required=True
        ),
        description="Date when the medical service was provided"
    ),
    "submitted_procedure": FieldMapping(
        canonical_name="submitted_procedure",
        variations=["submitted procedure", "Submitted Procedure", "procedure", "procedure_code", "proc_code"],
        validation=ValidationRule(
            field_type=FieldType.PROCEDURE_CODE,
            required=True,
            starts_with="D",  # Dental codes start with D
            pattern=r"^D\d{4}$"  # D followed by 4 digits
        ),
        description="Procedure code submitted for the claim"
    ),
    "quadrant": FieldMapping(
        canonical_name="quadrant",
        variations=["quadrant", "Quadrant", "QUADRANT", "quad"],
        validation=ValidationRule(
            field_type=FieldType.STRING,
            required=False,  # Only optional field
            pattern=r"^(UR|UL|LR|LL)$"  # Upper/Lower Right/Left
        ),
        description="Dental quadrant where procedure was performed"
    ),
    "plan_group_number": FieldMapping(
        canonical_name="plan_group_number",
        variations=["Plan/Group #", "plan/group #", "Plan Group", "group_number", "plan_number"],
        validation=ValidationRule(
            field_type=FieldType.STRING,
            required=True
        ),
        description="Insurance plan/group identifier"
    ),
    "subscriber_number": FieldMapping(
        canonical_name="subscriber_number",
        variations=["Subscriber#", "subscriber#", "Subscriber #", "subscriber_id", "member_id"],
        validation=ValidationRule(
            field_type=FieldType.STRING,
            required=True,
            min_length=5,
            max_length=20
        ),
        description="Member/subscriber identifier"
    ),
    "provider_npi": FieldMapping(
        canonical_name="provider_npi",
        variations=["Provider NPI", "provider NPI", "provider_npi", "npi", "NPI"],
        validation=ValidationRule(
            field_type=FieldType.NPI,
            required=True,
            pattern=r"^\d{10}$",  # Exactly 10 digits
            min_length=10,
            max_length=10
        ),
        description="National Provider Identifier - 10 digit number"
    ),
    "provider_fees": FieldMapping(
        canonical_name="provider_fees",
        variations=["provider fees", "Provider Fees", "provider_fees", "billed_amount"],
        validation=ValidationRule(
            field_type=FieldType.CURRENCY,
            required=True,
            min_value=0
        ),
        description="Amount billed by the provider"
    ),
    "allowed_fees": FieldMapping(
        canonical_name="allowed_fees",
        variations=["Allowed fees", "allowed fees", "allowed_fees", "allowed_amount"],
        validation=ValidationRule(
            field_type=FieldType.CURRENCY,
            required=True,
            min_value=0
        ),
        description="Maximum amount insurance will pay"
    ),
    "member_coinsurance": FieldMapping(
        canonical_name="member_coinsurance",
        variations=["member coinsurance", "Member Coinsurance", "coinsurance", "member_coins"],
        validation=ValidationRule(
            field_type=FieldType.CURRENCY,
            required=True,
            min_value=0
        ),
        description="Member's coinsurance responsibility"
    ),
    "member_copay": FieldMapping(
        canonical_name="member_copay",
        variations=["member copay", "Member Copay", "copay", "member_copayment"],
        validation=ValidationRule(
            field_type=FieldType.CURRENCY,
            required=True,
            min_value=0
        ),
        description="Member's copay amount"
    )
}

# Calculation formulas as metadata
CALCULATION_FORMULAS: Dict[str, CalculationFormula] = {
    "net_fee": CalculationFormula(
        name="net_fee",
        formula="provider_fees + member_coinsurance + member_copay - allowed_fees",
        fields_required=["provider_fees", "member_coinsurance", "member_copay", "allowed_fees"],
        description="Net fee calculation for claim processing"
    ),
    "member_responsibility": CalculationFormula(
        name="member_responsibility",
        formula="member_coinsurance + member_copay",
        fields_required=["member_coinsurance", "member_copay"],
        description="Total amount member owes"
    ),
    "provider_adjustment": CalculationFormula(
        name="provider_adjustment",
        formula="provider_fees - allowed_fees",
        fields_required=["provider_fees", "allowed_fees"],
        description="Provider write-off amount"
    )
}

# Additional metadata for extensibility
VALIDATION_MESSAGES = {
    "npi_invalid": "Provider NPI must be exactly 10 digits",
    "procedure_invalid": "Procedure code must start with 'D' followed by 4 digits",
    "required_field": "Field '{field}' is required",
    "pattern_mismatch": "Field '{field}' does not match required pattern",
    "value_too_low": "Field '{field}' value is below minimum",
}

# Feature flags for easy feature toggling
FEATURES = {
    "enable_caching": True,
    "cache_ttl_seconds": 300,
    "rate_limit_per_minute": 10,
    "enable_audit_logging": True,
    "enable_field_normalization": True,
    "enable_async_processing": True,
    "max_claim_lines_per_request": 1000,
}

# Database configuration
DB_CONFIG = {
    "pool_size": 20,
    "max_overflow": 40,
    "pool_pre_ping": True,
    "echo_sql": False,  # Set to True for debugging
}