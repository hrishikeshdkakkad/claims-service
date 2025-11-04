"""
Simplified, denormalized database models using SQLModel with JSONB for flexibility.
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
from decimal import Decimal

from sqlmodel import Field, SQLModel, Column, JSON, Index, UniqueConstraint
from sqlalchemy import BigInteger, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy import JSON as SQLAlchemyJSON
from pydantic import field_validator, model_validator

from claim_process.services.field_mapper import (
    get_field_mapper,
    FieldMappingError,
)


class Claim(SQLModel, table=True):
    """
    Denormalized claims table with metadata-driven JSONB storage.

    Design Philosophy:
    - Single denormalized table for simplicity and performance
    - JSONB for flexibility and extensibility
    - Indexed columns for frequent queries
    - Calculated fields stored for performance
    """
    __tablename__ = "claims"
    __table_args__ = (
        # Unique constraint on external_claim_id to prevent duplicates
        UniqueConstraint("external_claim_id", name="uq_claims_external_claim_id"),
        # Composite index for top providers query
        Index("ix_claims_provider_npi_net_fee", "provider_npi", "net_fee"),
        # Index for time-based queries
        Index("ix_claims_created_at", "created_at"),
        # Index for claim lookup
        Index("ix_claims_external_id", "external_claim_id"),
    )

    # Primary key
    claim_id: UUID = Field(
        default_factory=uuid4,
        sa_column=Column(PGUUID(as_uuid=True), primary_key=True),
        description="Unique claim identifier"
    )

    # Core indexed fields for queries
    external_claim_id: Optional[str] = Field(
        default=None,
        index=True,
        description="External claim identifier from source system"
    )

    provider_npi: str = Field(
        index=True,
        description="Provider NPI - 10 digit identifier"
    )

    subscriber_number: str = Field(
        description="Member/Subscriber ID"
    )

    plan_group_number: str = Field(
        description="Insurance plan/group number"
    )

    # Calculated fields (denormalized for performance)
    net_fee: Decimal = Field(
        default=Decimal("0.00"),
        description="Calculated net fee (provider_fees + coinsurance + copay - allowed_fees)"
    )

    total_provider_fees: Decimal = Field(
        default=Decimal("0.00"),
        description="Sum of all line item provider fees"
    )

    total_allowed_fees: Decimal = Field(
        default=Decimal("0.00"),
        description="Sum of all line item allowed fees"
    )

    total_member_coinsurance: Decimal = Field(
        default=Decimal("0.00"),
        description="Sum of all line item member coinsurance"
    )

    total_member_copay: Decimal = Field(
        default=Decimal("0.00"),
        description="Sum of all line item member copay"
    )

    # Metadata fields
    line_count: int = Field(
        default=0,
        description="Number of claim lines"
    )

    status: str = Field(
        default="pending",
        description="Claim processing status"
    )

    # JSON storage for flexibility (works with both SQLite and PostgreSQL)
    raw_data: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(SQLAlchemyJSON, nullable=False, default={}),
        description="Original claim data as received (before normalization)"
    )

    normalized_lines: List[Dict[str, Any]] = Field(
        default_factory=list,
        sa_column=Column(SQLAlchemyJSON, nullable=False, default=[]),
        description="Normalized claim line items"
    )

    claim_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(SQLAlchemyJSON, nullable=False, default={}),
        description="Additional metadata (validation results, processing info, etc.)"
    )

    # Timestamps
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), server_default=func.now()),
        description="When the claim was created"
    )

    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            server_default=func.now(),
            onupdate=func.now()
        ),
        description="When the claim was last updated"
    )

    @field_validator("provider_npi")
    @classmethod
    def validate_npi(cls, v: str) -> str:
        """Validate NPI is 10 digits"""
        if not v or not v.isdigit() or len(v) != 10:
            raise ValueError("Provider NPI must be exactly 10 digits")
        return v

    def calculate_net_fee(self) -> Decimal:
        """Calculate net fee from components"""
        return (
            self.total_provider_fees +
            self.total_member_coinsurance +
            self.total_member_copay -
            self.total_allowed_fees
        )

    def to_response_dict(self) -> dict:
        """Convert to dictionary for API response"""
        return {
            "claim_id": str(self.claim_id),
            "external_claim_id": self.external_claim_id,
            "provider_npi": self.provider_npi,
            "subscriber_number": self.subscriber_number,
            "plan_group_number": self.plan_group_number,
            "net_fee": f"{self.net_fee:.2f}",  # Format to 2 decimal places
            "line_count": self.line_count,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "metadata": self.claim_metadata
        }


class ClaimLineRequest(SQLModel):
    """Clean API model for claim line - properly formatted fields."""
    service_date: str
    submitted_procedure: str
    quadrant: Optional[str] = None
    plan_group_number: str
    subscriber_number: str
    provider_npi: str
    provider_fees: str  # String to handle decimal precision
    allowed_fees: str
    member_coinsurance: str
    member_copay: str


class ClaimCreateRequest(SQLModel):
    """Request model for creating a claim - clean API contract."""
    external_claim_id: Optional[str] = None
    lines: List[ClaimLineRequest]
    claim_metadata: Optional[Dict[str, Any]] = None

    @model_validator(mode="before")
    @classmethod
    def normalize_lines(cls, data: Any) -> Any:
        """Allow messy field names by normalizing before validation."""
        if not isinstance(data, dict):
            return data

        raw_lines = data.get("lines")
        if raw_lines is None:
            return data

        mapper = get_field_mapper()
        normalized_lines: List[Dict[str, Any]] = []

        for idx, line in enumerate(raw_lines, start=1):
            # Already-validated line objects pass through untouched
            if isinstance(line, ClaimLineRequest):
                normalized_lines.append(line.model_dump())
                continue

            if not isinstance(line, dict):
                raise TypeError(f"Line {idx}: unsupported payload type '{type(line)}'")

            if cls._is_canonical_line(line):
                normalized_lines.append(line)
                continue

            try:
                mapped = mapper.normalize_record(line)
            except FieldMappingError as exc:  # pragma: no cover - exercised via tests
                raise ValueError(f"Line {idx}: {exc}") from exc

            normalized_lines.append(cls._prepare_clean_line(mapped))

        data["lines"] = normalized_lines
        return data

    @staticmethod
    def _prepare_clean_line(mapped: Dict[str, Any]) -> Dict[str, Any]:
        """Convert normalized record to ClaimLineRequest payload."""

        def currency(value: Any) -> str:
            if value is None:
                return "0.00"
            return str(value)

        return {
            "service_date": mapped.get("service_date"),
            "submitted_procedure": mapped.get("submitted_procedure"),
            "quadrant": mapped.get("quadrant"),
            "plan_group_number": mapped.get("plan_group_number"),
            "subscriber_number": mapped.get("subscriber_number"),
            "provider_npi": mapped.get("provider_npi"),
            "provider_fees": currency(mapped.get("provider_fees")),
            "allowed_fees": currency(mapped.get("allowed_fees")),
            "member_coinsurance": currency(mapped.get("member_coinsurance")),
            "member_copay": currency(mapped.get("member_copay")),
        }

    @staticmethod
    def _is_canonical_line(line: Dict[str, Any]) -> bool:
        """Check if the payload already matches ClaimLineRequest contract."""
        expected_keys = set(ClaimLineRequest.model_fields.keys())
        return set(line.keys()).issubset(expected_keys)


class ClaimResponse(SQLModel):
    """Response model for claim endpoints"""
    claim_id: str
    external_claim_id: Optional[str]
    provider_npi: str
    subscriber_number: str
    plan_group_number: str
    net_fee: str
    line_count: int
    status: str
    created_at: str
    claim_metadata: Dict[str, Any]


class TopProviderResponse(SQLModel):
    """Response model for top providers endpoint"""
    provider_npi: str
    total_net_fees: str
    claim_count: int
    rank: int