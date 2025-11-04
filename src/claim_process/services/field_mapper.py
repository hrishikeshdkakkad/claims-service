"""
Field mapping service for normalizing inconsistent field names.
Handles case-insensitive matching and field name variations.
"""
from typing import Dict, Any, Optional, List
from decimal import Decimal
import logging

from claim_process.config import FIELD_MAPPINGS, FieldMapping

logger = logging.getLogger(__name__)


class FieldMappingError(Exception):
    """Raised when field mapping fails."""
    pass


class FieldMapper:
    """
    Service for mapping various field name formats to canonical names.

    This service handles:
    - Case-insensitive field matching
    - Multiple naming variations (e.g., "Provider NPI" vs "provider_npi")
    - Field validation metadata
    - Default value handling
    """

    def __init__(self, field_mappings: Optional[Dict[str, FieldMapping]] = None):
        """
        Initialize the field mapper.

        Args:
            field_mappings: Optional custom field mappings. Defaults to config.
        """
        self.field_mappings = field_mappings or FIELD_MAPPINGS
        self._build_variation_index()

    def _build_variation_index(self) -> None:
        """Build an index for fast lookup of field variations."""
        self.variation_index: Dict[str, str] = {}

        for canonical_name, mapping in self.field_mappings.items():
            for variation in mapping.variations:
                # Store lowercase for case-insensitive matching
                self.variation_index[variation.lower()] = canonical_name

    def get_canonical_name(self, field_name: str) -> Optional[str]:
        """
        Get the canonical name for a field.

        Args:
            field_name: The field name to normalize

        Returns:
            The canonical field name, or None if not found
        """
        return self.variation_index.get(field_name.lower())

    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a single record by mapping field names to canonical names.

        Args:
            record: The record with potentially inconsistent field names

        Returns:
            A normalized record with canonical field names

        Raises:
            FieldMappingError: If required fields are missing
        """
        normalized = {}
        unmapped_fields = []

        for field_name, value in record.items():
            canonical_name = self.get_canonical_name(field_name)

            if canonical_name:
                # Clean the value (remove currency symbols, whitespace, etc.)
                cleaned_value = self._clean_value(canonical_name, value)
                normalized[canonical_name] = cleaned_value
            else:
                # Keep track of unmapped fields for debugging
                unmapped_fields.append(field_name)

        if unmapped_fields:
            logger.warning(f"Unmapped fields found: {unmapped_fields}")

        # Check for required fields
        self._validate_required_fields(normalized)

        return normalized

    def normalize_claim_lines(self, lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize multiple claim lines.

        Args:
            lines: List of claim line records

        Returns:
            List of normalized claim lines
        """
        normalized_lines = []

        for idx, line in enumerate(lines):
            try:
                normalized_line = self.normalize_record(line)
                normalized_line['line_number'] = idx + 1
                normalized_lines.append(normalized_line)
            except FieldMappingError as e:
                logger.error(f"Error normalizing line {idx + 1}: {e}")
                raise FieldMappingError(f"Line {idx + 1}: {str(e)}")

        return normalized_lines

    def _clean_value(self, canonical_name: str, value: Any) -> Any:
        """
        Clean and convert field values based on their type.

        Args:
            canonical_name: The canonical field name
            value: The raw value to clean

        Returns:
            The cleaned value
        """
        if value is None or value == "":
            return None

        field_mapping = self.field_mappings.get(canonical_name)
        if not field_mapping:
            return value

        field_type = field_mapping.validation.field_type.value

        # Handle currency fields
        if field_type == "currency":
            return self._parse_currency(value)

        # Handle date fields
        elif field_type == "date":
            return self._parse_date(value)

        # Handle string fields
        elif field_type == "string":
            return str(value).strip()

        # Handle NPI (remove any non-digits)
        elif field_type == "npi":
            return ''.join(filter(str.isdigit, str(value)))

        # Handle procedure codes
        elif field_type == "procedure_code":
            return str(value).strip().upper()

        return value

    def _parse_currency(self, value: Any) -> Decimal:
        """
        Parse currency values, removing symbols and converting to Decimal.

        Args:
            value: The currency value (e.g., "$100.00", "100.00")

        Returns:
            Decimal representation of the value
        """
        if isinstance(value, (int, float, Decimal)):
            return Decimal(str(value))

        # Remove currency symbols and whitespace
        cleaned = str(value).replace('$', '').replace(',', '').strip()

        try:
            return Decimal(cleaned)
        except:
            raise FieldMappingError(f"Invalid currency value: {value}")

    def _parse_date(self, value: Any) -> str:
        """
        Parse date values to ISO format.

        Args:
            value: The date value

        Returns:
            ISO formatted date string
        """
        from datetime import datetime
        import dateutil.parser

        if isinstance(value, str):
            try:
                # Parse the date using dateutil for flexibility
                dt = dateutil.parser.parse(value)
                return dt.isoformat()
            except:
                raise FieldMappingError(f"Invalid date value: {value}")

        return str(value)

    def _validate_required_fields(self, normalized: Dict[str, Any]) -> None:
        """
        Validate that all required fields are present.

        Args:
            normalized: The normalized record

        Raises:
            FieldMappingError: If required fields are missing
        """
        missing_required = []

        for canonical_name, mapping in self.field_mappings.items():
            if mapping.validation.required and canonical_name != "quadrant":
                if canonical_name not in normalized or normalized[canonical_name] is None:
                    missing_required.append(canonical_name)

        if missing_required:
            raise FieldMappingError(
                f"Missing required fields: {', '.join(missing_required)}"
            )

    def extract_claim_header_fields(self, normalized_lines: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract claim header fields from the first line.

        Args:
            normalized_lines: List of normalized claim lines

        Returns:
            Dictionary with claim header fields
        """
        if not normalized_lines:
            raise FieldMappingError("No claim lines provided")

        first_line = normalized_lines[0]

        return {
            'provider_npi': first_line.get('provider_npi'),
            'subscriber_number': first_line.get('subscriber_number'),
            'plan_group_number': first_line.get('plan_group_number'),
        }


# Singleton instance
_field_mapper: Optional[FieldMapper] = None


def get_field_mapper() -> FieldMapper:
    """Get or create the field mapper singleton."""
    global _field_mapper
    if _field_mapper is None:
        _field_mapper = FieldMapper()
    return _field_mapper