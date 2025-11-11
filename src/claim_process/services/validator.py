"""
Validation service with metadata-driven rules.
Implements flexible validation that can be extended without code changes.
"""
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
import re
import logging

from claim_process.config import FIELD_MAPPINGS, FieldMapping, ValidationRule, FieldType

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class ValidationResult:
    """Result of validation with errors and warnings."""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.field_errors: Dict[str, List[str]] = {}

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return len(self.errors) == 0 and len(self.field_errors) == 0

    def add_error(self, message: str, field: Optional[str] = None):
        """Add an error message."""
        if field:
            if field not in self.field_errors:
                self.field_errors[field] = []
            self.field_errors[field].append(message)
        else:
            self.errors.append(message)

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/API response."""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'field_errors': self.field_errors
        }


class ClaimValidator:
    """
    Metadata-driven validation service for claims.

    Features:
    - Configurable validation rules
    - Field-specific validation
    - Custom validation functions
    - Detailed error reporting
    """

    def __init__(self, field_mappings: Optional[Dict[str, FieldMapping]] = None):
        """
        Initialize the validator.

        Args:
            field_mappings: Optional custom field mappings. Defaults to config.
        """
        self.field_mappings = field_mappings or FIELD_MAPPINGS

        # Register custom validators
        self.custom_validators = {
            'validate_npi_checksum': self._validate_npi_checksum,
            'validate_procedure_format': self._validate_procedure_format,
        }

    def validate_claim_lines(self, lines: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate all claim lines.

        Args:
            lines: List of normalized claim lines

        Returns:
            ValidationResult with all errors and warnings
        """
        result = ValidationResult()

        if not lines:
            result.add_error("No claim lines provided")
            return result

        for idx, line in enumerate(lines):
            line_num = idx + 1
            line_result = self.validate_record(line)

            # Add line number context to errors
            for field, errors in line_result.field_errors.items():
                for error in errors:
                    result.add_error(f"Line {line_num}: {error}", field)

            for error in line_result.errors:
                result.add_error(f"Line {line_num}: {error}")

            for warning in line_result.warnings:
                result.add_warning(f"Line {line_num}: {warning}")

        # Validate claim-level rules
        self._validate_claim_level_rules(lines, result)

        return result

    def validate_record(self, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single record against metadata-driven rules.

        Args:
            record: Normalized record to validate

        Returns:
            ValidationResult with errors and warnings
        """
        result = ValidationResult()

        for field_name, value in record.items():
            if field_name in self.field_mappings:
                field_mapping = self.field_mappings[field_name]
                self._validate_field(field_name, value, field_mapping, result)

        return result

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_mapping: FieldMapping,
        result: ValidationResult
    ) -> None:
        """
        Validate a single field against its rules.

        Args:
            field_name: The field name
            value: The field value
            field_mapping: The field mapping with validation rules
            result: ValidationResult to populate
        """
        validation_rule = field_mapping.validation

        # Check required
        if validation_rule.required and (value is None or value == ""):
            result.add_error(f"{field_name} is required", field_name)
            return

        # Skip further validation if value is None and not required
        if value is None:
            return

        # Type-specific validation
        field_type = validation_rule.field_type

        if field_type == FieldType.NPI:
            self._validate_npi(field_name, value, validation_rule, result)

        elif field_type == FieldType.PROCEDURE_CODE:
            self._validate_procedure_code(field_name, value, validation_rule, result)

        elif field_type == FieldType.CURRENCY:
            self._validate_currency(field_name, value, validation_rule, result)

        elif field_type == FieldType.STRING:
            self._validate_string(field_name, value, validation_rule, result)

        # Pattern validation
        if validation_rule.pattern:
            self._validate_pattern(field_name, value, validation_rule.pattern, result)

        # Custom validator
        if validation_rule.custom_validator:
            self._run_custom_validator(
                field_name, value, validation_rule.custom_validator, result
            )

    def _validate_npi(
        self,
        field_name: str,
        value: Any,
        rule: ValidationRule,
        result: ValidationResult
    ) -> None:
        """Validate NPI field (10-digit number)."""
        str_value = str(value)

        if not str_value.isdigit():
            result.add_error(f"{field_name} must contain only digits", field_name)
            return

        if len(str_value) != 10:
            result.add_error(f"{field_name} must be exactly 10 digits", field_name)
            return

        # Note: NPI checksum validation (Luhn algorithm) is handled via custom_validator
        # if configured in the field mapping metadata

    def _validate_procedure_code(
        self,
        field_name: str,
        value: Any,
        rule: ValidationRule,
        result: ValidationResult
    ) -> None:
        """Validate procedure code (must start with 'D')."""
        if value is None:
            if rule.required:
                result.add_error(f"{field_name} is required", field_name)
            return

        str_value = str(value).strip().upper()

        if rule.starts_with and not str_value.startswith(rule.starts_with):
            result.add_error(
                f"{field_name} must start with '{rule.starts_with}'",
                field_name
            )
            return

        # Validate format (D followed by 4 digits)
        if not self._validate_procedure_format(str_value):
            result.add_error(
                f"{field_name} must be in format 'D' followed by 4 digits",
                field_name
            )

    def _validate_currency(
        self,
        field_name: str,
        value: Any,
        rule: ValidationRule,
        result: ValidationResult
    ) -> None:
        """Validate currency field."""
        if not isinstance(value, (int, float, Decimal)):
            result.add_error(f"{field_name} must be a numeric value", field_name)
            return

        decimal_value = Decimal(str(value))

        if rule.min_value is not None and decimal_value < Decimal(str(rule.min_value)):
            result.add_error(
                f"{field_name} must be at least {rule.min_value}",
                field_name
            )

        if rule.max_value is not None and decimal_value > Decimal(str(rule.max_value)):
            result.add_error(
                f"{field_name} must not exceed {rule.max_value}",
                field_name
            )

    def _validate_string(
        self,
        field_name: str,
        value: Any,
        rule: ValidationRule,
        result: ValidationResult
    ) -> None:
        """Validate string field."""
        str_value = str(value)

        if rule.min_length and len(str_value) < rule.min_length:
            result.add_error(
                f"{field_name} must be at least {rule.min_length} characters",
                field_name
            )

        if rule.max_length and len(str_value) > rule.max_length:
            result.add_error(
                f"{field_name} must not exceed {rule.max_length} characters",
                field_name
            )

    def _validate_pattern(
        self,
        field_name: str,
        value: Any,
        pattern: str,
        result: ValidationResult
    ) -> None:
        """Validate field against regex pattern."""
        str_value = str(value)

        if not re.match(pattern, str_value):
            result.add_error(
                f"{field_name} does not match required pattern",
                field_name
            )

    def _run_custom_validator(
        self,
        field_name: str,
        value: Any,
        validator_name: str,
        result: ValidationResult
    ) -> None:
        """Run a custom validator function."""
        if validator_name in self.custom_validators:
            validator_func = self.custom_validators[validator_name]
            is_valid = validator_func(value)

            if not is_valid:
                # Provide specific error messages based on validator type
                if validator_name == "validate_npi_checksum":
                    result.add_error(
                        f"{field_name} failed Luhn algorithm checksum validation",
                        field_name
                    )
                elif validator_name == "validate_procedure_format":
                    result.add_error(
                        f"{field_name} does not match required format (D followed by 4 digits)",
                        field_name
                    )
                else:
                    result.add_error(
                        f"{field_name} failed custom validation ({validator_name})",
                        field_name
                    )

    def _validate_npi_checksum(self, npi: str) -> bool:
        """
        Validate NPI using Luhn algorithm.

        Args:
            npi: 10-digit NPI string

        Returns:
            True if valid, False otherwise
        """
        if len(npi) != 10:
            return False

        # Add prefix for Luhn check (80840 for US providers)
        full_npi = "80840" + npi[:-1]

        # Apply Luhn algorithm
        total = 0
        for i, digit in enumerate(reversed(full_npi)):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n = n - 9
            total += n

        check_digit = (10 - (total % 10)) % 10
        return check_digit == int(npi[-1])

    def _validate_procedure_format(self, code: str) -> bool:
        """
        Validate dental procedure code format.

        Args:
            code: Procedure code

        Returns:
            True if valid format
        """
        return bool(re.match(r'^D\d{4}$', code))

    def _validate_claim_level_rules(
        self,
        lines: List[Dict[str, Any]],
        result: ValidationResult
    ) -> None:
        """
        Validate claim-level business rules.

        Args:
            lines: All claim lines
            result: ValidationResult to populate
        """
        # Check consistency of header fields across lines
        if len(lines) > 1:
            first_line = lines[0]
            header_fields = ['provider_npi', 'subscriber_number', 'plan_group_number']

            for field in header_fields:
                base_value = first_line.get(field)
                for idx, line in enumerate(lines[1:], start=2):
                    if line.get(field) != base_value:
                        result.add_warning(
                            f"Inconsistent {field} across claim lines"
                        )

        # Check for duplicate procedure codes
        procedures = [line.get('submitted_procedure') for line in lines]
        duplicates = set([p for p in procedures if procedures.count(p) > 1])
        if duplicates:
            result.add_warning(
                f"Duplicate procedures found: {', '.join(duplicates)}"
            )


# Singleton instance
_validator: Optional[ClaimValidator] = None


def get_validator() -> ClaimValidator:
    """Get or create the validator singleton."""
    global _validator
    if _validator is None:
        _validator = ClaimValidator()
    return _validator