"""Tests for service layer components."""
import pytest
from decimal import Decimal
from claim_process.services.field_mapper import FieldMapper
from claim_process.services.validator import ClaimValidator
from claim_process.services.calculator import NetFeeCalculator


class TestFieldMapper:
    """Test field mapping functionality."""

    def test_normalize_record(self):
        """Test normalizing a record with inconsistent field names."""
        mapper = FieldMapper()

        # Input with inconsistent capitalization
        record = {
            "Service Date": "2024-01-15",
            "submitted procedure": "D0180",
            "Provider NPI": "1234567890",
            "provider fees": "$100.00",
            "Allowed fees": "$80.00",
            "member coinsurance": "$10.00",
            "member copay": "$5.00",
            "Plan/Group #": "GRP-1000",
            "Subscriber#": "12345"
        }

        normalized = mapper.normalize_record(record)

        # Check canonical names
        assert "service_date" in normalized
        assert "submitted_procedure" in normalized
        assert "provider_npi" in normalized
        assert normalized["submitted_procedure"] == "D0180"
        assert normalized["provider_npi"] == "1234567890"

    def test_parse_currency(self):
        """Test currency parsing."""
        mapper = FieldMapper()

        assert mapper._parse_currency("$100.00") == Decimal("100.00")
        assert mapper._parse_currency("100.00") == Decimal("100.00")
        assert mapper._parse_currency("$1,234.56") == Decimal("1234.56")
        assert mapper._parse_currency(100) == Decimal("100")

    def test_extract_claim_header_fields(self):
        """Test extracting header fields from lines."""
        mapper = FieldMapper()

        lines = [
            {
                "provider_npi": "1234567890",
                "subscriber_number": "12345",
                "plan_group_number": "GRP-1000"
            }
        ]

        header = mapper.extract_claim_header_fields(lines)

        assert header["provider_npi"] == "1234567890"
        assert header["subscriber_number"] == "12345"
        assert header["plan_group_number"] == "GRP-1000"


class TestClaimValidator:
    """Test validation functionality."""

    def test_validate_npi(self):
        """Test NPI validation."""
        validator = ClaimValidator()

        record = {"provider_npi": "1234567890"}  # Valid 10-digit NPI
        result = validator.validate_record(record)
        assert result.is_valid

        record = {"provider_npi": "12345"}  # Too short
        result = validator.validate_record(record)
        assert not result.is_valid
        assert "provider_npi" in result.field_errors

    def test_validate_procedure_code(self):
        """Test procedure code validation."""
        validator = ClaimValidator()

        record = {"submitted_procedure": "D0180"}
        result = validator.validate_record(record)
        assert result.is_valid

        record = {"submitted_procedure": "X0180"}  # Doesn't start with D
        result = validator.validate_record(record)
        assert not result.is_valid

    def test_validate_currency_fields(self):
        """Test currency field validation."""
        validator = ClaimValidator()

        record = {
            "provider_fees": Decimal("100.00"),
            "allowed_fees": Decimal("80.00"),
            "member_coinsurance": Decimal("-10.00")  # Negative value
        }

        result = validator.validate_record(record)
        assert not result.is_valid
        assert "member_coinsurance" in result.field_errors


class TestNetFeeCalculator:
    """Test net fee calculation."""

    def test_calculate_line_net_fee(self):
        """Test net fee calculation for a single line using metadata-driven formula."""
        calculator = NetFeeCalculator()

        line = {
            "provider_fees": Decimal("100.00"),
            "allowed_fees": Decimal("80.00"),
            "member_coinsurance": Decimal("10.00"),
            "member_copay": Decimal("5.00")
        }

        net_fee = calculator.apply_custom_formula('net_fee', line)
        # Net fee = 100 + 10 + 5 - 80 = 35
        assert net_fee == Decimal("35.00")

    def test_calculate_claim_totals(self):
        """Test calculating totals for multiple lines."""
        calculator = NetFeeCalculator()

        lines = [
            {
                "provider_fees": Decimal("100.00"),
                "allowed_fees": Decimal("80.00"),
                "member_coinsurance": Decimal("10.00"),
                "member_copay": Decimal("5.00")
            },
            {
                "provider_fees": Decimal("200.00"),
                "allowed_fees": Decimal("180.00"),
                "member_coinsurance": Decimal("20.00"),
                "member_copay": Decimal("10.00")
            }
        ]

        totals = calculator.calculate_claim_totals(lines)

        assert totals["total_provider_fees"] == Decimal("300.00")
        assert totals["total_allowed_fees"] == Decimal("260.00")
        assert totals["total_member_coinsurance"] == Decimal("30.00")
        assert totals["total_member_copay"] == Decimal("15.00")
        # Total net fee = 300 + 30 + 15 - 260 = 85
        assert totals["total_net_fee"] == Decimal("85.00")

    def test_member_responsibility(self):
        """Test member responsibility calculation using metadata-driven formula."""
        calculator = NetFeeCalculator()

        totals = {
            "total_member_coinsurance": Decimal("30.00"),
            "total_member_copay": Decimal("15.00")
        }

        member_resp = calculator.apply_custom_formula('total_member_responsibility', totals)
        assert member_resp == Decimal("45.00")

    def test_provider_adjustment(self):
        """Test provider adjustment calculation using metadata-driven formula."""
        calculator = NetFeeCalculator()

        totals = {
            "total_provider_fees": Decimal("300.00"),
            "total_allowed_fees": Decimal("260.00")
        }

        adjustment = calculator.apply_custom_formula('total_provider_adjustment', totals)
        assert adjustment == Decimal("40.00")