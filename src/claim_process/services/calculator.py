"""
Calculation service for net fee and other claim calculations.
Uses metadata-driven formulas for flexibility.
"""
from typing import Dict, Any, List, Optional
from decimal import Decimal
import logging

from claim_process.config import CALCULATION_FORMULAS, CalculationFormula

logger = logging.getLogger(__name__)


class CalculationError(Exception):
    """Raised when calculation fails."""
    pass


class NetFeeCalculator:
    """
    Service for calculating net fees and other claim amounts.

    Features:
    - Metadata-driven formula execution
    - Support for multiple calculation types
    - Aggregation across claim lines
    - Detailed calculation tracking
    """

    def __init__(self, formulas: Optional[Dict[str, CalculationFormula]] = None):
        """
        Initialize the calculator.

        Args:
            formulas: Optional custom formulas. Defaults to config.
        """
        self.formulas = formulas or CALCULATION_FORMULAS

    def calculate_line_net_fee(self, line: Dict[str, Any]) -> Decimal:
        """
        Calculate net fee for a single claim line.

        Args:
            line: Normalized claim line

        Returns:
            Calculated net fee

        Raises:
            CalculationError: If required fields are missing
        """
        formula = self.formulas.get('net_fee')
        if not formula:
            raise CalculationError("Net fee formula not configured")

        # Extract required fields
        try:
            provider_fees = self._to_decimal(line.get('provider_fees', 0))
            member_coinsurance = self._to_decimal(line.get('member_coinsurance', 0))
            member_copay = self._to_decimal(line.get('member_copay', 0))
            allowed_fees = self._to_decimal(line.get('allowed_fees', 0))
        except Exception as e:
            raise CalculationError(f"Invalid numeric value: {e}")

        # Apply formula: provider_fees + member_coinsurance + member_copay - allowed_fees
        net_fee = provider_fees + member_coinsurance + member_copay - allowed_fees

        logger.debug(
            f"Calculated net fee: {net_fee} "
            f"(provider: {provider_fees}, coinsurance: {member_coinsurance}, "
            f"copay: {member_copay}, allowed: {allowed_fees})"
        )

        return net_fee

    def calculate_claim_totals(self, lines: List[Dict[str, Any]]) -> Dict[str, Decimal]:
        """
        Calculate all totals for a claim.

        Args:
            lines: List of normalized claim lines

        Returns:
            Dictionary with all calculated totals
        """
        totals = {
            'total_provider_fees': Decimal('0.00'),
            'total_allowed_fees': Decimal('0.00'),
            'total_member_coinsurance': Decimal('0.00'),
            'total_member_copay': Decimal('0.00'),
            'total_net_fee': Decimal('0.00'),
            'line_net_fees': []
        }

        for line in lines:
            # Calculate line-level net fee
            line_net_fee = self.calculate_line_net_fee(line)
            totals['line_net_fees'].append(line_net_fee)

            # Aggregate totals
            totals['total_provider_fees'] += self._to_decimal(
                line.get('provider_fees', 0)
            )
            totals['total_allowed_fees'] += self._to_decimal(
                line.get('allowed_fees', 0)
            )
            totals['total_member_coinsurance'] += self._to_decimal(
                line.get('member_coinsurance', 0)
            )
            totals['total_member_copay'] += self._to_decimal(
                line.get('member_copay', 0)
            )
            totals['total_net_fee'] += line_net_fee

        # Calculate additional metrics
        totals['member_responsibility'] = self.calculate_member_responsibility(totals)
        totals['provider_adjustment'] = self.calculate_provider_adjustment(totals)
        totals['average_net_fee'] = self._calculate_average(totals['line_net_fees'])

        return totals

    def calculate_member_responsibility(self, totals: Dict[str, Decimal]) -> Decimal:
        """
        Calculate total member responsibility.

        Args:
            totals: Dictionary with claim totals

        Returns:
            Total amount member owes
        """
        formula = self.formulas.get('member_responsibility')
        if not formula:
            # Default calculation if formula not configured
            return totals['total_member_coinsurance'] + totals['total_member_copay']

        return totals['total_member_coinsurance'] + totals['total_member_copay']

    def calculate_provider_adjustment(self, totals: Dict[str, Decimal]) -> Decimal:
        """
        Calculate provider adjustment (write-off amount).

        Args:
            totals: Dictionary with claim totals

        Returns:
            Provider adjustment amount
        """
        formula = self.formulas.get('provider_adjustment')
        if not formula:
            # Default calculation if formula not configured
            return totals['total_provider_fees'] - totals['total_allowed_fees']

        return totals['total_provider_fees'] - totals['total_allowed_fees']

    def apply_custom_formula(
        self,
        formula_name: str,
        context: Dict[str, Any]
    ) -> Decimal:
        """
        Apply a custom formula from configuration.

        Args:
            formula_name: Name of the formula to apply
            context: Dictionary with values for formula variables

        Returns:
            Calculated result

        Raises:
            CalculationError: If formula not found or calculation fails
        """
        formula = self.formulas.get(formula_name)
        if not formula:
            raise CalculationError(f"Formula '{formula_name}' not found")

        # Check required fields
        missing_fields = []
        for field in formula.fields_required:
            if field not in context:
                missing_fields.append(field)

        if missing_fields:
            raise CalculationError(
                f"Missing required fields for formula '{formula_name}': "
                f"{', '.join(missing_fields)}"
            )

        # Create safe evaluation context with Decimal values
        eval_context = {}
        for field in formula.fields_required:
            eval_context[field] = self._to_decimal(context[field])

        try:
            # Safely evaluate the formula
            # In production, consider using a proper expression evaluator
            result = eval(formula.formula, {"__builtins__": {}}, eval_context)
            return self._to_decimal(result)
        except Exception as e:
            raise CalculationError(
                f"Error evaluating formula '{formula_name}': {e}"
            )

    def _to_decimal(self, value: Any) -> Decimal:
        """
        Convert value to Decimal safely.

        Args:
            value: Value to convert

        Returns:
            Decimal representation
        """
        if isinstance(value, Decimal):
            return value

        if value is None or value == "":
            return Decimal('0.00')

        try:
            return Decimal(str(value))
        except:
            raise CalculationError(f"Cannot convert '{value}' to Decimal")

    def _calculate_average(self, values: List[Decimal]) -> Decimal:
        """
        Calculate average of Decimal values.

        Args:
            values: List of values

        Returns:
            Average value
        """
        if not values:
            return Decimal('0.00')

        total = sum(values)
        count = len(values)

        return total / Decimal(str(count))

    def generate_calculation_summary(
        self,
        lines: List[Dict[str, Any]],
        totals: Dict[str, Decimal]
    ) -> Dict[str, Any]:
        """
        Generate a detailed summary of all calculations.

        Args:
            lines: Claim lines
            totals: Calculated totals

        Returns:
            Detailed calculation summary
        """
        return {
            'line_count': len(lines),
            'totals': {
                'provider_fees': str(totals['total_provider_fees']),
                'allowed_fees': str(totals['total_allowed_fees']),
                'member_coinsurance': str(totals['total_member_coinsurance']),
                'member_copay': str(totals['total_member_copay']),
                'net_fee': str(totals['total_net_fee']),
            },
            'calculated_fields': {
                'member_responsibility': str(totals['member_responsibility']),
                'provider_adjustment': str(totals['provider_adjustment']),
                'average_net_fee': str(totals['average_net_fee']),
            },
            'line_details': [
                {
                    'line_number': idx + 1,
                    'net_fee': str(net_fee),
                    'procedure': line.get('submitted_procedure')
                }
                for idx, (line, net_fee) in enumerate(
                    zip(lines, totals['line_net_fees'])
                )
            ]
        }


# Singleton instance
_calculator: Optional[NetFeeCalculator] = None


def get_calculator() -> NetFeeCalculator:
    """Get or create the calculator singleton."""
    global _calculator
    if _calculator is None:
        _calculator = NetFeeCalculator()
    return _calculator