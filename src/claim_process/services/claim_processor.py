"""
Main claim processing service.
Orchestrates field mapping, validation, calculation, and persistence.
"""
from typing import Dict, Any, List
from uuid import UUID
import logging

from claim_process.models import Claim, ClaimCreateRequest
from claim_process.services.field_mapper import get_field_mapper
from claim_process.services.validator import get_validator
from claim_process.services.calculator import get_calculator
from claim_process.repositories.claim_repository import ClaimRepository
from claim_process.utils import serialize_for_json

logger = logging.getLogger(__name__)


class ClaimProcessingError(Exception):
    """Raised when claim processing fails."""
    pass


class ClaimProcessor:
    """
    Orchestrates the claim processing workflow.

    Flow:
    1. Map fields to canonical names
    2. Validate data
    3. Calculate net fees
    4. Persist to database
    5. Send to downstream services
    """

    def __init__(self, repository: ClaimRepository):
        """Initialize with repository."""
        self.repository = repository
        self.field_mapper = get_field_mapper()
        self.validator = get_validator()
        self.calculator = get_calculator()

    def process_claim(self, request: ClaimCreateRequest) -> Claim:
        """
        Process a claim from raw input to persistence.

        Args:
            request: Claim creation request with lines

        Returns:
            Created claim

        Raises:
            ClaimProcessingError: If processing fails
        """
        try:
            # Check for duplicate external_claim_id if provided
            if request.external_claim_id:
                existing_claim = self.repository.get_by_external_id(request.external_claim_id)
                if existing_claim:
                    raise ClaimProcessingError(
                        f"Claim with external_claim_id '{request.external_claim_id}' already exists"
                    )
            # Convert ClaimLineRequest objects to dictionaries
            lines_as_dicts = [line.model_dump() for line in request.lines]

            # Step 1: Request parsing normalizes field names, convert amounts to Decimal
            normalized_lines = []
            for line in lines_as_dicts:
                normalized_line = {
                    **line,
                    'provider_fees': self.calculator._to_decimal(line['provider_fees']),
                    'allowed_fees': self.calculator._to_decimal(line['allowed_fees']),
                    'member_coinsurance': self.calculator._to_decimal(line['member_coinsurance']),
                    'member_copay': self.calculator._to_decimal(line['member_copay'])
                }
                normalized_lines.append(normalized_line)

            # Step 2: Validate
            validation_result = self.validator.validate_claim_lines(normalized_lines)
            if not validation_result.is_valid:
                raise ClaimProcessingError(
                    f"Validation failed: {validation_result.errors}"
                )

            # Step 3: Extract header fields from first line
            header_fields = {
                'provider_npi': normalized_lines[0]['provider_npi'],
                'subscriber_number': normalized_lines[0]['subscriber_number'],
                'plan_group_number': normalized_lines[0]['plan_group_number']
            }

            # Step 4: Calculate totals
            totals = self.calculator.calculate_claim_totals(normalized_lines)

            # Step 5: Create claim entity with proper JSON serialization
            claim = Claim(
                external_claim_id=request.external_claim_id,
                provider_npi=header_fields['provider_npi'],
                subscriber_number=header_fields['subscriber_number'],
                plan_group_number=header_fields['plan_group_number'],
                net_fee=totals['total_net_fee'],
                total_provider_fees=totals['total_provider_fees'],
                total_allowed_fees=totals['total_allowed_fees'],
                total_member_coinsurance=totals['total_member_coinsurance'],
                total_member_copay=totals['total_member_copay'],
                line_count=len(normalized_lines),
                status='processed',
                raw_data=serialize_for_json({'lines': lines_as_dicts}),
                normalized_lines=serialize_for_json(normalized_lines),
                claim_metadata=serialize_for_json({
                    'validation': validation_result.to_dict(),
                    'calculations': self.calculator.generate_calculation_summary(
                        normalized_lines, totals
                    ),
                    **(request.claim_metadata or {})
                })
            )

            # Step 6: Persist
            created_claim = self.repository.create(claim)

            # Step 7: Send to payments service (async in production)
            self._send_to_payments(created_claim)

            logger.info(f"Successfully processed claim {created_claim.claim_id}")
            return created_claim

        except ValueError as e:
            # Handle duplicate external_claim_id errors from repository
            logger.error(f"Validation error: {e}")
            raise ClaimProcessingError(str(e))
        except Exception as e:
            logger.error(f"Error processing claim: {e}")
            raise ClaimProcessingError(f"Failed to process claim: {str(e)}")

    def _send_to_payments(self, claim: Claim) -> None:
        """
        Send claim to payments service.

        Implementation options:
        1. Message Queue (RabbitMQ/Kafka) - Recommended for production
        2. Direct HTTP call - Simple but coupled
        3. Event sourcing - Most robust

        For production, use message queue for:
        - Decoupling services
        - Retry capability
        - Handling service downtime
        - Load balancing

        Pseudo-code for message queue approach:
        ```
        # Using Celery with RabbitMQ
        from claim_process.tasks import send_claim_to_payments

        # Async task with retry
        send_claim_to_payments.delay(
            claim_id=str(claim.claim_id),
            provider_npi=claim.provider_npi,
            net_fee=str(claim.net_fee),
            retry_on_failure=True,
            max_retries=3
        )
        ```

        For failures and rollback:
        - Use saga pattern or two-phase commit
        - Store payment status in claim metadata
        - Implement compensating transactions
        """
        # TODO: Implement actual payment service integration
        logger.info(f"Would send claim {claim.claim_id} to payments service")
        logger.info(f"Net fee to process: {claim.net_fee}")