"""
Repository pattern for claim data access.
Minimal, focused implementation.
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
import logging

from sqlmodel import Session, select
from claim_process.models import Claim
from claim_process.count_min_sketch import get_tracker

logger = logging.getLogger(__name__)


class ClaimRepository:
    """
    Repository for claim data operations.
    Focused on essential CRUD and Count-Min Sketch integration.
    """

    def __init__(self, session: Session):
        """Initialize with database session."""
        self.session = session

    def create(self, claim: Claim) -> Claim:
        """
        Create a new claim and update Count-Min Sketch.

        Args:
            claim: Claim instance to create

        Returns:
            Created claim with ID
        """
        try:
            self.session.add(claim)
            self.session.commit()
            self.session.refresh(claim)

            # Update Count-Min Sketch for top providers tracking
            tracker = get_tracker()
            tracker.add_claim(claim.provider_npi, claim.net_fee)

            logger.info(f"Created claim {claim.claim_id}")
            return claim

        except Exception as e:
            self.session.rollback()
            logger.error(f"Error creating claim: {e}")
            # Re-raise with more context if it's a unique constraint violation
            error_str = str(e).lower()
            if "unique" in error_str or "duplicate" in error_str or "uq_claims_external_claim_id" in error_str:
                if claim.external_claim_id:
                    raise ValueError(
                        f"Claim with external_claim_id '{claim.external_claim_id}' already exists"
                    ) from e
            raise

    def get_by_id(self, claim_id: Any) -> Optional[Claim]:
        """Get claim by ID."""
        # Handle both string and UUID inputs
        if isinstance(claim_id, str):
            try:
                claim_id = UUID(claim_id)
            except ValueError:
                return None
        statement = select(Claim).where(Claim.claim_id == claim_id)
        return self.session.exec(statement).first()

    def get_by_external_id(self, external_claim_id: str) -> Optional[Claim]:
        """
        Get claim by external claim ID.
        
        Args:
            external_claim_id: External claim identifier
            
        Returns:
            Claim if found, None otherwise
        """
        if not external_claim_id:
            return None
        statement = select(Claim).where(Claim.external_claim_id == external_claim_id)
        return self.session.exec(statement).first()

    def get_top_providers(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top providers by net fees using Count-Min Sketch.

        Args:
            limit: Number of top providers to return (default 10)

        Returns:
            List of top providers with their statistics
        """
        tracker = get_tracker()
        top_providers = tracker.get_top_k()

        return [
            {
                'provider_npi': provider.provider_npi,
                'total_net_fees': str(provider.net_fee_total),
                'claim_count': provider.claim_count,
                'rank': idx + 1
            }
            for idx, provider in enumerate(top_providers[:limit])
        ]