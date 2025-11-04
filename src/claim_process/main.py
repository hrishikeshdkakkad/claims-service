"""
FastAPI application for claims processing service.
"""
import logging
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlmodel import Session
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from claim_process.database import get_session, init_db
from claim_process.models import ClaimCreateRequest, ClaimResponse, TopProviderResponse
from claim_process.repositories.claim_repository import ClaimRepository
from claim_process.services.claim_processor import ClaimProcessor, ClaimProcessingError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiting setup
limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize app on startup."""
    logger.info("Starting claim processing service...")
    init_db()
    yield
    logger.info("Shutting down claim processing service...")


# Create FastAPI app
app = FastAPI(
    title="Claim Processing Service",
    description="Healthcare claims processing with net fee calculation",
    version="1.0.0",
    lifespan=lifespan
)

# Add rate limiting error handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "claim_process"}


@app.post("/claims", response_model=ClaimResponse)
def process_claim(
    request: ClaimCreateRequest,
    session: Session = Depends(get_session)
) -> ClaimResponse:
    """
    Process a new claim.

    This endpoint:
    1. Normalizes field names (handles inconsistent capitalization)
    2. Validates all fields according to metadata rules
    3. Calculates net fee per line and total
    4. Stores in database
    5. Updates Count-Min Sketch for top providers
    6. Prepares for downstream payment service

    Args:
        request: Claim with multiple line items

    Returns:
        Processed claim with calculated net fee

    Raises:
        HTTPException: If processing fails
    """
    try:
        repository = ClaimRepository(session)
        
        # Early validation: Check for duplicate external_claim_id at API level
        if request.external_claim_id:
            existing_claim = repository.get_by_external_id(request.external_claim_id)
            if existing_claim:
                raise HTTPException(
                    status_code=409,
                    detail=f"Claim with external_claim_id '{request.external_claim_id}' already exists"
                )
        
        processor = ClaimProcessor(repository)

        # Process the claim
        claim = processor.process_claim(request)

        # Convert to response model
        return ClaimResponse(
            claim_id=str(claim.claim_id),
            external_claim_id=claim.external_claim_id,
            provider_npi=claim.provider_npi,
            subscriber_number=claim.subscriber_number,
            plan_group_number=claim.plan_group_number,
            net_fee=f"{claim.net_fee:.2f}",
            line_count=claim.line_count,
            status=claim.status,
            created_at=claim.created_at.isoformat(),
            claim_metadata=claim.claim_metadata
        )

    except HTTPException:
        # Re-raise HTTP exceptions (like duplicate claims)
        raise
    except ClaimProcessingError as e:
        logger.error(f"Claim processing error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/top-providers", response_model=List[TopProviderResponse])
@limiter.limit("10/minute")
def get_top_providers(
    request: Request,
    limit: int = 10,
    session: Session = Depends(get_session)
) -> List[TopProviderResponse]:
    """
    Get top provider NPIs by net fees.

    Uses Count-Min Sketch algorithm for efficient tracking:
    - O(log K) time complexity for updates
    - O(1) space complexity (fixed 54KB memory)
    - 99% accuracy with 0.1% error margin

    Rate limited to 10 requests per minute.

    Args:
        limit: Number of top providers to return (default 10)

    Returns:
        List of top providers sorted by net fees
    """
    try:
        repository = ClaimRepository(session)
        top_providers = repository.get_top_providers(limit=limit)

        return [
            TopProviderResponse(
                provider_npi=provider['provider_npi'],
                total_net_fees=provider['total_net_fees'],
                claim_count=provider['claim_count'],
                rank=provider['rank']
            )
            for provider in top_providers
        ]

    except Exception as e:
        logger.error(f"Error fetching top providers: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/claims/{claim_id}")
def get_claim(
    claim_id: str,
    session: Session = Depends(get_session)
) -> ClaimResponse:
    """
    Get a specific claim by ID.

    Args:
        claim_id: UUID of the claim

    Returns:
        Claim details

    Raises:
        HTTPException: If claim not found
    """
    try:
        repository = ClaimRepository(session)
        claim = repository.get_by_id(claim_id)

        if not claim:
            raise HTTPException(status_code=404, detail="Claim not found")

        return ClaimResponse(
            claim_id=str(claim.claim_id),
            external_claim_id=claim.external_claim_id,
            provider_npi=claim.provider_npi,
            subscriber_number=claim.subscriber_number,
            plan_group_number=claim.plan_group_number,
            net_fee=f"{claim.net_fee:.2f}",
            line_count=claim.line_count,
            status=claim.status,
            created_at=claim.created_at.isoformat(),
            claim_metadata=claim.claim_metadata
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching claim: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred"}
    )


# For running directly
def main():
    """Run the application."""
    import uvicorn
    uvicorn.run(
        "claim_process.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


if __name__ == "__main__":
    main()