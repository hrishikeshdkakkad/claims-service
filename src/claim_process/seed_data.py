"""Seed initial claim data from CSV."""
from pathlib import Path
import logging
import os

from sqlmodel import Session, select  # type: ignore

from claim_process.csv_converter import convert_csv_to_api_format
from claim_process.database import engine
from claim_process.models import Claim, ClaimCreateRequest
from claim_process.repositories.claim_repository import ClaimRepository
from claim_process.services.claim_processor import ClaimProcessor, ClaimProcessingError


logger = logging.getLogger(__name__)


def _default_seed_path() -> str:
    return os.getenv("SEED_CSV_PATH", "claim_1234.csv")


def _resolve_csv_path(raw_path: Path) -> Path | None:
    """Handle paths that might point to a directory or missing file."""
    if raw_path.is_file():
        return raw_path

    if raw_path.is_dir():
        candidate = raw_path / "claim_1234.csv"
        if candidate.is_file():
            return candidate
        csv_files = list(raw_path.glob("*.csv"))
        if csv_files:
            return csv_files[0]
        logger.warning("No CSV files found under directory '%s'", raw_path)
        return None

    # Try resolving relative to project root (two levels up from src/claim_process)
    project_root = Path(__file__).resolve().parents[2]
    fallback = project_root / raw_path
    if fallback.is_file():
        return fallback

    logger.warning("Seed CSV '%s' not found (checked %s)", raw_path, fallback)
    return None


def seed_from_csv(csv_path: str | None = None) -> None:
    """Load the provided CSV file and persist its claim if absent."""
    requested = Path(csv_path or _default_seed_path())
    path = _resolve_csv_path(requested)
    if path is None:
        logger.info("Seed data skipped; CSV '%s' not available", requested)
        return

    payload = convert_csv_to_api_format(str(path))
    external_id = payload.get("external_claim_id")

    with Session(engine) as session:
        existing = session.exec(
            select(Claim).where(Claim.external_claim_id == external_id)
        ).first()

        if existing:
            logger.info("Seed claim '%s' already present; skipping", external_id)
            return

        request = ClaimCreateRequest.model_validate(payload)
        repository = ClaimRepository(session)
        processor = ClaimProcessor(repository)

        try:
            processor.process_claim(request)
            logger.info("Seeded claim '%s' from '%s'", external_id, path)
        except ClaimProcessingError as exc:  # pragma: no cover - startup path
            logger.error("Failed to seed data from '%s': %s", path, exc)
            raise


def main() -> None:
    seed_from_csv()


if __name__ == "__main__":
    main()

