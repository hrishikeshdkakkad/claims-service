# Claim Processing Service

Healthcare claims processing service with net fee calculation.

## Quick Start

```bash
# Local development
uv sync --all-extras
uv run alembic upgrade head
uv run uvicorn claim_process.main:app --reload
```

## Docker Compose Usage

```bash
APP_PORT=18100 POSTGRES_PORT=25432 docker-compose up --build
```

- Binds the API to `http://localhost:18100` and Postgres to `localhost:25432` to avoid common port clashes.
- The container entrypoint runs `alembic upgrade head` automatically before starting Uvicorn, so schema migrations are applied on boot.
- After migrations, the container seeds `claim_1234.csv` (or `SEED_CSV_PATH`) once—subsequent starts detect the existing `external_claim_id` and skip.
- Stop the stack with `docker-compose down` once you are finished.

**Postman collection:** import `postman/claim_process_collection.json` to exercise the health check, claim submission, and top-provider endpoints against the compose environment.

## API Documentation

### Clean API Format

```json
{
  "external_claim_id": "claim_123",
  "lines": [
    {
      "service_date": "2024-03-28",
      "submitted_procedure": "D0180",
      "quadrant": null,
      "plan_group_number": "GRP-1000",
      "subscriber_number": "3730189502",
      "provider_npi": "1497775530",
      "provider_fees": "100.00",
      "allowed_fees": "100.00",
      "member_coinsurance": "0.00",
      "member_copay": "0.00"
    }
  ]
}
```

## Features

- Clean API with snake_case field names
- Automatically normalizes messy CSV-style field names on ingest
- CSV converter for messy data
- Count-Min Sketch for efficient top providers (54KB fixed memory)
- Net fee calculation
- Rate limiting (10 req/min)
- Docker support with PostgreSQL

## Testing

```bash
uv run pytest
```