# Idempotent REST API-to-PostgreSQL Ingestion Pipeline

Production-ready incremental data ingestion pipeline extracting hourly telemetry from Open-Meteo into PostgreSQL with Pydantic schema validation, exponential retry backoff, composite primary-key deduplication, and stateful watermarking.

## Core Architecture
- **Watermarking (`etl_watermarks`)**: Tracks `last_processed_ts` per pipeline to guarantee incremental-only extraction and idempotent re-runs.
- **Schema Validation (`src/models.py`)**: Enforces strict domain bounds (`temperature_c` between `-90.0` and `65.0`, `windspeed_kmh` between `0.0` and `450.0`) via Pydantic v2, quarantining malformed records without halting batch execution.
- **Idempotent Upserts (`src/db.py`)**: Uses PostgreSQL `ON CONFLICT (city, recorded_at) DO UPDATE` combined with `GREATEST()` watermark progression.
- **Fault Tolerance (`src/pipeline.py`)**: Implements `tenacity` exponential backoff (`2s` to `16s`, max 4 attempts) on transient HTTP/network errors.
- **CI Verification (`.github/workflows/ci.yml`)**: Runs `pytest` unit/integration tests and two consecutive live API passes against a PostgreSQL 16 service container.

## Local Execution (Optional)
```bash
docker compose up --build --abort-on-container-exit
```
