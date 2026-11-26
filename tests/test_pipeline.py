from datetime import datetime
from unittest.mock import patch
from src.db import get_connection, init_schema, get_watermark, upsert_weather_records
from src.models import WeatherRecord
from src.pipeline import run_pipeline, PIPELINE_NAME


MOCK_PAYLOAD = {
    "hourly": {
        "time": ["2026-11-25T00:00", "2026-11-25T01:00", "2026-11-25T02:00"],
        "temperature_2m": [28.5, 28.1, 999.0],  # 3rd record violates <= 65.0C constraint
        "wind_speed_10m": [12.0, 11.5, 10.0],
    }
}


@patch("src.pipeline.fetch_raw_weather", return_value=MOCK_PAYLOAD)
def test_idempotency_watermarking_and_quarantine(mock_fetch):
    conn = get_connection()
    init_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE hourly_weather, etl_watermarks;")
    conn.commit()

    # Run 1: Should ingest 2 valid records and quarantine the 999.0C outlier
    inserted_run_1 = run_pipeline()
    assert inserted_run_1 == 2
    assert get_watermark(conn, PIPELINE_NAME) == datetime(2026, 11, 25, 1, 0)

    # Run 2: Re-running identical payload must insert 0 rows (watermark blocks reprocessing)
    inserted_run_2 = run_pipeline()
    assert inserted_run_2 == 0

    # Direct Upsert Deduplication Check: Re-inserting duplicate composite key updates in-place without row duplication
    dup_batch = [
        WeatherRecord(
            city="Mumbai",
            recorded_at=datetime(2026, 11, 25, 1, 0),
            temperature_c=29.0,
            windspeed_kmh=14.0,
        )
    ]
    upsert_weather_records(conn, PIPELINE_NAME, dup_batch)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MAX(temperature_c) FROM hourly_weather;")
        count, updated_temp = cur.fetchone()
        assert count == 2
        assert updated_temp == 29.0

    conn.close()
