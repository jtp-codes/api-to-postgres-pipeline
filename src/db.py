import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from src.models import WeatherRecord

DEFAULT_WATERMARK = datetime(1970, 1, 1, 0, 0, 0)


def get_connection():
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://de_user:de_password@localhost:5432/telemetry_dw",
    )
    return psycopg2.connect(dsn)


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_watermarks (
                pipeline_name VARCHAR(100) PRIMARY KEY,
                last_processed_ts TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS hourly_weather (
                city VARCHAR(64) NOT NULL,
                recorded_at TIMESTAMP NOT NULL,
                temperature_c DOUBLE PRECISION NOT NULL,
                windspeed_kmh DOUBLE PRECISION NOT NULL,
                ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (city, recorded_at)
            );
            """
        )
    conn.commit()


def get_watermark(conn, pipeline_name: str) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_ts FROM etl_watermarks WHERE pipeline_name = %s;",
            (pipeline_name,),
        )
        row = cur.fetchone()
        return row[0] if row else DEFAULT_WATERMARK


def upsert_weather_records(
    conn, pipeline_name: str, records: list[WeatherRecord]
) -> int:
    if not records:
        return 0

    # Deduplicate in-memory batch on composite primary key (city, recorded_at)
    deduped = {(r.city, r.recorded_at): r for r in records}
    rows = [
        (r.city, r.recorded_at, r.temperature_c, r.windspeed_kmh)
        for r in deduped.values()
    ]
    max_ts = max(r.recorded_at for r in deduped.values())

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO hourly_weather (city, recorded_at, temperature_c, windspeed_kmh)
            VALUES %s
            ON CONFLICT (city, recorded_at) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                windspeed_kmh = EXCLUDED.windspeed_kmh,
                ingested_at = NOW();
            """,
            rows,
        )
        cur.execute(
            """
            INSERT INTO etl_watermarks (pipeline_name, last_processed_ts, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (pipeline_name) DO UPDATE SET
                last_processed_ts = GREATEST(etl_watermarks.last_processed_ts, EXCLUDED.last_processed_ts),
                updated_at = NOW();
            """,
            (pipeline_name, max_ts),
        )
    conn.commit()
    return len(rows)
