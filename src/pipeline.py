import logging
import requests
from pydantic import ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.models import WeatherRecord
from src.db import get_connection, init_schema, get_watermark, upsert_weather_records

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

PIPELINE_NAME = "open_meteo_mumbai_hourly"
API_URL = "https://api.open-meteo.com/v1/forecast"


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=16),
    retry=retry_if_exception_type((requests.RequestException,)),
    reraise=True,
)
def fetch_raw_weather(lat: float = 19.0760, lon: float = 72.8777) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_10m",
        "past_days": 2,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def parse_and_filter(payload: dict, city: str, watermark) -> list[WeatherRecord]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])

    valid_records: list[WeatherRecord] = []
    for ts_raw, temp_raw, wind_raw in zip(times, temps, winds):
        if temp_raw is None or wind_raw is None:
            continue
        try:
            record = WeatherRecord(
                city=city,
                recorded_at=ts_raw,
                temperature_c=temp_raw,
                windspeed_kmh=wind_raw,
            )
            if record.recorded_at > watermark:
                valid_records.append(record)
        except ValidationError as exc:
            logger.warning("Quarantined malformed record (%s): %s", ts_raw, exc)

    return valid_records


def run_pipeline() -> int:
    conn = get_connection()
    try:
        init_schema(conn)
        watermark = get_watermark(conn, PIPELINE_NAME)
        logger.info("Current watermark for %s: %s", PIPELINE_NAME, watermark.isoformat())

        payload = fetch_raw_weather()
        records = parse_and_filter(payload, city="Mumbai", watermark=watermark)
        inserted = upsert_weather_records(conn, PIPELINE_NAME, records)

        logger.info("Upserted %d new records.", inserted)
        return inserted
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()
