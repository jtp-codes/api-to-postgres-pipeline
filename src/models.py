from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class WeatherRecord(BaseModel):
    city: str = Field(..., min_length=1)
    recorded_at: datetime
    temperature_c: float = Field(..., ge=-90.0, le=65.0)
    windspeed_kmh: float = Field(..., ge=0.0, le=450.0)

    @field_validator("recorded_at", mode="before")
    @classmethod
    def parse_iso_timestamp(cls, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value)
