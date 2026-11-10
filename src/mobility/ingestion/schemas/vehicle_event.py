"""Pydantic model for vehicle position events."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class VehicleStatus(StrEnum):
    """Supported operational states of a vehicle."""

    IN_SERVICE = "in_service"
    OUT_OF_SERVICE = "out_of_service"
    STOPPED = "stopped"


class VehicleEvent(BaseModel):
    """Validated vehicle position and operational status event."""

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
    )

    event_id: UUID
    schema_version: Literal["1.0"]
    event_type: Literal["vehicle.position.updated"]

    vehicle_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    trip_id: str | None = None

    event_time: datetime
    produced_at: datetime

    latitude: float = Field(ge=-90, le=90, allow_inf_nan=False)
    longitude: float = Field(ge=-180, le=180, allow_inf_nan=False)
    speed_kmh: float = Field(ge=0, allow_inf_nan=False)
    heading_deg: float = Field(ge=0, lt=360, allow_inf_nan=False)

    status: VehicleStatus
    source: str = Field(min_length=1)

    @field_validator(
        "vehicle_id",
        "route_id",
        "source",
        "trip_id",
        mode="before",
    )
    @classmethod
    def validate_non_empty_strings(cls, value: object) -> object:
        """Reject empty or whitespace-only string values."""
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("Value must not be empty or whitespace-only")

        return value

    @field_validator("event_time", "produced_at")
    @classmethod
    def normalize_timestamp_to_utc(cls, value: datetime) -> datetime:
        """Require timezone-aware timestamps and normalize them to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Timestamp must include timezone information")

        return value.astimezone(UTC)
