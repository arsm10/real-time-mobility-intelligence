"""Pydantic response schemas for analytics endpoints."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class AverageSpeedResponse(BaseModel):
    model_config = ConfigDict(strict=True)

    route_id: str
    average_speed_kmh: float


class HourlyEventsResponse(BaseModel):
    model_config = ConfigDict(strict=True)

    event_hour: datetime
    event_count: int = Field(ge=0)


class VehicleEventCountResponse(BaseModel):
    model_config = ConfigDict(strict=True)

    vehicle_id: str
    event_count: int = Field(ge=0)


class VehicleEventResponse(BaseModel):
    model_config = ConfigDict(strict=True)

    event_id: UUID
    schema_version: str
    event_type: str
    vehicle_id: str
    route_id: str
    trip_id: str | None
    event_time: datetime
    produced_at: datetime
    latitude: float
    longitude: float
    speed_kmh: float
    heading_deg: float
    status: str
    source: str
