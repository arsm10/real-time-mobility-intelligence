from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from mobility.api.schemas.analytics import (
    AverageSpeedResponse,
    HourlyEventsResponse,
    VehicleEventCountResponse,
    VehicleEventResponse,
)


def test_average_speed_response() -> None:
    response = AverageSpeedResponse(
        route_id="ROUTE_001",
        average_speed_kmh=32.5,
    )

    assert response.route_id == "ROUTE_001"
    assert response.average_speed_kmh == 32.5


def test_hourly_events_response() -> None:
    event_hour = datetime.now(UTC)

    response = HourlyEventsResponse(
        event_hour=event_hour,
        event_count=15,
    )

    assert response.event_hour == event_hour
    assert response.event_count == 15


def test_hourly_events_response_rejects_negative_count() -> None:
    with pytest.raises(ValidationError):
        HourlyEventsResponse(
            event_hour=datetime.now(UTC),
            event_count=-1,
        )


def test_vehicle_event_count_response() -> None:
    response = VehicleEventCountResponse(
        vehicle_id="BUS_001",
        event_count=25,
    )

    assert response.event_count == 25


def test_vehicle_event_response() -> None:
    now = datetime.now(UTC)
    event_id = uuid4()

    response = VehicleEventResponse(
        event_id=event_id,
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS_001",
        route_id="ROUTE_001",
        trip_id=None,
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status="IN_SERVICE",
        source="synthetic",
    )

    assert response.event_id == event_id
    assert response.trip_id is None
    assert response.vehicle_id == "BUS_001"
