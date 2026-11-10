"""Unit tests for the synthetic vehicle event factory."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from mobility.ingestion.schemas.vehicle_event import VehicleStatus
from mobility.ingestion.simulator.config import VehicleConfig
from mobility.ingestion.simulator.event_factory import create_vehicle_event
from mobility.ingestion.simulator.movement import create_vehicle_state
from mobility.ingestion.simulator.route import GeoPoint, Route


@pytest.fixture
def vehicle_state():
    """Create a deterministic simulated vehicle state."""
    config = VehicleConfig(
        vehicle_id="BUS-001",
        route_id="ROUTE-10",
        initial_speed_kmh=36.0,
    )

    route = Route(
        route_id="ROUTE-10",
        waypoints=(
            GeoPoint(latitude=51.3397, longitude=12.3731),
            GeoPoint(latitude=51.3400, longitude=12.3800),
        ),
    )

    return create_vehicle_state(config, route)


@pytest.fixture
def timestamps():
    """Provide deterministic UTC timestamps."""
    return {
        "event_time": datetime(2026, 11, 10, 20, 0, tzinfo=UTC),
        "produced_at": datetime(2026, 11, 10, 20, 0, 1, tzinfo=UTC),
    }


def test_create_vehicle_event_maps_vehicle_state(vehicle_state, timestamps):
    """Verify that simulator state is correctly mapped to a domain event."""
    event = create_vehicle_event(vehicle_state, **timestamps)

    assert isinstance(event.event_id, UUID)
    assert event.schema_version == "1.0"
    assert event.event_type == "vehicle.position.updated"

    assert event.vehicle_id == "BUS-001"
    assert event.route_id == "ROUTE-10"
    assert event.trip_id is None

    assert event.event_time == timestamps["event_time"]
    assert event.produced_at == timestamps["produced_at"]

    assert event.latitude == vehicle_state.position.latitude
    assert event.longitude == vehicle_state.position.longitude
    assert event.speed_kmh == vehicle_state.speed_kmh
    assert event.heading_deg == vehicle_state.heading_deg

    assert event.status == VehicleStatus.IN_SERVICE
    assert event.source == "synthetic"


def test_create_vehicle_event_generates_unique_event_ids(
    vehicle_state,
    timestamps,
):
    """Verify that every generated event receives a unique UUID."""
    first_event = create_vehicle_event(vehicle_state, **timestamps)
    second_event = create_vehicle_event(vehicle_state, **timestamps)

    assert first_event.event_id != second_event.event_id


def test_create_vehicle_event_accepts_trip_id(vehicle_state, timestamps):
    """Verify that an optional trip ID is included in the event."""
    event = create_vehicle_event(
        vehicle_state,
        **timestamps,
        trip_id="TRIP-100",
    )

    assert event.trip_id == "TRIP-100"


def test_create_vehicle_event_accepts_custom_source(
    vehicle_state,
    timestamps,
):
    """Verify that the event source can be configured."""
    event = create_vehicle_event(
        vehicle_state,
        **timestamps,
        source="local-simulator",
    )

    assert event.source == "local-simulator"


@pytest.mark.parametrize("timestamp_field", ["event_time", "produced_at"])
def test_create_vehicle_event_rejects_naive_timestamps(
    vehicle_state,
    timestamps,
    timestamp_field,
):
    """Verify that timestamps without timezone information are rejected."""
    invalid_timestamps = timestamps.copy()
    invalid_timestamps[timestamp_field] = datetime(2026, 11, 10, 20, 0)

    with pytest.raises(ValidationError):
        create_vehicle_event(vehicle_state, **invalid_timestamps)


def test_create_vehicle_event_rejects_empty_source(vehicle_state, timestamps):
    """Verify that an empty source is rejected by event validation."""
    with pytest.raises(ValidationError):
        create_vehicle_event(
            vehicle_state,
            **timestamps,
            source="   ",
        )


def test_create_vehicle_event_normalizes_timestamps_to_utc(
    vehicle_state,
    timestamps,
):
    """Verify that timezone-aware timestamps are normalized to UTC."""
    event_time = datetime(
        2026,
        11,
        10,
        21,
        0,
        tzinfo=timezone(timedelta(hours=1)),
    )

    event = create_vehicle_event(
        vehicle_state,
        event_time=event_time,
        produced_at=timestamps["produced_at"],
    )

    assert event.event_time == datetime(2026, 11, 10, 20, 0, tzinfo=UTC)
    assert event.event_time.utcoffset() == timedelta(0)
