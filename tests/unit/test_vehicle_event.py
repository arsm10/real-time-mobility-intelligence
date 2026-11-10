"""Unit tests for the VehicleEvent model."""

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from mobility.ingestion.schemas.vehicle_event import (
    VehicleEvent,
    VehicleStatus,
)


@pytest.fixture
def valid_event_data() -> dict:
    """Return a valid vehicle event payload."""
    return {
        "event_id": str(uuid4()),
        "schema_version": "1.0",
        "event_type": "vehicle.position.updated",
        "vehicle_id": "TRAM_001",
        "route_id": "LINE_03",
        "trip_id": "TRIP_001",
        "event_time": "2026-11-10T10:30:15.000Z",
        "produced_at": "2026-11-10T10:30:15.025Z",
        "latitude": 51.3397,
        "longitude": 12.3731,
        "speed_kmh": 32.5,
        "heading_deg": 145.0,
        "status": "in_service",
        "source": "simulator",
    }


def test_valid_event_is_created(valid_event_data: dict) -> None:
    event = VehicleEvent.model_validate(valid_event_data)

    assert event.vehicle_id == "TRAM_001"
    assert event.route_id == "LINE_03"
    assert event.status == VehicleStatus.IN_SERVICE
    assert event.latitude == pytest.approx(51.3397)


def test_event_id_is_parsed_as_uuid(valid_event_data: dict) -> None:
    event = VehicleEvent.model_validate(valid_event_data)

    assert event.event_id == UUID(valid_event_data["event_id"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", -90.1),
        ("latitude", 90.1),
        ("longitude", -180.1),
        ("longitude", 180.1),
        ("speed_kmh", -1),
        ("heading_deg", -1),
        ("heading_deg", 360),
        ("latitude", float("nan")),
        ("longitude", float("inf")),
        ("speed_kmh", float("-inf")),
    ],
)
def test_invalid_numeric_values_are_rejected(
    valid_event_data: dict,
    field: str,
    value: float,
) -> None:
    valid_event_data[field] = value

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("vehicle_id", ""),
        ("vehicle_id", "   "),
        ("route_id", ""),
        ("route_id", "   "),
        ("source", ""),
        ("source", "   "),
        ("trip_id", ""),
        ("trip_id", "   "),
    ],
)
def test_empty_strings_are_rejected(
    valid_event_data: dict,
    field: str,
    value: str,
) -> None:
    valid_event_data[field] = value

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "2.0"),
        ("event_type", "vehicle.location.changed"),
        ("status", "maintenance"),
    ],
)
def test_unsupported_values_are_rejected(
    valid_event_data: dict,
    field: str,
    value: str,
) -> None:
    valid_event_data[field] = value

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


def test_invalid_uuid_is_rejected(valid_event_data: dict) -> None:
    valid_event_data["event_id"] = "not-a-uuid"

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


def test_naive_timestamp_is_rejected(valid_event_data: dict) -> None:
    valid_event_data["event_time"] = datetime(2026, 11, 10, 10, 30)

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


def test_timestamp_is_normalized_to_utc(valid_event_data: dict) -> None:
    offset_timestamp = datetime(2026, 11, 10, 11, 30, tzinfo=timezone(timedelta(hours=1)))
    valid_event_data["event_time"] = offset_timestamp

    event = VehicleEvent.model_validate(valid_event_data)

    assert event.event_time.hour == 10
    assert event.event_time.utcoffset() == timedelta(0)


def test_unknown_fields_are_rejected(valid_event_data: dict) -> None:
    valid_event_data["unexpected_field"] = "unexpected"

    with pytest.raises(ValidationError):
        VehicleEvent.model_validate(valid_event_data)


def test_trip_id_is_optional(valid_event_data: dict) -> None:
    valid_event_data["trip_id"] = None

    event = VehicleEvent.model_validate(valid_event_data)

    assert event.trip_id is None


def test_assignment_validation_is_enabled(valid_event_data: dict) -> None:
    event = VehicleEvent.model_validate(valid_event_data)

    with pytest.raises(ValidationError):
        event.speed_kmh = -5


@pytest.mark.parametrize(
    "field",
    ["vehicle_id", "route_id", "source", "trip_id"],
)
def test_string_fields_are_trimmed(
    valid_event_data: dict,
    field: str,
) -> None:
    valid_event_data[field] = "  TEST_VALUE  "

    event = VehicleEvent.model_validate(valid_event_data)

    assert getattr(event, field) == "TEST_VALUE"
