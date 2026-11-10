"""Unit tests for vehicle event processing."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus
from mobility.processing.event_processor import (
    ProcessingResult,
    VehicleEventProcessor,
)


@pytest.fixture
def processor() -> VehicleEventProcessor:
    """Return a fresh vehicle event processor."""
    return VehicleEventProcessor()


@pytest.fixture
def base_event() -> VehicleEvent:
    """Return a valid vehicle event."""
    now = datetime.now(UTC)

    return VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS-001",
        route_id="R1",
        trip_id="TRIP-001",
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="unit-test",
    )


def test_first_event_is_accepted(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    result = processor.process(base_event)

    assert result == ProcessingResult.ACCEPTED


def test_duplicate_event_is_rejected(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    result = processor.process(base_event)

    assert result == ProcessingResult.DUPLICATE


def test_newer_event_for_same_vehicle_is_accepted(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    newer_event = base_event.model_copy(
        update={
            "event_id": uuid4(),
            "event_time": base_event.event_time + timedelta(seconds=10),
        }
    )

    result = processor.process(newer_event)

    assert result == ProcessingResult.ACCEPTED


def test_older_event_for_same_vehicle_is_out_of_order(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    older_event = base_event.model_copy(
        update={
            "event_id": uuid4(),
            "event_time": base_event.event_time - timedelta(seconds=10),
        }
    )

    result = processor.process(older_event)

    assert result == ProcessingResult.OUT_OF_ORDER


def test_out_of_order_event_is_marked_as_processed(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    older_event = base_event.model_copy(
        update={
            "event_id": uuid4(),
            "event_time": base_event.event_time - timedelta(seconds=10),
        }
    )

    processor.process(older_event)

    result = processor.process(older_event)

    assert result == ProcessingResult.DUPLICATE


def test_same_timestamp_with_different_event_id_is_accepted(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    same_time_event = base_event.model_copy(update={"event_id": uuid4()})

    result = processor.process(same_time_event)

    assert result == ProcessingResult.ACCEPTED


def test_event_ordering_is_independent_per_vehicle(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    other_vehicle_event = base_event.model_copy(
        update={
            "event_id": uuid4(),
            "vehicle_id": "BUS-002",
            "event_time": base_event.event_time - timedelta(minutes=5),
        }
    )

    result = processor.process(other_vehicle_event)

    assert result == ProcessingResult.ACCEPTED


def test_latest_event_time_is_updated(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    assert processor.get_latest_event_time("BUS-001") == base_event.event_time


def test_latest_event_time_is_not_updated_by_out_of_order_event(
    processor: VehicleEventProcessor,
    base_event: VehicleEvent,
) -> None:
    processor.process(base_event)

    older_event = base_event.model_copy(
        update={
            "event_id": uuid4(),
            "event_time": base_event.event_time - timedelta(seconds=10),
        }
    )

    processor.process(older_event)

    assert processor.get_latest_event_time("BUS-001") == base_event.event_time


def test_latest_event_time_is_none_for_unknown_vehicle(
    processor: VehicleEventProcessor,
) -> None:
    assert processor.get_latest_event_time("UNKNOWN") is None
