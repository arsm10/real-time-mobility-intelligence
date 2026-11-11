from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from geoalchemy2.elements import WKTElement

from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus
from mobility.storage.repository import save_event


def create_test_event() -> VehicleEvent:
    """Create a valid vehicle event for repository tests."""
    now = datetime.now(UTC)

    return VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS_TEST_001",
        route_id="ROUTE_TEST_001",
        trip_id=None,
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="synthetic",
    )


@patch("mobility.storage.repository.get_database_engine")
@patch("mobility.storage.repository.Session")
def test_save_event_returns_true_when_inserted(
    mock_session_class: MagicMock,
    mock_get_database_engine: MagicMock,
) -> None:
    """Return True when PostgreSQL inserts a new event."""
    event = create_test_event()

    mock_session = MagicMock()
    mock_session_class.return_value.__enter__.return_value = mock_session
    mock_session.execute.return_value.scalar_one_or_none.return_value = event.event_id

    result = save_event(event)

    assert result is True
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("mobility.storage.repository.get_database_engine")
@patch("mobility.storage.repository.Session")
def test_save_event_returns_false_for_duplicate(
    mock_session_class: MagicMock,
    mock_get_database_engine: MagicMock,
) -> None:
    """Return False when the event already exists."""
    event = create_test_event()

    mock_session = MagicMock()
    mock_session_class.return_value.__enter__.return_value = mock_session
    mock_session.execute.return_value.scalar_one_or_none.return_value = None

    result = save_event(event)

    assert result is False
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("mobility.storage.repository.get_database_engine")
@patch("mobility.storage.repository.Session")
def test_save_event_builds_postgis_location(
    mock_session_class: MagicMock,
    mock_get_database_engine: MagicMock,
) -> None:
    """Include a correctly ordered PostGIS point in the insert statement."""
    event = create_test_event()

    mock_session = MagicMock()
    mock_session_class.return_value.__enter__.return_value = mock_session
    mock_session.execute.return_value.scalar_one_or_none.return_value = event.event_id

    save_event(event)

    statement = mock_session.execute.call_args.args[0]
    location = statement.compile().params

    assert any(isinstance(value, WKTElement) for value in location.values())
