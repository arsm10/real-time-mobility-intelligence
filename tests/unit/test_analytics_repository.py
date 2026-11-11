from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from mobility.analytics.repository import (
    get_average_speed_by_route,
    get_hourly_event_counts,
    get_vehicle_event_count,
    get_vehicle_events,
    save_event,
)
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus


def create_test_event() -> VehicleEvent:
    """Create a valid vehicle event for analytics tests."""
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


@patch("mobility.analytics.repository.get_analytics_client")
def test_save_event_inserts_all_event_fields(
    mock_get_client: MagicMock,
) -> None:
    """Insert all vehicle event fields into ClickHouse."""
    event = create_test_event()
    save_event(event)

    mock_get_client.return_value.insert.assert_called_once()

    call = mock_get_client.return_value.insert.call_args

    assert call.args[0] == "vehicle_events"
    assert call.kwargs["column_names"]
    assert call.args[1][0][0] == event.event_id


@patch("mobility.analytics.repository.get_analytics_client")
def test_get_average_speed_by_route(
    mock_get_client: MagicMock,
) -> None:
    """Return average speed grouped by route."""
    expected = [("ROUTE_TEST_001", 30.0)]
    mock_get_client.return_value.query.return_value.result_rows = expected

    assert get_average_speed_by_route() == expected


@patch("mobility.analytics.repository.get_analytics_client")
def test_get_hourly_event_counts(
    mock_get_client: MagicMock,
) -> None:
    """Return hourly event counts."""
    expected = [(datetime.now(UTC), 5)]
    mock_get_client.return_value.query.return_value.result_rows = expected

    assert get_hourly_event_counts() == expected


@patch("mobility.analytics.repository.get_analytics_client")
def test_get_vehicle_event_count(
    mock_get_client: MagicMock,
) -> None:
    """Return the number of events for a vehicle."""
    mock_get_client.return_value.query.return_value.result_rows = [(12,)]

    result = get_vehicle_event_count("BUS_TEST_001")

    assert result == 12
    mock_get_client.return_value.query.assert_called_once()


@patch("mobility.analytics.repository.get_analytics_client")
def test_get_vehicle_events(
    mock_get_client: MagicMock,
) -> None:
    """Return paginated events for a vehicle."""
    expected = [("event-1", "1.0", "vehicle.position.updated")]
    mock_get_client.return_value.query.return_value.result_rows = expected

    result = get_vehicle_events("BUS_TEST_001", limit=50, offset=10)

    assert result == expected

    call = mock_get_client.return_value.query.call_args
    assert call.kwargs["parameters"] == {
        "vehicle_id": "BUS_TEST_001",
        "limit": 50,
        "offset": 10,
    }
    assert "ORDER BY event_time DESC" in call.args[0]
