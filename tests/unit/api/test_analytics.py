"""Unit tests for analytics API endpoints."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mobility.api.routes.analytics import router

app = FastAPI()
app.include_router(router)

client = TestClient(app)


@patch("mobility.api.routes.analytics.get_average_speed_by_route")
def test_average_speed_by_route(mock_get_data: MagicMock) -> None:
    mock_get_data.return_value = [("route-1", 32.5), ("route-2", 28.0)]

    response = client.get("/api/v1/analytics/routes/average-speed")

    assert response.status_code == 200
    assert response.json() == [
        {"route_id": "route-1", "average_speed_kmh": 32.5},
        {"route_id": "route-2", "average_speed_kmh": 28.0},
    ]


@patch("mobility.api.routes.analytics.get_hourly_event_counts")
def test_hourly_event_counts(mock_get_data: MagicMock) -> None:
    event_hour = datetime(2026, 11, 11, 10, tzinfo=UTC)
    mock_get_data.return_value = [(event_hour, 25)]

    response = client.get("/api/v1/analytics/events/hourly")

    assert response.status_code == 200
    assert response.json() == [{"event_hour": "2026-11-11T10:00:00Z", "event_count": 25}]


@patch("mobility.api.routes.analytics.get_vehicle_events")
def test_vehicle_events(mock_get_data: MagicMock) -> None:
    event_id = uuid4()
    event_time = datetime(2026, 11, 11, 10, tzinfo=UTC)
    produced_at = datetime(2026, 11, 11, 10, 1, tzinfo=UTC)

    mock_get_data.return_value = [
        (
            event_id,
            "1.0",
            "vehicle.position",
            "vehicle-1",
            "route-1",
            None,
            event_time,
            produced_at,
            51.5,
            -0.12,
            32.5,
            180.0,
            "moving",
            "simulator",
        )
    ]

    response = client.get("/api/v1/analytics/vehicles/vehicle-1/events?limit=50&offset=10")

    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["event_id"] == str(event_id)
    assert response.json()[0]["vehicle_id"] == "vehicle-1"
    assert response.json()[0]["trip_id"] is None

    mock_get_data.assert_called_once_with(
        vehicle_id="vehicle-1",
        limit=50,
        offset=10,
    )


@patch("mobility.api.routes.analytics.get_vehicle_events")
def test_vehicle_events_returns_empty_list(mock_get_data: MagicMock) -> None:
    mock_get_data.return_value = []

    response = client.get("/api/v1/analytics/vehicles/vehicle-1/events")

    assert response.status_code == 200
    assert response.json() == []


def test_vehicle_events_rejects_invalid_pagination() -> None:
    invalid_urls = [
        "/api/v1/analytics/vehicles/vehicle-1/events?limit=0",
        "/api/v1/analytics/vehicles/vehicle-1/events?limit=1001",
        "/api/v1/analytics/vehicles/vehicle-1/events?offset=-1",
    ]

    for url in invalid_urls:
        response = client.get(url)
        assert response.status_code == 422


@patch("mobility.api.routes.analytics.get_vehicle_events")
def test_vehicle_events_returns_500_when_repository_fails(
    mock_get_data: MagicMock,
) -> None:
    mock_get_data.side_effect = Exception("ClickHouse connection failed")

    response = client.get("/api/v1/analytics/vehicles/vehicle-1/events")

    assert response.status_code == 500
    assert response.json() == {"detail": "Unable to retrieve vehicle events."}
