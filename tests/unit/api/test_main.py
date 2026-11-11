"""Tests for the FastAPI application entry point."""

from fastapi.testclient import TestClient

from mobility.api.main import app

client = TestClient(app)


def test_application_registers_health_routes() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_application_registers_analytics_routes() -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]

    assert "/api/v1/analytics/routes/average-speed" in paths
    assert "/api/v1/analytics/events/hourly" in paths
    assert "/api/v1/analytics/vehicles/{vehicle_id}/events" in paths
