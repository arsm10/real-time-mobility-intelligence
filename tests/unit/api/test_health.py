from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mobility.api.routes.health import router

app = FastAPI()
app.include_router(router)

client = TestClient(app)


def test_health_check_returns_healthy() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


@patch("mobility.api.routes.health.get_analytics_client")
def test_readiness_check_returns_ready(mock_get_client: MagicMock) -> None:
    mock_get_client.return_value.ping.return_value = True

    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    mock_get_client.return_value.ping.assert_called_once()


@patch("mobility.api.routes.health.get_analytics_client")
def test_readiness_check_returns_503_when_ping_fails(
    mock_get_client: MagicMock,
) -> None:
    mock_get_client.return_value.ping.return_value = False

    response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": "ClickHouse is unavailable."}


@patch("mobility.api.routes.health.get_analytics_client")
def test_readiness_check_returns_503_when_connection_raises(
    mock_get_client: MagicMock,
) -> None:
    mock_get_client.side_effect = Exception("Connection failed")

    response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"detail": "ClickHouse is unavailable."}
