from unittest.mock import MagicMock, patch

import pytest

from mobility.analytics.config import ClickHouseConfig
from mobility.analytics.database import (
    close_analytics_client,
    get_analytics_client,
)
from mobility.analytics.schema import (
    CREATE_VEHICLE_EVENTS_TABLE,
    initialize_analytics_schema,
)


def test_clickhouse_config_returns_connection_settings() -> None:
    """Return configured ClickHouse connection settings."""
    config = ClickHouseConfig.get_connection_config()

    assert config["host"] == ClickHouseConfig.HOST
    assert config["port"] == ClickHouseConfig.PORT
    assert config["username"] == ClickHouseConfig.USER
    assert config["database"] == ClickHouseConfig.DATABASE
    assert config["password"] == ClickHouseConfig.PASSWORD


def test_clickhouse_config_requires_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise an error when the ClickHouse password is missing."""
    monkeypatch.setattr(ClickHouseConfig, "PASSWORD", None)

    with pytest.raises(ValueError, match="CLICKHOUSE_PASSWORD"):
        ClickHouseConfig.get_connection_config()


@patch("mobility.analytics.database.clickhouse_connect.get_client")
def test_get_analytics_client_caches_connection(mock_get_client: MagicMock) -> None:
    """Reuse the cached ClickHouse client."""
    get_analytics_client.cache_clear()

    try:
        first_client = get_analytics_client()
        second_client = get_analytics_client()

        assert first_client is second_client
        mock_get_client.assert_called_once()
    finally:
        get_analytics_client.cache_clear()


@patch("mobility.analytics.database.clickhouse_connect.get_client")
def test_close_analytics_client(mock_get_client: MagicMock) -> None:
    """Close the cached ClickHouse client and clear the cache."""
    get_analytics_client.cache_clear()

    try:
        client = get_analytics_client()
        close_analytics_client()

        client.close.assert_called_once()
        assert get_analytics_client.cache_info().currsize == 0
    finally:
        get_analytics_client.cache_clear()


def test_close_analytics_client_without_connection() -> None:
    """Allow closing when no client has been created."""
    get_analytics_client.cache_clear()

    close_analytics_client()

    assert get_analytics_client.cache_info().currsize == 0


@patch("mobility.analytics.schema.get_analytics_client")
def test_initialize_analytics_schema(mock_get_client: MagicMock) -> None:
    """Execute the ClickHouse table creation statement."""
    initialize_analytics_schema()

    mock_get_client.return_value.command.assert_called_once_with(CREATE_VEHICLE_EVENTS_TABLE)
