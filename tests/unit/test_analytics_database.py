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


def test_clickhouse_config_returns_connection_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return configured ClickHouse connection settings."""
    monkeypatch.setattr(ClickHouseConfig, "PASSWORD", "test-password")
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
def test_get_analytics_client_caches_connection(
    mock_get_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reuse the cached ClickHouse client."""
    monkeypatch.setattr(ClickHouseConfig, "PASSWORD", "test-password")
    get_analytics_client.cache_clear()

    try:
        first_client = get_analytics_client()
        second_client = get_analytics_client()

        assert first_client is second_client
        mock_get_client.assert_called_once()
    finally:
        get_analytics_client.cache_clear()


@patch("mobility.analytics.database.clickhouse_connect.get_client")
def test_close_analytics_client(
    mock_get_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Close the cached ClickHouse client and clear the cache."""
    monkeypatch.setattr(ClickHouseConfig, "PASSWORD", "test-password")
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
def test_initialize_analytics_schema_with_replacing_merge_tree(
    mock_get_client: MagicMock,
) -> None:
    """Create the table and accept the expected engine."""
    client = mock_get_client.return_value
    client.database = "mobility_analytics"
    client.query.return_value.result_rows = [("ReplacingMergeTree",)]

    initialize_analytics_schema()

    client.command.assert_called_once_with(CREATE_VEHICLE_EVENTS_TABLE)
    client.query.assert_called_once()

    query_call = client.query.call_args
    assert "FROM system.tables" in query_call.args[0]
    assert query_call.kwargs["parameters"] == {"database": "mobility_analytics"}


@patch("mobility.analytics.schema.get_analytics_client")
def test_initialize_analytics_schema_rejects_wrong_engine(
    mock_get_client: MagicMock,
) -> None:
    """Reject an existing table that uses a different engine."""
    client = mock_get_client.return_value
    client.database = "mobility_analytics"
    client.query.return_value.result_rows = [("MergeTree",)]

    with pytest.raises(RuntimeError, match="must use ReplacingMergeTree"):
        initialize_analytics_schema()

    client.command.assert_called_once_with(CREATE_VEHICLE_EVENTS_TABLE)


@patch("mobility.analytics.schema.get_analytics_client")
def test_initialize_analytics_schema_rejects_missing_table(
    mock_get_client: MagicMock,
) -> None:
    """Reject initialization when the table is not found."""
    client = mock_get_client.return_value
    client.database = "mobility_analytics"
    client.query.return_value.result_rows = []

    with pytest.raises(RuntimeError, match="was not found"):
        initialize_analytics_schema()
