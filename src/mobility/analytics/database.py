"""ClickHouse analytics database connection."""

from functools import lru_cache

import clickhouse_connect
from clickhouse_connect.driver import Client

from mobility.analytics.config import ClickHouseConfig


@lru_cache(maxsize=1)
def get_analytics_client() -> Client:
    """Create and cache a ClickHouse client."""
    return clickhouse_connect.get_client(**ClickHouseConfig.get_connection_config())


def close_analytics_client() -> None:
    """Close the cached ClickHouse client."""
    if get_analytics_client.cache_info().currsize:
        get_analytics_client().close()
        get_analytics_client.cache_clear()
