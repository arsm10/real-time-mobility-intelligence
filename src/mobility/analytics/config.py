"""ClickHouse analytics database configuration."""

import os

from dotenv import load_dotenv

load_dotenv()


class ClickHouseConfig:
    """ClickHouse connection configuration."""

    HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    USER = os.getenv("CLICKHOUSE_USER", "mobility")
    PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
    DATABASE = os.getenv("CLICKHOUSE_DATABASE", "mobility_analytics")

    @classmethod
    def get_connection_config(cls) -> dict[str, str | int]:
        """Return ClickHouse connection settings."""
        if not cls.PASSWORD:
            raise ValueError("CLICKHOUSE_PASSWORD is not configured.")

        return {
            "host": cls.HOST,
            "port": cls.PORT,
            "username": cls.USER,
            "password": cls.PASSWORD,
            "database": cls.DATABASE,
        }
