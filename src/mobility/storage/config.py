import os

from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """PostgreSQL database configuration."""

    DB_NAME = os.getenv("POSTGRES_DB", "mobility")
    DB_USER = os.getenv("POSTGRES_USER", "mobility")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

    @classmethod
    def get_database_url(cls) -> str:
        """Build the SQLAlchemy database connection URL."""
        if not cls.DB_PASSWORD:
            raise ValueError("POSTGRES_PASSWORD is not configured.")

        return (
            f"postgresql+psycopg://{cls.DB_USER}:{cls.DB_PASSWORD}"
            f"@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        )
