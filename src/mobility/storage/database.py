from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from mobility.storage.config import DatabaseConfig
from mobility.storage.models import Base


@lru_cache(maxsize=1)
def get_database_engine() -> Engine:
    """Create and cache a SQLAlchemy engine for PostgreSQL."""
    database_url = DatabaseConfig.get_database_url()

    return create_engine(
        database_url,
        pool_pre_ping=True,
        echo=False,
    )


def initialize_database() -> None:
    """Create all registered database tables."""
    Base.metadata.create_all(bind=get_database_engine())
