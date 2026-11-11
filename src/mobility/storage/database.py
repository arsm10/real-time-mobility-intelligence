from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from mobility.storage.config import DatabaseConfig
from mobility.storage.models import Base


def create_database_engine() -> Engine:
    """Create a SQLAlchemy engine for PostgreSQL."""
    database_url = DatabaseConfig.get_database_url()

    return create_engine(
        database_url,
        pool_pre_ping=True,
        echo=False,
    )


engine = create_database_engine()


def initialize_database() -> None:
    """Create all registered database tables."""
    Base.metadata.create_all(bind=engine)
