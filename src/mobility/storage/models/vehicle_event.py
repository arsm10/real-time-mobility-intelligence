from datetime import datetime
from uuid import UUID

from geoalchemy2 import Geography
from sqlalchemy import DateTime, Float, Index, String
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from mobility.storage.models.base import Base


class VehicleEventRecord(Base):
    """SQLAlchemy model for persisted vehicle events."""

    __tablename__ = "vehicle_events"

    __table_args__ = (
        Index(
            "ix_vehicle_events_location",
            "location",
            postgresql_using="gist",
        ),
    )

    event_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True),
        primary_key=True,
    )

    schema_version: Mapped[str] = mapped_column(String(10), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)

    vehicle_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    route_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    trip_id: Mapped[str | None] = mapped_column(String, nullable=True)

    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    produced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)

    location = mapped_column(
        Geography(geometry_type="POINT", srid=4326, spatial_index=False),
        nullable=False,
    )

    speed_kmh: Mapped[float] = mapped_column(Float, nullable=False)
    heading_deg: Mapped[float] = mapped_column(Float, nullable=False)

    status: Mapped[str] = mapped_column(String(20), nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
