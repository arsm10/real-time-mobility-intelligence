"""Repository for persisting vehicle events in PostgreSQL."""

from geoalchemy2.elements import WKTElement
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from mobility.ingestion.schemas.vehicle_event import VehicleEvent
from mobility.storage.database import get_database_engine
from mobility.storage.models import VehicleEventRecord


def save_event(event: VehicleEvent) -> bool:
    """Persist a vehicle event, ignoring duplicate event IDs."""

    statement = (
        insert(VehicleEventRecord)
        .values(
            event_id=event.event_id,
            schema_version=event.schema_version,
            event_type=event.event_type,
            vehicle_id=event.vehicle_id,
            route_id=event.route_id,
            trip_id=event.trip_id,
            event_time=event.event_time,
            produced_at=event.produced_at,
            latitude=event.latitude,
            longitude=event.longitude,
            location=WKTElement(
                f"POINT({event.longitude} {event.latitude})",
                srid=4326,
            ),
            speed_kmh=event.speed_kmh,
            heading_deg=event.heading_deg,
            status=event.status.value,
            source=event.source,
        )
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(VehicleEventRecord.event_id)
    )

    with Session(get_database_engine()) as session:
        inserted_event_id = session.execute(statement).scalar_one_or_none()
        session.commit()

    return inserted_event_id is not None
