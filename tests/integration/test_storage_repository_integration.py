"""Integration tests for PostgreSQL vehicle event persistence."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from mobility.ingestion.simulator.config import VehicleConfig
from mobility.ingestion.simulator.event_factory import create_vehicle_event
from mobility.ingestion.simulator.movement import create_vehicle_state
from mobility.ingestion.simulator.route import GeoPoint, Route
from mobility.storage.database import get_database_engine
from mobility.storage.models import VehicleEventRecord
from mobility.storage.repository import save_event


@pytest.mark.integration
def test_save_event_persists_event_and_prevents_duplicates() -> None:
    """Persist a vehicle event, verify its location, and reject duplicates."""
    route_id = f"ROUTE_{uuid4().hex}"
    vehicle_id = f"BUS_{uuid4().hex}"

    route = Route(
        route_id=route_id,
        waypoints=(
            GeoPoint(latitude=51.3397, longitude=12.3731),
            GeoPoint(latitude=51.3400, longitude=12.3740),
        ),
    )

    config = VehicleConfig(
        vehicle_id=vehicle_id,
        route_id=route_id,
    )

    state = create_vehicle_state(config, route)
    now = datetime.now(UTC)

    event = create_vehicle_event(
        state,
        event_time=now,
        produced_at=now,
    )

    try:
        assert save_event(event) is True
        assert save_event(event) is False

        with Session(get_database_engine()) as session:
            record = session.execute(
                select(
                    VehicleEventRecord.vehicle_id,
                    VehicleEventRecord.route_id,
                    VehicleEventRecord.latitude,
                    VehicleEventRecord.longitude,
                    func.ST_AsText(VehicleEventRecord.location),
                ).where(VehicleEventRecord.event_id == event.event_id)
            ).one()

        assert record.vehicle_id == vehicle_id
        assert record.route_id == route_id
        assert record.latitude == 51.3397
        assert record.longitude == 12.3731
        assert record[4] == "POINT(12.3731 51.3397)"

    finally:
        with Session(get_database_engine()) as session:
            session.execute(
                delete(VehicleEventRecord).where(VehicleEventRecord.event_id == event.event_id)
            )
            session.commit()
