"""Integration tests for ClickHouse vehicle event analytics."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from mobility.analytics.database import get_analytics_client
from mobility.analytics.repository import (
    get_average_speed_by_route,
    get_hourly_event_counts,
    get_vehicle_event_count,
    save_event,
)
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus


@pytest.mark.integration
def test_save_event_and_query_analytics() -> None:
    """Persist a vehicle event and verify ClickHouse analytics queries."""
    event_id = uuid4()
    route_id = f"ROUTE_{uuid4().hex}"
    vehicle_id = f"BUS_{uuid4().hex}"
    now = datetime.now(UTC)

    event = VehicleEvent(
        event_id=event_id,
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id=vehicle_id,
        route_id=route_id,
        trip_id=None,
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="synthetic",
    )

    client = get_analytics_client()

    try:
        save_event(event)

        assert get_vehicle_event_count(vehicle_id) == 1

        route_results = get_average_speed_by_route()
        route_result = next(row for row in route_results if row[0] == route_id)
        assert route_result[1] == 30.0

        hourly_results = get_hourly_event_counts()
        expected_hour = now.replace(
            minute=0,
            second=0,
            microsecond=0,
        ).replace(tzinfo=None)

        assert any(
            event_hour == expected_hour and event_count >= 1
            for event_hour, event_count in hourly_results
        )

    finally:
        client.command(
            """
            ALTER TABLE vehicle_events
            DELETE WHERE event_id = {event_id:UUID}
            SETTINGS mutations_sync = 1
            """,
            parameters={"event_id": str(event_id)},
        )
