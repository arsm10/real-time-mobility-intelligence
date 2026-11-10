"""Factory for converting simulated vehicle states into domain events."""

from datetime import datetime
from uuid import uuid4

from mobility.ingestion.schemas.vehicle_event import VehicleEvent
from mobility.ingestion.simulator.movement import VehicleState


def create_vehicle_event(
    state: VehicleState,
    *,
    event_time: datetime,
    produced_at: datetime,
    source: str = "synthetic",
    trip_id: str | None = None,
) -> VehicleEvent:
    """Create a validated vehicle position event from the current state."""
    return VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id=state.vehicle_id,
        route_id=state.route.route_id,
        trip_id=trip_id,
        event_time=event_time,
        produced_at=produced_at,
        latitude=state.position.latitude,
        longitude=state.position.longitude,
        speed_kmh=state.speed_kmh,
        heading_deg=state.heading_deg,
        status=state.status,
        source=source,
    )
