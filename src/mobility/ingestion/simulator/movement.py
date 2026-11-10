"""Vehicle movement engine for the synthetic mobility simulator."""

from dataclasses import dataclass, replace
from math import asin, atan2, cos, degrees, isfinite, radians, sin

from mobility.ingestion.schemas.vehicle_event import VehicleStatus
from mobility.ingestion.simulator.config import VehicleConfig
from mobility.ingestion.simulator.route import (
    EARTH_RADIUS_M,
    GeoPoint,
    Route,
    calculate_bearing_deg,
    haversine_distance_m,
)


@dataclass(frozen=True, slots=True)
class VehicleState:
    """Current state of a simulated vehicle."""

    vehicle_id: str
    route: Route
    position: GeoPoint
    waypoint_index: int
    speed_kmh: float
    heading_deg: float = 0.0
    status: VehicleStatus = VehicleStatus.IN_SERVICE

    def __post_init__(self) -> None:
        """Validate vehicle state."""
        if not self.vehicle_id.strip():
            raise ValueError("vehicle_id must not be empty")

        if not isfinite(self.speed_kmh) or self.speed_kmh < 0:
            raise ValueError("speed_kmh must be finite and non-negative")

        if not isfinite(self.heading_deg) or not 0 <= self.heading_deg < 360:
            raise ValueError("heading_deg must be between 0 and 360 degrees")

        if not 1 <= self.waypoint_index <= len(self.route.waypoints):
            raise ValueError("waypoint_index must identify a valid target or completed route")


def create_vehicle_state(
    config: VehicleConfig,
    route: Route,
) -> VehicleState:
    """Create the initial state of a vehicle at its route's first waypoint."""
    if config.route_id != route.route_id:
        raise ValueError("Vehicle configuration and route IDs must match")

    return VehicleState(
        vehicle_id=config.vehicle_id,
        route=route,
        position=route.waypoints[0],
        waypoint_index=1,
        speed_kmh=config.initial_speed_kmh,
        heading_deg=calculate_bearing_deg(
            route.waypoints[0],
            route.waypoints[1],
        ),
    )


def _destination_point(
    start: GeoPoint,
    bearing_deg: float,
    distance_m: float,
) -> GeoPoint:
    """Calculate a destination coordinate along a great-circle path."""
    angular_distance = distance_m / EARTH_RADIUS_M
    bearing = radians(bearing_deg)

    latitude_1 = radians(start.latitude)
    longitude_1 = radians(start.longitude)

    latitude_2 = asin(
        sin(latitude_1) * cos(angular_distance)
        + cos(latitude_1) * sin(angular_distance) * cos(bearing)
    )

    longitude_2 = longitude_1 + atan2(
        sin(bearing) * sin(angular_distance) * cos(latitude_1),
        cos(angular_distance) - sin(latitude_1) * sin(latitude_2),
    )

    longitude = (degrees(longitude_2) + 180) % 360 - 180

    return GeoPoint(
        latitude=degrees(latitude_2),
        longitude=longitude,
    )


def move_vehicle(
    state: VehicleState,
    elapsed_seconds: float,
) -> VehicleState:
    """Move a vehicle along its route for the specified elapsed time."""
    if not isfinite(elapsed_seconds) or elapsed_seconds < 0:
        raise ValueError("elapsed_seconds must be finite and non-negative")

    if elapsed_seconds == 0 or state.speed_kmh == 0:
        return state

    remaining_distance_m = state.speed_kmh * (1000 / 3600) * elapsed_seconds

    position = state.position
    waypoint_index = state.waypoint_index
    heading_deg = state.heading_deg

    while waypoint_index < len(state.route.waypoints):
        target = state.route.waypoints[waypoint_index]
        segment_distance_m = haversine_distance_m(position, target)

        if segment_distance_m <= 1e-6:
            position = target
            waypoint_index += 1
            continue

        heading_deg = calculate_bearing_deg(position, target)

        if remaining_distance_m + 1e-6 >= segment_distance_m:
            remaining_distance_m = max(
                0.0,
                remaining_distance_m - segment_distance_m,
            )
            position = target
            waypoint_index += 1

            if remaining_distance_m <= 1e-6:
                remaining_distance_m = 0.0
                break

            continue

        position = _destination_point(
            start=position,
            bearing_deg=heading_deg,
            distance_m=remaining_distance_m,
        )
        remaining_distance_m = 0.0
        break

    return replace(
        state,
        position=position,
        waypoint_index=waypoint_index,
        heading_deg=heading_deg,
    )
