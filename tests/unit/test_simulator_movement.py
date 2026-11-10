"""Unit tests for the synthetic vehicle movement engine."""

from dataclasses import FrozenInstanceError

import pytest

from mobility.ingestion.schemas.vehicle_event import VehicleStatus
from mobility.ingestion.simulator.config import VehicleConfig
from mobility.ingestion.simulator.movement import (
    VehicleState,
    create_vehicle_state,
    move_vehicle,
)
from mobility.ingestion.simulator.route import (
    GeoPoint,
    Route,
    haversine_distance_m,
)


@pytest.fixture
def route() -> Route:
    """Provide a route with two approximately 111-metre segments."""
    return Route(
        route_id="route-1",
        waypoints=(
            GeoPoint(0.0, 0.0),
            GeoPoint(0.0, 0.001),
            GeoPoint(0.0, 0.002),
        ),
    )


@pytest.fixture
def vehicle_config() -> VehicleConfig:
    """Provide a valid vehicle configuration."""
    return VehicleConfig(
        vehicle_id="vehicle-1",
        route_id="route-1",
        initial_speed_kmh=36.0,
    )


@pytest.fixture
def vehicle_state(
    vehicle_config: VehicleConfig,
    route: Route,
) -> VehicleState:
    """Provide an initialized vehicle state."""
    return create_vehicle_state(vehicle_config, route)


def test_create_vehicle_state(
    vehicle_config: VehicleConfig,
    route: Route,
) -> None:
    state = create_vehicle_state(vehicle_config, route)

    assert state.vehicle_id == "vehicle-1"
    assert state.position == route.waypoints[0]
    assert state.waypoint_index == 1
    assert state.speed_kmh == 36.0
    assert state.heading_deg == 90.0
    assert state.status == VehicleStatus.IN_SERVICE


def test_create_vehicle_state_rejects_route_id_mismatch() -> None:
    config = VehicleConfig(vehicle_id="vehicle-1", route_id="route-1")
    route = Route(
        route_id="route-2",
        waypoints=(GeoPoint(0, 0), GeoPoint(0, 0.001)),
    )

    with pytest.raises(ValueError, match="route IDs must match"):
        create_vehicle_state(config, route)


def test_move_vehicle_partially_along_segment(
    vehicle_state: VehicleState,
) -> None:
    moved = move_vehicle(vehicle_state, elapsed_seconds=5)

    travelled = haversine_distance_m(vehicle_state.position, moved.position)

    assert travelled == pytest.approx(50.0, abs=0.01)
    assert moved.waypoint_index == 1
    assert moved.heading_deg == pytest.approx(90.0)


def test_move_vehicle_reaches_exact_waypoint(
    vehicle_state: VehicleState,
    route: Route,
) -> None:
    segment_distance = haversine_distance_m(
        route.waypoints[0],
        route.waypoints[1],
    )

    elapsed_seconds = segment_distance / 10.0

    moved = move_vehicle(vehicle_state, elapsed_seconds)

    assert moved.position == route.waypoints[1]
    assert moved.waypoint_index == 2


def test_move_vehicle_carries_remaining_distance_to_next_waypoint(
    vehicle_state: VehicleState,
    route: Route,
) -> None:
    first_segment = haversine_distance_m(
        route.waypoints[0],
        route.waypoints[1],
    )

    second_segment = haversine_distance_m(
        route.waypoints[1],
        route.waypoints[2],
    )

    moved = move_vehicle(
        vehicle_state,
        elapsed_seconds=(first_segment + 50.0) / 10.0,
    )

    travelled_after_first_waypoint = haversine_distance_m(
        route.waypoints[1],
        moved.position,
    )

    assert moved.waypoint_index == 2
    assert travelled_after_first_waypoint == pytest.approx(50.0, abs=0.01)
    assert second_segment > travelled_after_first_waypoint


def test_move_vehicle_with_zero_elapsed_time(
    vehicle_state: VehicleState,
) -> None:
    moved = move_vehicle(vehicle_state, elapsed_seconds=0)

    assert moved is vehicle_state


def test_move_vehicle_with_zero_speed(
    vehicle_state: VehicleState,
) -> None:
    stationary_state = VehicleState(
        vehicle_id=vehicle_state.vehicle_id,
        route=vehicle_state.route,
        position=vehicle_state.position,
        waypoint_index=vehicle_state.waypoint_index,
        speed_kmh=0,
        heading_deg=vehicle_state.heading_deg,
    )

    moved = move_vehicle(stationary_state, elapsed_seconds=10)

    assert moved is stationary_state


def test_move_vehicle_stops_at_route_end(
    vehicle_state: VehicleState,
    route: Route,
) -> None:
    total_distance = sum(
        haversine_distance_m(start, end)
        for start, end in zip(
            route.waypoints,
            route.waypoints[1:],
            strict=False,
        )
    )

    moved = move_vehicle(
        vehicle_state,
        elapsed_seconds=(total_distance + 100.0) / 10.0,
    )

    assert moved.position == route.waypoints[-1]
    assert moved.waypoint_index == len(route.waypoints)


@pytest.mark.parametrize("elapsed_seconds", [-1, float("nan"), float("inf")])
def test_move_vehicle_rejects_invalid_elapsed_time(
    vehicle_state: VehicleState,
    elapsed_seconds: float,
) -> None:
    with pytest.raises(ValueError, match="elapsed_seconds"):
        move_vehicle(vehicle_state, elapsed_seconds)


@pytest.mark.parametrize("speed_kmh", [-1, float("nan"), float("inf")])
def test_vehicle_state_rejects_invalid_speed(
    vehicle_state: VehicleState,
    speed_kmh: float,
) -> None:
    with pytest.raises(ValueError, match="speed_kmh"):
        VehicleState(
            vehicle_id=vehicle_state.vehicle_id,
            route=vehicle_state.route,
            position=vehicle_state.position,
            waypoint_index=vehicle_state.waypoint_index,
            speed_kmh=speed_kmh,
        )


def test_vehicle_state_rejects_invalid_waypoint_index(
    vehicle_state: VehicleState,
) -> None:
    with pytest.raises(ValueError, match="waypoint_index"):
        VehicleState(
            vehicle_id=vehicle_state.vehicle_id,
            route=vehicle_state.route,
            position=vehicle_state.position,
            waypoint_index=0,
            speed_kmh=vehicle_state.speed_kmh,
        )


def test_vehicle_state_is_immutable(
    vehicle_state: VehicleState,
) -> None:
    with pytest.raises(FrozenInstanceError):
        vehicle_state.speed_kmh = 50.0  # type: ignore[misc]


def test_move_vehicle_does_not_modify_original_state(
    vehicle_state: VehicleState,
) -> None:
    original_position = vehicle_state.position

    moved = move_vehicle(vehicle_state, elapsed_seconds=5)

    assert vehicle_state.position == original_position
    assert moved.position != original_position


def test_vehicle_state_rejects_empty_vehicle_id(
    vehicle_state: VehicleState,
) -> None:
    with pytest.raises(ValueError, match="vehicle_id must not be empty"):
        VehicleState(
            vehicle_id=" ",
            route=vehicle_state.route,
            position=vehicle_state.position,
            waypoint_index=vehicle_state.waypoint_index,
            speed_kmh=vehicle_state.speed_kmh,
        )


@pytest.mark.parametrize(
    "heading_deg",
    [-1.0, 360.0, float("nan"), float("inf")],
)
def test_vehicle_state_rejects_invalid_heading(
    vehicle_state: VehicleState,
    heading_deg: float,
) -> None:
    with pytest.raises(ValueError, match="heading_deg"):
        VehicleState(
            vehicle_id=vehicle_state.vehicle_id,
            route=vehicle_state.route,
            position=vehicle_state.position,
            waypoint_index=vehicle_state.waypoint_index,
            speed_kmh=vehicle_state.speed_kmh,
            heading_deg=heading_deg,
        )


def test_move_vehicle_skips_zero_length_route_segment() -> None:
    route = Route(
        route_id="route-1",
        waypoints=(
            GeoPoint(0.0, 0.0),
            GeoPoint(0.0, 0.0),
            GeoPoint(0.0, 0.001),
        ),
    )
    config = VehicleConfig(
        vehicle_id="vehicle-1",
        route_id="route-1",
        initial_speed_kmh=36.0,
    )
    state = create_vehicle_state(config, route)

    moved = move_vehicle(state, elapsed_seconds=5)

    assert moved.waypoint_index == 2
    assert haversine_distance_m(state.position, moved.position) == pytest.approx(
        50.0,
        abs=0.01,
    )
