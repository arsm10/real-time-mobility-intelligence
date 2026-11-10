"""Unit tests for the synthetic vehicle simulation runner."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from mobility.ingestion.simulator.config import (
    SimulationConfig,
    VehicleConfig,
)
from mobility.ingestion.simulator.movement import VehicleState
from mobility.ingestion.simulator.route import GeoPoint, Route
from mobility.ingestion.simulator.runner import SimulationRunner


@pytest.fixture
def route() -> Route:
    """Provide a route with three geographic waypoints."""
    return Route(
        route_id="R1",
        waypoints=(
            GeoPoint(latitude=51.3397, longitude=12.3731),
            GeoPoint(latitude=51.3400, longitude=12.3740),
            GeoPoint(latitude=51.3410, longitude=12.3750),
        ),
    )


@pytest.fixture
def config() -> SimulationConfig:
    """Provide a simulation configuration with two vehicles."""
    return SimulationConfig(
        vehicles=(
            VehicleConfig(vehicle_id="BUS-001", route_id="R1"),
            VehicleConfig(vehicle_id="BUS-002", route_id="R1"),
        ),
        event_interval_seconds=5.0,
    )


@pytest.fixture
def runner(config: SimulationConfig, route: Route) -> SimulationRunner:
    """Provide an initialized simulation runner."""
    return SimulationRunner(
        config=config,
        routes={"R1": route},
        start_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
    )


def test_runner_initializes_vehicle_states(runner: SimulationRunner) -> None:
    """Initialize one state for each configured vehicle."""
    states = runner.states

    assert len(states) == 2
    assert all(isinstance(state, VehicleState) for state in states)
    assert [state.vehicle_id for state in states] == ["BUS-001", "BUS-002"]


def test_runner_initializes_simulation_time(runner: SimulationRunner) -> None:
    """Preserve the configured initial simulation time."""
    assert runner.current_time == datetime(2026, 11, 10, 10, 0, tzinfo=UTC)


def test_runner_rejects_naive_start_time(
    config: SimulationConfig,
    route: Route,
) -> None:
    """Reject a start time without timezone information."""
    with pytest.raises(ValueError, match="start_time must include timezone"):
        SimulationRunner(
            config=config,
            routes={"R1": route},
            start_time=datetime(2026, 11, 10, 10, 0),
        )


def test_runner_normalizes_start_time_to_utc(
    config: SimulationConfig,
    route: Route,
) -> None:
    """Normalize the simulation clock to UTC."""
    local_time = datetime(
        2026,
        11,
        10,
        11,
        0,
        tzinfo=timezone(timedelta(hours=1)),
    )

    simulation = SimulationRunner(
        config=config,
        routes={"R1": route},
        start_time=local_time,
    )

    assert simulation.current_time == datetime(
        2026,
        11,
        10,
        10,
        0,
        tzinfo=UTC,
    )


def test_runner_rejects_empty_source(
    config: SimulationConfig,
    route: Route,
) -> None:
    """Reject an empty event source."""
    with pytest.raises(ValueError, match="source must not be empty"):
        SimulationRunner(
            config=config,
            routes={"R1": route},
            start_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
            source="   ",
        )


def test_runner_rejects_missing_route(config: SimulationConfig) -> None:
    """Reject a vehicle whose route is not configured."""
    with pytest.raises(ValueError, match="Route 'R1' not found"):
        SimulationRunner(
            config=config,
            routes={},
            start_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
        )


def test_runner_rejects_mismatched_route_mapping_key(
    config: SimulationConfig,
    route: Route,
) -> None:
    """Reject a route mapping whose key differs from its route ID."""
    mismatched_route = Route(
        route_id="OTHER",
        waypoints=route.waypoints,
    )

    with pytest.raises(ValueError, match="does not match route ID"):
        SimulationRunner(
            config=config,
            routes={"R1": mismatched_route},
            start_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
        )


def test_step_generates_one_event_per_vehicle(
    runner: SimulationRunner,
) -> None:
    """Generate one event for every configured vehicle."""
    events = runner.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    assert len(events) == 2
    assert {event.vehicle_id for event in events} == {
        "BUS-001",
        "BUS-002",
    }


def test_step_advances_simulation_clock(
    runner: SimulationRunner,
) -> None:
    """Advance simulation time by the configured interval."""
    runner.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    assert runner.current_time == datetime(
        2026,
        11,
        10,
        10,
        0,
        5,
        tzinfo=UTC,
    )


def test_step_uses_next_simulation_time_for_events(
    runner: SimulationRunner,
) -> None:
    """Use the advanced simulation time as each event timestamp."""
    events = runner.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    assert all(event.event_time == datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC) for event in events)


def test_step_uses_supplied_produced_at_timestamp(
    runner: SimulationRunner,
) -> None:
    """Use the supplied production timestamp for every event."""
    produced_at = datetime(2026, 11, 10, 10, 0, 7, tzinfo=UTC)

    events = runner.step(produced_at=produced_at)

    assert all(event.produced_at == produced_at for event in events)


def test_step_uses_configured_event_source(
    config: SimulationConfig,
    route: Route,
) -> None:
    """Apply the configured source to generated events."""
    simulation = SimulationRunner(
        config=config,
        routes={"R1": route},
        start_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
        source="test-simulator",
    )

    events = simulation.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    assert all(event.source == "test-simulator" for event in events)


def test_step_updates_vehicle_states(
    runner: SimulationRunner,
) -> None:
    """Persist vehicle movement between simulation steps."""
    initial_states = runner.states

    runner.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    updated_states = runner.states

    assert len(updated_states) == len(initial_states)

    for initial, updated in zip(initial_states, updated_states, strict=True):
        assert initial.vehicle_id == updated.vehicle_id
        assert updated != initial


def test_step_preserves_vehicle_order(
    runner: SimulationRunner,
) -> None:
    """Keep event ordering consistent with configured vehicle ordering."""
    events = runner.step(
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
    )

    assert [event.vehicle_id for event in events] == [
        "BUS-001",
        "BUS-002",
    ]


def test_multiple_steps_advance_simulation_clock(
    runner: SimulationRunner,
) -> None:
    """Advance simulation time cumulatively across multiple steps."""
    for second in (5, 10, 15):
        runner.step(
            produced_at=datetime(
                2026,
                11,
                10,
                10,
                0,
                second,
                tzinfo=UTC,
            ),
        )

    assert runner.current_time == datetime(
        2026,
        11,
        10,
        10,
        0,
        15,
        tzinfo=UTC,
    )


def test_step_rejects_naive_produced_at_without_changing_state(
    runner: SimulationRunner,
) -> None:
    """Reject naive production timestamps without advancing the simulation."""
    initial_time = runner.current_time
    initial_states = runner.states

    with pytest.raises(ValueError, match="produced_at must include timezone"):
        runner.step(produced_at=datetime(2026, 11, 10, 10, 0, 5))

    assert runner.current_time == initial_time
    assert runner.states == initial_states
