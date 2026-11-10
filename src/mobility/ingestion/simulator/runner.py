"""Simulation runner coordinating multiple synthetic vehicles."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from mobility.ingestion.schemas.vehicle_event import VehicleEvent
from mobility.ingestion.simulator.config import SimulationConfig
from mobility.ingestion.simulator.event_factory import create_vehicle_event
from mobility.ingestion.simulator.movement import (
    VehicleState,
    create_vehicle_state,
    move_vehicle,
)
from mobility.ingestion.simulator.route import Route


class SimulationRunner:
    """Manage vehicle states and generate events at configured intervals."""

    def __init__(
        self,
        config: SimulationConfig,
        routes: Mapping[str, Route],
        start_time: datetime,
        source: str = "synthetic",
    ) -> None:
        """Initialize vehicle states and the simulation clock."""
        if start_time.tzinfo is None or start_time.utcoffset() is None:
            raise ValueError("start_time must include timezone information")

        if not source.strip():
            raise ValueError("source must not be empty")

        self._config = config
        self._current_time = start_time.astimezone(UTC)
        self._source = source

        self._states: dict[str, VehicleState] = {}

        for vehicle_config in config.vehicles:
            route = routes.get(vehicle_config.route_id)

            if route is None:
                raise ValueError(f"Route '{vehicle_config.route_id}' not found")

            if route.route_id != vehicle_config.route_id:
                raise ValueError(
                    f"Route mapping key '{vehicle_config.route_id}' "
                    f"does not match route ID '{route.route_id}'"
                )

            self._states[vehicle_config.vehicle_id] = create_vehicle_state(
                vehicle_config,
                route,
            )

    @property
    def current_time(self) -> datetime:
        """Return the current simulation time."""
        return self._current_time

    @property
    def states(self) -> tuple[VehicleState, ...]:
        """Return the current states of all simulated vehicles."""
        return tuple(self._states.values())

    def step(self, *, produced_at: datetime) -> tuple[VehicleEvent, ...]:
        """Advance all vehicles and generate one event per vehicle."""
        if produced_at.tzinfo is None or produced_at.utcoffset() is None:
            raise ValueError("produced_at must include timezone information")

        next_time = self._current_time + timedelta(seconds=self._config.event_interval_seconds)

        next_states: dict[str, VehicleState] = {}
        events: list[VehicleEvent] = []

        for vehicle_id, state in self._states.items():
            next_state = move_vehicle(
                state,
                elapsed_seconds=self._config.event_interval_seconds,
            )

            event = create_vehicle_event(
                next_state,
                event_time=next_time,
                produced_at=produced_at,
                source=self._source,
            )

            next_states[vehicle_id] = next_state
            events.append(event)

        self._states = next_states
        self._current_time = next_time

        return tuple(events)
