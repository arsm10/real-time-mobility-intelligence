"""Unit tests for synthetic vehicle simulator configuration."""

from dataclasses import FrozenInstanceError

import pytest

from mobility.ingestion.simulator.config import (
    SimulationConfig,
    VehicleConfig,
)


def test_vehicle_config_accepts_valid_values() -> None:
    vehicle = VehicleConfig(
        vehicle_id="BUS-001",
        route_id="ROUTE-01",
        initial_speed_kmh=30.0,
    )

    assert vehicle.vehicle_id == "BUS-001"
    assert vehicle.route_id == "ROUTE-01"
    assert vehicle.initial_speed_kmh == 30.0


def test_vehicle_config_rejects_empty_vehicle_id() -> None:
    with pytest.raises(ValueError, match="vehicle_id must not be empty"):
        VehicleConfig(vehicle_id="", route_id="ROUTE-01")


def test_vehicle_config_rejects_whitespace_vehicle_id() -> None:
    with pytest.raises(ValueError, match="vehicle_id must not be empty"):
        VehicleConfig(vehicle_id="   ", route_id="ROUTE-01")


def test_vehicle_config_rejects_empty_route_id() -> None:
    with pytest.raises(ValueError, match="route_id must not be empty"):
        VehicleConfig(vehicle_id="BUS-001", route_id="")


def test_vehicle_config_rejects_negative_speed() -> None:
    with pytest.raises(
        ValueError,
        match="initial_speed_kmh must be finite and non-negative",
    ):
        VehicleConfig(
            vehicle_id="BUS-001",
            route_id="ROUTE-01",
            initial_speed_kmh=-1.0,
        )


def test_simulation_config_accepts_valid_values() -> None:
    vehicles = (
        VehicleConfig("BUS-001", "ROUTE-01"),
        VehicleConfig("BUS-002", "ROUTE-01"),
    )

    config = SimulationConfig(
        vehicles=vehicles,
        event_interval_seconds=5.0,
        random_seed=42,
    )

    assert len(config.vehicles) == 2
    assert config.event_interval_seconds == 5.0
    assert config.random_seed == 42


def test_simulation_config_rejects_empty_vehicle_collection() -> None:
    with pytest.raises(ValueError, match="At least one vehicle must be configured"):
        SimulationConfig(vehicles=())


@pytest.mark.parametrize("interval", [0, -1, -5.0])
def test_simulation_config_rejects_non_positive_event_interval(
    interval: float,
) -> None:
    vehicle = VehicleConfig("BUS-001", "ROUTE-01")

    with pytest.raises(
        ValueError,
        match="event_interval_seconds must be finite and positive",
    ):
        SimulationConfig(
            vehicles=(vehicle,),
            event_interval_seconds=interval,
        )


def test_simulation_config_rejects_duplicate_vehicle_ids() -> None:
    vehicles = (
        VehicleConfig("BUS-001", "ROUTE-01"),
        VehicleConfig("BUS-001", "ROUTE-02"),
    )

    with pytest.raises(ValueError, match="Vehicle IDs must be unique"):
        SimulationConfig(vehicles=vehicles)


def test_vehicle_config_is_immutable() -> None:
    vehicle = VehicleConfig("BUS-001", "ROUTE-01")

    with pytest.raises(FrozenInstanceError):
        vehicle.vehicle_id = "BUS-002"  # type: ignore[misc]


def test_simulation_config_is_immutable() -> None:
    config = SimulationConfig(
        vehicles=(VehicleConfig("BUS-001", "ROUTE-01"),),
    )

    with pytest.raises(FrozenInstanceError):
        config.event_interval_seconds = 10.0  # type: ignore[misc]


@pytest.mark.parametrize(
    "speed",
    [float("nan"), float("inf"), float("-inf")],
)
def test_vehicle_config_rejects_non_finite_speed(speed: float) -> None:
    with pytest.raises(
        ValueError,
        match="initial_speed_kmh must be finite and non-negative",
    ):
        VehicleConfig(
            vehicle_id="BUS-001",
            route_id="ROUTE-01",
            initial_speed_kmh=speed,
        )


@pytest.mark.parametrize(
    "interval",
    [float("nan"), float("inf"), float("-inf")],
)
def test_simulation_config_rejects_non_finite_event_interval(
    interval: float,
) -> None:
    vehicle = VehicleConfig("BUS-001", "ROUTE-01")

    with pytest.raises(
        ValueError,
        match="event_interval_seconds must be finite and positive",
    ):
        SimulationConfig(
            vehicles=(vehicle,),
            event_interval_seconds=interval,
        )
