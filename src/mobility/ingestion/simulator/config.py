"""Configuration models for the synthetic vehicle simulator."""

from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True, slots=True)
class VehicleConfig:
    """Configuration for an individual simulated vehicle."""

    vehicle_id: str
    route_id: str
    initial_speed_kmh: float = 30.0

    def __post_init__(self) -> None:
        """Validate vehicle configuration."""
        if not self.vehicle_id.strip():
            raise ValueError("vehicle_id must not be empty")

        if not self.route_id.strip():
            raise ValueError("route_id must not be empty")

        if not isfinite(self.initial_speed_kmh) or self.initial_speed_kmh < 0:
            raise ValueError("initial_speed_kmh must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    """Global configuration for a simulation run."""

    vehicles: tuple[VehicleConfig, ...]
    event_interval_seconds: float = 5.0
    random_seed: int = 42

    def __post_init__(self) -> None:
        """Validate simulation configuration."""
        if not self.vehicles:
            raise ValueError("At least one vehicle must be configured")

        if not isfinite(self.event_interval_seconds) or self.event_interval_seconds <= 0:
            raise ValueError("event_interval_seconds must be finite and positive")

        vehicle_ids = [vehicle.vehicle_id for vehicle in self.vehicles]

        if len(vehicle_ids) != len(set(vehicle_ids)):
            raise ValueError("Vehicle IDs must be unique")
