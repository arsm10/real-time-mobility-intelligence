"""Geographic route models and calculations for vehicle simulation."""

from dataclasses import dataclass
from math import atan2, cos, degrees, isfinite, radians, sin, sqrt

EARTH_RADIUS_M = 6_371_000.0


@dataclass(frozen=True, slots=True)
class GeoPoint:
    """A geographic coordinate represented by latitude and longitude."""

    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        """Validate geographic coordinate boundaries."""
        if not isfinite(self.latitude):
            raise ValueError("Latitude must be finite")

        if not isfinite(self.longitude):
            raise ValueError("Longitude must be finite")

        if not -90 <= self.latitude <= 90:
            raise ValueError("Latitude must be between -90 and 90 degrees")

        if not -180 <= self.longitude <= 180:
            raise ValueError("Longitude must be between -180 and 180 degrees")


@dataclass(frozen=True, slots=True)
class Route:
    """An ordered collection of geographic waypoints."""

    route_id: str
    waypoints: tuple[GeoPoint, ...]

    def __post_init__(self) -> None:
        """Validate route configuration."""
        if not self.route_id.strip():
            raise ValueError("route_id must not be empty")

        if len(self.waypoints) < 2:
            raise ValueError("A route must contain at least two waypoints")


def haversine_distance_m(start: GeoPoint, end: GeoPoint) -> float:
    """Calculate great-circle distance between two points in metres."""
    latitude_1 = radians(start.latitude)
    latitude_2 = radians(end.latitude)

    latitude_delta = radians(end.latitude - start.latitude)
    longitude_delta = radians(end.longitude - start.longitude)

    haversine = (
        sin(latitude_delta / 2) ** 2
        + cos(latitude_1) * cos(latitude_2) * sin(longitude_delta / 2) ** 2
    )

    haversine = max(0.0, min(1.0, haversine))
    central_angle = 2 * atan2(sqrt(haversine), sqrt(1 - haversine))

    return EARTH_RADIUS_M * central_angle


def calculate_bearing_deg(start: GeoPoint, end: GeoPoint) -> float:
    """Calculate initial bearing from start to end in degrees."""
    latitude_1 = radians(start.latitude)
    latitude_2 = radians(end.latitude)

    longitude_delta = radians(end.longitude - start.longitude)

    y = sin(longitude_delta) * cos(latitude_2)

    x = cos(latitude_1) * sin(latitude_2) - sin(latitude_1) * cos(latitude_2) * cos(longitude_delta)

    bearing = (degrees(atan2(y, x)) + 360) % 360

    return bearing
