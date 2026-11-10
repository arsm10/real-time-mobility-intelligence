"""Unit tests for geographic route models and calculations."""

import pytest

from mobility.ingestion.simulator.route import (
    GeoPoint,
    Route,
    calculate_bearing_deg,
    haversine_distance_m,
)


def test_geo_point_accepts_valid_coordinates() -> None:
    point = GeoPoint(latitude=51.3397, longitude=12.3731)

    assert point.latitude == 51.3397
    assert point.longitude == 12.3731


@pytest.mark.parametrize("latitude", [-91, 91])
def test_geo_point_rejects_invalid_latitude(latitude: float) -> None:
    with pytest.raises(ValueError, match="Latitude must be between"):
        GeoPoint(latitude=latitude, longitude=12.3731)


@pytest.mark.parametrize("longitude", [-181, 181])
def test_geo_point_rejects_invalid_longitude(longitude: float) -> None:
    with pytest.raises(ValueError, match="Longitude must be between"):
        GeoPoint(latitude=51.3397, longitude=longitude)


def test_route_accepts_valid_waypoints() -> None:
    route = Route(
        route_id="ROUTE-01",
        waypoints=(
            GeoPoint(51.3397, 12.3731),
            GeoPoint(51.3400, 12.3800),
        ),
    )

    assert route.route_id == "ROUTE-01"
    assert len(route.waypoints) == 2


def test_route_rejects_empty_route_id() -> None:
    with pytest.raises(ValueError, match="route_id must not be empty"):
        Route(
            route_id=" ",
            waypoints=(
                GeoPoint(51.3397, 12.3731),
                GeoPoint(51.3400, 12.3800),
            ),
        )


def test_route_rejects_fewer_than_two_waypoints() -> None:
    with pytest.raises(ValueError, match="at least two waypoints"):
        Route(
            route_id="ROUTE-01",
            waypoints=(GeoPoint(51.3397, 12.3731),),
        )


def test_haversine_distance_is_zero_for_identical_points() -> None:
    point = GeoPoint(51.3397, 12.3731)

    assert haversine_distance_m(point, point) == 0.0


def test_haversine_distance_calculates_known_distance() -> None:
    start = GeoPoint(0.0, 0.0)
    end = GeoPoint(0.0, 1.0)

    distance = haversine_distance_m(start, end)

    assert distance == pytest.approx(111_194.9, abs=1.0)


@pytest.mark.parametrize(
    ("end", "expected_bearing"),
    [
        (GeoPoint(1.0, 0.0), 0.0),
        (GeoPoint(0.0, 1.0), 90.0),
        (GeoPoint(-1.0, 0.0), 180.0),
        (GeoPoint(0.0, -1.0), 270.0),
    ],
)
def test_calculate_bearing_for_cardinal_directions(
    end: GeoPoint,
    expected_bearing: float,
) -> None:
    start = GeoPoint(0.0, 0.0)

    bearing = calculate_bearing_deg(start, end)

    assert bearing == pytest.approx(expected_bearing)


def test_calculate_bearing_returns_value_between_zero_and_360() -> None:
    start = GeoPoint(51.3397, 12.3731)
    end = GeoPoint(51.3400, 12.3800)

    bearing = calculate_bearing_deg(start, end)

    assert 0 <= bearing < 360


@pytest.mark.parametrize(
    "latitude",
    [float("nan"), float("inf"), float("-inf")],
)
def test_geo_point_rejects_non_finite_latitude(latitude: float) -> None:
    with pytest.raises(ValueError, match="Latitude must be finite"):
        GeoPoint(latitude=latitude, longitude=13.4)


@pytest.mark.parametrize(
    "longitude",
    [float("nan"), float("inf"), float("-inf")],
)
def test_geo_point_rejects_non_finite_longitude(longitude: float) -> None:
    with pytest.raises(ValueError, match="Longitude must be finite"):
        GeoPoint(latitude=51.3, longitude=longitude)
