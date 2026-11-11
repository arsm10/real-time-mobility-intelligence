"""Analytics API endpoints."""

from fastapi import APIRouter, HTTPException, Query, status

from mobility.analytics.repository import (
    get_average_speed_by_route,
    get_hourly_event_counts,
    get_vehicle_events,
)
from mobility.api.schemas.analytics import (
    AverageSpeedResponse,
    HourlyEventsResponse,
    VehicleEventResponse,
)

router = APIRouter(
    prefix="/api/v1/analytics",
    tags=["Analytics"],
)


@router.get(
    "/routes/average-speed",
    response_model=list[AverageSpeedResponse],
)
def average_speed_by_route() -> list[AverageSpeedResponse]:
    """Return average vehicle speed grouped by route."""
    rows = get_average_speed_by_route()

    return [
        AverageSpeedResponse(
            route_id=row[0],
            average_speed_kmh=row[1],
        )
        for row in rows
    ]


@router.get(
    "/events/hourly",
    response_model=list[HourlyEventsResponse],
)
def hourly_event_counts() -> list[HourlyEventsResponse]:
    """Return vehicle event counts grouped by hour."""
    rows = get_hourly_event_counts()

    return [
        HourlyEventsResponse(
            event_hour=row[0],
            event_count=row[1],
        )
        for row in rows
    ]


@router.get(
    "/vehicles/{vehicle_id}/events",
    response_model=list[VehicleEventResponse],
)
def vehicle_events(
    vehicle_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[VehicleEventResponse]:
    """Return paginated events for a specific vehicle."""
    try:
        rows = get_vehicle_events(
            vehicle_id=vehicle_id,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to retrieve vehicle events.",
        ) from exc

    return [
        VehicleEventResponse(
            event_id=row[0],
            schema_version=row[1],
            event_type=row[2],
            vehicle_id=row[3],
            route_id=row[4],
            trip_id=row[5],
            event_time=row[6],
            produced_at=row[7],
            latitude=row[8],
            longitude=row[9],
            speed_kmh=row[10],
            heading_deg=row[11],
            status=row[12],
            source=row[13],
        )
        for row in rows
    ]
