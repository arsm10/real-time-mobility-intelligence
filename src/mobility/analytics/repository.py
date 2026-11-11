"""Repository for ClickHouse vehicle event analytics."""

from mobility.analytics.database import get_analytics_client
from mobility.ingestion.schemas.vehicle_event import VehicleEvent


def save_event(event: VehicleEvent) -> None:
    """Insert a vehicle event into ClickHouse."""

    client = get_analytics_client()

    client.insert(
        "vehicle_events",
        [
            [
                event.event_id,
                event.schema_version,
                event.event_type,
                event.vehicle_id,
                event.route_id,
                event.trip_id,
                event.event_time,
                event.produced_at,
                event.latitude,
                event.longitude,
                event.speed_kmh,
                event.heading_deg,
                event.status.value,
                event.source,
            ]
        ],
        column_names=[
            "event_id",
            "schema_version",
            "event_type",
            "vehicle_id",
            "route_id",
            "trip_id",
            "event_time",
            "produced_at",
            "latitude",
            "longitude",
            "speed_kmh",
            "heading_deg",
            "status",
            "source",
        ],
    )


def get_average_speed_by_route() -> list[tuple]:
    """Return average vehicle speed grouped by route."""

    result = get_analytics_client().query(
        """
        SELECT
            route_id,
            round(avg(speed_kmh), 2) AS average_speed_kmh
        FROM vehicle_events
        GROUP BY route_id
        ORDER BY route_id
        """
    )

    return result.result_rows


def get_hourly_event_counts() -> list[tuple]:
    """Return vehicle event counts grouped by hour."""

    result = get_analytics_client().query(
        """
        SELECT
            toStartOfHour(event_time) AS event_hour,
            count() AS event_count
        FROM vehicle_events
        GROUP BY event_hour
        ORDER BY event_hour
        """
    )

    return result.result_rows


def get_vehicle_event_count(vehicle_id: str) -> int:
    """Return the number of events recorded for a vehicle."""

    result = get_analytics_client().query(
        """
        SELECT count()
        FROM vehicle_events
        WHERE vehicle_id = {vehicle_id:String}
        """,
        parameters={"vehicle_id": vehicle_id},
    )

    return result.result_rows[0][0]
