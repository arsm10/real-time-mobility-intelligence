"""ClickHouse analytics table schema."""

from mobility.analytics.database import get_analytics_client

CREATE_VEHICLE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS vehicle_events
(
    event_id UUID,
    schema_version String,
    event_type LowCardinality(String),
    vehicle_id String,
    route_id String,
    trip_id Nullable(String),
    event_time DateTime64(3, 'UTC'),
    produced_at DateTime64(3, 'UTC'),
    latitude Float64,
    longitude Float64,
    speed_kmh Float64,
    heading_deg Float64,
    status LowCardinality(String),
    source LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (route_id, event_time, vehicle_id, event_id)
"""


def initialize_analytics_schema() -> None:
    """Create the idempotent vehicle events table and validate its engine."""
    client = get_analytics_client()
    client.command(CREATE_VEHICLE_EVENTS_TABLE)

    result = client.query(
        """
        SELECT engine
        FROM system.tables
        WHERE database = {database:String}
          AND name = 'vehicle_events'
        """,
        parameters={"database": client.database},
    )

    if not result.result_rows:
        raise RuntimeError("ClickHouse vehicle_events table was not found after initialization.")

    engine = result.result_rows[0][0]

    if engine != "ReplacingMergeTree":
        raise RuntimeError(
            "ClickHouse vehicle_events table must use ReplacingMergeTree; "
            f"found {engine}. Run the required database migration before "
            "starting the ingestion worker."
        )
