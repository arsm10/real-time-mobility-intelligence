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
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (route_id, event_time, vehicle_id, event_id)
"""


def initialize_analytics_schema() -> None:
    """Create the historical vehicle events table."""
    get_analytics_client().command(CREATE_VEHICLE_EVENTS_TABLE)
