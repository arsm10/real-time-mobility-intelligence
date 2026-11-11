"""End-to-end tests for the Kafka-to-ClickHouse-to-API pipeline."""

from datetime import UTC, datetime
from time import monotonic, sleep
from uuid import uuid4

import httpx
import pytest

from mobility.ingestion.kafka.producer import VehicleEventProducer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "mobility.test"
API_BASE_URL = "http://localhost:8000"


@pytest.mark.e2e
def test_vehicle_event_flows_from_kafka_to_analytics_api() -> None:
    """Verify an event reaches ClickHouse and is returned by the API."""
    event_id = uuid4()
    vehicle_id = f"BUS-E2E-{uuid4().hex}"

    now = datetime.now(UTC)

    event = VehicleEvent(
        event_id=event_id,
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id=vehicle_id,
        route_id="R-E2E",
        trip_id=f"TRIP-{uuid4().hex}",
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=32.5,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="e2e-test",
    )

    delivery_errors = []

    producer = VehicleEventProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        on_delivery=lambda error, delivered_event: delivery_errors.append(error),
    )

    try:
        producer.publish(event)

        remaining = producer.flush(timeout=10)

        assert remaining == 0, "Kafka producer still has undelivered messages"
        assert delivery_errors == [None], "Kafka reported a delivery error"

        deadline = monotonic() + 30
        received_event = None

        while monotonic() < deadline:
            response = httpx.get(
                f"{API_BASE_URL}/api/v1/analytics/vehicles/{vehicle_id}/events",
                params={"limit": 10},
                timeout=5,
            )
            response.raise_for_status()

            events = response.json()

            received_event = next(
                (item for item in events if item["event_id"] == str(event_id)),
                None,
            )

            if received_event is not None:
                break

            sleep(0.5)

        assert received_event is not None, (
            "Published Kafka event did not become available through the "
            "analytics API within 30 seconds"
        )

        assert received_event["vehicle_id"] == vehicle_id
        assert received_event["route_id"] == "R-E2E"
        assert received_event["latitude"] == pytest.approx(51.3397)
        assert received_event["longitude"] == pytest.approx(12.3731)
        assert received_event["speed_kmh"] == pytest.approx(32.5)
        assert received_event["source"] == "e2e-test"

    finally:
        producer.close()
