"""Integration tests for the Kafka vehicle event consumer."""

from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError

from mobility.ingestion.kafka.consumer import VehicleEventConsumer
from mobility.ingestion.kafka.producer import VehicleEventProducer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "mobility.test"


@pytest.mark.integration
def test_vehicle_event_is_consumed_and_validated_from_kafka() -> None:
    """Publish a vehicle event and verify consumer deserialization and processing."""
    event = VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id=f"BUS-CONSUMER-{uuid4()}",
        route_id="R1",
        trip_id="TRIP-CONSUMER-001",
        event_time=datetime.now(UTC),
        produced_at=datetime.now(UTC),
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="consumer-integration-test",
    )

    delivery_errors: list[KafkaError | None] = []

    producer = VehicleEventProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        on_delivery=lambda error, delivered_event: delivery_errors.append(error),
    )

    consumer = VehicleEventConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        group_id=f"mobility-consumer-integration-{uuid4()}",
    )

    try:
        producer.publish(event)

        remaining = producer.flush(timeout=10)

        assert remaining == 0, "Kafka producer still has undelivered messages"
        assert delivery_errors == [None], "Kafka reported a delivery error"

        consumer.subscribe()

        received_event = None
        consumption_deadline = monotonic() + 15

        while received_event is None and monotonic() < consumption_deadline:
            consumed_event = consumer.process_next(timeout=0.5)

            if consumed_event is not None and consumed_event.event_id == event.event_id:
                received_event = consumed_event

        assert received_event is not None, (
            "Expected vehicle event was not consumed within 15 seconds"
        )

        assert received_event == event
        assert received_event.vehicle_id == event.vehicle_id
        assert received_event.latitude == event.latitude
        assert received_event.longitude == event.longitude

    finally:
        consumer.close()
        producer.close()
