"""Integration tests for the Kafka vehicle event producer."""

from datetime import UTC, datetime
from json import loads
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError

from mobility.ingestion.kafka.producer import VehicleEventProducer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "mobility.test"


@pytest.mark.integration
def test_vehicle_event_is_published_and_consumed_from_kafka() -> None:
    """Publish a vehicle event and verify its key and JSON payload."""
    event = VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS-INTEGRATION-001",
        route_id="R1",
        trip_id="TRIP-INTEGRATION-001",
        event_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="integration-test",
    )

    delivery_errors: list[KafkaError | None] = []

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"mobility-integration-{uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )

    producer = VehicleEventProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        on_delivery=lambda error, delivered_event: delivery_errors.append(error),
    )

    try:
        consumer.subscribe([TOPIC])

        # Wait until Kafka assigns the topic partition to this consumer.
        assignment_deadline = monotonic() + 10

        while not consumer.assignment() and monotonic() < assignment_deadline:
            consumer.poll(0.2)

        assert consumer.assignment(), "Kafka consumer did not receive a partition assignment"

        producer.publish(event)

        remaining = producer.flush(timeout=10)

        assert remaining == 0, "Kafka producer still has undelivered messages"
        assert delivery_errors == [None], "Kafka reported a delivery error"

        # Read the newly published message.
        message = None
        consumption_deadline = monotonic() + 10

        while message is None and monotonic() < consumption_deadline:
            message = consumer.poll(0.5)

        assert message is not None, "Kafka message was not received within 10 seconds"
        assert message.error() is None, f"Kafka consumer error: {message.error()}"

        assert message.key() == event.vehicle_id.encode("utf-8")

        received_payload = loads(message.value().decode("utf-8"))
        expected_payload = loads(event.model_dump_json())

        assert received_payload == expected_payload

    finally:
        consumer.close()
        producer.close()
