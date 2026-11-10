"""Integration tests for the Kafka vehicle event consumer."""

from base64 import b64encode
from datetime import UTC, datetime
from json import loads
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, Producer

from mobility.ingestion.kafka.consumer import VehicleEventConsumer
from mobility.ingestion.kafka.dead_letter import DeadLetterPublisher
from mobility.ingestion.kafka.producer import VehicleEventProducer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "mobility.test"


@pytest.mark.integration
def test_vehicle_event_is_consumed_and_validated_from_kafka() -> None:
    """Publish a vehicle event and verify consumer deserialization and processing."""
    topic = f"mobility.test.{uuid4().hex}"

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
        topic=topic,
        on_delivery=lambda error, delivered_event: delivery_errors.append(error),
    )

    consumer = VehicleEventConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic,
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

            if consumed_event is not None:
                received_event = consumed_event

        assert received_event is not None, "Kafka consumer did not receive the event"
        assert received_event.event_id == event.event_id
        assert received_event.vehicle_id == event.vehicle_id
        assert received_event.route_id == event.route_id
        assert received_event.latitude == event.latitude
        assert received_event.longitude == event.longitude
        assert received_event.status == event.status

    finally:
        consumer.close()
        producer.close()


@pytest.mark.integration
def test_invalid_kafka_message_is_sent_to_dead_letter_queue() -> None:
    """Send an invalid Kafka message to the DLQ and verify its metadata."""
    topic = f"mobility.test.{uuid4().hex}"

    invalid_payload = f"invalid-json-{uuid4()}".encode()
    expected_payload_base64 = b64encode(invalid_payload).decode("ascii")

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    dead_letter_publisher = DeadLetterPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic="mobility.dlq",
    )

    consumer = VehicleEventConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic,
        group_id=f"mobility-dlq-source-{uuid4()}",
        dead_letter_publisher=dead_letter_publisher,
    )

    dlq_consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"mobility-dlq-verification-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    try:
        producer.produce(
            topic,
            key=f"dlq-test-{uuid4()}".encode(),
            value=invalid_payload,
        )
        assert producer.flush(timeout=10) == 0

        consumer.subscribe()
        dlq_consumer.subscribe(["mobility.dlq"])

        deadline = monotonic() + 30
        received_failure = None

        while monotonic() < deadline:
            consumer.process_next(timeout=0.5)

            dlq_message = dlq_consumer.poll(timeout=0.2)

            if dlq_message is None:
                continue

            if dlq_message.error() is not None:
                if dlq_message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(dlq_message.error()))

            failure = loads(dlq_message.value().decode("utf-8"))

            if failure["original_payload_base64"] != expected_payload_base64:
                continue

            received_failure = failure

            expected_key = (
                f"{failure['original_topic']}:"
                f"{failure['original_partition']}:"
                f"{failure['original_offset']}"
            ).encode()

            assert dlq_message.key() == expected_key
            break

        assert received_failure is not None, "Invalid Kafka message was not found in the DLQ"

        assert received_failure["original_topic"] == topic
        assert received_failure["error_type"] == "ValueError"
        assert received_failure["original_payload_encoding"] == "base64"
        assert received_failure["original_payload_base64"] == expected_payload_base64
        assert received_failure["original_partition"] >= 0
        assert received_failure["original_offset"] >= 0
        assert received_failure["error_message"] == ("Kafka message contains invalid JSON")

    finally:
        consumer.close()
        dlq_consumer.close()
        dead_letter_publisher.close()
        producer.flush(timeout=10)
