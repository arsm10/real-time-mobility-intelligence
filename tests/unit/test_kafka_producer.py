"""Unit tests for the Kafka vehicle event producer."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError

from mobility.ingestion.kafka.producer import VehicleEventProducer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus


@pytest.fixture
def event() -> VehicleEvent:
    """Provide a valid vehicle event."""
    return VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS-001",
        route_id="R1",
        trip_id="TRIP-001",
        event_time=datetime(2026, 11, 10, 10, 0, tzinfo=UTC),
        produced_at=datetime(2026, 11, 10, 10, 0, 5, tzinfo=UTC),
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="synthetic",
    )


@pytest.fixture
def producer() -> tuple[VehicleEventProducer, MagicMock]:
    """Provide a producer with a mocked Kafka client."""
    with patch("mobility.ingestion.kafka.producer.Producer") as producer_class:
        kafka_client = MagicMock()
        producer_class.return_value = kafka_client

        instance = VehicleEventProducer()

        yield instance, kafka_client


def test_producer_initializes_kafka_client() -> None:
    """Initialize Kafka with the configured bootstrap server."""
    with patch("mobility.ingestion.kafka.producer.Producer") as producer_class:
        VehicleEventProducer(bootstrap_servers="kafka:9092")

    producer_class.assert_called_once_with({"bootstrap.servers": "kafka:9092"})


def test_producer_uses_default_topic(
    producer: tuple[VehicleEventProducer, MagicMock],
) -> None:
    """Use the default mobility topic."""
    instance, _ = producer

    assert instance.topic == "mobility.test"


def test_producer_accepts_custom_topic() -> None:
    """Accept a custom Kafka topic."""
    with patch("mobility.ingestion.kafka.producer.Producer"):
        instance = VehicleEventProducer(topic="mobility.events")

    assert instance.topic == "mobility.events"


@pytest.mark.parametrize(
    ("bootstrap_servers", "topic", "message"),
    [
        ("", "mobility.test", "bootstrap_servers"),
        ("   ", "mobility.test", "bootstrap_servers"),
        ("localhost:9092", "", "topic"),
        ("localhost:9092", "   ", "topic"),
    ],
)
def test_producer_rejects_empty_configuration(
    bootstrap_servers: str,
    topic: str,
    message: str,
) -> None:
    """Reject empty Kafka configuration values."""
    with pytest.raises(ValueError, match=message):
        VehicleEventProducer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )


def test_publish_sends_vehicle_id_as_message_key(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Use the vehicle ID as the Kafka message key."""
    instance, kafka_client = producer

    instance.publish(event)

    kwargs = kafka_client.produce.call_args.kwargs

    assert kwargs["key"] == b"BUS-001"


def test_publish_serializes_event_as_json(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Serialize the complete event as JSON."""
    instance, kafka_client = producer

    instance.publish(event)

    kwargs = kafka_client.produce.call_args.kwargs

    assert kwargs["value"] == event.model_dump_json().encode("utf-8")


def test_publish_uses_configured_topic(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Publish to the configured Kafka topic."""
    instance, kafka_client = producer

    instance.publish(event)

    assert kafka_client.produce.call_args.kwargs["topic"] == "mobility.test"


def test_publish_polls_kafka_client(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Poll the Kafka client after queueing a message."""
    instance, kafka_client = producer

    instance.publish(event)

    kafka_client.poll.assert_called_once_with(0)


def test_publish_registers_delivery_callback(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Register a delivery callback for every message."""
    instance, kafka_client = producer

    instance.publish(event)

    assert callable(kafka_client.produce.call_args.kwargs["on_delivery"])


def test_delivery_callback_reports_success(
    event: VehicleEvent,
) -> None:
    """Report successful message delivery."""
    delivery_results = []

    with patch("mobility.ingestion.kafka.producer.Producer") as producer_class:
        kafka_client = MagicMock()
        producer_class.return_value = kafka_client

        instance = VehicleEventProducer(
            on_delivery=lambda error, delivered_event: delivery_results.append(
                (error, delivered_event)
            )
        )

        instance.publish(event)

        callback = kafka_client.produce.call_args.kwargs["on_delivery"]
        callback(None, MagicMock())

    assert delivery_results == [(None, event)]


def test_delivery_callback_reports_error(
    event: VehicleEvent,
) -> None:
    """Report Kafka delivery errors without raising callback exceptions."""
    delivery_results = []

    with patch("mobility.ingestion.kafka.producer.Producer") as producer_class:
        kafka_client = MagicMock()
        producer_class.return_value = kafka_client

        instance = VehicleEventProducer(
            on_delivery=lambda error, delivered_event: delivery_results.append(
                (error, delivered_event)
            )
        )

        instance.publish(event)

        callback = kafka_client.produce.call_args.kwargs["on_delivery"]
        kafka_error = KafkaError(KafkaError._MSG_TIMED_OUT)

        callback(kafka_error, MagicMock())

    assert delivery_results == [(kafka_error, event)]


def test_delivery_callback_without_custom_handler(
    producer: tuple[VehicleEventProducer, MagicMock],
    event: VehicleEvent,
) -> None:
    """Allow delivery callbacks without a custom handler."""
    instance, kafka_client = producer

    instance.publish(event)

    callback = kafka_client.produce.call_args.kwargs["on_delivery"]

    callback(None, MagicMock())


def test_flush_returns_outstanding_message_count(
    producer: tuple[VehicleEventProducer, MagicMock],
) -> None:
    """Return the number of messages remaining after flush."""
    instance, kafka_client = producer
    kafka_client.flush.return_value = 2

    result = instance.flush(timeout=5.0)

    kafka_client.flush.assert_called_once_with(5.0)
    assert result == 2


def test_close_flushes_pending_messages(
    producer: tuple[VehicleEventProducer, MagicMock],
) -> None:
    """Flush outstanding messages when closing."""
    instance, kafka_client = producer
    kafka_client.flush.return_value = 0

    instance.close()

    kafka_client.flush.assert_called_once_with(10.0)


def test_close_raises_timeout_when_messages_remain(
    producer: tuple[VehicleEventProducer, MagicMock],
) -> None:
    """Raise a timeout when messages remain undelivered."""
    instance, kafka_client = producer
    kafka_client.flush.return_value = 3

    with pytest.raises(TimeoutError, match="3 Kafka message"):
        instance.close()
