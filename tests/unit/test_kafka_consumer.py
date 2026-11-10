"""Unit tests for the Kafka vehicle event consumer."""

from datetime import UTC, datetime
from json import dumps
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException
from pydantic import ValidationError

from mobility.ingestion.kafka.consumer import VehicleEventConsumer
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus


@pytest.fixture
def valid_event() -> VehicleEvent:
    """Return a valid vehicle event."""
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
        source="unit-test",
    )


@pytest.fixture
def consumer() -> VehicleEventConsumer:
    """Return a consumer with a mocked Kafka client."""
    with patch("mobility.ingestion.kafka.consumer.Consumer") as mock_consumer:
        instance = VehicleEventConsumer()
        instance._consumer = mock_consumer.return_value
        yield instance
        instance.close()


def create_message(payload: bytes | None = None) -> MagicMock:
    """Create a mocked Kafka message."""
    message = MagicMock()
    message.value.return_value = payload
    message.error.return_value = None
    return message


def test_consumer_uses_default_configuration() -> None:
    """Verify the default Kafka consumer configuration."""
    with patch("mobility.ingestion.kafka.consumer.Consumer") as mock_consumer:
        VehicleEventConsumer()

    config = mock_consumer.call_args.args[0]

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == "mobility-consumer"
    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("bootstrap_servers", ""),
        ("topic", ""),
        ("group_id", ""),
        ("bootstrap_servers", "   "),
        ("topic", "   "),
        ("group_id", "   "),
    ],
)
def test_consumer_rejects_empty_configuration(
    argument: str,
    value: str,
) -> None:
    """Reject empty Kafka configuration values."""
    with pytest.raises(ValueError):
        VehicleEventConsumer(**{argument: value})


def test_consumer_accepts_custom_configuration() -> None:
    """Accept custom Kafka connection settings."""
    with patch("mobility.ingestion.kafka.consumer.Consumer") as mock_consumer:
        instance = VehicleEventConsumer(
            bootstrap_servers="kafka.example:9092",
            topic="mobility.events",
            group_id="mobility-worker",
        )

    assert instance.topic == "mobility.events"

    config = mock_consumer.call_args.args[0]
    assert config["bootstrap.servers"] == "kafka.example:9092"
    assert config["group.id"] == "mobility-worker"


def test_subscribe_uses_configured_topic(consumer: VehicleEventConsumer) -> None:
    """Subscribe to the configured Kafka topic."""
    consumer.subscribe()

    consumer._consumer.subscribe.assert_called_once_with(["mobility.test"])
    assert consumer._subscribed is True


def test_subscribe_rejects_closed_consumer(consumer: VehicleEventConsumer) -> None:
    """Reject subscription after closing the consumer."""
    consumer.close()

    with pytest.raises(RuntimeError, match="already closed"):
        consumer.subscribe()


def test_deserialize_valid_message(valid_event: VehicleEvent) -> None:
    """Deserialize a valid JSON event."""
    message = create_message(valid_event.model_dump_json().encode("utf-8"))

    result = VehicleEventConsumer.deserialize_message(message)

    assert result == valid_event


def test_deserialize_rejects_empty_payload() -> None:
    """Reject a Kafka message without a payload."""
    message = create_message(None)

    with pytest.raises(ValueError, match="must not be empty"):
        VehicleEventConsumer.deserialize_message(message)


@pytest.mark.parametrize(
    "payload",
    [
        b"{invalid-json}",
        b"\xff\xfe",
        b"",
    ],
)
def test_deserialize_rejects_invalid_json(payload: bytes) -> None:
    """Reject invalid JSON or invalid UTF-8."""
    message = create_message(payload)

    with pytest.raises(ValueError, match="invalid JSON"):
        VehicleEventConsumer.deserialize_message(message)


@pytest.mark.parametrize(
    "payload",
    [
        b"null",
        b"[]",
        b'"vehicle"',
        b"123",
        b"true",
    ],
)
def test_deserialize_rejects_non_object_json(payload: bytes) -> None:
    """Reject valid JSON values that are not objects."""
    message = create_message(payload)

    with pytest.raises(ValueError, match="must be an object"):
        VehicleEventConsumer.deserialize_message(message)


def test_deserialize_rejects_invalid_event_schema() -> None:
    """Reject JSON that does not satisfy the vehicle event schema."""
    message = create_message(dumps({"vehicle_id": "BUS-001"}).encode("utf-8"))

    with pytest.raises(ValidationError):
        VehicleEventConsumer.deserialize_message(message)


def test_process_next_requires_subscription(
    consumer: VehicleEventConsumer,
) -> None:
    """Require subscription before polling Kafka."""
    with pytest.raises(RuntimeError, match="subscribe"):
        consumer.process_next()


def test_process_next_rejects_closed_consumer(
    consumer: VehicleEventConsumer,
) -> None:
    """Reject polling after closing the consumer."""
    consumer.close()

    with pytest.raises(RuntimeError, match="already closed"):
        consumer.process_next()


def test_process_next_rejects_negative_timeout(
    consumer: VehicleEventConsumer,
) -> None:
    """Reject negative polling timeouts."""
    consumer.subscribe()

    with pytest.raises(ValueError, match="non-negative"):
        consumer.process_next(timeout=-1)


def test_process_next_returns_none_when_no_message(
    consumer: VehicleEventConsumer,
) -> None:
    """Return None when Kafka has no available message."""
    consumer.subscribe()
    consumer._consumer.poll.return_value = None

    result = consumer.process_next()

    assert result is None
    consumer._consumer.commit.assert_not_called()


def test_process_next_returns_none_on_partition_eof(
    consumer: VehicleEventConsumer,
) -> None:
    """Ignore partition EOF notifications."""
    consumer.subscribe()

    error = MagicMock()
    error.code.return_value = KafkaError._PARTITION_EOF

    message = create_message()
    message.error.return_value = error

    consumer._consumer.poll.return_value = message

    assert consumer.process_next() is None
    consumer._consumer.commit.assert_not_called()


def test_process_next_raises_on_kafka_error(
    consumer: VehicleEventConsumer,
) -> None:
    """Raise KafkaException for non-EOF Kafka errors."""
    consumer.subscribe()

    error = MagicMock()
    error.code.return_value = KafkaError._TRANSPORT

    message = create_message()
    message.error.return_value = error

    consumer._consumer.poll.return_value = message

    with pytest.raises(KafkaException):
        consumer.process_next()

    consumer._consumer.commit.assert_not_called()


def test_process_next_processes_and_commits_valid_event(
    consumer: VehicleEventConsumer,
    valid_event: VehicleEvent,
) -> None:
    """Process a valid event and commit its Kafka offset."""
    consumer.subscribe()

    message = create_message(valid_event.model_dump_json().encode("utf-8"))
    consumer._consumer.poll.return_value = message

    handler = MagicMock()
    consumer._event_handler = handler

    result = consumer.process_next()

    assert result == valid_event
    handler.assert_called_once_with(valid_event)
    consumer._consumer.commit.assert_called_once_with(
        message=message,
        asynchronous=False,
    )


def test_process_next_without_handler_commits_valid_event(
    consumer: VehicleEventConsumer,
    valid_event: VehicleEvent,
) -> None:
    """Commit a valid event when no custom handler is configured."""
    consumer.subscribe()

    message = create_message(valid_event.model_dump_json().encode("utf-8"))
    consumer._consumer.poll.return_value = message

    result = consumer.process_next()

    assert result == valid_event
    consumer._consumer.commit.assert_called_once_with(
        message=message,
        asynchronous=False,
    )


def test_process_next_does_not_commit_invalid_json(
    consumer: VehicleEventConsumer,
) -> None:
    """Do not commit a message containing invalid JSON."""
    consumer.subscribe()
    consumer._consumer.poll.return_value = create_message(b"{invalid}")

    with pytest.raises(ValueError):
        consumer.process_next()

    consumer._consumer.commit.assert_not_called()


def test_process_next_does_not_commit_invalid_schema(
    consumer: VehicleEventConsumer,
) -> None:
    """Do not commit a message with an invalid event schema."""
    consumer.subscribe()
    consumer._consumer.poll.return_value = create_message(b'{"vehicle_id": "BUS-001"}')

    with pytest.raises(ValidationError):
        consumer.process_next()

    consumer._consumer.commit.assert_not_called()


def test_process_next_does_not_commit_when_handler_fails(
    consumer: VehicleEventConsumer,
    valid_event: VehicleEvent,
) -> None:
    """Do not commit a message when event processing fails."""
    consumer.subscribe()

    message = create_message(valid_event.model_dump_json().encode("utf-8"))
    consumer._consumer.poll.return_value = message

    handler = MagicMock(side_effect=RuntimeError("Processing failed"))
    consumer._event_handler = handler

    with pytest.raises(RuntimeError, match="Processing failed"):
        consumer.process_next()

    consumer._consumer.commit.assert_not_called()


def test_process_next_propagates_commit_failure(
    consumer: VehicleEventConsumer,
    valid_event: VehicleEvent,
) -> None:
    """Propagate Kafka commit failures."""
    consumer.subscribe()

    message = create_message(valid_event.model_dump_json().encode("utf-8"))
    consumer._consumer.poll.return_value = message
    consumer._consumer.commit.side_effect = KafkaException(KafkaError(KafkaError._TRANSPORT))

    with pytest.raises(KafkaException):
        consumer.process_next()


def test_close_closes_kafka_consumer(
    consumer: VehicleEventConsumer,
) -> None:
    """Close the underlying Kafka consumer."""
    consumer.close()

    consumer._consumer.close.assert_called_once()


def test_close_is_idempotent(
    consumer: VehicleEventConsumer,
) -> None:
    """Close the Kafka consumer only once."""
    consumer.close()
    consumer.close()

    consumer._consumer.close.assert_called_once()
