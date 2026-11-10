"""Unit tests for the Kafka dead-letter publisher."""

from base64 import b64decode
from json import loads
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from mobility.ingestion.kafka.dead_letter import DeadLetterPublisher


@pytest.fixture
def publisher() -> DeadLetterPublisher:
    """Return a publisher with a mocked Kafka producer."""
    with patch("mobility.ingestion.kafka.dead_letter.Producer") as mock_producer:
        instance = DeadLetterPublisher()
        instance._producer = mock_producer.return_value
        yield instance


def test_publisher_uses_default_configuration() -> None:
    """Verify default Kafka producer configuration."""
    with patch("mobility.ingestion.kafka.dead_letter.Producer") as mock_producer:
        DeadLetterPublisher()

    config = mock_producer.call_args.args[0]

    assert config["bootstrap.servers"] == "localhost:9092"


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("bootstrap_servers", ""),
        ("topic", ""),
        ("bootstrap_servers", "   "),
        ("topic", "   "),
    ],
)
def test_publisher_rejects_empty_configuration(
    argument: str,
    value: str,
) -> None:
    """Reject empty Kafka configuration values."""
    with pytest.raises(ValueError):
        DeadLetterPublisher(**{argument: value})


@pytest.mark.parametrize("timeout", [0, -1, -0.5])
def test_publisher_rejects_non_positive_delivery_timeout(
    timeout: float,
) -> None:
    """Reject non-positive delivery timeouts."""
    with pytest.raises(ValueError, match="delivery_timeout"):
        DeadLetterPublisher(delivery_timeout=timeout)


def test_publisher_accepts_custom_configuration() -> None:
    """Accept custom Kafka publisher settings."""
    with patch("mobility.ingestion.kafka.dead_letter.Producer") as mock_producer:
        instance = DeadLetterPublisher(
            bootstrap_servers="kafka.example:9092",
            topic="mobility.dead-letter",
            delivery_timeout=5.0,
        )

    assert instance.topic == "mobility.dead-letter"
    assert mock_producer.call_args.args[0]["bootstrap.servers"] == ("kafka.example:9092")


def test_publish_sends_message_and_waits_for_delivery(
    publisher: DeadLetterPublisher,
) -> None:
    """Publish a dead-letter message and confirm delivery."""
    publisher._producer.flush.return_value = 0

    publisher.publish(
        original_topic="mobility.test",
        original_partition=1,
        original_offset=42,
        original_payload=b'{"invalid": true}',
        error_type="ValueError",
        error_message="Invalid event",
    )

    publisher._producer.produce.assert_called_once()
    publisher._producer.flush.assert_called_once_with(10.0)


def test_publish_preserves_failure_metadata(
    publisher: DeadLetterPublisher,
) -> None:
    """Include original Kafka metadata in the published message."""
    publisher._producer.flush.return_value = 0

    publisher.publish(
        original_topic="mobility.test",
        original_partition=2,
        original_offset=100,
        original_payload=b'{"invalid": true}',
        error_type="ValidationError",
        error_message="Missing vehicle_id",
    )

    call = publisher._producer.produce.call_args.kwargs
    payload = loads(call["value"].decode())

    assert call["topic"] == "mobility.dlq"
    assert call["key"] == b"mobility.test:2:100"
    assert payload["original_topic"] == "mobility.test"
    assert payload["original_partition"] == 2
    assert payload["original_offset"] == 100
    assert payload["error_type"] == "ValidationError"
    assert payload["error_message"] == "Missing vehicle_id"
    assert b64decode(payload["original_payload_base64"]) == (b'{"invalid": true}')
    assert payload["original_payload_encoding"] == "base64"
    assert "failed_at" in payload


def test_publish_handles_empty_original_payload(
    publisher: DeadLetterPublisher,
) -> None:
    """Preserve a missing original payload as None."""
    publisher._producer.flush.return_value = 0

    publisher.publish(
        original_topic="mobility.test",
        original_partition=0,
        original_offset=1,
        original_payload=None,
        error_type="ValueError",
        error_message="Missing payload",
    )

    call = publisher._producer.produce.call_args.kwargs
    payload = loads(call["value"].decode())

    assert payload["original_payload_base64"] is None
    assert payload["original_payload_encoding"] == "base64"


def test_publish_preserves_invalid_utf8_bytes(
    publisher: DeadLetterPublisher,
) -> None:
    """Preserve invalid UTF-8 bytes exactly through Base64 encoding."""
    publisher._producer.flush.return_value = 0

    publisher.publish(
        original_topic="mobility.test",
        original_partition=0,
        original_offset=2,
        original_payload=b"\xff\xfe",
        error_type="ValueError",
        error_message="Invalid payload",
    )

    call = publisher._producer.produce.call_args.kwargs
    payload = loads(call["value"].decode())

    assert b64decode(payload["original_payload_base64"]) == b"\xff\xfe"
    assert payload["original_payload_encoding"] == "base64"


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("original_topic", ""),
        ("error_type", ""),
        ("error_message", ""),
    ],
)
def test_publish_rejects_empty_failure_metadata(
    publisher: DeadLetterPublisher,
    argument: str,
    value: str,
) -> None:
    """Reject missing required failure metadata."""
    metadata = {
        "original_topic": "mobility.test",
        "original_partition": 0,
        "original_offset": 1,
        "original_payload": b"{}",
        "error_type": "ValueError",
        "error_message": "Invalid event",
    }
    metadata[argument] = value

    with pytest.raises(ValueError):
        publisher.publish(**metadata)


def test_publish_raises_when_delivery_times_out(
    publisher: DeadLetterPublisher,
) -> None:
    """Raise TimeoutError when a message remains undelivered."""
    publisher._producer.flush.return_value = 1

    with pytest.raises(TimeoutError, match="not delivered"):
        publisher.publish(
            original_topic="mobility.test",
            original_partition=0,
            original_offset=1,
            original_payload=b"{}",
            error_type="ValueError",
            error_message="Invalid event",
        )


def test_publish_raises_when_delivery_fails(
    publisher: DeadLetterPublisher,
) -> None:
    """Raise RuntimeError when Kafka reports delivery failure."""
    kafka_error = KafkaError(KafkaError._MSG_TIMED_OUT)

    def fail_delivery(**kwargs: object) -> None:
        callback = kwargs["on_delivery"]
        callback(kafka_error, MagicMock())

    publisher._producer.produce.side_effect = fail_delivery
    publisher._producer.flush.return_value = 0

    with pytest.raises(RuntimeError, match="delivery failed"):
        publisher.publish(
            original_topic="mobility.test",
            original_partition=0,
            original_offset=1,
            original_payload=b"{}",
            error_type="ValueError",
            error_message="Invalid event",
        )


def test_close_flushes_pending_messages(
    publisher: DeadLetterPublisher,
) -> None:
    """Flush pending messages when closing the publisher."""
    publisher._producer.flush.return_value = 0

    publisher.close()

    publisher._producer.flush.assert_called_once_with(10.0)


def test_close_raises_when_messages_remain(
    publisher: DeadLetterPublisher,
) -> None:
    """Raise TimeoutError if pending messages remain."""
    publisher._producer.flush.return_value = 2

    with pytest.raises(TimeoutError, match="remain undelivered"):
        publisher.close()
