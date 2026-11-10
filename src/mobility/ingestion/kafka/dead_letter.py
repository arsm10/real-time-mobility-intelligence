"""Kafka dead-letter publisher for failed vehicle events."""

from base64 import b64encode
from datetime import UTC, datetime
from json import dumps
from typing import Any

from confluent_kafka import KafkaError, Producer


class DeadLetterPublisher:
    """Publish failed Kafka messages to a dead-letter topic."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "mobility.dlq",
        delivery_timeout: float = 10.0,
    ) -> None:
        """Initialize the dead-letter publisher."""
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")

        if not topic.strip():
            raise ValueError("topic must not be empty")

        if delivery_timeout <= 0:
            raise ValueError("delivery_timeout must be positive")

        self._topic = topic
        self._delivery_timeout = delivery_timeout
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    @property
    def topic(self) -> str:
        """Return the configured dead-letter topic."""
        return self._topic

    def publish(
        self,
        *,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        original_payload: bytes | None,
        error_type: str,
        error_message: str,
    ) -> None:
        """Publish a failed message and wait for delivery confirmation."""
        if not original_topic.strip():
            raise ValueError("original_topic must not be empty")

        if not error_type.strip():
            raise ValueError("error_type must not be empty")

        if not error_message.strip():
            raise ValueError("error_message must not be empty")

        payload: dict[str, Any] = {
            "original_topic": original_topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
            "error_type": error_type,
            "error_message": error_message,
            "failed_at": datetime.now(UTC).isoformat(),
            "original_payload_base64": (
                b64encode(original_payload).decode("ascii")
                if original_payload is not None
                else None
            ),
            "original_payload_encoding": "base64",
        }

        delivery_error: KafkaError | None = None

        def delivery_report(
            error: KafkaError | None,
            message: object,
        ) -> None:
            nonlocal delivery_error
            delivery_error = error

        self._producer.produce(
            topic=self._topic,
            key=f"{original_topic}:{original_partition}:{original_offset}".encode(),
            value=dumps(payload).encode("utf-8"),
            on_delivery=delivery_report,
        )

        remaining = self._producer.flush(self._delivery_timeout)

        if remaining:
            raise TimeoutError("Dead-letter message was not delivered before timeout")

        if delivery_error is not None:
            raise RuntimeError(f"Dead-letter delivery failed: {delivery_error}") from None

    def close(self) -> None:
        """Flush pending messages before closing the publisher."""
        remaining = self._producer.flush(self._delivery_timeout)

        if remaining:
            raise TimeoutError(f"{remaining} dead-letter message(s) remain undelivered")
