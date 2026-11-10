"""Kafka consumer for validated vehicle position events."""

from collections.abc import Callable
from json import JSONDecodeError, loads
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError

from mobility.ingestion.kafka.dead_letter import DeadLetterPublisher
from mobility.ingestion.schemas.vehicle_event import VehicleEvent

EventHandler = Callable[[VehicleEvent], None]


class VehicleEventConsumer:
    """Consume, validate and process vehicle events from Apache Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "mobility.test",
        group_id: str = "mobility-consumer",
        event_handler: EventHandler | None = None,
        dead_letter_publisher: DeadLetterPublisher | None = None,
    ) -> None:
        """Initialize the Kafka consumer."""
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")

        if not topic.strip():
            raise ValueError("topic must not be empty")

        if not group_id.strip():
            raise ValueError("group_id must not be empty")

        self._topic = topic
        self._event_handler = event_handler
        self._dead_letter_publisher = dead_letter_publisher

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )

        self._subscribed = False
        self._closed = False

    @property
    def topic(self) -> str:
        """Return the configured Kafka topic."""
        return self._topic

    def subscribe(self) -> None:
        """Subscribe to the configured Kafka topic."""
        if self._closed:
            raise RuntimeError("Consumer is already closed")

        self._consumer.subscribe([self._topic])
        self._subscribed = True

    @staticmethod
    def deserialize_message(message: Message) -> VehicleEvent:
        """Deserialize and validate a Kafka message."""
        payload = message.value()

        if payload is None:
            raise ValueError("Kafka message payload must not be empty")

        try:
            data: Any = loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, JSONDecodeError) as error:
            raise ValueError("Kafka message contains invalid JSON") from error

        if not isinstance(data, dict):
            raise ValueError("Kafka message JSON payload must be an object")

        try:
            return VehicleEvent.model_validate(data)
        except ValidationError:
            raise

    def process_next(self, timeout: float = 1.0) -> VehicleEvent | None:
        """Consume, validate, process and commit one Kafka event."""
        if self._closed:
            raise RuntimeError("Consumer is already closed")

        if not self._subscribed:
            raise RuntimeError("Consumer must subscribe before polling")

        if timeout < 0:
            raise ValueError("timeout must be non-negative")

        message = self._consumer.poll(timeout)

        if message is None:
            return None

        error = message.error()

        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None

            raise KafkaException(error)

        try:
            event = self.deserialize_message(message)
        except (ValueError, ValidationError) as error:
            if self._dead_letter_publisher is None:
                raise

            self._dead_letter_publisher.publish(
                original_topic=message.topic(),
                original_partition=message.partition(),
                original_offset=message.offset(),
                original_payload=message.value(),
                error_type=type(error).__name__,
                error_message=str(error),
            )

            self._consumer.commit(message=message, asynchronous=False)
            return None

        if self._event_handler is not None:
            self._event_handler(event)

        self._consumer.commit(message=message, asynchronous=False)

        return event

    def close(self) -> None:
        """Close the Kafka consumer."""
        if not self._closed:
            self._consumer.close()
            self._closed = True
