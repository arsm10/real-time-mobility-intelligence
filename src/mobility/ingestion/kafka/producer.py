"""Kafka producer for validated vehicle position events."""

from collections.abc import Callable

from confluent_kafka import KafkaError, Producer

from mobility.ingestion.schemas.vehicle_event import VehicleEvent

DeliveryCallback = Callable[[KafkaError | None, VehicleEvent], None]


class VehicleEventProducer:
    """Publish validated vehicle events to an Apache Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "mobility.test",
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        """Initialize the Kafka producer."""
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")

        if not topic.strip():
            raise ValueError("topic must not be empty")

        self._topic = topic
        self._on_delivery = on_delivery
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    @property
    def topic(self) -> str:
        """Return the configured Kafka topic."""
        return self._topic

    def publish(self, event: VehicleEvent) -> None:
        """Queue a vehicle event for asynchronous Kafka delivery."""

        def delivery_report(error: KafkaError | None, message: object) -> None:
            if self._on_delivery is not None:
                self._on_delivery(error, event)

        self._producer.produce(
            topic=self._topic,
            key=event.vehicle_id.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            on_delivery=delivery_report,
        )

        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Wait for queued messages and return the number still outstanding."""
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Flush outstanding messages before closing the producer."""
        remaining = self.flush()

        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) were not delivered before timeout")
