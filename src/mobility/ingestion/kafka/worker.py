"""Kafka-to-ClickHouse ingestion worker."""

import os
from threading import Event

from mobility.analytics.database import close_analytics_client
from mobility.analytics.repository import save_event
from mobility.analytics.schema import initialize_analytics_schema
from mobility.ingestion.kafka.consumer import VehicleEventConsumer
from mobility.ingestion.kafka.dead_letter import DeadLetterPublisher
from mobility.ingestion.schemas.vehicle_event import VehicleEvent


class KafkaClickHouseWorker:
    """Consume validated Kafka events and persist them to ClickHouse."""

    def __init__(self) -> None:
        """Initialize the Kafka consumer and dead-letter publisher."""
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )
        topic = os.getenv("KAFKA_TOPIC", "mobility.test")
        group_id = os.getenv("KAFKA_GROUP_ID", "mobility-clickhouse-worker")
        dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "mobility.dlq")

        self._dead_letter_publisher = DeadLetterPublisher(
            bootstrap_servers=bootstrap_servers,
            topic=dlq_topic,
        )

        try:
            self._consumer = VehicleEventConsumer(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=group_id,
                event_handler=self._handle_event,
                dead_letter_publisher=self._dead_letter_publisher,
            )
        except Exception:
            self._dead_letter_publisher.close()
            raise

        self._closed = False

    @staticmethod
    def _handle_event(event: VehicleEvent) -> None:
        """Persist one validated event to ClickHouse."""
        save_event(event)

    def run(self, stop_event: Event) -> None:
        """Consume events until a shutdown signal is received."""
        initialize_analytics_schema()
        self._consumer.subscribe()

        while not stop_event.is_set():
            self._consumer.process_next(timeout=1.0)

    def close(self) -> None:
        """Close Kafka, flush the DLQ publisher and release ClickHouse."""
        if self._closed:
            return

        self._closed = True

        try:
            self._consumer.close()
        finally:
            try:
                self._dead_letter_publisher.close()
            finally:
                close_analytics_client()


def main() -> None:
    """Run the ingestion worker with graceful shutdown support."""
    import signal

    stop_event = Event()
    worker = KafkaClickHouseWorker()

    def request_shutdown(signum: int, frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    try:
        worker.run(stop_event)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
