"""Kafka-to-ClickHouse ingestion worker."""

import json
import logging
import os
import signal
import time
from threading import Event

from clickhouse_connect.driver.exceptions import (
    InterfaceError,
    OperationalError,
    StreamClosedError,
    StreamFailureError,
)

from mobility.analytics.database import close_analytics_client
from mobility.analytics.repository import save_event
from mobility.analytics.schema import initialize_analytics_schema
from mobility.ingestion.kafka.consumer import VehicleEventConsumer
from mobility.ingestion.kafka.dead_letter import DeadLetterPublisher
from mobility.ingestion.schemas.vehicle_event import VehicleEvent

logger = logging.getLogger(__name__)


class JsonLogFormatter(logging.Formatter):
    """Format log records as structured JSON."""

    converter = time.gmtime

    EXTRA_FIELDS = (
        "event_id",
        "vehicle_id",
        "source_topic",
        "partition",
        "offset",
        "error_type",
        "attempt",
        "next_retry_seconds",
    )

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a log record and its available metadata as JSON."""
        log_data = {
            "timestamp": self.formatTime(
                record,
                datefmt="%Y-%m-%dT%H:%M:%SZ",
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for field in self.EXTRA_FIELDS:
            if hasattr(record, field):
                log_data[field] = getattr(record, field)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


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
        """Persist one validated event to ClickHouse with retries."""
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            try:
                save_event(event)
            except (
                OperationalError,
                InterfaceError,
                StreamClosedError,
                StreamFailureError,
            ):
                if attempt == max_attempts:
                    logger.exception(
                        "ClickHouse insert failed after all retry attempts",
                        extra={
                            "event_id": str(event.event_id),
                            "vehicle_id": event.vehicle_id,
                            "attempt": attempt,
                        },
                    )
                    raise

                delay = 2 ** (attempt - 1)

                logger.warning(
                    "ClickHouse insert failed; retrying",
                    extra={
                        "event_id": str(event.event_id),
                        "vehicle_id": event.vehicle_id,
                        "attempt": attempt,
                        "next_retry_seconds": delay,
                    },
                    exc_info=True,
                )

                time.sleep(delay)
            else:
                logger.info(
                    "Vehicle event stored in ClickHouse",
                    extra={
                        "event_id": str(event.event_id),
                        "vehicle_id": event.vehicle_id,
                    },
                )
                return

    def run(self, stop_event: Event) -> None:
        """Consume events until a shutdown signal is received."""
        logger.info("Initializing ClickHouse analytics schema")
        initialize_analytics_schema()

        self._consumer.subscribe()
        logger.info("Kafka consumer subscribed to topic")

        logger.info("Kafka-to-ClickHouse worker started")

        while not stop_event.is_set():
            try:
                self._consumer.process_next(timeout=1.0)
            except Exception:
                logger.exception("Error while processing Kafka message")
                raise

        logger.info("Worker shutdown requested")

    def close(self) -> None:
        """Close Kafka, flush the DLQ publisher and release ClickHouse."""
        if self._closed:
            return

        self._closed = True
        logger.info("Closing Kafka-to-ClickHouse worker")

        try:
            self._consumer.close()
        finally:
            try:
                self._dead_letter_publisher.close()
            finally:
                try:
                    close_analytics_client()
                finally:
                    logger.info("Kafka-to-ClickHouse worker stopped")


def main() -> None:
    """Run the ingestion worker with graceful shutdown support."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    configured_level = getattr(logging, log_level, logging.INFO)

    if not isinstance(configured_level, int):
        configured_level = logging.INFO

    log_handler = logging.StreamHandler()
    log_handler.setFormatter(JsonLogFormatter())

    logging.basicConfig(
        level=configured_level,
        handlers=[log_handler],
        force=True,
    )

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
