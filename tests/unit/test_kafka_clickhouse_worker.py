"""Unit tests for the Kafka-to-ClickHouse ingestion worker."""

import json
import logging
from datetime import UTC, datetime
from threading import Event
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError

from mobility.ingestion.kafka.worker import (
    JsonLogFormatter,
    KafkaClickHouseWorker,
    main,
)
from mobility.ingestion.schemas.vehicle_event import VehicleEvent, VehicleStatus


@pytest.fixture
def valid_event() -> VehicleEvent:
    """Return a valid vehicle event."""
    now = datetime.now(UTC)

    return VehicleEvent(
        event_id=uuid4(),
        schema_version="1.0",
        event_type="vehicle.position.updated",
        vehicle_id="BUS-001",
        route_id="R1",
        trip_id="TRIP-001",
        event_time=now,
        produced_at=now,
        latitude=51.3397,
        longitude=12.3731,
        speed_kmh=30.0,
        heading_deg=90.0,
        status=VehicleStatus.IN_SERVICE,
        source="unit-test",
    )


@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_worker_uses_environment_configuration(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Initialize Kafka components using environment configuration."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.example:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "mobility.events")
    monkeypatch.setenv("KAFKA_GROUP_ID", "analytics-worker")
    monkeypatch.setenv("KAFKA_DLQ_TOPIC", "mobility.failed")

    KafkaClickHouseWorker()

    mock_dlq.assert_called_once_with(
        bootstrap_servers="kafka.example:9092",
        topic="mobility.failed",
    )
    mock_consumer.assert_called_once_with(
        bootstrap_servers="kafka.example:9092",
        topic="mobility.events",
        group_id="analytics-worker",
        event_handler=mock_consumer.call_args.kwargs["event_handler"],
        dead_letter_publisher=mock_dlq.return_value,
    )


@patch("mobility.ingestion.kafka.worker.save_event")
def test_handle_event_persists_to_clickhouse(
    mock_save_event: MagicMock,
    valid_event: VehicleEvent,
) -> None:
    """Persist the validated event through the analytics repository."""
    KafkaClickHouseWorker._handle_event(valid_event)

    mock_save_event.assert_called_once_with(valid_event)


@patch("mobility.ingestion.kafka.worker.initialize_analytics_schema")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_run_subscribes_and_processes_until_shutdown(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_initialize_schema: MagicMock,
) -> None:
    """Subscribe and continue polling until the stop event is set."""
    worker = KafkaClickHouseWorker()
    stop_event = Event()

    def process_next(timeout: float) -> None:
        assert timeout == 1.0
        stop_event.set()

    mock_consumer.return_value.process_next.side_effect = process_next

    worker.run(stop_event)

    mock_initialize_schema.assert_called_once_with()
    mock_consumer.return_value.subscribe.assert_called_once()
    mock_consumer.return_value.process_next.assert_called_once_with(timeout=1.0)


@patch("mobility.ingestion.kafka.worker.close_analytics_client")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_close_releases_all_resources(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_close_analytics: MagicMock,
) -> None:
    """Close the Kafka consumer, DLQ publisher and ClickHouse client."""
    worker = KafkaClickHouseWorker()

    worker.close()

    mock_consumer.return_value.close.assert_called_once()
    mock_dlq.return_value.close.assert_called_once()
    mock_close_analytics.assert_called_once()


@patch("mobility.ingestion.kafka.worker.close_analytics_client")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_close_is_idempotent(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_close_analytics: MagicMock,
) -> None:
    """Close resources only once."""
    worker = KafkaClickHouseWorker()

    worker.close()
    worker.close()

    mock_consumer.return_value.close.assert_called_once()
    mock_dlq.return_value.close.assert_called_once()
    mock_close_analytics.assert_called_once()


@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_worker_closes_dlq_if_consumer_initialization_fails(
    mock_dlq: MagicMock,
) -> None:
    """Release the DLQ publisher if Kafka consumer initialization fails."""
    with (
        patch(
            "mobility.ingestion.kafka.worker.VehicleEventConsumer",
            side_effect=RuntimeError("Kafka unavailable"),
        ),
        pytest.raises(RuntimeError, match="Kafka unavailable"),
    ):
        KafkaClickHouseWorker()

    mock_dlq.return_value.close.assert_called_once()


@patch("mobility.ingestion.kafka.worker.save_event")
def test_handle_event_propagates_clickhouse_failure(
    mock_save_event: MagicMock,
    valid_event: VehicleEvent,
) -> None:
    """Propagate ClickHouse failures so the Kafka offset remains uncommitted."""
    mock_save_event.side_effect = RuntimeError("ClickHouse unavailable")

    with pytest.raises(RuntimeError, match="ClickHouse unavailable"):
        KafkaClickHouseWorker._handle_event(valid_event)


@patch("mobility.ingestion.kafka.worker.time.sleep")
@patch("mobility.ingestion.kafka.worker.save_event")
def test_handle_event_retries_transient_failure(
    mock_save_event: MagicMock,
    mock_sleep: MagicMock,
    valid_event: VehicleEvent,
) -> None:
    """Retry a transient ClickHouse failure and succeed on the second attempt."""
    mock_save_event.side_effect = [
        OperationalError("Temporary ClickHouse failure"),
        None,
    ]

    KafkaClickHouseWorker._handle_event(valid_event)

    assert mock_save_event.call_count == 2
    mock_sleep.assert_called_once_with(1)


@patch("mobility.ingestion.kafka.worker.time.sleep")
@patch("mobility.ingestion.kafka.worker.save_event")
def test_handle_event_raises_after_retry_exhaustion(
    mock_save_event: MagicMock,
    mock_sleep: MagicMock,
    valid_event: VehicleEvent,
) -> None:
    """Raise after all three transient-failure attempts are exhausted."""
    mock_save_event.side_effect = OperationalError("ClickHouse unavailable")

    with pytest.raises(OperationalError, match="ClickHouse unavailable"):
        KafkaClickHouseWorker._handle_event(valid_event)

    assert mock_save_event.call_count == 3
    assert [call.args[0] for call in mock_sleep.call_args_list] == [1, 2]


@patch("mobility.ingestion.kafka.worker.time.sleep")
@patch("mobility.ingestion.kafka.worker.save_event")
def test_handle_event_does_not_retry_permanent_failure(
    mock_save_event: MagicMock,
    mock_sleep: MagicMock,
    valid_event: VehicleEvent,
) -> None:
    """Do not retry permanent ClickHouse programming errors."""
    mock_save_event.side_effect = ProgrammingError("Invalid query")

    with pytest.raises(ProgrammingError, match="Invalid query"):
        KafkaClickHouseWorker._handle_event(valid_event)

    mock_save_event.assert_called_once_with(valid_event)
    mock_sleep.assert_not_called()


@patch("mobility.ingestion.kafka.worker.initialize_analytics_schema")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_run_propagates_schema_initialization_failure(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_initialize_schema: MagicMock,
) -> None:
    """Stop worker startup when ClickHouse schema initialization fails."""
    worker = KafkaClickHouseWorker()
    stop_event = Event()
    mock_initialize_schema.side_effect = RuntimeError("Schema unavailable")

    with pytest.raises(RuntimeError, match="Schema unavailable"):
        worker.run(stop_event)

    mock_consumer.return_value.subscribe.assert_not_called()
    mock_consumer.return_value.process_next.assert_not_called()


@patch("mobility.ingestion.kafka.worker.initialize_analytics_schema")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_run_propagates_processing_failure(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_initialize_schema: MagicMock,
) -> None:
    """Log and propagate Kafka processing failures."""
    worker = KafkaClickHouseWorker()
    stop_event = Event()
    mock_consumer.return_value.process_next.side_effect = RuntimeError("Kafka processing failed")

    with (
        patch("mobility.ingestion.kafka.worker.logger") as mock_logger,
        pytest.raises(RuntimeError, match="Kafka processing failed"),
    ):
        worker.run(stop_event)

    mock_consumer.return_value.subscribe.assert_called_once()
    mock_logger.exception.assert_called_once_with("Error while processing Kafka message")


@patch("mobility.ingestion.kafka.worker.close_analytics_client")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_close_releases_remaining_resources_when_consumer_close_fails(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_close_analytics: MagicMock,
) -> None:
    """Close remaining resources when the Kafka consumer fails to close."""
    worker = KafkaClickHouseWorker()
    mock_consumer.return_value.close.side_effect = RuntimeError("Kafka close failed")

    with pytest.raises(RuntimeError, match="Kafka close failed"):
        worker.close()

    mock_dlq.return_value.close.assert_called_once()
    mock_close_analytics.assert_called_once()


@patch("mobility.ingestion.kafka.worker.close_analytics_client")
@patch("mobility.ingestion.kafka.worker.VehicleEventConsumer")
@patch("mobility.ingestion.kafka.worker.DeadLetterPublisher")
def test_close_releases_clickhouse_when_dlq_close_fails(
    mock_dlq: MagicMock,
    mock_consumer: MagicMock,
    mock_close_analytics: MagicMock,
) -> None:
    """Close ClickHouse even when the DLQ publisher fails to close."""
    worker = KafkaClickHouseWorker()
    mock_dlq.return_value.close.side_effect = RuntimeError("DLQ close failed")

    with pytest.raises(RuntimeError, match="DLQ close failed"):
        worker.close()

    mock_consumer.return_value.close.assert_called_once()
    mock_close_analytics.assert_called_once()


@patch("mobility.ingestion.kafka.worker.signal.signal")
@patch("mobility.ingestion.kafka.worker.KafkaClickHouseWorker")
@patch("mobility.ingestion.kafka.worker.logging.basicConfig")
def test_main_configures_logging_and_closes_worker(
    mock_basic_config: MagicMock,
    mock_worker_class: MagicMock,
    mock_signal: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configure logging, register signals and close the worker."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    main()

    mock_basic_config.assert_called_once()
    assert mock_basic_config.call_args.kwargs["level"] == 10
    assert mock_signal.call_count == 2
    mock_worker_class.return_value.run.assert_called_once()
    mock_worker_class.return_value.close.assert_called_once()


def test_json_log_formatter_includes_extra_fields() -> None:
    """Serialize structured metadata into valid JSON."""
    record = logging.LogRecord(
        name="mobility.worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Vehicle event stored in ClickHouse",
        args=(),
        exc_info=None,
    )
    record.event_id = "event-123"
    record.vehicle_id = "BUS-001"
    record.partition = 2
    record.offset = 42

    formatted = JsonLogFormatter().format(record)
    log_data = json.loads(formatted)

    assert log_data["level"] == "INFO"
    assert log_data["logger"] == "mobility.worker"
    assert log_data["message"] == "Vehicle event stored in ClickHouse"
    assert log_data["event_id"] == "event-123"
    assert log_data["vehicle_id"] == "BUS-001"
    assert log_data["partition"] == 2
    assert log_data["offset"] == 42
    assert log_data["timestamp"].endswith("Z")


def test_json_log_formatter_includes_exception() -> None:
    """Include exception details in structured JSON logs."""
    try:
        raise ValueError("Test failure")
    except ValueError as error:
        exc_info = (type(error), error, error.__traceback__)

    record = logging.LogRecord(
        name="mobility.worker",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="Processing failed",
        args=(),
        exc_info=exc_info,
    )

    formatted = JsonLogFormatter().format(record)
    log_data = json.loads(formatted)

    assert log_data["level"] == "ERROR"
    assert log_data["message"] == "Processing failed"
    assert "ValueError: Test failure" in log_data["exception"]
