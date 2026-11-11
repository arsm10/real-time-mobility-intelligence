"""Unit tests for the Kafka-to-ClickHouse ingestion worker."""

from datetime import UTC, datetime
from threading import Event
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from mobility.ingestion.kafka.worker import KafkaClickHouseWorker
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
