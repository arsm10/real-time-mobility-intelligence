"""Unit tests for Kafka worker metrics."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError

import pytest

from mobility.ingestion.kafka.metrics import WorkerMetrics


def test_metrics_snapshot_is_immutable() -> None:
    metrics = WorkerMetrics()
    snapshot = metrics.snapshot()

    with pytest.raises(FrozenInstanceError):
        snapshot.consumed = 10


def test_metrics_start_at_zero() -> None:
    metrics = WorkerMetrics()

    snapshot = metrics.snapshot()

    assert snapshot.consumed == 0
    assert snapshot.stored == 0
    assert snapshot.failed == 0
    assert snapshot.retried == 0
    assert snapshot.dead_lettered == 0


def test_metrics_record_all_operations() -> None:
    metrics = WorkerMetrics()

    metrics.record_consumed()
    metrics.record_stored()
    metrics.record_failed()
    metrics.record_retry()
    metrics.record_dead_lettered()

    snapshot = metrics.snapshot()

    assert snapshot.consumed == 1
    assert snapshot.stored == 1
    assert snapshot.failed == 1
    assert snapshot.retried == 1
    assert snapshot.dead_lettered == 1


def test_metrics_are_thread_safe() -> None:
    metrics = WorkerMetrics()
    operations_per_thread = 1_000
    thread_count = 8

    def record_operations() -> None:
        for _ in range(operations_per_thread):
            metrics.record_consumed()
            metrics.record_stored()

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        list(executor.map(lambda _: record_operations(), range(thread_count)))

    snapshot = metrics.snapshot()
    expected = operations_per_thread * thread_count

    assert snapshot.consumed == expected
    assert snapshot.stored == expected
