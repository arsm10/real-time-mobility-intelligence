"""Thread-safe operational metrics for the Kafka ingestion worker."""

from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable snapshot of worker processing metrics."""

    consumed: int
    stored: int
    failed: int
    retried: int
    dead_lettered: int


class WorkerMetrics:
    """Maintain thread-safe counters for worker operations."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._consumed = 0
        self._stored = 0
        self._failed = 0
        self._retried = 0
        self._dead_lettered = 0

    def record_consumed(self) -> None:
        with self._lock:
            self._consumed += 1

    def record_stored(self) -> None:
        with self._lock:
            self._stored += 1

    def record_failed(self) -> None:
        with self._lock:
            self._failed += 1

    def record_retry(self) -> None:
        with self._lock:
            self._retried += 1

    def record_dead_lettered(self) -> None:
        with self._lock:
            self._dead_lettered += 1

    def snapshot(self) -> MetricsSnapshot:
        """Return a consistent snapshot of all counters."""
        with self._lock:
            return MetricsSnapshot(
                consumed=self._consumed,
                stored=self._stored,
                failed=self._failed,
                retried=self._retried,
                dead_lettered=self._dead_lettered,
            )
