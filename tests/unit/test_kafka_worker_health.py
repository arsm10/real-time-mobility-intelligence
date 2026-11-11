"""Unit tests for worker health and metrics HTTP endpoints."""

from threading import Event
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from mobility.ingestion.kafka.health import WorkerHealthServer
from mobility.ingestion.kafka.metrics import WorkerMetrics


@pytest.fixture
def health_server() -> WorkerHealthServer:
    """Start a health server on an available local port."""
    metrics = WorkerMetrics()
    ready = Event()
    stopping = Event()

    server = WorkerHealthServer(
        metrics=metrics,
        ready=ready,
        stopping=stopping,
        host="127.0.0.1",
        port=0,
    )
    server.start()

    yield server

    server.stop()


def get_response(url: str) -> tuple[int, str]:
    """Return the HTTP status and response body."""
    try:
        with urlopen(url, timeout=2) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, error.read().decode("utf-8")


def test_health_live_returns_ok(
    health_server: WorkerHealthServer,
) -> None:
    """Report liveness while the HTTP server is running."""
    status, body = get_response(f"http://127.0.0.1:{health_server.port}/health/live")

    assert status == 200
    assert body == "alive\n"


def test_health_ready_reflects_worker_state(
    health_server: WorkerHealthServer,
) -> None:
    """Return 503 before readiness and after shutdown begins."""
    url = f"http://127.0.0.1:{health_server.port}/health/ready"

    status, body = get_response(url)
    assert status == 503
    assert body == "not ready\n"

    health_server._ready.set()

    status, body = get_response(url)
    assert status == 200
    assert body == "ready\n"

    health_server._stopping.set()

    status, body = get_response(url)
    assert status == 503
    assert body == "not ready\n"


def test_metrics_endpoint_exposes_prometheus_counters(
    health_server: WorkerHealthServer,
) -> None:
    """Expose all worker counters in Prometheus text format."""
    health_server._metrics.record_consumed()
    health_server._metrics.record_consumed()
    health_server._metrics.record_stored()
    health_server._metrics.record_failed()
    health_server._metrics.record_retry()
    health_server._metrics.record_retry()
    health_server._metrics.record_dead_lettered()

    status, body = get_response(f"http://127.0.0.1:{health_server.port}/metrics")

    assert status == 200
    assert "# TYPE mobility_worker_consumed_total counter" in body
    assert "mobility_worker_consumed_total 2" in body
    assert "mobility_worker_stored_total 1" in body
    assert "mobility_worker_failed_total 1" in body
    assert "mobility_worker_retried_total 2" in body
    assert "mobility_worker_dead_lettered_total 1" in body


def test_unknown_endpoint_returns_not_found(
    health_server: WorkerHealthServer,
) -> None:
    """Return HTTP 404 for unsupported paths."""
    status, body = get_response(f"http://127.0.0.1:{health_server.port}/unknown")

    assert status == 404
    assert body == "not found\n"


def test_health_server_stop_is_safe(
    health_server: WorkerHealthServer,
) -> None:
    """Allow repeated shutdown calls."""
    health_server.stop()
    health_server.stop()
