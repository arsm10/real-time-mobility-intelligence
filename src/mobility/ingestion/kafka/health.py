"""HTTP health and Prometheus metrics endpoints for the ingestion worker."""

import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from typing import cast

from mobility.ingestion.kafka.metrics import WorkerMetrics

logger = logging.getLogger(__name__)


class WorkerHealthServer:
    """Expose worker liveness, readiness and operational metrics over HTTP."""

    def __init__(
        self,
        metrics: WorkerMetrics,
        ready: Event,
        stopping: Event,
        host: str = "0.0.0.0",
        port: int = 8001,
    ) -> None:
        """Initialize the health server."""
        self._metrics = metrics
        self._ready = ready
        self._stopping = stopping
        self._thread: Thread | None = None

        metrics_ref = self._metrics
        ready_ref = self._ready
        stopping_ref = self._stopping

        class HealthRequestHandler(BaseHTTPRequestHandler):
            """Handle worker health and metrics requests."""

            def do_GET(self) -> None:
                """Respond to supported GET endpoints."""
                if self.path == "/health/live":
                    self._send_response(HTTPStatus.OK, "alive\n")
                    return

                if self.path == "/health/ready":
                    if ready_ref.is_set() and not stopping_ref.is_set():
                        self._send_response(HTTPStatus.OK, "ready\n")
                    else:
                        self._send_response(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            "not ready\n",
                        )
                    return

                if self.path == "/metrics":
                    self._send_response(
                        HTTPStatus.OK,
                        self._format_metrics(metrics_ref),
                        content_type="text/plain; version=0.0.4; charset=utf-8",
                    )
                    return

                self._send_response(HTTPStatus.NOT_FOUND, "not found\n")

            def _send_response(
                self,
                status: HTTPStatus,
                body: str,
                content_type: str = "text/plain; charset=utf-8",
            ) -> None:
                """Write an HTTP response."""
                encoded = body.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            @staticmethod
            def _format_metrics(metrics: WorkerMetrics) -> str:
                """Render worker counters in Prometheus text format."""
                snapshot = metrics.snapshot()

                counters = (
                    (
                        "mobility_worker_consumed_total",
                        "Total Kafka messages successfully processed and committed.",
                        snapshot.consumed,
                    ),
                    (
                        "mobility_worker_stored_total",
                        "Total successful ClickHouse event insert operations.",
                        snapshot.stored,
                    ),
                    (
                        "mobility_worker_failed_total",
                        "Total ClickHouse event persistence failures.",
                        snapshot.failed,
                    ),
                    (
                        "mobility_worker_retried_total",
                        "Total ClickHouse retry attempts.",
                        snapshot.retried,
                    ),
                    (
                        "mobility_worker_dead_lettered_total",
                        "Total messages successfully published to the dead-letter topic.",
                        snapshot.dead_lettered,
                    ),
                )

                lines: list[str] = []

                for name, description, value in counters:
                    lines.extend(
                        [
                            f"# HELP {name} {description}",
                            f"# TYPE {name} counter",
                            f"{name} {value}",
                        ]
                    )

                return "\n".join(lines) + "\n"

            def log_message(self, format: str, *args: object) -> None:
                """Use application logging instead of stderr request logs."""
                logger.debug("Health HTTP request: " + format, *args)

        self._server = ThreadingHTTPServer(
            (host, port),
            HealthRequestHandler,
        )
        self._server.daemon_threads = True

    @property
    def port(self) -> int:
        """Return the actual bound port."""
        return cast(tuple[str, int], self._server.server_address)[1]

    def start(self) -> None:
        """Start serving requests in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._thread = Thread(
            target=self._server.serve_forever,
            name="worker-health-server",
            daemon=True,
        )
        self._thread.start()
        logger.info("Worker health server started on port %s", self.port)

    def stop(self) -> None:
        """Stop the HTTP server and release its socket."""
        if self._thread is None:
            self._server.server_close()
            return

        if self._thread.is_alive():
            self._server.shutdown()
            self._thread.join(timeout=5)

        self._server.server_close()
        self._thread = None
        logger.info("Worker health server stopped")
