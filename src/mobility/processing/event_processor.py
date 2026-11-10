"""Vehicle event processing and ordering."""

from datetime import datetime
from enum import StrEnum

from mobility.common.deduplication import EventDeduplicator
from mobility.ingestion.schemas.vehicle_event import VehicleEvent


class ProcessingResult(StrEnum):
    """Possible outcomes of vehicle event processing."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"


class VehicleEventProcessor:
    """Deduplicate vehicle events and enforce event-time ordering."""

    def __init__(self) -> None:
        """Initialize event deduplication and vehicle timestamps."""
        self._deduplicator = EventDeduplicator()
        self._latest_event_times: dict[str, datetime] = {}

    def process(self, event: VehicleEvent) -> ProcessingResult:
        """Validate event uniqueness and chronological ordering."""
        if self._deduplicator.is_duplicate(event.event_id):
            return ProcessingResult.DUPLICATE

        latest_event_time = self._latest_event_times.get(event.vehicle_id)

        if latest_event_time is not None and event.event_time < latest_event_time:
            self._deduplicator.mark_processed(event.event_id)
            return ProcessingResult.OUT_OF_ORDER

        self._deduplicator.mark_processed(event.event_id)
        self._latest_event_times[event.vehicle_id] = event.event_time

        return ProcessingResult.ACCEPTED

    def get_latest_event_time(self, vehicle_id: str) -> datetime | None:
        """Return the latest accepted event timestamp for a vehicle."""
        return self._latest_event_times.get(vehicle_id)
