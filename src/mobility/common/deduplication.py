"""In-memory event deduplication."""

from uuid import UUID


class EventDeduplicator:
    """Track processed event IDs and identify duplicate events."""

    def __init__(self) -> None:
        """Initialize an empty set of processed event IDs."""
        self._processed_event_ids: set[UUID] = set()

    def is_duplicate(self, event_id: UUID) -> bool:
        """Return whether an event ID has already been processed."""
        return event_id in self._processed_event_ids

    def mark_processed(self, event_id: UUID) -> None:
        """Mark an event ID as successfully processed."""
        self._processed_event_ids.add(event_id)

    def clear(self) -> None:
        """Clear all tracked event IDs."""
        self._processed_event_ids.clear()

    @property
    def processed_count(self) -> int:
        """Return the number of unique processed event IDs."""
        return len(self._processed_event_ids)
