"""Unit tests for event deduplication."""

from uuid import uuid4

import pytest

from mobility.common.deduplication import EventDeduplicator


@pytest.fixture
def deduplicator() -> EventDeduplicator:
    """Return a fresh event deduplicator."""
    return EventDeduplicator()


def test_new_event_is_not_duplicate(deduplicator: EventDeduplicator) -> None:
    event_id = uuid4()

    assert not deduplicator.is_duplicate(event_id)


def test_processed_event_is_duplicate(deduplicator: EventDeduplicator) -> None:
    event_id = uuid4()

    deduplicator.mark_processed(event_id)

    assert deduplicator.is_duplicate(event_id)


def test_different_event_ids_are_not_duplicates(
    deduplicator: EventDeduplicator,
) -> None:
    first_event_id = uuid4()
    second_event_id = uuid4()

    deduplicator.mark_processed(first_event_id)

    assert not deduplicator.is_duplicate(second_event_id)


def test_marking_same_event_twice_does_not_increase_count(
    deduplicator: EventDeduplicator,
) -> None:
    event_id = uuid4()

    deduplicator.mark_processed(event_id)
    deduplicator.mark_processed(event_id)

    assert deduplicator.processed_count == 1


def test_processed_count_tracks_unique_events(
    deduplicator: EventDeduplicator,
) -> None:
    deduplicator.mark_processed(uuid4())
    deduplicator.mark_processed(uuid4())
    deduplicator.mark_processed(uuid4())

    assert deduplicator.processed_count == 3


def test_clear_removes_processed_event_ids(
    deduplicator: EventDeduplicator,
) -> None:
    event_id = uuid4()

    deduplicator.mark_processed(event_id)
    deduplicator.clear()

    assert not deduplicator.is_duplicate(event_id)
    assert deduplicator.processed_count == 0


def test_clear_on_empty_deduplicator_is_safe(
    deduplicator: EventDeduplicator,
) -> None:
    deduplicator.clear()

    assert deduplicator.processed_count == 0
