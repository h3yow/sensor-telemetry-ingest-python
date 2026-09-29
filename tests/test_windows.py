"""Tests for fixed-window boundary semantics, late folds and snapshots."""

from __future__ import annotations

import logging
import threading

import pytest

from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import IngestService
from sensor_telemetry.windows import WindowStore

from .conftest import make_report

logger = logging.getLogger("tests.test_windows")


def test_window_is_left_inclusive_right_exclusive() -> None:
    store = WindowStore(window_width_seconds=60.0)
    cases = [
        (0.0, 0.0, 60.0),
        (59.999, 0.0, 60.0),
        (60.0, 60.0, 120.0),
        (-1.0, -60.0, 0.0),
        (120.0, 120.0, 180.0),
    ]
    for ts, exp_start, exp_end in cases:
        start, end = store.window_for(ts)
        logger.info("window_for(ts=%s) = [%s, %s), expected [%s, %s)", ts, start, end, exp_start, exp_end)
        assert (start, end) == (exp_start, exp_end)


def test_aggregation_basic_counts_and_stats(service: IngestService) -> None:
    for seq, (ts, value) in enumerate([(0.0, 10.0), (30.0, 20.0), (59.99, 30.0)]):
        result = service.ingest(make_report(seq=seq, timestamp=ts, value=value))
        assert result.status is IngestStatus.ACCEPTED

    windows = service.query_windows("dev-1", "temperature", 0.0, 60.0)
    logger.info("window aggregate: %s", windows[0].to_dict())
    assert len(windows) == 1
    agg = windows[0]
    assert (agg.window_start, agg.window_end) == (0.0, 60.0)
    assert agg.count == 3
    assert (agg.min, agg.max, agg.sum) == (10.0, 30.0, 60.0)
    assert agg.average == 20.0


def test_value_at_right_boundary_goes_to_next_window(service: IngestService) -> None:
    # timestamp 60.0 must NOT belong to [0, 60)
    service.ingest(make_report(seq=0, timestamp=0.0, value=1.0))
    service.ingest(make_report(seq=1, timestamp=60.0, value=2.0))

    first = service.query_windows("dev-1", "temperature", 0.0, 60.0)
    second = service.query_windows("dev-1", "temperature", 60.0, 120.0)
    logger.info("boundary split: first.count=%s second.count=%s", first[0].count, second[0].count)
    assert first[0].count == 1
    assert second[0].count == 1


def test_late_reading_updates_existing_window_atomically_once(service: IngestService) -> None:
    service.ingest(make_report(seq=0, timestamp=10.0, value=10.0))
    service.ingest(make_report(seq=1, timestamp=70.0, value=100.0))
    assert service.query_windows("dev-1", "temperature", 0, 120)[0].count == 1

    # late gap-fill into the already-materialized [0,60) window
    late = service.ingest(make_report(seq=2, timestamp=20.0, value=5.0))
    assert late.status is IngestStatus.ACCEPTED

    windows = service.query_windows("dev-1", "temperature", 0, 120)
    old_window = windows[0]
    logger.info("after late fold into [0,60): %s", old_window.to_dict())
    assert old_window.count == 2
    assert old_window.sum == 15.0
    assert old_window.min == 5.0
    assert old_window.last_ts == 20.0

    # sending the same late reading again is a duplicate and must NOT fold again
    dup = service.ingest(make_report(seq=2, timestamp=20.0, value=5.0))
    assert dup.status is IngestStatus.DUPLICATE
    assert service.query_windows("dev-1", "temperature", 0, 120)[0].count == 2


def test_query_overlap_and_ordering(service: IngestService) -> None:
    for seq, ts in enumerate([0.0, 60.0, 120.0]):
        service.ingest(make_report(seq=seq, timestamp=ts, value=1.0))
    # [60,100) touches only [60,120): [0,60) is excluded because its right
    # endpoint 60 is not > start 60 (half-open overlap predicate).
    hits = service.query_windows("dev-1", "temperature", 60.0, 100.0)
    logger.info("overlap [60,100) hits=%s", [w.window_start for w in hits])
    assert [w.window_start for w in hits] == [60.0]

    all_hits = service.query_windows("dev-1", "temperature", 0.0, 180.0)
    assert [w.window_start for w in all_hits] == [0.0, 60.0, 120.0]


def test_concurrent_readers_never_see_half_updated_windows() -> None:
    store = WindowStore(window_width_seconds=60.0)
    stop = threading.Event()
    violations: list[str] = []

    def reader() -> None:
        from sensor_telemetry.models import Reading

        while not stop.is_set():
            for agg in store.query("dev-1", "temperature", 0.0, 60.0):
                # invariant: a consistent aggregate must satisfy
                # count >= 1 and min <= average <= max
                if not (agg.min <= agg.average <= agg.max and agg.count >= 1):
                    violations.append(f"inconsistent window: {agg}")

    from sensor_telemetry.models import Reading

    t = threading.Thread(target=reader)
    t.start()
    for i in range(2000):
        store.add_reading(Reading("dev-1", i, "temperature", float(i), "C", float(i % 60)))
    stop.set()
    t.join()
    final = store.query("dev-1", "temperature", 0.0, 60.0)[0]
    logger.info("concurrent folds done: final count=%s violations=%s", final.count, len(violations))
    assert final.count == 2000
    assert violations == []
