"""Tests for concurrent per-device reporting without double counting or
state corruption."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import IngestService

from .conftest import make_report

logger = logging.getLogger("tests.test_concurrency")


def test_concurrent_distinct_seqs_for_one_device_are_counted_once(
    service: IngestService,
) -> None:
    n = 500

    def send(seq: int):
        return service.ingest(make_report(device_id="dev-X", seq=seq, timestamp=1000.0 + seq))

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(send, range(n)))

    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    logger.info("concurrent distinct seqs: total=%s accepted=%s", n, len(accepted))
    assert len(accepted) == n
    status = service.device_status("dev-X")
    assert status["accepted_count"] == n
    assert status["latest_seq"] == n - 1

    windows = service.query_windows("dev-X", "temperature", 0, 10_000)
    total_count = sum(w.count for w in windows)
    total_sum = sum(w.sum for w in windows)
    logger.info("window fold check: total_count=%s total_sum=%s", total_count, total_sum)
    assert total_count == n
    assert total_sum == pytest.approx(20.0 * n)


def test_concurrent_duplicates_never_double_counted(service: IngestService) -> None:
    barrier = threading.Barrier(32)

    def send(_i: int):
        barrier.wait()  # maximize contention: all threads fire at once
        return service.ingest(make_report(device_id="dev-D", seq=42, timestamp=1000.0))

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(send, range(32)))

    accepted = sum(r.status is IngestStatus.ACCEPTED for r in results)
    duplicates = sum(r.status is IngestStatus.DUPLICATE for r in results)
    logger.info("same-seq fan-out: accepted=%s duplicates=%s", accepted, duplicates)
    assert accepted == 1
    assert duplicates == 31
    assert service.device_status("dev-D")["accepted_count"] == 1
    windows = service.query_windows("dev-D", "temperature", 0, 10_000)
    assert sum(w.count for w in windows) == 1


def test_concurrent_conflicting_same_seq_all_or_one_rejected(service: IngestService) -> None:
    """32 threads race seq=7 with distinct values: exactly one wins, every
    other result must be CONFLICT (no corruption of the winning reading)."""
    barrier = threading.Barrier(32)

    def send(i: int):
        barrier.wait()
        return service.ingest(
            make_report(device_id="dev-C", seq=7, timestamp=1000.0, value=float(i))
        )

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(send, range(32)))

    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    conflicts = [r for r in results if r.error_code is not None and r.error_code.value == "CONFLICT"]
    logger.info("conflicting fan-out: accepted=%s conflicts=%s", len(accepted), len(conflicts))
    assert len(accepted) == 1
    assert len(conflicts) == 31
    windows = service.query_windows("dev-C", "temperature", 0, 10_000)
    winner = accepted[0].reading.value
    assert windows[0].count == 1
    assert windows[0].sum == winner


def test_multiple_devices_concurrent_are_independent(service: IngestService) -> None:
    devices = [f"dev-{i}" for i in range(20)]
    per_device = 50

    def send(dev: str, seq: int):
        return service.ingest(make_report(device_id=dev, seq=seq, timestamp=1000.0 + seq))

    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [
            pool.submit(send, dev, seq)
            for dev in devices
            for seq in range(per_device)
        ]
        results = [f.result() for f in futures]

    assert sum(r.status is IngestStatus.ACCEPTED for r in results) == len(devices) * per_device
    for dev in devices:
        assert service.device_status(dev)["accepted_count"] == per_device
    logger.info("devices=%s each accepted=%s (no cross-device interference)",
                len(devices), per_device)
