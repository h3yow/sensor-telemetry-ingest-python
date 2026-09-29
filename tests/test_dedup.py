"""Tests for idempotency, conflict, regression and late-admission semantics."""

from __future__ import annotations

import logging

import pytest

from sensor_telemetry.errors import ErrorCode
from sensor_telemetry.models import IngestStatus, Reading
from sensor_telemetry.service import IngestService

from .conftest import make_report

logger = logging.getLogger("tests.test_dedup")


def ingest(service: IngestService, **overrides):
    payload = make_report(**overrides)
    result = service.ingest(payload)
    logger.info(
        "input device=%s seq=%s ts=%s value=%s -> status=%s code=%s reason=%s",
        payload["device_id"], payload["seq"], payload["timestamp"], payload["value"],
        result.status.value,
        result.error_code.value if result.error_code else None,
        result.reason,
    )
    return result


def test_duplicate_is_idempotent_and_counted_once(service: IngestService) -> None:
    first = ingest(service, seq=1, timestamp=1000.0)
    assert first.status is IngestStatus.ACCEPTED

    second = ingest(service, seq=1, timestamp=1000.0)
    assert second.status is IngestStatus.DUPLICATE
    assert second.duplicate is True

    status = service.device_status("dev-1")
    logger.info("after duplicate: accepted_count=%s counters=%s", status["accepted_count"], service.counters)
    assert status["accepted_count"] == 1
    assert service.counters["accepted"] == 1
    assert service.counters["duplicate"] == 1


def test_same_seq_conflicting_content_is_rejected_with_conflict(service: IngestService) -> None:
    ingest(service, seq=1, timestamp=1000.0, value=20.0)
    conflict_value = ingest(service, seq=1, timestamp=1000.0, value=21.5)
    conflict_ts = ingest(service, seq=1, timestamp=1001.0)

    assert conflict_value.error_code is ErrorCode.CONFLICT
    assert conflict_ts.error_code is ErrorCode.CONFLICT
    # the original reading is untouched
    windows = service.query_windows("dev-1", "temperature", 0, 2000)
    assert windows[0].count == 1
    assert windows[0].sum == 20.0


def test_sequence_regression_rejected_distinctly_from_late(service: IngestService) -> None:
    ingest(service, seq=5, timestamp=1000.0)
    # unseen lower seq, but timestamp is NOT retrograde -> counter regression
    regression = ingest(service, seq=2, timestamp=1000.0)
    assert regression.error_code is ErrorCode.SEQUENCE_REGRESSION


def test_late_gap_fill_within_tolerance_is_accepted(service: IngestService) -> None:
    ingest(service, seq=5, timestamp=1000.0)
    # unseen lower seq with an older in-tolerance timestamp -> late gap fill
    late = ingest(service, seq=4, timestamp=750.0)
    assert late.status is IngestStatus.ACCEPTED
    assert service.device_status("dev-1")["accepted_count"] == 2


def test_report_beyond_max_lateness_is_rejected_too_late(service: IngestService) -> None:
    ingest(service, seq=0, timestamp=1000.0)
    too_late = ingest(service, seq=9, timestamp=600.0)  # 400s behind > 300s
    assert too_late.error_code is ErrorCode.TOO_LATE

    # exactly at the boundary (300s behind) is still admitted
    boundary = ingest(service, seq=8, timestamp=700.0)
    assert boundary.status is IngestStatus.ACCEPTED


def test_rejection_reasons_are_all_distinct(service: IngestService) -> None:
    ingest(service, seq=0, timestamp=1000.0)
    codes = {
        "duplicate": ingest(service, seq=0, timestamp=1000.0).status,
        "conflict": ingest(service, seq=0, timestamp=1000.0, value=1.0).error_code,
        "regression": ingest(service, seq=1, timestamp=1005.0).error_code if False else None,
    }
    # seq=1 admitted first to build a frontier; then do regression explicitly
    ingest(service, seq=3, timestamp=1010.0)
    codes["regression"] = ingest(service, seq=2, timestamp=1010.0).error_code
    codes["too_late"] = ingest(service, seq=7, timestamp=10.0).error_code
    codes["invalid"] = service.ingest({"device_id": "dev-1"}).error_code

    logger.info("distinct codes observed: %s", {k: str(v) for k, v in codes.items()})
    assert codes["duplicate"] is IngestStatus.DUPLICATE
    assert codes["conflict"] is ErrorCode.CONFLICT
    assert codes["regression"] is ErrorCode.SEQUENCE_REGRESSION
    assert codes["too_late"] is ErrorCode.TOO_LATE
    assert codes["invalid"] is ErrorCode.MISSING_FIELD
    assert len({codes["conflict"], codes["regression"], codes["too_late"], codes["invalid"]}) == 4
