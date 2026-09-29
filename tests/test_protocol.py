"""Tests for payload validation and normalized error types."""

from __future__ import annotations

import logging

import pytest

from sensor_telemetry.errors import ErrorCode, ValidationIngestError
from sensor_telemetry.protocol import parse_report

from .conftest import make_report

logger = logging.getLogger("tests.test_protocol")


def test_valid_report_parses_and_normalizes_units() -> None:
    reading = parse_report(make_report(value=300.15, unit="K"))
    logger.info("input value=300.15K -> normalized value=%s unit=%s", reading.value, reading.unit)
    assert reading.unit == "C"
    assert reading.value == pytest.approx(27.0)

    pressure = parse_report(make_report(metric="pressure", value=100, unit="kPa", timestamp=1.0))
    assert pressure.unit == "Pa"
    assert pressure.value == pytest.approx(100_000.0)


@pytest.mark.parametrize(
    "case",
    [
        ("not an object", "x", ErrorCode.MALFORMED, None),
        ("a list", [], ErrorCode.MALFORMED, None),
        ("null body", None, ErrorCode.MALFORMED, None),
        ("missing seq", {"device_id": "d"}, ErrorCode.MISSING_FIELD, "seq"),
        ("null value", make_report(value=None), ErrorCode.MISSING_FIELD, "value"),
        ("bool seq", make_report(seq=True), ErrorCode.TYPE_ERROR, "seq"),
        ("string value", make_report(value="hot"), ErrorCode.TYPE_ERROR, "value"),
        ("list timestamp", make_report(timestamp=[]), ErrorCode.TYPE_ERROR, "timestamp"),
        ("unknown metric", make_report(metric="wind"), ErrorCode.UNIT_CONFLICT, "metric"),
        ("bad unit", make_report(unit="F"), ErrorCode.UNIT_CONFLICT, "unit"),
        ("negative seq", make_report(seq=-1), ErrorCode.OUT_OF_RANGE, "seq"),
        ("nan value", make_report(value=float("nan")), ErrorCode.OUT_OF_RANGE, "value"),
        ("infinite value", make_report(value=float("inf")), ErrorCode.OUT_OF_RANGE, "value"),
        ("out of high bound", make_report(value=9999.0), ErrorCode.OUT_OF_RANGE, "value"),
        ("below absolute zero", make_report(value=-300.0), ErrorCode.OUT_OF_RANGE, "value"),
        ("negative timestamp", make_report(timestamp=-1.0), ErrorCode.OUT_OF_RANGE, "timestamp"),
    ],
)
def test_invalid_payloads_map_to_distinct_error_codes(case) -> None:
    name, payload, expected_code, expected_field = case
    logger.info("case=%s input=%r expected=%s field=%s", name, payload, expected_code.value, expected_field)
    with pytest.raises(ValidationIngestError) as exc_info:
        parse_report(payload)
    err = exc_info.value
    assert err.code is expected_code, f"{name}: {err.code} != {expected_code}"
    assert err.field == expected_field


def test_rejected_payloads_are_distinguishable_from_duplicates() -> None:
    """Same seq with malformed content is a validation error, not CONFLICT:
    validation runs before admission and has its own code family."""
    good = make_report()
    parse_report(good)
    with pytest.raises(ValidationIngestError) as exc_info:
        parse_report({**good, "value": "nope"})
    assert exc_info.value.code is ErrorCode.TYPE_ERROR
