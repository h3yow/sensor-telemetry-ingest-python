"""校验与错误归一化：每类结构/语义错误都映射为可区分的错误码。"""

from __future__ import annotations

import pytest

from sensor_telemetry.config import METRIC_UNITS, VALUE_RANGES
from sensor_telemetry.errors import ErrorCode, TelemetryError
from sensor_telemetry.validator import PayloadValidator

from tests.helpers import make_reading


@pytest.fixture()
def validator():
    return PayloadValidator(METRIC_UNITS, VALUE_RANGES)


def _payloads():
    base = make_reading()
    return [
        ("valid", dict(base), None),
        ("payload-is-list", [], ErrorCode.MALFORMED_PAYLOAD),
        ("payload-is-string", "not-object", ErrorCode.MALFORMED_PAYLOAD),
        ("missing-seq", {k: v for k, v in base.items() if k != "seq"}, ErrorCode.MISSING_FIELD),
        ("null-seq", {**base, "seq": None}, ErrorCode.MISSING_FIELD),
        ("empty-device", {**base, "device_id": ""}, ErrorCode.MISSING_FIELD),
        ("seq-as-string", {**base, "seq": "1"}, ErrorCode.BAD_TYPE),
        ("seq-as-bool", {**base, "seq": True}, ErrorCode.BAD_TYPE),
        ("value-as-string", {**base, "value": "hot"}, ErrorCode.BAD_TYPE),
        ("value-nan", {**base, "value": float("nan")}, ErrorCode.BAD_TYPE),
        ("ts-as-float", {**base, "ts_ms": 1.5}, ErrorCode.BAD_TYPE),
        ("unknown-metric", {**base, "metric": "wind"}, ErrorCode.UNKNOWN_METRIC),
        ("unit-mismatch", {**base, "unit": "F"}, ErrorCode.UNIT_MISMATCH),
        ("value-too-high", {**base, "value": 999.0}, ErrorCode.VALUE_OUT_OF_RANGE),
        ("value-too-low", {**base, "value": -51.0}, ErrorCode.VALUE_OUT_OF_RANGE),
        ("ts-negative", {**base, "ts_ms": -1}, ErrorCode.TIMESTAMP_INVALID),
        ("ts-too-big", {**base, "ts_ms": 10**18}, ErrorCode.TIMESTAMP_INVALID),
        ("seq-negative", {**base, "seq": -1}, ErrorCode.BAD_TYPE),
    ]


@pytest.mark.parametrize(
    "name,payload,expected",
    _payloads(),
    ids=[p[0] for p in _payloads()],
)
def test_validation_error_codes(validator, name, payload, expected):
    print(f"case={name} input={payload!r}")
    if expected is None:
        reading = validator.validate(payload)
        print(f"decision=validated reading={reading}")
        assert reading.seq == 1
        return
    with pytest.raises(TelemetryError) as exc_info:
        validator.validate(payload)
    code = exc_info.value.code
    print(f"decision=reject reason={code.value} detail={exc_info.value.message}")
    assert code == expected
