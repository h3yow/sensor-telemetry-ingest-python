"""Report payload parsing, validation and error normalization.

Expected JSON payload::

    {
      "device_id": "sensor-01",   # non-empty string
      "seq": 0,                   # integer >= 0
      "metric": "temperature",    # one of the known metrics
      "value": 23.5,              # finite number within the metric bounds
      "unit": "C",                # unit compatible with the metric
      "timestamp": 1700000000.0   # finite epoch-seconds, >= 0
    }

Validation order; each failure maps to a distinct, stable ErrorCode:

1. MALFORMED       payload is not a JSON object / abnormal structure
2. MISSING_FIELD   a required field is absent or null
3. TYPE_ERROR      field has the wrong type (bool is not a number, etc.)
4. UNIT_CONFLICT   metric unknown, or unit incompatible with the metric
5. OUT_OF_RANGE    seq < 0, value NaN/inf or outside bounds, timestamp bad
"""

from __future__ import annotations

import math
from typing import Any

from .errors import ErrorCode, ValidationIngestError
from .models import Reading

# Canonical unit per metric and linear conversion factors:
# value_canonical = value * factor  (Celsius/Kelvin handled specially).
METRIC_UNITS: dict[str, dict[str, float]] = {
    "temperature": {"C": 1.0, "K": 1.0},
    "pressure": {"Pa": 1.0, "kPa": 1000.0, "hPa": 100.0},
    "humidity": {"%": 1.0},
}

CANONICAL_UNIT: dict[str, str] = {
    "temperature": "C",
    "pressure": "Pa",
    "humidity": "%",
}

# Bounds expressed in canonical units.
VALUE_BOUNDS: dict[str, tuple[float, float]] = {
    "temperature": (-273.15, 1000.0),
    "pressure": (0.0, 1_000_000.0),
    "humidity": (0.0, 100.0),
}

REQUIRED_FIELDS = ("device_id", "seq", "metric", "value", "unit", "timestamp")


def parse_report(payload: Any) -> Reading:
    """Validate an inbound payload and normalize it to a Reading.

    Always raises ValidationIngestError (never a raw internal exception) for
    bad input.
    """
    data = _require_object(payload)

    for name in REQUIRED_FIELDS:
        if name not in data or data[name] is None:
            raise ValidationIngestError(
                ErrorCode.MISSING_FIELD,
                f"required field '{name}' is missing or null",
                field=name,
            )

    device_id = _check_device_id(data["device_id"])
    seq = _check_seq(data["seq"])
    metric = _check_metric_name(data["metric"])
    unit = _check_unit_name(data["unit"], metric)
    raw_value = _check_number(data["value"], "value")
    timestamp = _check_number(data["timestamp"], "timestamp")

    value, canonical_unit = _convert_to_canonical(metric, unit, raw_value)
    _check_ranges(metric, value, timestamp)

    return Reading(
        device_id=device_id,
        seq=seq,
        metric=metric,
        value=value,
        unit=canonical_unit,
        timestamp=timestamp,
    )


def _require_object(payload: Any) -> dict:
    if isinstance(payload, bool) or not isinstance(payload, dict):
        raise ValidationIngestError(
            ErrorCode.MALFORMED,
            "report body must be a JSON object",
        )
    return payload


def _check_device_id(raw: Any) -> str:
    if not isinstance(raw, str):
        raise ValidationIngestError(
            ErrorCode.TYPE_ERROR,
            f"device_id must be a string, got {type(raw).__name__}",
            field="device_id",
        )
    device_id = raw.strip()
    if not device_id:
        raise ValidationIngestError(
            ErrorCode.OUT_OF_RANGE,
            "device_id must be a non-empty string",
            field="device_id",
        )
    return device_id


def _check_seq(raw: Any) -> int:
    # bool is a subclass of int but must not be accepted as a sequence number.
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValidationIngestError(
            ErrorCode.TYPE_ERROR,
            f"seq must be an integer, got {type(raw).__name__}",
            field="seq",
        )
    if raw < 0:
        raise ValidationIngestError(
            ErrorCode.OUT_OF_RANGE,
            f"seq must be >= 0, got {raw}",
            field="seq",
        )
    return raw


def _check_metric_name(raw: Any) -> str:
    if not isinstance(raw, str):
        raise ValidationIngestError(
            ErrorCode.TYPE_ERROR,
            f"metric must be a string, got {type(raw).__name__}",
            field="metric",
        )
    metric = raw.strip()
    if metric not in METRIC_UNITS:
        raise ValidationIngestError(
            ErrorCode.UNIT_CONFLICT,
            f"unknown metric '{metric}'; expected one of "
            f"{sorted(METRIC_UNITS)}",
            field="metric",
        )
    return metric


def _check_unit_name(raw: Any, metric: str) -> str:
    if not isinstance(raw, str):
        raise ValidationIngestError(
            ErrorCode.TYPE_ERROR,
            f"unit must be a string, got {type(raw).__name__}",
            field="unit",
        )
    unit = raw.strip()
    if unit not in METRIC_UNITS[metric]:
        raise ValidationIngestError(
            ErrorCode.UNIT_CONFLICT,
            f"unit '{unit}' is not compatible with metric '{metric}'; "
            f"allowed units: {sorted(METRIC_UNITS[metric])}",
            field="unit",
        )
    return unit


def _check_number(raw: Any, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValidationIngestError(
            ErrorCode.TYPE_ERROR,
            f"{field} must be a number, got {type(raw).__name__}",
            field=field,
        )
    number = float(raw)
    if not math.isfinite(number):
        raise ValidationIngestError(
            ErrorCode.OUT_OF_RANGE,
            f"{field} must be finite, got {raw!r}",
            field=field,
        )
    return number


def _check_ranges(metric: str, value: float, timestamp: float) -> None:
    low, high = VALUE_BOUNDS[metric]
    if not (low <= value <= high):
        raise ValidationIngestError(
            ErrorCode.OUT_OF_RANGE,
            f"value {value} {CANONICAL_UNIT[metric]} out of allowed range "
            f"[{low}, {high}] for metric '{metric}'",
            field="value",
        )
    if timestamp < 0:
        raise ValidationIngestError(
            ErrorCode.OUT_OF_RANGE,
            f"timestamp must be >= 0 epoch seconds, got {timestamp}",
            field="timestamp",
        )


def _convert_to_canonical(metric: str, unit: str, value: float) -> tuple[float, str]:
    """Normalize to the metric's canonical unit.

    Temperature: Kelvin is converted to Celsius linearly. Other metrics use a
    simple multiplicative factor from METRIC_UNITS.
    """
    canonical = CANONICAL_UNIT[metric]
    if metric == "temperature" and unit == "K":
        return value - 273.15, canonical
    factor = METRIC_UNITS[metric][unit]
    return value * factor, canonical
