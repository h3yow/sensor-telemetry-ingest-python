"""Domain model definitions for telemetry ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .errors import ErrorCode


class IngestStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class Reading:
    """A single normalized sensor reading.

    ``unit`` holds the canonical unit after protocol-level conversion.
    """

    device_id: str
    seq: int
    metric: str
    value: float
    unit: str
    timestamp: float  # epoch seconds

    def fingerprint(self) -> tuple[str, int, str, float, str, float]:
        """Content identity used to detect same-seq/conflicting duplicates."""
        return (
            self.device_id,
            self.seq,
            self.metric,
            self.value,
            self.unit,
            self.timestamp,
        )


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    reason: str | None = None
    reading: Reading | None = None
    error_code: ErrorCode | None = None
    duplicate: bool = False

    @classmethod
    def accepted(cls, reading: Reading) -> "IngestResult":
        return cls(status=IngestStatus.ACCEPTED, reading=reading, reason="accepted")

    @classmethod
    def duplicated(cls, reading: Reading, reason: str) -> "IngestResult":
        return cls(
            status=IngestStatus.DUPLICATE,
            reading=reading,
            reason=reason,
            duplicate=True,
        )

    @classmethod
    def rejected(cls, code: ErrorCode, reason: str) -> "IngestResult":
        return cls(status=IngestStatus.REJECTED, error_code=code, reason=reason)

    def to_dict(self) -> dict:
        d: dict = {
            "status": self.status.value,
            "reason": self.reason,
            "duplicate": self.duplicate,
        }
        if self.reading is not None:
            d["reading"] = {
                "device_id": self.reading.device_id,
                "seq": self.reading.seq,
                "metric": self.reading.metric,
                "value": self.reading.value,
                "unit": self.reading.unit,
                "timestamp": self.reading.timestamp,
            }
        if self.error_code is not None:
            d["error_code"] = self.error_code.value
        return d


@dataclass(slots=True)
class DeviceState:
    """Per-device sequence tracking. Guarded by its device lock."""

    device_id: str
    max_seq: int = -1
    latest_ts: float = 0.0
    accepted: dict[int, Reading] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WindowAggregate:
    """Aggregated statistics for one [start, end) window."""

    device_id: str
    metric: str
    window_start: float
    window_end: float
    count: int
    min: float
    max: float
    sum: float
    first_ts: float
    last_ts: float

    @property
    def average(self) -> float:
        return self.sum / self.count if self.count else 0.0

    def to_dict(self) -> dict:
        return {
            "device_id": self.device_id,
            "metric": self.metric,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "min": self.min,
            "max": self.max,
            "sum": self.sum,
            "average": self.average,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
        }
