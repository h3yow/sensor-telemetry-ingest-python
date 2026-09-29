"""Error types normalized for telemetry ingestion.

Every user-facing failure maps to one of the error codes below; internal
exceptions never leak directly to callers.
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """Distinguishable rejection reasons."""

    MALFORMED = "MALFORMED"
    MISSING_FIELD = "MISSING_FIELD"
    TYPE_ERROR = "TYPE_ERROR"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNIT_CONFLICT = "UNIT_CONFLICT"
    SEQUENCE_REGRESSION = "SEQUENCE_REGRESSION"
    TOO_LATE = "TOO_LATE"
    CONFLICT = "CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND"
    QUERY_INVALID = "QUERY_INVALID"


class IngestError(Exception):
    """Base class for all normalized ingestion/query errors."""

    def __init__(self, code: ErrorCode, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field

    def to_dict(self) -> dict:
        d: dict = {"code": self.code.value, "message": self.message}
        if self.field is not None:
            d["field"] = self.field
        return d


class ValidationIngestError(IngestError):
    """Raised for payload validation failures (4xx semantics)."""


# Stable HTTP status for each normalized error code.
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.MALFORMED: 400,
    ErrorCode.MISSING_FIELD: 400,
    ErrorCode.TYPE_ERROR: 422,
    ErrorCode.OUT_OF_RANGE: 422,
    ErrorCode.UNIT_CONFLICT: 422,
    ErrorCode.SEQUENCE_REGRESSION: 409,
    ErrorCode.TOO_LATE: 422,
    ErrorCode.CONFLICT: 409,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.DEVICE_NOT_FOUND: 404,
    ErrorCode.QUERY_INVALID: 400,
}
