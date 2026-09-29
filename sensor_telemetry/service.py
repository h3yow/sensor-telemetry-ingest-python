"""Core ingestion service wiring validation, rate limiting, deduplication
and window aggregation behind one consistent admission path.

Order of admission (the first failing stage decides the error code):

1. structural/value validation (parse_report)  -> 4xx ValidationIngestError
2. per-device rate limiter (token bucket)      -> RATE_LIMITED
3. per-device sequence admission               -> DUPLICATE / CONFLICT /
                                                  SEQUENCE_REGRESSION / TOO_LATE
4. atomic window fold                          -> ACCEPTED

Only stage 4 mutates queryable state, and stage 3's commit and stage 4 happen
sequentially after all rejection checks, so rejected reports can never corrupt
already-accepted data.
"""

from __future__ import annotations

import logging
import time

from .dedup import SequenceTracker
from .errors import ErrorCode, IngestError, ValidationIngestError
from .models import IngestResult, WindowAggregate
from .protocol import parse_report
from .rate_limit import RateLimitConfig, RateLimiter
from .windows import WindowStore

logger = logging.getLogger("sensor_telemetry")


class IngestService:
    def __init__(
        self,
        window_width_seconds: float = 60.0,
        max_lateness_seconds: float = 300.0,
        rate_limit: RateLimitConfig | None = None,
    ) -> None:
        self._windows = WindowStore(window_width_seconds)
        self._tracker = SequenceTracker(max_lateness_seconds)
        self._rate_limiter = RateLimiter(rate_limit)
        self._window_width = window_width_seconds
        self._max_lateness = max_lateness_seconds
        self._counters = {"accepted": 0, "duplicate": 0, "rejected": 0}

    @property
    def counters(self) -> dict[str, int]:
        return dict(self._counters)

    @property
    def window_width(self) -> float:
        return self._window_width

    @property
    def max_lateness(self) -> float:
        return self._max_lateness

    def ingest(self, payload: object) -> IngestResult:
        """Validate, rate-limit, dedupe and aggregate one report.

        Never raises for user errors; returns a classified IngestResult.
        """
        # Stage 1: validation/normalization (no state mutation on failure).
        try:
            reading = parse_report(payload)
        except ValidationIngestError as exc:
            logger.info(
                "rejected report validation input=%r code=%s reason=%s",
                payload,
                exc.code.value,
                exc.message,
            )
            self._counters["rejected"] += 1
            return IngestResult.rejected(exc.code, exc.message)
        except Exception:  # defensive: never leak internal exceptions
            logger.exception("unexpected failure while parsing report")
            self._counters["rejected"] += 1
            return IngestResult.rejected(
                ErrorCode.MALFORMED, "report could not be processed"
            )

        # Stage 2: rate limiting. A rejection consumes nothing.
        if not self._rate_limiter.acquire(reading.device_id):
            logger.info(
                "rate limited device=%s seq=%s available=%.2f",
                reading.device_id,
                reading.seq,
                self._rate_limiter.available(reading.device_id),
            )
            self._counters["rejected"] += 1
            return IngestResult.rejected(
                ErrorCode.RATE_LIMITED,
                f"rate limit exceeded for device '{reading.device_id}'",
            )

        # Stage 3: per-device sequence admission (atomic check + commit).
        admission = self._tracker.check_and_commit(reading)
        if admission.duplicate:
            logger.info(
                "duplicate device=%s seq=%s ts=%s (idempotent, not re-counted)",
                reading.device_id,
                reading.seq,
                reading.timestamp,
            )
            self._counters["duplicate"] += 1
            return IngestResult.duplicated(reading, admission.reason or "duplicate")
        if not admission.admitted:
            logger.info(
                "rejected device=%s seq=%s code=%s reason=%s",
                reading.device_id,
                reading.seq,
                admission.error_code.value if admission.error_code else "?",
                admission.reason,
            )
            self._counters["rejected"] += 1
            return IngestResult.rejected(
                admission.error_code or ErrorCode.CONFLICT,
                admission.reason or "rejected",
            )

        # Stage 4: fold into the aggregate window (atomic, exactly once).
        agg = self._windows.add_reading(reading)
        self._counters["accepted"] += 1
        logger.info(
            "accepted device=%s seq=%s ts=%s value=%s%s -> window [%s,%s) count=%d",
            reading.device_id,
            reading.seq,
            reading.timestamp,
            reading.value,
            reading.unit,
            agg.window_start,
            agg.window_end,
            agg.count,
        )
        return IngestResult.accepted(reading)

    def query_windows(
        self,
        device_id: str,
        metric: str,
        start: float | None = None,
        end: float | None = None,
    ) -> list[WindowAggregate]:
        if not isinstance(device_id, str) or not device_id.strip():
            raise IngestError(
                ErrorCode.QUERY_INVALID, "device_id is required", field="device_id"
            )
        if not isinstance(metric, str) or not metric.strip():
            raise IngestError(
                ErrorCode.QUERY_INVALID, "metric is required", field="metric"
            )
        now = time.time()
        q_start = 0.0 if start is None else start
        q_end = now if end is None else end
        if not isinstance(q_start, (int, float)) or not isinstance(q_end, (int, float)):
            raise IngestError(
                ErrorCode.QUERY_INVALID,
                "start/end must be epoch-second numbers",
            )
        if q_end < q_start:
            raise IngestError(
                ErrorCode.QUERY_INVALID,
                f"end ({q_end}) must be >= start ({q_start})",
            )
        return self._windows.query(device_id.strip(), metric.strip(), q_start, q_end)

    def device_status(self, device_id: str) -> dict:
        state = self._tracker.state_of(device_id)
        if state is None:
            raise IngestError(
                ErrorCode.DEVICE_NOT_FOUND,
                f"no data for device '{device_id}'",
                field="device_id",
            )
        return {
            "device_id": state.device_id,
            "latest_seq": state.max_seq,
            "latest_ts": state.latest_ts,
            "accepted_count": len(state.accepted),
            "available_tokens": self._rate_limiter.available(device_id),
        }

    def reset(self) -> None:
        self._windows.reset()
        self._tracker.reset()
        self._rate_limiter.reset()
        self._counters = {"accepted": 0, "duplicate": 0, "rejected": 0}
