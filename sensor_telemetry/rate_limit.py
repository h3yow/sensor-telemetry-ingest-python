"""Per-device rate limiting (token bucket) with thread-safe admission control.

A rejected (rate-limited) request never consumes a token, so overload cannot
destroy capacity that already-accepted data depends on.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """Token bucket configuration.

    capacity:     maximum burst size (tokens)
    refill_rate:  tokens added per second
    """

    capacity: float = 100.0
    refill_rate: float = 50.0


@dataclass(slots=True)
class _Bucket:
    tokens: float
    last_refill: float


class RateLimiter:
    """One lazily-created token bucket per device, each guarded by its own lock."""

    def __init__(self, config: RateLimitConfig | None = None) -> None:
        self._config = config or RateLimitConfig()
        self._buckets: dict[str, tuple[threading.Lock, _Bucket]] = {}
        self._registry_lock = threading.Lock()

    def _get_bucket(self, device_id: str, now: float) -> tuple[threading.Lock, _Bucket]:
        bucket = self._buckets.get(device_id)
        if bucket is not None:
            return bucket
        with self._registry_lock:
            bucket = self._buckets.get(device_id)
            if bucket is None:
                bucket = (
                    threading.Lock(),
                    _Bucket(tokens=self._config.capacity, last_refill=now),
                )
                self._buckets[device_id] = bucket
            return bucket

    def acquire(self, device_id: str, *, now: float | None = None) -> bool:
        """Consume one token if available.

        Returns True when admitted, False when over capacity. On rejection the
        bucket state is left untouched apart from time-based refill.
        """
        current = time.monotonic() if now is None else now
        lock, bucket = self._get_bucket(device_id, current)
        with lock:
            self._refill(bucket, current)
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True
            return False

    def _refill(self, bucket: _Bucket, now: float) -> None:
        elapsed = max(0.0, now - bucket.last_refill)
        if elapsed <= 0.0:
            return
        bucket.tokens = min(
            self._config.capacity,
            bucket.tokens + elapsed * self._config.refill_rate,
        )
        bucket.last_refill = now

    def available(self, device_id: str, *, now: float | None = None) -> float:
        current = time.monotonic() if now is None else now
        lock, bucket = self._get_bucket(device_id, current)
        with lock:
            self._refill(bucket, current)
            return bucket.tokens

    def reset(self) -> None:
        with self._registry_lock:
            self._buckets.clear()
