"""每设备令牌桶限流，提供背压保护。

- 每个 device_id 独立一个令牌桶，互不影响；
- 桶内操作在锁内完成，多线程并发上报不会超发；
- 被限流的请求在进入业务判定前直接拒绝，不会触碰已接收数据。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Callable


class TokenBucket:
    """标准令牌桶：按固定速率补充，容量封顶。"""

    __slots__ = ("capacity", "refill_per_sec", "_tokens", "_last_refill", "_lock", "_clock")

    def __init__(
        self,
        capacity: float,
        refill_per_sec: float,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if refill_per_sec <= 0:
            raise ValueError("refill_per_sec must be positive")
        self.capacity = float(capacity)
        self.refill_per_sec = float(refill_per_sec)
        self._tokens = float(capacity)
        self._clock = clock or time.monotonic
        self._last_refill = self._clock()
        self._lock = threading.Lock()

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """尝试取走 ``tokens`` 个令牌；不足时原样返回 False，不预支不欠账。"""
        with self._lock:
            now = self._clock()
            elapsed = now - self._last_refill
            if elapsed > 0:
                self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_sec)
                self._last_refill = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False


class PerDeviceRateLimiter:
    """懒创建设备桶，并用一把元锁保护桶字典本身。"""

    def __init__(self, capacity: float, refill_per_sec: float) -> None:
        self._capacity = capacity
        self._refill = refill_per_sec
        self._buckets: dict[str, TokenBucket] = {}
        self._meta_lock = threading.Lock()

    def _bucket_for(self, device_id: str) -> TokenBucket:
        bucket = self._buckets.get(device_id)
        if bucket is None:
            with self._meta_lock:
                bucket = self._buckets.get(device_id)
                if bucket is None:
                    bucket = TokenBucket(self._capacity, self._refill)
                    self._buckets[device_id] = bucket
        return bucket

    def allow(self, device_id: str) -> bool:
        return self._bucket_for(device_id).try_acquire(1.0)
