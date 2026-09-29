"""Fixed time-window aggregation with atomic window updates.

Boundary convention (stable and documented):

* Windows are half-open intervals ``[start, end)``: left endpoint included,
  right endpoint excluded.
* Window 0 starts at ``epoch_origin`` (default 1970-01-01T00:00:00Z); for a
  reading at timestamp ``t``::

      index = floor((t - origin) / width)
      start = origin + index * width
      end   = start + width

* A late reading that belongs to an already-existing window folds into that
  window exactly once (reading admission upstream guarantees one fold per
  unique reading). Each fold is atomic: concurrent queries either see the
  aggregate before or after the fold, never a half-updated window.
"""

from __future__ import annotations

import math
import threading

from .models import Reading, WindowAggregate

DEFAULT_EPOCH_ORIGIN = 0.0


class WindowStore:
    """Thread-safe store of fixed-window aggregates keyed by
    (device_id, metric, window_start).

    A single store lock serializes folds and snapshots query results. Under
    contention this trades a little throughput for a simple, strong guarantee:
    readers can never observe a partially updated window.
    """

    def __init__(
        self,
        window_width_seconds: float,
        epoch_origin: float = DEFAULT_EPOCH_ORIGIN,
    ) -> None:
        if window_width_seconds <= 0:
            raise ValueError("window_width_seconds must be positive")
        self._width = float(window_width_seconds)
        self._origin = float(epoch_origin)
        self._windows: dict[tuple[str, str, float], WindowAggregate] = {}
        self._lock = threading.Lock()

    @property
    def width(self) -> float:
        return self._width

    def window_for(
        self,
        timestamp: float,
        width: float | None = None,
        origin: float | None = None,
    ) -> tuple[float, float]:
        """Return the ``[start, end)`` window containing ``timestamp``."""
        w = self._width if width is None else width
        o = self._origin if origin is None else origin
        index = math.floor((timestamp - o) / w)
        start = o + index * w
        return start, start + w

    def add_reading(self, reading: Reading) -> WindowAggregate:
        """Atomically fold one reading into its window; return new aggregate."""
        start, end = self.window_for(reading.timestamp)
        key = (reading.device_id, reading.metric, start)
        with self._lock:
            agg = self._windows.get(key)
            if agg is None:
                agg = WindowAggregate(
                    device_id=reading.device_id,
                    metric=reading.metric,
                    window_start=start,
                    window_end=end,
                    count=1,
                    min=reading.value,
                    max=reading.value,
                    sum=reading.value,
                    first_ts=reading.timestamp,
                    last_ts=reading.timestamp,
                )
            else:
                agg = WindowAggregate(
                    device_id=agg.device_id,
                    metric=agg.metric,
                    window_start=agg.window_start,
                    window_end=agg.window_end,
                    count=agg.count + 1,
                    min=min(agg.min, reading.value),
                    max=max(agg.max, reading.value),
                    sum=agg.sum + reading.value,
                    first_ts=min(agg.first_ts, reading.timestamp),
                    last_ts=max(agg.last_ts, reading.timestamp),
                )
            self._windows[key] = agg
            return agg

    def query(
        self,
        device_id: str,
        metric: str,
        start: float,
        end: float,
    ) -> list[WindowAggregate]:
        """Return stored windows whose [window_start, window_end) interval
        overlaps [start, end), ordered by window_start ascending.

        Because WindowAggregate is immutable and the list is materialized
        while holding the store lock, callers get a consistent point-in-time
        snapshot even while folds are in progress.
        """
        if end < start:
            raise ValueError("query end must be >= start")
        with self._lock:
            candidates = [
                agg
                for (dev, met, _wstart), agg in self._windows.items()
                if dev == device_id
                and met == metric
                and agg.window_start < end
                and agg.window_end > start
            ]
        candidates.sort(key=lambda a: a.window_start)
        return candidates

    def all_for_device(self, device_id: str) -> list[WindowAggregate]:
        with self._lock:
            result = [
                agg
                for (dev, _met, _w), agg in self._windows.items()
                if dev == device_id
            ]
        result.sort(key=lambda a: (a.metric, a.window_start))
        return result

    def reset(self) -> None:
        with self._lock:
            self._windows.clear()
