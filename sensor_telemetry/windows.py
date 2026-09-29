"""固定时间窗口聚合。

边界约定
--------
窗口一律 **左闭右开** ``[start, end)``，起点通过整数除法对齐::

    start = (ts_ms // window_size_ms) * window_size_ms

因此 ``ts_ms == end`` 的读数属于下一个窗口，边界行为稳定、不依赖四舍五入。

并发与迟到语义
--------------
- 每个 (metric, window_start) 一把锁；“读旧值 -> 合并 -> 写新值”全程持锁，
  边写边查只能看到更新前或更新后的完整窗口，读不到半更新状态。
- 读数只有首次被接受时才进入聚合；重复上报在服务层被幂等吞掉，
  因此同一读数对迟到落入已存在窗口的更新**只生效一次**。
"""

from __future__ import annotations

import threading

from .models import Reading, WindowAggregate
from .storage import LocalStore


class WindowManager:
    def __init__(self, window_size_ms: int, store: LocalStore) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        self._size = window_size_ms
        self._store = store
        # 维度键: (metric, window_start_ms) -> 聚合
        self._windows: dict[tuple[str, int], WindowAggregate] = {}
        self._locks: dict[tuple[str, int], threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def window_start_for(self, ts_ms: int) -> int:
        return (ts_ms // self._size) * self._size

    def _lock_for(self, key: tuple[str, int]) -> threading.Lock:
        lock = self._locks.get(key)
        if lock is None:
            with self._meta_lock:
                lock = self._locks.get(key)
                if lock is None:
                    lock = threading.Lock()
                    self._locks[key] = lock
        return lock

    def add_reading(self, reading: Reading) -> WindowAggregate:
        """把一条**首次接受**的读数并入窗口，返回更新后的完整聚合。"""
        start = self.window_start_for(reading.ts_ms)
        key = (reading.metric, start)
        # 新键的插入必须在元锁内完成，避免与 query 的快照遍历并发时 dict resize；
        # 已存在的键只做值替换（不改变 dict 结构），拿设备锁后直接更新即可。
        with self._lock_for(key):
            with self._meta_lock:
                old = self._windows.get(key)
                if old is None:
                    agg = WindowAggregate(
                        window_start_ms=start,
                        window_end_ms=start + self._size,
                        metric=reading.metric,
                        count=1,
                        min_value=reading.value,
                        max_value=reading.value,
                        sum_value=reading.value,
                    )
                    self._windows[key] = agg
            if old is not None:
                agg = WindowAggregate(
                    window_start_ms=old.window_start_ms,
                    window_end_ms=old.window_end_ms,
                    metric=old.metric,
                    count=old.count + 1,
                    min_value=min(old.min_value, reading.value),
                    max_value=max(old.max_value, reading.value),
                    sum_value=old.sum_value + reading.value,
                )
                # 整体替换不可变对象：查询方拿到的永远是完整快照
                self._windows[key] = agg
        self._store.append_window(agg)
        return agg

    def get_window(self, metric: str, window_start_ms: int) -> WindowAggregate | None:
        return self._windows.get((metric, window_start_ms))

    def query(self, metric: str, start_ms: int, end_ms: int) -> list[WindowAggregate]:
        """返回与 ``[start_ms, end_ms)`` 相交、且已有数据的窗口，按起点升序。"""
        if end_ms <= start_ms:
            return []
        # 元锁内取快照：遍历时即使有新窗口键插入也不会读到半更新/触发 resize
        with self._meta_lock:
            snapshot = [agg for (m, _), agg in self._windows.items() if m == metric]
        out: list[WindowAggregate] = []
        for agg in snapshot:
            w_end = agg.window_end_ms
            if agg.window_start_ms < end_ms and w_end > start_ms:
                out.append(agg)
        out.sort(key=lambda a: a.window_start_ms)
        return out

    def rebuild_from(self, readings: list[Reading]) -> None:
        """按接受顺序重放读数重建窗口（仅供启动恢复调用）。"""
        for reading in readings:
            self.add_reading(reading)
