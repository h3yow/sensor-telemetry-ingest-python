"""本地存储：内存索引 + 追加日志（JSONL），按设备加锁保证并发安全。

- 每台设备一把锁，同一设备的“判定 + 写入”在服务层持锁完成，
  因此并发上报不会出现重复计数或状态错乱；不同设备互不阻塞。
- 已接收的读数按接受顺序追加到 JSONL 日志；重启时按序重放，
  内存状态与窗口可完整重建（用本地持久化模拟设备侧持续上报）。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from .models import Reading, WindowAggregate


class DeviceState:
    """单台设备的接收状态：已接受序号 -> 读数、最大序号与时间戳水位线。"""

    __slots__ = ("seen", "max_seq", "max_ts_ms")

    def __init__(self) -> None:
        self.seen: dict[int, Reading] = {}
        self.max_seq: int = -1
        self.max_ts_ms: int | None = None


class LocalStore:
    def __init__(self, log_path: str | Path | None = None) -> None:
        self._states: dict[str, DeviceState] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._log_path: Path | None = Path(log_path) if log_path else None
        self._log_fh = None
        self._replayed: list[Reading] = []
        if self._log_path is not None:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._replayed = self._replay_log()
            self._log_fh = self._log_path.open("a", encoding="utf-8")

    # ---- 并发原语 -------------------------------------------------------

    def lock_for(self, device_id: str) -> threading.Lock:
        """返回设备专属锁；懒创建，元锁只保护字典本身。"""
        lock = self._locks.get(device_id)
        if lock is None:
            with self._meta_lock:
                lock = self._locks.get(device_id)
                if lock is None:
                    lock = threading.Lock()
                    self._locks[device_id] = lock
        return lock

    def _state_for(self, device_id: str) -> DeviceState:
        state = self._states.get(device_id)
        if state is None:
            # 新建设备状态属于结构变更，放元锁内，避免与快照遍历并发 resize
            with self._meta_lock:
                state = self._states.get(device_id)
                if state is None:
                    state = DeviceState()
                    self._states[device_id] = state
        return state

    # ---- 查询（调用方持有设备锁）----------------------------------------

    def is_duplicate(self, device_id: str, seq: int) -> bool:
        state = self._states.get(device_id)
        return state is not None and seq in state.seen

    def get_received(self, device_id: str, seq: int) -> Reading | None:
        state = self._states.get(device_id)
        if state is None:
            return None
        return state.seen.get(seq)

    def max_seq(self, device_id: str) -> int:
        state = self._states.get(device_id)
        return state.max_seq if state is not None else -1

    def max_ts_ms(self, device_id: str) -> int | None:
        """设备已接收读数中的最大时间戳（迟到判定水位线）。"""
        state = self._states.get(device_id)
        return state.max_ts_ms if state is not None else None

    # ---- 写入（调用方持有设备锁）----------------------------------------

    def insert(self, reading: Reading) -> None:
        state = self._state_for(reading.device_id)
        state.seen[reading.seq] = reading
        if reading.seq > state.max_seq:
            state.max_seq = reading.seq
        if state.max_ts_ms is None or reading.ts_ms > state.max_ts_ms:
            state.max_ts_ms = reading.ts_ms
        self._append_log(
            {
                "type": "reading",
                "device_id": reading.device_id,
                "seq": reading.seq,
                "metric": reading.metric,
                "value": reading.value,
                "unit": reading.unit,
                "ts_ms": reading.ts_ms,
            }
        )

    def append_window(self, agg: WindowAggregate) -> None:
        """窗口变更审计记录（只用于可解释性，重放时以读数为准重建）。"""
        if self._log_path is None:
            return
        self._append_log(
            {
                "type": "window",
                "window_start_ms": agg.window_start_ms,
                "window_end_ms": agg.window_end_ms,
                "metric": agg.metric,
                "count": agg.count,
                "min_value": agg.min_value,
                "max_value": agg.max_value,
                "sum_value": agg.sum_value,
            }
        )

    def all_readings(self) -> list[Reading]:
        """快照式列出全部已接收读数（测试/诊断用）。

        元锁内只拷贝设备列表（避免与新建设备的结构变更竞争），随后释放元锁，
        再逐台获取设备锁读取——锁顺序始终是“设备锁 -> 元锁”，不会与
        insert 路径形成 AB-BA 死锁。
        """
        with self._meta_lock:
            devices = list(self._states.items())
        out: list[Reading] = []
        for device_id, state in devices:
            with self.lock_for(device_id):
                out.extend(state.seen.values())
        return out

    def replayed_readings(self) -> list[Reading]:
        """启动时从日志重放出来的读数（按接受顺序）。"""
        return list(self._replayed)

    # ---- 日志 -----------------------------------------------------------

    def _append_log(self, record: dict[str, object]) -> None:
        if self._log_fh is None:
            return
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._log_lock:
            self._log_fh.write(line + "\n")
            self._log_fh.flush()

    def _replay_log(self) -> list[Reading]:
        if not self._log_path or not self._log_path.exists():
            return []
        readings: list[Reading] = []
        with self._log_path.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # 最后一行可能因崩溃截断；跳过坏行而不是让服务起不来
                    continue
                if record.get("type") != "reading":
                    continue
                reading = Reading(
                    device_id=record["device_id"],
                    seq=int(record["seq"]),
                    metric=record["metric"],
                    value=float(record["value"]),
                    unit=record["unit"],
                    ts_ms=int(record["ts_ms"]),
                )
                state = self._state_for(reading.device_id)
                state.seen[reading.seq] = reading
                if reading.seq > state.max_seq:
                    state.max_seq = reading.seq
                if state.max_ts_ms is None or reading.ts_ms > state.max_ts_ms:
                    state.max_ts_ms = reading.ts_ms
                readings.append(reading)
        return readings

    def close(self) -> None:
        if self._log_fh is not None:
            with self._log_lock:
                self._log_fh.close()
                self._log_fh = None
