"""编排：校验 -> 限流 -> 设备串行判定 -> 存储与窗口更新。

顺序设计：先做无副作用的结构/语义校验（非法报文不消耗令牌）；
然后在设备锁内完成限流检查与全部状态判定/写入（锁顺序固定为
设备锁 -> 窗口锁），因此：

- 并发上报同一 (device, seq) 只有一个被接受，其余幂等重复，不重复计数；
- 重复上报：内容一致 -> DUPLICATE；内容冲突 -> SEQ_CONFLICT 拒绝；
- 序号回退：未见过的新序号比 max_seq 落后超过 max_seq_gap -> SEQ_REGRESSED；
  落后不超过跨度的视为乱序补缺，正常接收（并发提交天然乱序）；
- 迟到但允许（不超过 max_lateness_ms）-> 接受，窗口只更新这一次；
- 迟到超限 -> TOO_LATE 拒绝；
- 限流在任何状态变更前生效 -> RATE_LIMITED，已接收数据不受影响。
"""

from __future__ import annotations

import logging
from typing import Any

from .config import METRIC_UNITS, VALUE_RANGES, Settings
from .errors import ErrorCode, TelemetryError
from .models import IngestResult, IngestStatus, Reading, WindowAggregate
from .rate_limiter import PerDeviceRateLimiter
from .storage import LocalStore
from .validator import PayloadValidator
from .windows import WindowManager

logger = logging.getLogger("sensor_telemetry.service")


class TelemetryService:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._store = LocalStore(self._settings.log_path)
        self._windows = WindowManager(self._settings.window_size_ms, self._store)
        self._validator = PayloadValidator(METRIC_UNITS, VALUE_RANGES)
        self._limiter = PerDeviceRateLimiter(
            self._settings.rate_capacity,
            self._settings.rate_refill_per_sec,
        )
        # 启动恢复：按接受顺序重放日志，重建窗口聚合
        replayed = self._store.replayed_readings()
        if replayed:
            self._windows.rebuild_from(replayed)
            logger.info("startup replay restored %d readings", len(replayed))

    # ---- 写入 -----------------------------------------------------------

    def ingest(self, payload: Any) -> IngestResult:
        # 1) 结构与语义校验（归一化错误，不让内部异常透出）
        try:
            reading = self._validator.validate(payload)
        except TelemetryError as exc:
            logger.info("reject input=%r reason=%s detail=%s", payload, exc.code.value, exc.message)
            return self._rejected(exc)

        # 2) 同一设备串行化；锁内完成限流检查与全部状态判定/写入
        with self._store.lock_for(reading.device_id):
            if not self._limiter.allow(reading.device_id):
                msg = (
                    f"设备 {reading.device_id} 触发限流 "
                    f"(容量={self._settings.rate_capacity}, "
                    f"补充速率={self._settings.rate_refill_per_sec}/s)"
                )
                logger.info(
                    "reject device=%s seq=%s reason=%s",
                    reading.device_id, reading.seq, ErrorCode.RATE_LIMITED.value,
                )
                return IngestResult(IngestStatus.REJECTED, ErrorCode.RATE_LIMITED.value, msg)

            existing = self._store.get_received(reading.device_id, reading.seq)
            if existing is not None:
                # 同一幂等键已存在：内容一致才算重复，否则是冲突
                if existing.same_payload(reading):
                    logger.info(
                        "duplicate device=%s seq=%s -> idempotent accept",
                        reading.device_id, reading.seq,
                    )
                    return IngestResult(
                        IngestStatus.DUPLICATE,
                        window_start_ms=self._windows.window_start_for(reading.ts_ms),
                    )
                logger.info(
                    "reject device=%s seq=%s reason=%s existing=%r incoming=%r",
                    reading.device_id, reading.seq, ErrorCode.SEQ_CONFLICT.value,
                    existing, reading,
                )
                return IngestResult(
                    IngestStatus.REJECTED,
                    ErrorCode.SEQ_CONFLICT.value,
                    f"序号 {reading.seq} 已存在但内容与本次上报不一致",
                )

            max_seq = self._store.max_seq(reading.device_id)
            gap = max_seq - reading.seq
            if gap > self._settings.max_seq_gap:
                # 未见过的序号落后水位线过多：真正的序号回退/异常重放
                logger.info(
                    "reject device=%s seq=%s reason=%s max_seq=%s gap=%s",
                    reading.device_id, reading.seq,
                    ErrorCode.SEQ_REGRESSED.value, max_seq, gap,
                )
                return IngestResult(
                    IngestStatus.REJECTED,
                    ErrorCode.SEQ_REGRESSED.value,
                    (
                        f"序号回退：收到 {reading.seq}，设备最大已接收序号 {max_seq}，"
                        f"落后 {gap} 超过允许跨度 {self._settings.max_seq_gap}"
                    ),
                )

            max_ts = self._store.max_ts_ms(reading.device_id)
            if max_ts is not None:
                lateness = max_ts - reading.ts_ms
                if lateness > self._settings.max_lateness_ms:
                    logger.info(
                        "reject device=%s seq=%s reason=%s ts=%s watermark=%s lateness=%sms",
                        reading.device_id, reading.seq, ErrorCode.TOO_LATE.value,
                        reading.ts_ms, max_ts, lateness,
                    )
                    return IngestResult(
                        IngestStatus.REJECTED,
                        ErrorCode.TOO_LATE.value,
                        (
                            f"迟到超限：ts={reading.ts_ms} 落后水位线 {max_ts} "
                            f"共 {lateness}ms，超过允许的 {self._settings.max_lateness_ms}ms"
                        ),
                    )

            # 3) 通过：先落存储，再更新窗口；窗口替换是原子的
            self._store.insert(reading)
            agg = self._windows.add_reading(reading)
            logger.info(
                "accept device=%s seq=%s metric=%s value=%s ts=%s window=[%s,%s) count=%s",
                reading.device_id, reading.seq, reading.metric, reading.value,
                reading.ts_ms, agg.window_start_ms, agg.window_end_ms, agg.count,
            )
            return IngestResult(
                IngestStatus.ACCEPTED,
                window_start_ms=agg.window_start_ms,
            )

    # ---- 查询 -----------------------------------------------------------

    def query_windows(self, metric: str, start_ms: int, end_ms: int) -> list[WindowAggregate]:
        return self._windows.query(metric, start_ms, end_ms)

    def all_readings(self) -> list[Reading]:
        return self._store.all_readings()

    @staticmethod
    def _rejected(exc: TelemetryError) -> IngestResult:
        return IngestResult(IngestStatus.REJECTED, exc.code.value, exc.message)
