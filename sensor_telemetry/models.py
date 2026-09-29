"""领域模型：读数、写入结果、窗口聚合。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class IngestStatus(str, Enum):
    #: 新读数已接收
    ACCEPTED = "ACCEPTED"
    #: 幂等命中：同一 (device_id, seq) 且内容完全一致，不重复计数
    DUPLICATE = "DUPLICATE"
    #: 被拒绝（原因见 IngestResult.error_code）
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class Reading:
    device_id: str
    seq: int
    metric: str
    value: float
    unit: str
    ts_ms: int  # epoch 毫秒

    @property
    def key(self) -> tuple[str, int]:
        """幂等键：(设备, 序号)。"""
        return (self.device_id, self.seq)

    def same_payload(self, other: Reading) -> bool:
        """除幂等键外的内容是否一致（用于区分重复与冲突）。"""
        return (
            self.metric == other.metric
            and self.ts_ms == other.ts_ms
            and self.unit == other.unit
            and self.value == other.value
        )


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    error_code: str | None = None
    message: str | None = None
    window_start_ms: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "error_code": self.error_code,
            "message": self.message,
            "window_start_ms": self.window_start_ms,
        }


@dataclass(frozen=True, slots=True)
class WindowAggregate:
    """固定窗口聚合。

    窗口边界约定为**左闭右开** ``[window_start_ms, window_end_ms)``：
    恰好落在起点的读数属于本窗口，恰好落在终点的读数属于下一窗口。
    """

    window_start_ms: int
    window_end_ms: int
    metric: str
    count: int
    min_value: float
    max_value: float
    sum_value: float

    @property
    def avg_value(self) -> float:
        return self.sum_value / self.count if self.count else 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "metric": self.metric,
            "count": self.count,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "sum_value": self.sum_value,
            "avg_value": self.avg_value,
        }
