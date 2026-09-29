"""默认配置：单位、量程、迟到容忍度、限流参数。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    #: 聚合窗口大小（毫秒）
    window_size_ms: int = 60_000
    #: 允许的最大迟到时间（毫秒）：ts 落后设备水位线超过此值即拒
    max_lateness_ms: int = 300_000
    #: 允许的序号回补跨度：新 seq 与 max_seq 的差距在此范围内视为乱序补缺
    max_seq_gap: int = 1024
    #: 每设备令牌桶容量（突发上限）
    rate_capacity: float = 10.0
    #: 每设备令牌桶补充速率（令牌/秒）
    rate_refill_per_sec: float = 5.0
    #: JSONL 追加日志路径；None 表示纯内存
    log_path: str | None = None


METRIC_UNITS: dict[str, str] = {
    "temperature": "C",
    "pressure": "kPa",
    "humidity": "%",
}

VALUE_RANGES: dict[str, tuple[float, float]] = {
    "temperature": (-50.0, 100.0),
    "pressure": (0.0, 500.0),
    "humidity": (0.0, 100.0),
}
