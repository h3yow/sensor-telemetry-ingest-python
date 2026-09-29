"""测试辅助构造函数。"""

from __future__ import annotations


def make_reading(
    device_id="d1",
    seq=1,
    metric="temperature",
    value=20.0,
    unit="C",
    ts_ms=60_000,
):
    return {
        "device_id": device_id,
        "seq": seq,
        "metric": metric,
        "value": value,
        "unit": unit,
        "ts_ms": ts_ms,
    }
