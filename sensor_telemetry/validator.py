"""上报内容校验与错误归一化。

校验顺序固定，保证错误类型稳定可解释：
1. 结构必须是 JSON 对象；
2. 必填字段齐全（device_id / seq / metric / value / unit / ts_ms）；
3. 字段类型正确（bool 不被当作 int）；
4. 指标必须已知；
5. 单位必须与指标约定一致；
6. 数值必须落在量程内；
7. 时间戳必须是合法的 epoch 毫秒（非负且不超上界）。

任何一步失败都抛 :class:`TelemetryError`，不允许内部异常（TypeError 等）透出。
"""

from __future__ import annotations

import math
from typing import Any

from .errors import ErrorCode, TelemetryError
from .models import Reading

_REQUIRED_FIELDS = ("device_id", "seq", "metric", "value", "unit", "ts_ms")
# 允许的 epoch 毫秒上界：约公元 10000 年，只用于拦截明显的结构错误
_MAX_TS_MS = 253_402_300_799_999


class PayloadValidator:
    def __init__(
        self,
        metric_units: dict[str, str],
        value_ranges: dict[str, tuple[float, float]],
    ) -> None:
        self._metric_units = metric_units
        self._value_ranges = value_ranges

    def validate(self, payload: Any) -> Reading:
        if not isinstance(payload, dict):
            raise TelemetryError(
                ErrorCode.MALFORMED_PAYLOAD,
                "请求体必须是 JSON 对象",
            )

        for name in _REQUIRED_FIELDS:
            if name not in payload or payload[name] is None:
                raise TelemetryError(
                    ErrorCode.MISSING_FIELD,
                    f"缺少必填字段: {name}",
                    field=name,
                )

        device_id = self._check_str(payload, "device_id")
        if not device_id:
            raise TelemetryError(ErrorCode.MISSING_FIELD, "device_id 不能为空", field="device_id")
        seq = self._check_int(payload, "seq")
        if seq < 0:
            raise TelemetryError(ErrorCode.BAD_TYPE, "seq 必须是非负整数", field="seq")
        metric = self._check_str(payload, "metric")
        unit = self._check_str(payload, "unit")
        value = self._check_number(payload, "value")
        ts_ms = self._check_int(payload, "ts_ms")

        if metric not in self._metric_units:
            raise TelemetryError(
                ErrorCode.UNKNOWN_METRIC,
                f"未知指标: {metric!r}",
                field="metric",
            )

        expected_unit = self._metric_units[metric]
        if unit != expected_unit:
            raise TelemetryError(
                ErrorCode.UNIT_MISMATCH,
                f"指标 {metric!r} 的单位必须是 {expected_unit!r}，收到 {unit!r}",
                field="unit",
            )

        low, high = self._value_ranges[metric]
        if not (low <= value <= high):
            raise TelemetryError(
                ErrorCode.VALUE_OUT_OF_RANGE,
                f"{metric} 读数 {value} 超出允许范围 [{low}, {high}]",
                field="value",
            )

        if not (0 <= ts_ms <= _MAX_TS_MS):
            raise TelemetryError(
                ErrorCode.TIMESTAMP_INVALID,
                f"ts_ms 必须是 [0, {_MAX_TS_MS}] 内的 epoch 毫秒",
                field="ts_ms",
            )

        return Reading(
            device_id=device_id,
            seq=seq,
            metric=metric,
            value=value,
            unit=unit,
            ts_ms=ts_ms,
        )

    @staticmethod
    def _check_str(payload: dict[str, Any], field: str) -> str:
        raw = payload[field]
        if not isinstance(raw, str):
            raise TelemetryError(ErrorCode.BAD_TYPE, f"{field} 必须是字符串", field=field)
        return raw

    @staticmethod
    def _check_int(payload: dict[str, Any], field: str) -> int:
        raw = payload[field]
        # bool 是 int 的子类，必须显式排除
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise TelemetryError(ErrorCode.BAD_TYPE, f"{field} 必须是整数", field=field)
        return raw

    @staticmethod
    def _check_number(payload: dict[str, Any], field: str) -> float:
        raw = payload[field]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise TelemetryError(ErrorCode.BAD_TYPE, f"{field} 必须是数值", field=field)
        value = float(raw)
        if not math.isfinite(value):
            raise TelemetryError(
                ErrorCode.BAD_TYPE,
                f"{field} 必须是有限数值（不能是 NaN/Infinity）",
                field=field,
            )
        return value
