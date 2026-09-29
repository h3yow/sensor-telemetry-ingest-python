"""错误归一化：所有可预期的拒绝原因都映射为可区分的错误码。

任何上报失败都以 :class:`TelemetryError` 的形式抛出，携带稳定的
:class:`ErrorCode`；适配层只认错误码，不向调用方透出内部堆栈。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """上报被拒绝的可区分原因（取值即对外稳定的错误码字符串）。"""

    #: 请求体不是对象 / 无法解析的结构异常
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    #: 缺少必填字段
    MISSING_FIELD = "MISSING_FIELD"
    #: 字段类型不符合协议（如 seq 为字符串、value 为对象）
    BAD_TYPE = "BAD_TYPE"
    #: 未知指标名
    UNKNOWN_METRIC = "UNKNOWN_METRIC"
    #: 指标与单位不一致
    UNIT_MISMATCH = "UNIT_MISMATCH"
    #: 数值超出该指标允许范围
    VALUE_OUT_OF_RANGE = "VALUE_OUT_OF_RANGE"
    #: 时间戳非法（非整数毫秒、超出合理范围）
    TIMESTAMP_INVALID = "TIMESTAMP_INVALID"
    #: 序号相对已接收序号回退
    SEQ_REGRESSED = "SEQ_REGRESSED"
    #: 同一 (device, seq) 已存在，但内容与本次冲突
    SEQ_CONFLICT = "SEQ_CONFLICT"
    #: 序号未回退，但时间戳早于允许的迟到下界
    TOO_LATE = "TOO_LATE"
    #: 触发每设备限流（背压保护）
    RATE_LIMITED = "RATE_LIMITED"


# 错误码到对外 HTTP 状态码的稳定映射
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.MALFORMED_PAYLOAD: 400,
    ErrorCode.MISSING_FIELD: 422,
    ErrorCode.BAD_TYPE: 422,
    ErrorCode.UNKNOWN_METRIC: 422,
    ErrorCode.UNIT_MISMATCH: 422,
    ErrorCode.VALUE_OUT_OF_RANGE: 422,
    ErrorCode.TIMESTAMP_INVALID: 422,
    ErrorCode.SEQ_REGRESSED: 409,
    ErrorCode.SEQ_CONFLICT: 409,
    ErrorCode.TOO_LATE: 409,
    ErrorCode.RATE_LIMITED: 429,
}


class TelemetryError(Exception):
    """归一化后的业务错误，不向调用方透出内部异常细节。"""

    def __init__(self, code: ErrorCode, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field

    def to_dict(self) -> dict[str, str | None]:
        return {"error_code": self.code.value, "message": self.message, "field": self.field}
