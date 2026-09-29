"""FastAPI 适配层。

- POST /readings          上报一条读数
- GET  /metrics/{m}/windows?start_ms&end_ms  查询窗口聚合
- GET  /healthz           存活检查

业务失败统一返回 ``{"error_code", "message", "field"}`` 与可区分的
HTTP 状态码（见 errors.HTTP_STATUS）；任何未预期异常也被兜底为
INTERNAL 500，不向前端泄露堆栈。
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import HTTP_STATUS, TelemetryError
from .service import TelemetryService

logger = logging.getLogger("sensor_telemetry.api")


def create_app(service: TelemetryService | None = None) -> FastAPI:
    svc = service or TelemetryService()
    app = FastAPI(title="sensor-telemetry-ingest", version="0.1.0")

    @app.post("/readings")
    async def ingest_reading(request: Request) -> JSONResponse:
        # 手动解析，保证坏 JSON 也归一化为 MALFORMED_PAYLOAD，而不是框架默认 422
        try:
            payload: Any = await request.json()
        except Exception:
            return JSONResponse(
                status_code=400,
                content={
                    "error_code": "MALFORMED_PAYLOAD",
                    "message": "请求体不是合法 JSON",
                    "field": None,
                },
            )
        try:
            result = svc.ingest(payload)
        except TelemetryError as exc:  # 理论上 service 已内部归一化，双保险
            return JSONResponse(status_code=HTTP_STATUS[exc.code], content=exc.to_dict())
        except Exception:  # noqa: BLE001 - 兜底：内部异常不外泄
            logger.exception("unexpected internal error")
            return JSONResponse(
                status_code=500,
                content={"error_code": "INTERNAL", "message": "服务内部错误", "field": None},
            )

        if result.status.value == "REJECTED":
            from .errors import ErrorCode

            code = result.error_code or "INTERNAL"
            try:
                status = HTTP_STATUS[ErrorCode(code)]
            except ValueError:
                status = 400
            return JSONResponse(
                status_code=status,
                content={"error_code": code, "message": result.message, "field": None},
            )

        return JSONResponse(status_code=200, content=result.to_dict())

    @app.get("/metrics/{metric}/windows")
    async def query_windows(metric: str, start_ms: int, end_ms: int) -> dict[str, object]:
        rows = svc.query_windows(metric, start_ms, end_ms)
        return {
            "metric": metric,
            "range": [start_ms, end_ms],
            "window_count": len(rows),
            "windows": [row.to_dict() for row in rows],
        }

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
