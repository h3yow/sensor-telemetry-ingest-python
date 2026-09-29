"""HTTP route definitions for local telemetry ingestion and queries."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .errors import HTTP_STATUS, ErrorCode
from .errors import IngestError
from .models import IngestStatus
from .service import IngestService

logger = logging.getLogger("sensor_telemetry")


def build_router(service: IngestService) -> APIRouter:
    router = APIRouter()

    @router.post("/api/v1/reports")
    async def ingest_report(request: Request) -> JSONResponse:
        # Parse JSON defensively: malformed JSON must normalize to MALFORMED,
        # never surface as a framework 500.
        try:
            payload = await request.json()
        except Exception:
            logger.info("rejected report: body is not valid JSON")
            return JSONResponse(
                status_code=HTTP_STATUS[ErrorCode.MALFORMED],
                content={
                    "status": IngestStatus.REJECTED.value,
                    "error_code": "MALFORMED",
                    "reason": "request body is not valid JSON",
                },
            )

        result = service.ingest(payload)
        status_code = 200
        if result.status is IngestStatus.REJECTED:
            code = result.error_code
            status_code = HTTP_STATUS[code] if code else 400
        body = result.to_dict()
        return JSONResponse(status_code=status_code, content=body)

    @router.get("/api/v1/devices/{device_id}/metrics/{metric}/windows")
    async def query_windows(
        device_id: str,
        metric: str,
        start: float | None = None,
        end: float | None = None,
    ) -> JSONResponse:
        try:
            windows = service.query_windows(device_id, metric, start, end)
        except IngestError as exc:
            return JSONResponse(
                status_code=HTTP_STATUS[exc.code],
                content={
                    "status": IngestStatus.REJECTED.value,
                    "error_code": exc.code.value,
                    "reason": exc.message,
                },
            )
        return JSONResponse(
            content={
                "device_id": device_id,
                "metric": metric,
                "window_seconds": service.window_width,
                "boundary": "[start, end)",
                "windows": [w.to_dict() for w in windows],
            }
        )

    @router.get("/api/v1/devices/{device_id}")
    async def device_status(device_id: str) -> JSONResponse:
        try:
            status = service.device_status(device_id)
        except IngestError as exc:
            return JSONResponse(
                status_code=HTTP_STATUS[exc.code],
                content={
                    "error_code": exc.code.value,
                    "reason": exc.message,
                },
            )
        return JSONResponse(content=status)

    @router.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    return router
