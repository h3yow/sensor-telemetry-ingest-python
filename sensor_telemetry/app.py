"""FastAPI application exposing the local telemetry ingestion API."""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .api import build_router
from .errors import HTTP_STATUS, IngestError
from .config import load_settings
from .service import IngestService

logger = logging.getLogger("sensor_telemetry")

def create_app(service: IngestService | None = None) -> FastAPI:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app = FastAPI(
        title="Local Sensor Telemetry Ingest",
        version="0.1.0",
    )
    settings = load_settings()
    svc = service or IngestService(
        window_width_seconds=settings.window_seconds,
        max_lateness_seconds=settings.max_lateness_seconds,
        rate_limit=settings.rate_limit_config,
    )
    app.state.service = svc
    register_exception_handlers(app)
    app.include_router(build_router(svc))

    @app.get("/")
    def root() -> dict:
        return {
            "service": "sensor-telemetry-ingest",
            "window_seconds": svc.window_width,
            "max_lateness_seconds": svc.max_lateness,
            "endpoints": [
                "POST /api/v1/reports",
                "GET /api/v1/devices/{id}/metrics/{metric}/windows",
                "GET /api/v1/devices/{id}",
                "GET /health",
            ],
        }

    return app


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(IngestError)
    async def ingest_error_handler(_request: Request, exc: IngestError) -> JSONResponse:
        return JSONResponse(
            status_code=HTTP_STATUS[exc.code],
            content={
                "error_code": exc.code.value,
                "reason": exc.message,
                "field": exc.field,
            },
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        # Last-resort normalization: clients must never see a raw 500 body.
        logger.exception("unhandled error: %s", exc)
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "INTERNAL_ERROR",
                "reason": "internal error; request rejected without side effects",
            },
        )
