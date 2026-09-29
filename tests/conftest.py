"""公共夹具与测试日志：打印关键输入与判定依据。"""

from __future__ import annotations

import logging
import sys

import pytest

from sensor_telemetry.config import Settings
from sensor_telemetry.service import TelemetryService

# 让单测日志打印判定依据（accept/duplicate/reject reason）
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(levelname)s %(name)s: %(message)s",
    force=True,
)


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        window_size_ms=60_000,
        max_lateness_ms=5_000,
        rate_capacity=100,
        rate_refill_per_sec=1000,
        log_path=str(tmp_path / "telemetry.jsonl"),
    )


@pytest.fixture()
def service(settings):
    return TelemetryService(settings)


