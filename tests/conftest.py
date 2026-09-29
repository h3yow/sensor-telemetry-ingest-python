"""Shared fixtures and logging setup.

Logs at INFO print key inputs and the admission decision/reason for every
report, which is how test runs document their judgment basis.
"""

from __future__ import annotations

import logging

import pytest

from sensor_telemetry.rate_limit import RateLimitConfig
from sensor_telemetry.service import IngestService


def pytest_configure(config: pytest.Config) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        force=True,
    )


@pytest.fixture
def service() -> IngestService:
    return IngestService(
        window_width_seconds=60.0,
        max_lateness_seconds=300.0,
        rate_limit=RateLimitConfig(capacity=1000.0, refill_rate=1000.0),
    )


@pytest.fixture
def tight_service() -> IngestService:
    """Service with a small bucket for deterministic rate-limit tests."""
    return IngestService(
        window_width_seconds=60.0,
        max_lateness_seconds=300.0,
        rate_limit=RateLimitConfig(capacity=10.0, refill_rate=0.0),
    )


def make_report(
    device_id: str = "dev-1",
    seq: int = 0,
    metric: str = "temperature",
    value: float = 20.0,
    unit: str = "C",
    timestamp: float = 1000.0,
) -> dict:
    return {
        "device_id": device_id,
        "seq": seq,
        "metric": metric,
        "value": value,
        "unit": unit,
        "timestamp": timestamp,
    }


@pytest.fixture
def report_factory():
    return make_report
