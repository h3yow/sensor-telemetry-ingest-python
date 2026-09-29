"""Runtime configuration loaded from environment variables.

Variables (all optional, with local-friendly defaults):

TELEMETRY_WINDOW_SECONDS     fixed aggregation window width (default 60)
TELEMETRY_MAX_LATENESS_SECONDS  max tolerated report lateness (default 300)
TELEMETRY_RATE_CAPACITY      token bucket burst capacity per device (default 100)
TELEMETRY_RATE_REFILL_RATE   tokens refilled per second per device (default 50)
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .rate_limit import RateLimitConfig


@dataclass(frozen=True, slots=True)
class Settings:
    window_seconds: float
    max_lateness_seconds: float
    rate_capacity: float
    rate_refill_rate: float

    @property
    def rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            capacity=self.rate_capacity,
            refill_rate=self.rate_refill_rate,
        )


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value


def load_settings() -> Settings:
    return Settings(
        window_seconds=_env_float("TELEMETRY_WINDOW_SECONDS", 60.0),
        max_lateness_seconds=_env_float("TELEMETRY_MAX_LATENESS_SECONDS", 300.0),
        rate_capacity=_env_float("TELEMETRY_RATE_CAPACITY", 100.0),
        rate_refill_rate=_env_float("TELEMETRY_RATE_REFILL_RATE", 50.0),
    )
