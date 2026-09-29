"""End-to-end HTTP tests covering duplicate, late, over-limit and error
normalization scenarios."""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from sensor_telemetry.app import create_app
from sensor_telemetry.rate_limit import RateLimitConfig
from sensor_telemetry.service import IngestService

from .conftest import make_report

logger = logging.getLogger("tests.test_api")


@pytest.fixture
def client() -> TestClient:
    svc = IngestService(
        window_width_seconds=60.0,
        max_lateness_seconds=300.0,
        rate_limit=RateLimitConfig(capacity=5.0, refill_rate=0.0),
    )
    return TestClient(create_app(svc))


def test_ingest_and_query_happy_path(client: TestClient) -> None:
    payload = make_report(seq=0, timestamp=1000.0, value=23.5)
    resp = client.post("/api/v1/reports", json=payload)
    logger.info("POST report payload=%s -> %s %s", payload, resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["status"] == "ACCEPTED"

    query = client.get(
        "/api/v1/devices/dev-1/metrics/temperature/windows",
        params={"start": 960.0, "end": 1020.0},
    )
    body = query.json()
    logger.info("GET windows -> %s", body)
    assert query.status_code == 200
    assert body["boundary"] == "[start, end)"
    assert body["windows"][0]["count"] == 1
    assert body["windows"][0]["average"] == 23.5


def test_duplicate_returns_200_duplicate_not_double_counted(client: TestClient) -> None:
    payload = make_report(seq=1, timestamp=1000.0)
    client.post("/api/v1/reports", json=payload)
    resp = client.post("/api/v1/reports", json=payload)
    logger.info("duplicate POST -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "DUPLICATE"
    assert body["duplicate"] is True

    status = client.get("/api/v1/devices/dev-1").json()
    assert status["accepted_count"] == 1


def test_conflict_returns_409_distinct_code(client: TestClient) -> None:
    client.post("/api/v1/reports", json=make_report(seq=1, timestamp=1000.0, value=20.0))
    resp = client.post(
        "/api/v1/reports", json=make_report(seq=1, timestamp=1000.0, value=20.1)
    )
    logger.info("conflict POST -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 409
    assert resp.json()["error_code"] == "CONFLICT"


def test_too_late_returns_422_too_late(client: TestClient) -> None:
    client.post("/api/v1/reports", json=make_report(seq=0, timestamp=1000.0))
    resp = client.post("/api/v1/reports", json=make_report(seq=5, timestamp=600.0))
    logger.info("late POST -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "TOO_LATE"


def test_validation_errors_normalized(client: TestClient) -> None:
    cases = [
        (b"{bad json", 400, "MALFORMED"),
        ({"device_id": "dev-1"}, 400, "MISSING_FIELD"),
        (make_report(seq="one"), 422, "TYPE_ERROR"),
        (make_report(unit="X"), 422, "UNIT_CONFLICT"),
        (make_report(value=99999.0), 422, "OUT_OF_RANGE"),
    ]
    for payload, expected_http, expected_code in cases:
        if isinstance(payload, bytes):
            resp = client.post("/api/v1/reports", content=payload)
        else:
            resp = client.post("/api/v1/reports", json=payload)
        logger.info("normalization input=%r -> %s %s", payload, resp.status_code, resp.json())
        assert resp.status_code == expected_http
        assert resp.json()["error_code"] == expected_code


def test_rate_limit_returns_429_and_accepted_data_survives(client: TestClient) -> None:
    # bucket capacity is 5
    statuses = []
    for i in range(8):
        resp = client.post(
            "/api/v1/reports", json=make_report(seq=i, timestamp=1000.0 + i)
        )
        statuses.append((resp.status_code, resp.json().get("error_code")))
    logger.info("rate-limit sequence: %s", statuses)
    assert statuses[:5] == [(200, None)] * 5
    assert all(s == (429, "RATE_LIMITED") for s in statuses[5:])

    status = client.get("/api/v1/devices/dev-1").json()
    logger.info("post-overload device status: %s", status)
    assert status["accepted_count"] == 5
    assert status["latest_seq"] == 4


def test_unknown_device_is_404_and_health_ok(client: TestClient) -> None:
    resp = client.get("/api/v1/devices/ghost")
    assert resp.status_code == 404
    assert resp.json()["error_code"] == "DEVICE_NOT_FOUND"
    assert client.get("/health").json() == {"status": "ok"}
