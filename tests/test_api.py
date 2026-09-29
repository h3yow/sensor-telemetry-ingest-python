"""HTTP 适配层：状态码、错误码归一化，内部异常不外泄。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from sensor_telemetry.api import create_app
from sensor_telemetry.config import Settings
from sensor_telemetry.service import TelemetryService

from tests.helpers import make_reading


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        max_lateness_ms=5_000,
        rate_capacity=100,
        rate_refill_per_sec=1000,
        log_path=str(tmp_path / "api.jsonl"),
    )
    return TestClient(create_app(TelemetryService(settings)))


def test_accept_and_duplicate_over_http(client):
    payload = make_reading(seq=1, ts_ms=60_000)
    r1 = client.post("/readings", json=payload)
    r2 = client.post("/readings", json=payload)
    print(f"POST accept -> {r1.status_code} {r1.json()}")
    print(f"POST dup    -> {r2.status_code} {r2.json()}")
    assert r1.status_code == 200 and r1.json()["status"] == "ACCEPTED"
    assert r2.status_code == 200 and r2.json()["status"] == "DUPLICATE"


def test_rejection_status_codes_are_distinguishable(client):
    client.post("/readings", json=make_reading(seq=1, ts_ms=60_000, value=20.0))
    cases = [
        (make_reading(seq=1, ts_ms=60_000, value=21.0), 409, "SEQ_CONFLICT"),
        (make_reading(seq=999, ts_ms=1_000), 409, "TOO_LATE"),
        ({**make_reading(seq=2), "unit": "F"}, 422, "UNIT_MISMATCH"),
        ({**make_reading(seq=3), "metric": "wind"}, 422, "UNKNOWN_METRIC"),
        ({**make_reading(seq=4), "value": 999.0}, 422, "VALUE_OUT_OF_RANGE"),
        ({k: v for k, v in make_reading(seq=5).items() if k != "value"}, 422, "MISSING_FIELD"),
    ]
    for payload, expected_status, expected_code in cases:
        resp = client.post("/readings", json=payload)
        body = resp.json()
        print(f"input={payload} -> {resp.status_code} {body.get('error_code')}")
        assert resp.status_code == expected_status
        assert body["error_code"] == expected_code


def test_bad_json_is_normalized_not_framework_error(client):
    resp = client.post(
        "/readings",
        content=b"{not-json",
        headers={"content-type": "application/json"},
    )
    print(f"bad json -> {resp.status_code} {resp.json()}")
    assert resp.status_code == 400
    assert resp.json()["error_code"] == "MALFORMED_PAYLOAD"


def test_query_windows_endpoint(client):
    client.post("/readings", json=make_reading(seq=1, ts_ms=0, value=10.0))
    client.post("/readings", json=make_reading(seq=2, ts_ms=30_000, value=20.0))
    client.post("/readings", json=make_reading(seq=3, ts_ms=60_000, value=30.0))
    resp = client.get("/metrics/temperature/windows", params={"start_ms": 0, "end_ms": 120_000})
    body = resp.json()
    print(f"GET windows -> {resp.status_code} {body}")
    assert resp.status_code == 200
    assert body["window_count"] == 2
    assert [w["window_start_ms"] for w in body["windows"]] == [0, 60_000]
    assert body["windows"][0]["count"] == 2
    assert body["windows"][1]["avg_value"] == 30.0


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}
