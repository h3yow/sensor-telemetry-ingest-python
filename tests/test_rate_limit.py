"""限流/背压：超载请求被拒且可区分，已接收数据不被破坏。"""

from __future__ import annotations

from sensor_telemetry.config import Settings
from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import TelemetryService

from tests.helpers import make_reading


def _strict_service(tmp_path):
    # 容量 3，补充极慢：突发最多放过 3 条
    return TelemetryService(
        Settings(
            rate_capacity=3,
            rate_refill_per_sec=0.0001,
            max_lateness_ms=10**12,
            log_path=str(tmp_path / "rl.jsonl"),
        )
    )


def test_rate_limit_rejects_excess_with_distinct_code(tmp_path):
    svc = _strict_service(tmp_path)
    results = [svc.ingest(make_reading(seq=i, ts_ms=10_000 + i)) for i in range(10)]
    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    limited = [r for r in results if r.error_code == "RATE_LIMITED"]
    print(f"burst of 10 with capacity 3: accepted={len(accepted)} limited={len(limited)}")
    assert len(accepted) == 3
    assert len(limited) == 7
    for r in limited:
        assert r.status is IngestStatus.REJECTED


def test_rate_limited_requests_do_not_mutate_state(tmp_path):
    svc = _strict_service(tmp_path)
    for i in range(3):
        assert svc.ingest(make_reading(seq=i, ts_ms=10_000 + i)).status is IngestStatus.ACCEPTED
    # 后续全部被限流：即便内容是“本该冲突/回退”的请求，也只拿到 RATE_LIMITED，
    # 且限流检查先于状态判定 —— 任何被拒请求都不能落库或改变窗口
    conflict = make_reading(seq=0, ts_ms=10_000, value=99.0)  # 与 seq0 冲突
    regress = make_reading(seq=100, ts_ms=50_000, value=5.0)  # 乱序补缺本应接收
    r1 = svc.ingest(conflict)
    r2 = svc.ingest(regress)
    print(f"over-capacity conflict -> {r1.error_code}; over-capacity new seq -> {r2.error_code}")
    assert r1.error_code == "RATE_LIMITED"
    assert r2.error_code == "RATE_LIMITED"
    # 存储与窗口只反映前 3 条（冲突没覆盖、补缺没进入）
    seqs = sorted(r.seq for r in svc.all_readings())
    print(f"preserved seqs={seqs}")
    assert seqs == [0, 1, 2]
    win = svc.query_windows("temperature", 0, 120_000)[0]
    assert win.count == 3
    assert win.max_value == 20.0


def test_rate_limit_is_per_device(tmp_path):
    svc = _strict_service(tmp_path)
    # 两台设备各自突发：各自独立拥有 3 个令牌
    r_d1 = [svc.ingest(make_reading(device_id="d1", seq=i, ts_ms=10_000 + i)) for i in range(5)]
    r_d2 = [svc.ingest(make_reading(device_id="d2", seq=i, ts_ms=10_000 + i)) for i in range(5)]
    a1 = sum(r.status is IngestStatus.ACCEPTED for r in r_d1)
    a2 = sum(r.status is IngestStatus.ACCEPTED for r in r_d2)
    print(f"per-device accepted: d1={a1} d2={a2}")
    assert a1 == 3 and a2 == 3
