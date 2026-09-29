"""幂等去重、序号判定、迟到容忍与窗口聚合语义。"""

from __future__ import annotations

from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import TelemetryService

from tests.helpers import make_reading


def test_first_reading_accepted(service):
    result = service.ingest(make_reading(seq=1, ts_ms=60_000))
    print(f"input seq=1 ts=60000 -> {result.to_dict()}")
    assert result.status is IngestStatus.ACCEPTED
    assert result.window_start_ms == 60_000


def test_duplicate_is_idempotent_and_counted_once(service):
    payload = make_reading(seq=1, ts_ms=60_000, value=20.0)
    first = service.ingest(payload)
    print(f"first  -> {first.to_dict()}")
    for i in range(5):
        dup = service.ingest(dict(payload))
        print(f"dup#{i+1} -> {dup.to_dict()}")
        assert dup.status is IngestStatus.DUPLICATE

    windows = service.query_windows("temperature", 0, 120_000)
    assert len(windows) == 1
    print(f"window after 6 duplicates: {windows[0].to_dict()}")
    assert windows[0].count == 1  # 重复不重复计数
    assert windows[0].sum_value == 20.0
    assert len(service.all_readings()) == 1


def test_same_seq_with_conflicting_payload_rejected(service):
    service.ingest(make_reading(seq=1, ts_ms=60_000, value=20.0))
    # 同序号，value 冲突
    conflict_value = service.ingest(make_reading(seq=1, ts_ms=60_000, value=21.0))
    print(f"conflict-value -> {conflict_value.to_dict()}")
    assert conflict_value.status is IngestStatus.REJECTED
    assert conflict_value.error_code == "SEQ_CONFLICT"
    # 同序号，ts 冲突
    conflict_ts = service.ingest(make_reading(seq=1, ts_ms=60_001, value=20.0))
    print(f"conflict-ts    -> {conflict_ts.to_dict()}")
    assert conflict_ts.error_code == "SEQ_CONFLICT"
    # 同序号，metric 冲突
    conflict_metric = service.ingest(
        make_reading(seq=1, ts_ms=60_000, value=20.0, metric="humidity", unit="%")
    )
    print(f"conflict-metric-> {conflict_metric.to_dict()}")
    assert conflict_metric.error_code == "SEQ_CONFLICT"
    # 已接收数据未被覆盖
    windows = service.query_windows("temperature", 0, 120_000)
    assert windows[0].count == 1
    assert windows[0].sum_value == 20.0


def test_seq_regression_rejected(settings):
    # 收紧序号回补跨度：落后超过 2 即视为回退（frozen 配置，测试里直接替换字段）
    object.__setattr__(settings, "max_seq_gap", 2)
    svc = TelemetryService(settings)
    svc.ingest(make_reading(seq=10, ts_ms=60_000))
    # 落后 1~2：乱序补缺，接收
    gap1 = svc.ingest(make_reading(seq=9, ts_ms=60_100))
    print(f"seq 10 then 9 (gap=1) -> {gap1.to_dict()}")
    assert gap1.status is IngestStatus.ACCEPTED
    # 落后超过跨度：序号回退，拒绝
    result = svc.ingest(make_reading(seq=7, ts_ms=60_200))
    print(f"seq 10 then 7 (gap=3) -> {result.to_dict()}")
    assert result.status is IngestStatus.REJECTED
    assert result.error_code == "SEQ_REGRESSED"
    received = sorted(r.seq for r in svc.all_readings())
    print(f"preserved seqs={received}")
    assert received == [9, 10]


def test_late_within_tolerance_updates_existing_window_once(service):
    # settings.max_lateness_ms = 5000
    service.ingest(make_reading(seq=2, ts_ms=60_000, value=20.0))
    late = service.ingest(make_reading(seq=3, ts_ms=58_000, value=10.0))  # 迟到 2000ms
    print(f"late-within-tolerance -> {late.to_dict()}")
    assert late.status is IngestStatus.ACCEPTED
    assert late.window_start_ms == 0  # 落入已存在的更早窗口
    win0 = service.query_windows("temperature", 0, 60_000)[0]
    print(f"updated old window -> {win0.to_dict()}")
    assert win0.count == 1
    assert win0.min_value == 10.0
    # 迟到数据再重复上报，窗口不能二次更新
    again = service.ingest(make_reading(seq=3, ts_ms=58_000, value=10.0))
    print(f"late duplicate -> {again.to_dict()}")
    assert again.status is IngestStatus.DUPLICATE
    win0_after = service.query_windows("temperature", 0, 60_000)[0]
    assert win0_after.count == 1
    assert win0_after.sum_value == 10.0


def test_late_beyond_tolerance_rejected_and_data_preserved(service):
    service.ingest(make_reading(seq=1, ts_ms=60_000, value=20.0))
    too_late = service.ingest(make_reading(seq=2, ts_ms=1_000, value=99.0))  # 迟到 59000ms
    print(f"too-late -> {too_late.to_dict()}")
    assert too_late.status is IngestStatus.REJECTED
    assert too_late.error_code == "TOO_LATE"
    # 被拒读数没有进入任何窗口/存储
    assert service.query_windows("temperature", 0, 120_000)[0].count == 1
    assert len(service.all_readings()) == 1


def test_window_boundaries_are_left_closed_right_open(service):
    # 窗口 60000ms：[0,60000) 与 [60000,120000)
    r1 = service.ingest(make_reading(seq=1, ts_ms=0, value=1.0))
    r2 = service.ingest(make_reading(seq=2, ts_ms=59_999, value=2.0))
    r3 = service.ingest(make_reading(seq=3, ts_ms=60_000, value=3.0))
    print(f"ts=0->{r1.window_start_ms} ts=59999->{r2.window_start_ms} ts=60000->{r3.window_start_ms}")
    assert (r1.window_start_ms, r2.window_start_ms, r3.window_start_ms) == (0, 0, 60_000)
    first = service.query_windows("temperature", 0, 60_000)
    second = service.query_windows("temperature", 60_000, 120_000)
    assert first[0].count == 2
    assert second[0].count == 1
    # 恰好落在查询区间右端点的窗口不属于 [0,60000)
    assert all(x.window_start_ms < 60_000 for x in first)


def test_aggregate_stats(service):
    for seq, (ts, v) in enumerate(
        [(0, 10.0), (1000, 20.0), (2000, -5.0)], start=1
    ):
        service.ingest(make_reading(seq=seq, ts_ms=ts, value=v))
    win = service.query_windows("temperature", 0, 60_000)[0]
    print(f"aggregate -> {win.to_dict()}")
    assert win.count == 3
    assert win.min_value == -5.0
    assert win.max_value == 20.0
    assert win.sum_value == 25.0
    assert win.avg_value == 25.0 / 3
