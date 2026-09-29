"""本地日志重放：重启后已接收数据与窗口完整恢复，去重判定延续。"""

from __future__ import annotations

from pathlib import Path

from sensor_telemetry.config import Settings
from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import TelemetryService

from tests.helpers import make_reading


def test_replay_restores_readings_and_windows(tmp_path):
    log = str(Path(tmp_path) / "telemetry.jsonl")
    settings = Settings(max_lateness_ms=5_000, log_path=log)

    svc = TelemetryService(settings)
    svc.ingest(make_reading(seq=1, ts_ms=60_000, value=20.0))
    svc.ingest(make_reading(seq=2, ts_ms=61_000, value=24.0))
    svc.ingest(make_reading(seq=3, ts_ms=58_000, value=10.0))  # 迟到补缺
    svc.ingest(make_reading(seq=1, ts_ms=60_000, value=20.0))  # 重复，不重计
    assert len(svc.all_readings()) == 3

    # 模拟重启：同日志重建服务
    svc2 = TelemetryService(settings)
    assert len(svc2.all_readings()) == 3
    win0 = svc2.query_windows("temperature", 0, 60_000)[0]
    win1 = svc2.query_windows("temperature", 60_000, 120_000)[0]
    print(f"after replay win[0,60000)={win0.to_dict()} win[60000,120000)={win1.to_dict()}")
    assert win0.count == 1 and win0.sum_value == 10.0
    assert win1.count == 2 and win1.sum_value == 44.0

    # 去重状态延续：旧序号仍判重复，冲突仍被拒
    dup = svc2.ingest(make_reading(seq=1, ts_ms=60_000, value=20.0))
    conflict = svc2.ingest(make_reading(seq=1, ts_ms=60_000, value=20.1))
    print(f"post-restore duplicate -> {dup.status.value}; conflict -> {conflict.error_code}")
    assert dup.status is IngestStatus.DUPLICATE
    assert conflict.error_code == "SEQ_CONFLICT"
