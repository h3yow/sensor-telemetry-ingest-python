"""并发上报：同设备不重复计数/状态不错乱；边写边查读不到半更新窗口。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from sensor_telemetry.config import Settings
from sensor_telemetry.models import IngestStatus
from sensor_telemetry.service import TelemetryService

from tests.helpers import make_reading


def test_concurrent_duplicate_submissions_accepted_once(service):
    """N 个线程用同一 (device, seq) 并发上报：恰好 1 个 ACCEPTED，其余 DUPLICATE。"""
    payload = make_reading(seq=1, ts_ms=10_000, value=20.0)
    n = 32
    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(lambda _: service.ingest(dict(payload)), range(n)))

    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    duplicates = [r for r in results if r.status is IngestStatus.DUPLICATE]
    print(
        f"same-key x{n}: accepted={len(accepted)} duplicate={len(duplicates)} "
        f"rejected={n - len(accepted) - len(duplicates)}"
    )
    assert len(accepted) == 1
    assert len(duplicates) == n - 1
    win = service.query_windows("temperature", 0, 60_000)[0]
    print(f"window count={win.count} sum={win.sum_value}")
    assert win.count == 1
    assert win.sum_value == 20.0
    assert len(service.all_readings()) == 1


def test_concurrent_distinct_seqs_all_accepted_with_no_gaps(service):
    """同一设备并发提交互不相同的序号，全部接收，序号集合无丢失无重复。"""
    n = 100

    def submit(seq):
        return service.ingest(make_reading(seq=seq, ts_ms=10_000 + seq, value=float(seq)))

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit, range(n)))

    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    print(f"distinct-seq x{n}: accepted={len(accepted)} rejected={n - len(accepted)}")
    assert len(accepted) == n
    received = sorted(r.seq for r in service.all_readings() if r.device_id == "d1")
    assert received == list(range(n))
    assert service._store.max_seq("d1") == n - 1


def test_concurrent_multiple_devices_are_isolated(service):
    devices = [f"dev-{i}" for i in range(8)]
    per_device = 50

    def submit(dev_idx_seq):
        dev_idx, seq = dev_idx_seq
        dev = devices[dev_idx]
        return service.ingest(
            make_reading(device_id=dev, seq=seq, ts_ms=10_000 + seq, value=float(seq))
        )

    jobs = [(i, s) for i in range(len(devices)) for s in range(per_device)]
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(submit, jobs))

    print(f"multi-device jobs={len(jobs)} accepted={sum(r.status is IngestStatus.ACCEPTED for r in results)}")
    assert all(r.status is IngestStatus.ACCEPTED for r in results)
    for dev in devices:
        seqs = sorted(r.seq for r in service.all_readings() if r.device_id == dev)
        assert seqs == list(range(per_device))


def test_reader_never_sees_half_updated_window(tmp_path):
    """写入过程中并发查询：count 与 sum 必须始终自洽（同一批数据的完整快照）。"""
    svc = TelemetryService(
        Settings(
            max_lateness_ms=5_000,
            rate_capacity=10_000,
            rate_refill_per_sec=10_000,
            log_path=str(tmp_path / "c.jsonl"),
        )
    )
    stop = threading.Event()
    violations: list[str] = []

    def reader():
        while not stop.is_set():
            for win in svc.query_windows("temperature", 0, 120_000):
                # 不变量：count>0 时 min<=max，avg 落在 [min,max]
                if win.count == 0 or not (
                    win.min_value <= win.avg_value <= win.max_value
                    and win.sum_value >= win.count * win.min_value - 1e-9
                ):
                    violations.append(f"inconsistent window: {win.to_dict()}")

    def writer(seq):
        return svc.ingest(
            # ts 在 55000/60000 间交替：跨两个窗口，且迟到恰好在容忍边界内
            make_reading(seq=seq, ts_ms=55_000 + (seq % 2) * 5_000, value=float(seq % 7))
        )

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(writer, range(2000)))
    stop.set()
    t.join(timeout=2)

    accepted = sum(r.status is IngestStatus.ACCEPTED for r in results)
    total_count = sum(
        w.count for w in svc.query_windows("temperature", 0, 120_000)
    )
    print(f"accepted={accepted} summed window counts={total_count} violations={len(violations)}")
    assert not violations
    assert accepted == total_count == 2000


def test_many_new_windows_and_devices_while_querying(tmp_path):
    """持续创建大量新窗口键/新设备，同时并发查询与列举：不得抛
    'dictionary changed size during iteration' 或发生死锁。"""
    svc = TelemetryService(
        Settings(
            max_lateness_ms=10**12,
            rate_capacity=10_000,
            rate_refill_per_sec=10_000,
            log_path=str(tmp_path / "stress.jsonl"),
        )
    )
    errors: list[BaseException] = []
    stop = threading.Event()

    def reader():
        # 大范围查询会遍历窗口 dict；all_readings 会遍历设备 dict
        while not stop.is_set():
            try:
                svc.query_windows("temperature", 0, 10**9)
                svc.all_readings()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

    def writer(job):
        dev_idx, seq = job
        # ts 散布在 300 个不同窗口，强制大量新键插入（dict resize）
        ts = seq * 60_000
        return svc.ingest(
            make_reading(device_id=f"dev-{dev_idx}", seq=seq, ts_ms=ts, value=1.0)
        )

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    jobs = [(d, s) for d in range(20) for s in range(300)]
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(writer, jobs))
    stop.set()
    t.join(timeout=5)

    accepted = sum(r.status is IngestStatus.ACCEPTED for r in results)
    distinct_windows = len(svc.query_windows("temperature", 0, 10**9))
    print(
        f"jobs={len(jobs)} accepted={accepted} errors={len(errors)} "
        f"distinct_windows={distinct_windows} readings={len(svc.all_readings())}"
    )
    assert not errors
    assert accepted == len(jobs) == 6000
    assert len(svc.all_readings()) == 6000
    assert distinct_windows == 300  # seq 0..299 -> ts 0 .. 299*60000
