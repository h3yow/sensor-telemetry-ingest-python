# 本地传感器遥测接入与查询

一套纯本地运行的传感器遥测接入服务（FastAPI + 内存索引 + JSONL 追加日志），
用本地存储模拟设备侧的持续上报，重点解决多设备在**并发上报、重复上报、
迟到上报**下结果稳定、可解释的问题。

- 语言/运行时：Python ≥ 3.12（开发环境 3.14）
- 依赖：`fastapi[standard]`（测试额外需要 `pytest`）
- 无外部数据库 / 消息队列，`uv run` 即可起服务、跑测试

---

## 1. 快速开始

```bash
# 安装依赖（含 dev 测试依赖）
uv sync --group dev

# 启动 HTTP 服务（默认 127.0.0.1:8000）
uv run python main.py
# 或：uv run uvicorn main:app --reload

# 跑全部单测（日志会打印每条关键输入与判定依据，加 -s 可完整查看）
uv run pytest -s
```

---

## 2. 上报协议

### 2.1 上报读数

`POST /readings`，请求体为单个 JSON 对象：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `device_id` | 非空 string | 是 | 设备唯一标识 |
| `seq` | 非负整数（不接受 bool） | 是 | 设备内单调序号，与 `device_id` 共同构成**幂等键** |
| `metric` | string | 是 | 指标名，必须已注册（见下表） |
| `value` | 有限数值（不接受 NaN/Infinity） | 是 | 读数，必须落在指标量程内 |
| `unit` | string | 是 | 单位，必须与指标约定单位完全一致 |
| `ts_ms` | 非负整数 | 是 | 读数产生时刻的 epoch 毫秒（不是服务端接收时刻） |

已注册指标（`sensor_telemetry/config.py`）：

| metric | 约定单位 | 允许量程（含端点） |
| --- | --- | --- |
| `temperature` | `C` | `[-50, 100]` |
| `pressure` | `kPa` | `[0, 500]` |
| `humidity` | `%` | `[0, 100]` |

请求示例：

```json
{
  "device_id": "sensor-A",
  "seq": 42,
  "metric": "temperature",
  "value": 23.5,
  "unit": "C",
  "ts_ms": 1727700000000
}
```

成功响应 `200`：

```json
{ "status": "ACCEPTED", "error_code": null, "message": null, "window_start_ms": 1727700000000 }
```

幂等重复（同一幂等键、内容完全一致）同样返回 `200`，但
`status = "DUPLICATE"`，且**不会**再次计数。

被拒绝时返回 4xx，响应体形如：

```json
{ "error_code": "UNIT_MISMATCH", "message": "指标 'temperature' 的单位必须是 'C'，收到 'F'", "field": "unit" }
```

### 2.2 查询窗口聚合

`GET /metrics/{metric}/windows?start_ms=<毫秒>&end_ms=<毫秒>`

返回与查询区间 `[start_ms, end_ms)` 相交、且**已有数据**的固定窗口，按窗口起点升序：

```json
{
  "metric": "temperature",
  "range": [0, 120000],
  "window_count": 2,
  "windows": [
    { "window_start_ms": 0, "window_end_ms": 60000, "metric": "temperature",
      "count": 2, "min_value": 10.0, "max_value": 20.0, "sum_value": 30.0, "avg_value": 15.0 }
  ]
}
```

### 2.3 存活检查

`GET /healthz` → `{"status": "ok"}`

---

## 3. 去重与序号语义

幂等键为 `(device_id, seq)`。同一设备的“判定 + 写入”在**设备锁**内串行完成
（锁顺序固定：设备锁 → 窗口锁），不同设备互不阻塞。

针对同一幂等键：

1. **首次上报** → `ACCEPTED`，读数落库并并入窗口；
2. **内容完全一致的重复上报** → `DUPLICATE`，幂等成功，不计数、不改窗口，重复多少次都安全；
3. **同序号但内容冲突**（`metric/value/unit/ts_ms` 任一不同）→ 拒绝，
   错误码 `SEQ_CONFLICT`（HTTP 409），已接收的原始内容不会被覆盖。

序号推进：

- 已接收到 `max_seq` 后，新序号允许在 `max_seq - max_seq_gap` 范围内**乱序补缺**
  （并发提交天然乱序，乱序补缺是正常情况）；
- 未见过的新序号比 `max_seq` 落后超过 `max_seq_gap`（默认 1024）→ 拒绝，
  错误码 `SEQ_REGRESSED`（HTTP 409），用于拦截真正的序号回退/异常重放。

迟到判定（基于**设备自己的时间戳水位线**，而不是序号或服务端时钟）：

- 设设备已接收读数的最大 `ts_ms` 为水位线 `W`；
- `W - incoming.ts_ms <= max_lateness_ms`（默认 300000ms = 5 分钟）→ 接收，
  即使它落入一个**已经存在的更早窗口**，窗口也只更新这一次
  （之后该读数的重复上报走 DUPLICATE，窗口不会二次更新）；
- `W - incoming.ts_ms > max_lateness_ms` → 拒绝，错误码 `TOO_LATE`（HTTP 409）；
- 设备的第一条读数没有水位线，不做迟到判定。

---

## 4. 窗口聚合语义

- 窗口大小固定（默认 `window_size_ms = 60000`），起点通过整数除法对齐：
  `window_start = (ts_ms // window_size_ms) * window_size_ms`；
- 窗口一律 **左闭右开 `[start, end)`**：
  - `ts_ms == start` 属于本窗口；
  - `ts_ms == end` 属于**下一**个窗口；
  - 不依赖四舍五入，边界行为稳定；
- 聚合字段：`count / min / max / sum / avg`（`avg = sum / count`）；
- 每个 `(metric, window_start)` 一把锁，“读旧值 → 合并 → 整体替换不可变聚合对象”
  全程持锁：边写入边查询只能看到**更新前或更新后的完整窗口**，读不到半更新状态；
- 查询区间同样是左闭右开 `[start_ms, end_ms)`，返回与其相交的已有窗口。

---

## 5. 错误分类（错误归一化）

所有可预期失败都被归一化为稳定错误码，**绝不以内部异常/堆栈形式透出**；
未预期异常由适配层兜底为 `500 {"error_code": "INTERNAL"}`。

| error_code | HTTP | 含义 |
| --- | --- | --- |
| `MALFORMED_PAYLOAD` | 400 | 请求体不是 JSON 对象 / JSON 无法解析 |
| `MISSING_FIELD` | 422 | 必填字段缺失、为 null，或 `device_id` 为空 |
| `BAD_TYPE` | 422 | 字段类型错误（如 `seq` 是字符串/bool、`value` 是字符串或 NaN/Infinity） |
| `UNKNOWN_METRIC` | 422 | 指标未注册 |
| `UNIT_MISMATCH` | 422 | 单位与指标约定不一致 |
| `VALUE_OUT_OF_RANGE` | 422 | 数值超出指标量程 |
| `TIMESTAMP_INVALID` | 422 | `ts_ms` 不是合法的非负 epoch 毫秒 |
| `SEQ_REGRESSED` | 409 | 序号回退超出允许的补缺跨度 |
| `SEQ_CONFLICT` | 409 | 同一 `(device, seq)` 已存在但内容冲突 |
| `TOO_LATE` | 409 | 时间戳落后设备水位线超过迟到容忍度 |
| `RATE_LIMITED` | 429 | 触发每设备限流（背压保护） |
| `INTERNAL` | 500 | 兜底：未预期内部错误（不泄露细节） |

处理顺序固定，保证同一个输入永远得到同一个错误码：
**结构校验 → 字段/类型 → 指标 → 单位 → 量程 → 时间戳 →（设备锁内）限流 →
重复/冲突 → 序号回退 → 迟到 → 落库与窗口更新**。
注意：结构/语义非法的请求**不消耗**限流令牌；限流检查先于任何状态变更，
因此被限流的请求不可能破坏已接收数据。

---

## 6. 限流 / 背压配置

每个 `device_id` 独立一个**令牌桶**（设备间互不影响）：

- `rate_capacity`：桶容量，即允许的瞬时突发条数（默认 10）；
- `rate_refill_per_sec`：令牌补充速率（条/秒，默认 5）；
- 桶内操作在锁内完成，并发再高也不会超发；
- 拿不到令牌立即返回 `RATE_LIMITED`（HTTP 429），不排队、不预支、不欠账。

所有可调参数集中在 `sensor_telemetry/config.py` 的 `Settings`：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `window_size_ms` | 60000 | 聚合窗口大小 |
| `max_lateness_ms` | 300000 | 允许的最大迟到时间 |
| `max_seq_gap` | 1024 | 允许的序号乱序补缺跨度，超出即判回退 |
| `rate_capacity` | 10.0 | 每设备令牌桶容量 |
| `rate_refill_per_sec` | 5.0 | 每设备令牌补充速率 |
| `log_path` | `None`（纯内存） | JSONL 追加日志路径 |

---

## 7. 本地存储与恢复

- 已接受的读数按接受顺序**追加写入 JSONL**（每行一条记录），写入即 `flush`；
- 另有 `type=window` 的窗口变更审计记录，仅用于可解释性/排查；
- 服务重启时按序重放 `type=reading` 的记录，重建设备序号状态、时间戳水位线
  与全部窗口聚合；日志最后一行若因崩溃被截断会被跳过，不影响启动。

---

## 8. 本地验证方法

### 8.1 自动化测试

```bash
uv run pytest -s
```

覆盖要求的四类场景（共 30+ 用例）：

- **并发**（`tests/test_concurrency.py`）
  - 32 线程同一 `(device, seq)` 并发上报：恰好 1 个 ACCEPTED、其余 DUPLICATE，窗口只计数 1 次；
  - 同设备 100 个不同序号并发提交：全部接收、无丢失无重复、`max_seq` 正确；
  - 8 设备 × 50 条并发：设备间完全隔离；
  - 2000 条写入 + 后台查询线程：读者始终看到自洽的完整窗口，读不到半更新状态；
- **重复**（`tests/test_ingest.py`）：重复 6 次只计数 1 次；同序号 value/ts/metric
  冲突分别得到 `SEQ_CONFLICT`；
- **迟到**（`tests/test_ingest.py` / `test_persistence.py`）：容忍范围内迟到并入
  已存在窗口且只更新一次；超范围得到 `TOO_LATE` 且不污染数据；重启重放后窗口一致；
- **超限**（`tests/test_validator.py` / `tests/test_rate_limit.py`）：
  字段缺失、单位不一致、数值越界、结构异常映射为不同错误码；
  突发超载得到可区分的 `RATE_LIMITED`，且被拒请求不改变存储与窗口。

测试日志（`-s` 可见）会打印关键输入与判定依据，例如：

```
INFO sensor_telemetry.service: accept device=d1 seq=1 metric=temperature value=20.0 ts=60000 window=[60000,120000) count=1
INFO sensor_telemetry.service: duplicate device=d1 seq=1 -> idempotent accept
INFO sensor_telemetry.service: reject device=d1 seq=3 reason=TOO_LATE ts=1000 watermark=60000 lateness=59000ms
INFO sensor_telemetry.service: reject device=d1 seq=7 reason=SEQ_REGRESSED max_seq=10 gap=3
```

### 8.2 手工 curl 验证

```bash
# 正常上报
curl -s -X POST localhost:8000/readings -H 'content-type: application/json' \
  -d '{"device_id":"a","seq":1,"metric":"temperature","value":20,"unit":"C","ts_ms":60000}'

# 重复上报（status=DUPLICATE）
curl -s -X POST localhost:8000/readings -H 'content-type: application/json' \
  -d '{"device_id":"a","seq":1,"metric":"temperature","value":20,"unit":"C","ts_ms":60000}'

# 单位不一致（422 UNIT_MISMATCH）
curl -s -X POST localhost:8000/readings -H 'content-type: application/json' \
  -d '{"device_id":"a","seq":2,"metric":"temperature","value":20,"unit":"F","ts_ms":60000}'

# 查询窗口
curl -s 'localhost:8000/metrics/temperature/windows?start_ms=0&end_ms=120000'
```

---

## 9. 代码结构

```
sensor_telemetry/
├── errors.py        # 错误码枚举、HTTP 状态映射、TelemetryError
├── models.py        # Reading / IngestResult / WindowAggregate
├── config.py        # Settings、指标单位与量程
├── validator.py     # 报文校验与错误归一化
├── rate_limiter.py  # 令牌桶 + 每设备限流
├── storage.py       # 设备状态、设备锁、JSONL 追加与重放
├── windows.py       # 固定窗口聚合（左闭右开、锁内原子更新）
├── service.py       # 编排：校验->限流->判定->落库/窗口
└── api.py           # FastAPI 路由与错误->HTTP 映射
tests/               # validator / ingest / concurrency / rate_limit / persistence / api
main.py              # create_app() 入口
```
