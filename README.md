# 本地传感器遥测接入与查询服务（sensor-telemetry-ingest）

用本地内存存储模拟设备侧的持续上报，提供"上报接入 + 固定窗口聚合查询"的 HTTP 服务。
重点保证多设备在**并发上报、重复上报、迟到上报、超承载上报**下结果稳定、拒绝原因可区分、
已接收数据不丢失、不被半更新状态污染。

- Python 3.14 / FastAPI，存储为进程内线程安全结构（无外部依赖）
- 并发模型：每个设备一把独立锁（序号准入），窗口存储一把锁（聚合折叠）
- 全部拒绝都归一化为明确的错误码，不向客户端透出内部异常

---

## 1. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 运行全部单测（日志会打印每条上报的关键输入与判定依据）
uv run pytest -q

# 启动本地服务
uv run python main.py            # http://127.0.0.1:8000
# 或：uv run uvicorn main:app --reload
```

冒烟调用：

```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/reports \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"sensor-01","seq":0,"metric":"temperature",
       "value":23.5,"unit":"C","timestamp":1700000000.0}'

curl -s 'http://127.0.0.1:8000/api/v1/devices/sensor-01/metrics/temperature/windows?start=1700000000&end=1700000100'
curl -s http://127.0.0.1:8000/api/v1/devices/sensor-01
curl -s http://127.0.0.1:8000/health
```

测试覆盖（共 46 个用例）：

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/test_protocol.py` | 字段缺失/类型错/单位冲突/数值越界/结构异常，错误码互不相同 |
| `tests/test_dedup.py` | 幂等重复、同序号内容冲突、序号回退、迟到补洞、超迟到、错误码可区分 |
| `tests/test_windows.py` | 窗口左右边界、迟到数据只更新一次、读写不出现半更新窗口 |
| `tests/test_concurrency.py` | 同设备 500 并发序号、32 路相同序号、32 路冲突序号、20 设备并发 |
| `tests/test_rate_limit.py` | 突发+补充、超限拒绝不毁数据、设备间独立、并发准入不超过容量 |
| `tests/test_api.py` | HTTP 端到端：接受/重复/冲突/迟到/各类校验错误/429/404 |

测试日志示例（判定依据随日志打印）：

```
accepted device=s1 seq=0 ts=1000.0 value=23.5C -> window [960.0,1020.0) count=1
duplicate device=s1 seq=0 ts=1000.0 (idempotent, not re-counted)
rejected device=s1 seq=5 code=TOO_LATE reason=too late: timestamp 500.0 is 500.000s behind ...
rate limited device=s1 seq=7 available=0.80
```

---

## 2. 上报协议

### POST `/api/v1/reports`

请求体（JSON 对象）：

| 字段 | 类型 | 要求 |
| --- | --- | --- |
| `device_id` | string | 非空 |
| `seq` | integer | 非负整数，设备内单调（允许按时间戳迟到补洞，见第 3 节） |
| `metric` | string | 已知指标：`temperature` / `pressure` / `humidity` |
| `value` | number | 有限数，且落在指标取值范围内（换算到标准单位后判定） |
| `unit` | string | 与指标兼容的单位，见下 |
| `timestamp` | number | 有限数，epoch 秒，`>= 0` |

支持单位（入站单位会被线性换算为**标准单位**后存储/聚合）：

- `temperature`：`C`（标准）、`K`（开尔文自动换算为摄氏）；范围 `[-273.15, 1000]` °C
- `pressure`：`Pa`（标准）、`kPa`、`hPa`；范围 `[0, 1_000_000]` Pa
- `humidity`：`%`；范围 `[0, 100]`

响应：

- `200`，`status = ACCEPTED`：首次接收，已计入聚合
- `200`，`status = DUPLICATE`，`duplicate = true`：完全相同的重复上报，幂等成功但**不重复计数**
- `4xx`，`status = REJECTED` + `error_code` + `reason`：被拒绝（错误码见第 4 节）

响应体还会回带规范化后的读数（标准单位）。完整示例：

```json
{
  "status": "ACCEPTED",
  "reason": "accepted",
  "duplicate": false,
  "reading": {
    "device_id": "sensor-01", "seq": 0, "metric": "temperature",
    "value": 23.5, "unit": "C", "timestamp": 1700000000.0
  }
}
```

### 查询接口

`GET /api/v1/devices/{device_id}/metrics/{metric}/windows?start=&end=`

- `start`/`end`：epoch 秒；省略时分别为 `0` 与当前时间。
- 返回所有与 `[start, end)` **有重叠**的已物化窗口（无数据的窗口不补零），按 `window_start` 升序。
- 每个窗口含 `count/min/max/sum/average/first_ts/last_ts`。
- 响应中显式声明边界约定：`"boundary": "[start, end)"`。

`GET /api/v1/devices/{device_id}`：返回设备当前 `latest_seq`、水位线 `latest_ts`、
已接收读数条数 `accepted_count` 和当前剩余令牌 `available_tokens`。

---

## 3. 去重与窗口语义

### 3.1 上报准入顺序（先到的失败阶段决定错误码）

1. **结构/取值校验**（不改动任何状态）
2. **每设备限流**（令牌桶；被拒不消耗令牌，见第 5 节）
3. **每设备序号准入**（同一把设备锁内"检查 + 落账"原子完成）
4. **折叠进聚合窗口**（原子；每条被接收读数恰好折叠一次）

### 3.2 序号与重复判定（设备维度）

设备维护：`max_seq`（已确认的最大序号）、`latest_ts`（已接收的最大时间戳=水位线）、
以及 `seq -> 读数` 的已接收映射。判定规则：

| 情况 | 判定 | 错误码 / 状态 |
| --- | --- | --- |
| `seq` 已接收，且六元组指纹完全相同 | 幂等重复，不计数 | `DUPLICATE`（200） |
| `seq` 已接收，但任一内容不同 | 同序号内容冲突，拒绝 | `CONFLICT`（409） |
| 时间戳落后水位线超过 `max_lateness` | 超出允许范围的迟到，拒绝 | `TOO_LATE`（422） |
| `seq < max_seq` 的**未见**序号，但时间戳并不早于水位线 | 设备计数器回退，拒绝 | `SEQUENCE_REGRESSION`（409） |
| `seq < max_seq` 的未见序号，时间戳更早但在容忍窗口内 | 合法迟到补洞，接收 | `ACCEPTED` |
| 其它（正常顺序 / 跳号在前） | 接收并推进水位线 | `ACCEPTED` |

设计要点：**"序号回退"与"迟到补洞"靠时间戳方向区分**——真正的迟到补洞一定携带更早的
时间戳；序号变小而时间戳不回溯，说明设备侧计数器本身发生了回退。
迟到判定以设备时间戳水位线为准，容忍边界 `== max_lateness` 时仍接收（闭区间一侧）。
指纹为 `(device_id, seq, metric, value, unit, timestamp)` 六元组，因此
同序号但值/时间戳/单位不同都会被识别为 `CONFLICT`。

并发性：每个设备一把独立的 `threading.Lock`，"检查 + 落账"在锁内原子完成，
因此同设备并发上报不会出现重复计数或状态错乱；不同设备之间互不阻塞。

### 3.3 固定窗口边界约定（稳定且明确）

- 每个窗口都是**左闭右开**区间 `[start, end)`：包含左端点，不包含右端点。
- 窗口对齐到 epoch 原点（默认 `1970-01-01T00:00:00Z`）：

  ```
  index = floor((timestamp - origin) / width)
  start = origin + index * width
  end   = start + width
  ```

  因此 `t = 60.0` 属于 `[60, 120)` 而不属于 `[0, 60)`；负时间戳也有良定义的窗口。
- 迟到读数落入**已存在**的窗口时，会更新该窗口（`count/min/max/sum/first_ts/last_ts`），
  但由于上游序号准入保证每条读数只接收一次，窗口对每条读数**只更新一次**。
- 读写隔离：窗口折叠在存储锁内原子完成，`WindowAggregate` 为不可变值对象；
  查询在锁内拷贝快照后返回，边写边查只能看到"更新前"或"更新后"的完整窗口，
  读不到半更新状态（见 `tests/test_windows.py::test_concurrent_readers_never_see_half_updated_windows`）。

---

## 4. 错误分类（错误码归一化）

任何坏输入都映射为下列**稳定、可区分**的错误码之一；HTTP 层做状态码映射，
未预期异常由兜底处理器归一化为 `INTERNAL_ERROR`（500），不以堆栈形式透出：

| `error_code` | HTTP | 含义 |
| --- | --- | --- |
| `MALFORMED` | 400 | 请求体不是合法 JSON / 不是 JSON 对象（结构异常） |
| `MISSING_FIELD` | 400 | 必填字段缺失或为 null |
| `TYPE_ERROR` | 422 | 字段类型错误（如 `seq` 是字符串/布尔、`value` 非数值） |
| `UNIT_CONFLICT` | 422 | 指标未知，或单位与指标不兼容 |
| `OUT_OF_RANGE` | 422 | `seq < 0`、数值 NaN/无穷、超出指标范围、时间戳非法 |
| `SEQUENCE_REGRESSION` | 409 | 未见序号回退（时间戳不回溯） |
| `TOO_LATE` | 422 | 超出允许迟到范围（落后水位线超过 `max_lateness`） |
| `CONFLICT` | 409 | 同序号但内容冲突 |
| `RATE_LIMITED` | 429 | 超出设备承载能力（限流/背压） |
| `DEVICE_NOT_FOUND` | 404 | 查询的设备尚无数据 |
| `QUERY_INVALID` | 400 | 查询参数非法（如 `end < start`） |

被拒绝的上报在进入任何状态变更前即返回，因此**拒绝不会破坏已经接收的数据**。

---

## 5. 限流 / 背压配置

每设备独立的**令牌桶**（设备之间互不影响）：

- `capacity`：桶容量（允许的突发条数）
- `refill_rate`：每秒补充的令牌数（持续速率）
- 每条上报尝试取 1 个令牌；取不到立即返回 `RATE_LIMITED`（429），
  **被拒请求不消耗令牌**，也不会进入序号/窗口逻辑，已接收数据保持完整。

通过环境变量配置（默认值适合本地）：

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `TELEMETRY_WINDOW_SECONDS` | `60` | 聚合窗口宽度（秒） |
| `TELEMETRY_MAX_LATENESS_SECONDS` | `300` | 允许的最大迟到（秒） |
| `TELEMETRY_RATE_CAPACITY` | `100` | 每设备突发容量 |
| `TELEMETRY_RATE_REFILL_RATE` | `50` | 每设备每秒补充令牌数 |

```bash
TELEMETRY_RATE_CAPACITY=20 TELEMETRY_RATE_REFILL_RATE=10 \
  uv run python main.py
```

也可在代码中直接构造：

```python
from sensor_telemetry.app import create_app
from sensor_telemetry.service import IngestService
from sensor_telemetry.rate_limit import RateLimitConfig

svc = IngestService(
    window_width_seconds=60,
    max_lateness_seconds=300,
    rate_limit=RateLimitConfig(capacity=20, refill_rate=10),
)
app = create_app(svc)
```

---

## 6. 代码结构

```
sensor_telemetry/
  models.py       # Reading / IngestResult / DeviceState / WindowAggregate
  errors.py       # ErrorCode、IngestError、错误码 -> HTTP 状态码
  protocol.py     # 上报载荷解析、单位归一化、校验与错误归一化
  rate_limit.py   # 每设备令牌桶
  dedup.py        # 每设备序号准入：重复/冲突/回退/迟到（设备级锁）
  windows.py      # 固定窗口聚合与一致性快照查询（存储级锁）
  service.py      # 编排：校验 -> 限流 -> 准入 -> 折叠
  config.py       # 环境变量配置
  api.py / app.py # FastAPI 路由与应用装配、异常归一化
tests/            # 并发/重复/迟到/超限/协议/HTTP 端到端测试
main.py           # 本地启动入口
```
