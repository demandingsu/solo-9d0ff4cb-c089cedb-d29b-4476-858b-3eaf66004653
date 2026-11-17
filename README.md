# 港航 AIS 靠港停留事件 API

岸基 AIS 点位 → 靠港停留事件，供排放核算使用。FastAPI + SQLite（WAL），
提供 Docker Compose 启动、CSV 上传、事件查询与游标变更流。

## 快速开始

```bash
docker compose up --build
```

* Swagger UI: <http://localhost:8000/docs>
* OpenAPI JSON: <http://localhost:8000/openapi.json>
* 健康检查: <http://localhost:8000/healthz>

演示迟到补点与撤回：在 `/docs` 中展开 **demo → POST /demo/run → Try it out →
Execute**。每次调用创建独立的 `DEMO-XXXXXXXX` 港区，不会污染业务数据。

演示封存水位与续读：在 `/docs` 中展开 **demo → POST /demo/seal/run → Try it
out → Execute**。每次调用创建独立的 `SEAL-XXXXXXXX` 港区，响应给出封存水位、
已封存事件分页、续读游标，以及封存后越界补点的整批拒收结果。

双实例（共享同一 SQLite 卷，验证交错上传不丢更新）：

```bash
docker compose -f docker-compose.dual.yml up --build
# http://localhost:8001/docs 与 http://localhost:8002/docs
```

## 业务规则

| 规则 | 实现 |
| --- | --- |
| 固定港区矩形范围 | `POST /ports`，同一 `code` 重复登记内容一致时幂等（200），不一致 409 |
| 多次上传 CSV | `POST /ports/{code}/uploads`，含船舶号、UTC 时间、经纬度 |
| 同船同刻同坐标去重 | 批次内与库内均去重，计入 `rows_duplicate` |
| 同船同刻异坐标整批拒收 | 返回 409 `conflicting_coordinates`，整批回滚，不留半批结果 |
| 按时间重排 | 每船按 `(event_time, id)` 排序后推导事件 |
| 相邻在港点 ≤ 10 分钟 | 间隔 > 600 秒断开 |
| 首末相距 ≥ 30 分钟 | 不足 1800 秒不形成事件 |
| 出港点断开 | 矩形外的点作为退出标记，关闭当前事件且不并入 |
| 迟到点改变事件 | 未变事件保留 `event_id`；被改变事件旧号 retract + 新号 upsert |
| 游标顺序读取变更 | `GET /ports/{code}/changes?after=<seq>`，严格递增、可续读 |
| 上传原子生效 | 原始点、现役事件、变更记录同一 `BEGIN IMMEDIATE` 事务提交 |
| 两实例交错上传不丢更新 | SQLite WAL + `BEGIN IMMEDIATE` 写串行化 |
| 失败不推进游标 | 事务回滚，`seq` 不前进，无半批结果 |
| 重复上传不重发变更 | `Idempotency-Key`（缺省用内容 SHA-256）唯一约束，重放返回原批次 |
| 封存 UTC 结束水位 | `POST /ports/{code}/seals`，只交付 `end_time <= watermark` 的事件 |
| 封存返回游标 | 响应 `seal.changes_cursor` 与封存同一提交状态，用于续读增量 |
| 封存后仍可上传新观测 | 任何时间 `<= watermark + 600s` 的点使整批 409，不留半批 |
| 已封存事件冻结 | 不延长、不撤回、不换号；越界批次整体回滚 |
| 水位只前进 | 相同水位 200 幂等；更低水位不生效（`ignored=true`） |
| 已成功批次重试 | 同一 `Idempotency-Key` + 同内容返回原批次，即使点位现在落在拒收带内 |

### 封存（UTC 结束水位）语义

数据员向排放核算端交付时，先封存一个 UTC 结束水位，再按该水位分页读取事件：

```bash
# 1) 封存；返回 seal.changes_cursor
curl -X POST http://localhost:8000/ports/CNSHA-PD/seals \
  -H 'Content-Type: application/json' \
  -d '{"watermark": "2026-01-01T12:00:00Z"}'

# 2) 分页读取已封存事件（只含 end_time <= watermark）
curl 'http://localhost:8000/ports/CNSHA-PD/seals/1/events?after_event_id=0&limit=1000'
curl 'http://localhost:8000/ports/CNSHA-PD/seals/latest/events'   # 用最新水位

# 3) 保存第 1 步的 changes_cursor，续读封存之后的增量
curl 'http://localhost:8000/ports/CNSHA-PD/changes?after=<changes_cursor>'
```

* **交付口径**：`end_time <= watermark` 的事件才属于该封存；`end_time` 恰等于
  水位的事件包含在内。
* **拒收带**：封存后，任何 `event_time <= watermark + 600s` 的点都会使**整批**
  返回 409 `point_at_or_before_watermark`。600 秒取自在港相邻点最大间隔，落在
  带内的点仍可能并入已封存事件。带外的较新观测可正常上传。
* **冻结**：已封存事件不会被延长、撤回或换号。即使实现了拒收带，服务端仍在
  写事务内校验“封存事件集合前后一致”，不一致则回滚整批。
* **只前进**：相同水位重复封存返回 200 与原封存（`created=false`）；更低水位
  被忽略并返回当前最新封存（`ignored=true`），不会产生新结果。
* **原子性**：封存、`event_count`、`changes_cursor` 与已封存事件集合在同一个
  `BEGIN IMMEDIATE` 事务内读取/写入；与上传交错时，三者始终对应同一提交状态。
* **重试**：同一 `Idempotency-Key` 且内容相同的历史批次照常返回原批次
  （`replayed=true`），不受封存拒收带影响。

`seal` 对象字段：`seal_id`、`watermark`（epoch 秒）、`watermark_iso`、
`late_point_cutoff`/`late_point_cutoff_iso`（水位 + 600s）、`changes_cursor`、
`event_count`、`created_at`。封存请求接受 `watermark` 或 `watermark_time`，
值为 ISO-8601（无时区按 UTC，须为整秒）或 epoch 秒。

### 事件编号语义

事件以 `(port, vessel, start_time)` 为身份。迟到点只延长结束时间或增加点数时，
仍会 **retract 旧编号 + upsert 新编号**，保证消费方总能拿到完整快照而无需
patch 既有编号；起点不变、载荷不变的事件保留原编号。CSV 中落在港区矩形外的
点位也会入库（作为出港标记），`rows_outside_port` 会统计它们。

### CSV 格式

```csv
vessel,time,lat,lon
SHIP-001,2026-01-01T00:00:00Z,31.20,121.50
SHIP-001,2026-01-01T00:10:00Z,31.201,121.501
```

表头别名（大小写不敏感）：`vessel/船舶号/mmsi/ship_id`、`time/时间/timestamp`、
`lat/纬度`、`lon/lng/longitude/经度`。时间支持 ISO-8601（无时区按 UTC）或
epoch 秒/毫秒。坐标按 1e-6 度取整后比较。上传上限默认 10 MiB / 100,000 行，
可用 `AIS_MAX_UPLOAD_BYTES`、`AIS_MAX_ROWS` 调整。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ports` | 登记港区矩形范围（幂等） |
| GET | `/ports/{code}` | 读取港区定义 |
| POST | `/ports/{code}/uploads` | 上传 CSV 点位，整批原子生效 |
| GET | `/ports/{code}/events` | 现役事件，按 `event_id` 分页 |
| GET | `/ports/{code}/changes` | 变更流，`after` 游标 + `limit` |
| POST | `/ports/{code}/seals` | 封存 UTC 结束水位（只前进，幂等） |
| GET | `/ports/{code}/seals` | 列出全部封存及其游标 |
| GET | `/ports/{code}/seals/{seal_id}` | 读取单个封存及续读地址 |
| GET | `/ports/{code}/seals/{seal_id}/events` | 按封存水位分页读取事件（`seal_id` 可写 `latest`） |
| POST | `/demo/run` | 迟到补点 / retract 演示（`AIS_DEMO_ENABLED=1`） |
| POST | `/demo/seal/run` | 封存 / 拒收 / 续读演示（`AIS_DEMO_ENABLED=1`） |
| GET | `/healthz` | 健康检查 |

上传响应示例：

```json
{
  "replayed": false,
  "batch_id": 7,
  "rows_received": 15,
  "rows_new": 15,
  "rows_duplicate": 0,
  "rows_outside_port": 0,
  "event_changes": {"upserted": 3, "retracted": 0},
  "change_cursor": {"from": 12, "to": 14}
}
```

变更响应示例（`GET /ports/CNSHA-PD/changes?after=14`）：
```json
{
  "port_id": 1,
  "next_cursor": 16,
  "has_more": false,
  "changes": [
    {"cursor": 15, "type": "retract", "event_id": 41, "vessel_id": "SHIP-001", "...": "..."},
    {"cursor": 16, "type": "upsert", "event_id": 42, "event": {"event_id": 42, "...": "..."}}
  ]
}
```

`seq` 在全库单调递增，同一港区的游标可能不连续；按 `seq > after` 读取即可。
`GET /events` 与 `GET /seals/{id}/events` 返回的 `cursor` 都与变更流同源，
可用于「先快照、后增量」。

封存响应示例（`POST /ports/CNSHA-PD/seals`，`201` 新封存 / `200` 幂等重复）：

```json
{
  "created": true,
  "advanced": true,
  "requested_watermark": 1767268800,
  "requested_watermark_iso": "2026-01-01T12:00:00Z",
  "ignored": false,
  "seal": {
    "seal_id": 3,
    "port_id": 1,
    "watermark": 1767268800,
    "watermark_iso": "2026-01-01T12:00:00Z",
    "late_point_cutoff": 1767269400,
    "late_point_cutoff_iso": "2026-01-01T12:10:00Z",
    "changes_cursor": 42,
    "event_count": 17,
    "created_at": "2026-11-17T08:00:00+00:00"
  },
  "frozen_event_count": 17,
  "resume": {
    "events": "/ports/CNSHA-PD/seals/3/events?after_event_id=0&limit=1000",
    "changes": "/ports/CNSHA-PD/changes?after=42"
  }
}
```

已封存事件分页响应示例（`GET /ports/CNSHA-PD/seals/3/events`）：

```json
{
  "port_id": 1,
  "seal": {"seal_id": 3, "...": "..."},
  "cursor": 42,
  "count": 17,
  "has_more": false,
  "events": [{"event_id": 41, "end_time": 1767268700, "...": "..."}]
}
```

## 本地开发与测试

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
AIS_DEMO_ENABLED=1 uvicorn app.main:app --reload
# 对运行中的服务做端到端演示：
python scripts/demo_delay.py http://localhost:8000
python scripts/demo_seal.py  http://localhost:8000
```

测试覆盖：10 分钟/30 分钟边界、出港断开、批内与库内去重、异坐标整批回滚、
迟到点 retract/upsert 与编号保留、两事件合并、幂等重放、游标分页、港区隔离、
双实例（共享 DB 的并发线程与两个 FastAPI 实例）交错上传。

封存相关测试（`tests/test_seals.py`）额外覆盖：水位边界（`end_time ==
watermark` 入选、`watermark+600s` 整点拒收而 +601s 放行）、封存后新观测上传、
已封存事件不被延长/撤回/换号、单点越界导致的整批回滚、成功批次重试返回原批次、
重复/回退封存的幂等、跨港区隔离、旧库原地迁移，以及「写事务未提交时封存阻塞、
提交后只包含一次」与双实例交错的确定性验证。`docker build`
未能在本环境执行（无 Docker 守护进程），Compose 文件仅做了 YAML 解析校验。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `AIS_DB_PATH` | `./data/ais.sqlite3` | SQLite 路径（容器内 `/data/ais.sqlite3`） |
| `AIS_DEMO_ENABLED` | `0`（镜像内为 `1`） | 是否注册 `/demo/run` |
| `AIS_MAX_UPLOAD_BYTES` | `10485760` | 单次上传字节上限 |
| `AIS_MAX_ROWS` | `100000` | 单次上传唯一行数上限 |
| `AIS_SQLITE_BUSY_TIMEOUT_MS` | `30000` | 写锁等待时间，超时返回 503 |

## 已知限制

* 变更流只保留写入的记录、不提供保留期清理；长期运行需自行归档。
* 水位只按港区维护且单调递增；若需要“先封存低水位、再补封存高水位”的多次
  交付，请使用 `GET /seals` 中每次封存各自的 `changes_cursor`，不要期望封存
  记录本身被改写。
* 封存只冻结事件结果，不冻结原始点表：封存后仍会入库带外的较新点位。
* 事件推导按船全量重算该港区点位；单船超大数据量时应改为增量窗口。
* SQLite 共享卷适用于单机多实例；跨主机需换成服务端数据库。
* 不做鉴权；生产部署应置于反向代理/网关之后。
