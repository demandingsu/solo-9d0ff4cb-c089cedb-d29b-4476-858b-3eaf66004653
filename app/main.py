import hashlib
import sqlite3
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated, Iterator

from fastapi import Depends, FastAPI, File, Header, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from . import __version__, csv_ingest, db, service
from .config import Settings
from .csv_ingest import MAX_EPOCH, MIN_EPOCH, format_epoch_iso
from .errors import AppError


def _parse_utc_timestamp(value: str) -> int:
    """Parse an ISO-8601 timestamp for the seal APIs (no timezone means UTC).

    Plain integers are accepted as epoch seconds for callers that already use
    the numeric ``start_time``/``end_time`` representation.  Milliseconds are
    rejected: the seal must name an unambiguous UTC instant.
    """
    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.lstrip("-").isdigit():
        epoch = int(text)
        if not (MIN_EPOCH <= epoch <= MAX_EPOCH):
            raise ValueError(f"timestamp out of range: {value!r}")
        return epoch
    normalised = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValueError(
            f"invalid UTC timestamp {value!r}; use ISO-8601, e.g. 2026-01-01T00:00:00Z"
        ) from exc
    if parsed.microsecond:
        raise ValueError("watermark must resolve to whole UTC seconds")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    epoch = int(parsed.timestamp())
    if not (MIN_EPOCH <= epoch <= MAX_EPOCH):
        raise ValueError(f"timestamp out of range: {value!r}")
    return epoch


class SealIn(BaseModel):
    """Seal a UTC read watermark.  Supply one of the two equivalent fields."""

    model_config = {"extra": "forbid"}

    watermark: str | int | None = Field(
        default=None,
        description=(
            "UTC 结束水位：ISO-8601（如 2026-01-01T12:00:00Z）或 epoch 秒"
        ),
        examples=["2026-01-01T12:00:00Z"],
    )
    watermark_time: str | int | None = Field(
        default=None,
        description="`watermark` 的别名，便于调用方按需选择字段名",
        examples=["2026-01-01T12:00:00Z"],
    )

    @model_validator(mode="after")
    def _resolve(self) -> "SealIn":
        values = [
            str(value).strip()
            for value in (self.watermark, self.watermark_time)
            if value is not None and str(value).strip()
        ]
        if not values:
            raise ValueError("provide watermark (UTC ISO-8601 or epoch seconds)")
        if len(set(values)) > 1:
            raise ValueError("watermark and watermark_time disagree")
        return self

    def epoch(self) -> int:
        raw = str(
            next(
                value
                for value in (self.watermark, self.watermark_time)
                if value is not None and str(value).strip()
            )
        ).strip()
        return _parse_utc_timestamp(raw)


class PortIn(BaseModel):
    code: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
        examples=["CNSHA-PUDONG"],
    )
    name: str = Field(min_length=1, max_length=200, examples=["上海港浦东作业区"])
    min_lat: float = Field(examples=[31.0])
    max_lat: float = Field(examples=[31.4])
    min_lon: float = Field(examples=[121.2])
    max_lon: float = Field(examples=[121.8])

    @model_validator(mode="after")
    def _check_bounds(self) -> "PortIn":
        if not (-90 <= self.min_lat < self.max_lat <= 90):
            raise ValueError("latitude bounds must satisfy -90 <= min < max <= 90")
        if not (-180 <= self.min_lon < self.max_lon <= 180):
            raise ValueError(
                "longitude bounds must satisfy -180 <= min < max <= 180"
            )
        return self

    def e6_bounds(self) -> tuple[int, int, int, int]:
        def e6(value: float) -> int:
            return int(
                (Decimal(str(value)) * 1_000_000).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )

        return (
            e6(self.min_lat),
            e6(self.max_lat),
            e6(self.min_lon),
            e6(self.max_lon),
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        db.initialise(
            resolved.db_path,
            busy_timeout_ms=resolved.sqlite_busy_timeout_ms,
        )
        yield

    app = FastAPI(
        title="港航 AIS 靠港停留事件 API",
        version=__version__,
        lifespan=lifespan,
        description=(
            "岸基 AIS 点位 -> 靠港停留事件，供排放核算使用。\n\n"
            "**核心规则**\n"
            "- 每个港区登记为矩形范围，可重复登记（内容一致幂等，不一致 409）。\n"
            "- CSV 需包含船舶号、UTC 时间、经纬度；同船同刻同坐标去重。\n"
            "- 同船同刻不同坐标整批拒收（HTTP 409），不留下半批数据。\n"
            "- 按时间排序后，相邻在港点间隔 ≤ 10 分钟才连续；出港点或超时断开。\n"
            "- 停留首末相距 ≥ 30 分钟才形成事件。\n"
            "- 迟到点改变事件时，未变事件保留 event_id；被改变事件按旧号撤回"
            "（retract），新事件以新号加入（upsert）。变更按 `seq` 游标顺序读取。\n"
            "- 每次上传的原始点、现役事件、变更记录在同一个 SQLite 写事务中提交。\n"
            "- 可封存 UTC 结束水位：只交付 `end_time <= watermark` 的事件并返回"
            "封存时的游标；水位只前进，已封存事件不再延长、撤回或换号。\n\n"
            "**在 /docs 演示迟到补点与撤回**：展开 `POST /demo/run`，点击 "
            "Try it out -> Execute。响应内含修改前后事件、retract/upsert 变更"
            "及保留编号的事件，可直接观察。\n\n"
            "**在 /docs 演示封存与续读**：展开 `POST /demo/seal/run` 执行一次，"
            "响应给出封存水位、已封存事件、续读游标，以及封存后补点的拒收结果。"
        ),
        openapi_tags=[
            {"name": "ports", "description": "港区矩形范围登记"},
            {"name": "uploads", "description": "CSV 点位上传（原子、幂等）"},
            {"name": "events", "description": "当前现役靠港停留事件"},
            {"name": "changes", "description": "按游标顺序读取的事件变更流"},
            {"name": "seals", "description": "UTC 结束水位封存与已封存事件分页"},
            {"name": "demo", "description": "迟到补点与撤回演示"},
            {"name": "system", "description": "健康检查"},
        ],
    )
    app.state.settings = resolved

    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.code,
                    "message": exc.message,
                    "details": exc.details,
                }
            },
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        details = []
        for error in exc.errors():
            item = {key: value for key, value in error.items() if key != "url"}
            context = item.get("ctx")
            if context:
                item["ctx"] = {
                    key: value if isinstance(value, (str, int, float, bool)) else str(value)
                    for key, value in context.items()
                }
            details.append(item)
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "request_validation",
                    "message": "request validation failed",
                    "details": details,
                }
            },
        )

    @app.exception_handler(sqlite3.OperationalError)
    async def _sqlite_error(_: Request, exc: sqlite3.OperationalError) -> JSONResponse:
        # A busy/locked database means the batch transaction was not committed;
        # the cursor did not move and no partial batch is visible.
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "code": "database_busy",
                    "message": "database is busy; the upload was not applied",
                    "details": str(exc),
                }
            },
            headers={"Retry-After": "1"},
        )

    @app.exception_handler(sqlite3.IntegrityError)
    async def _integrity_error(
        _: Request, exc: sqlite3.IntegrityError
    ) -> JSONResponse:
        # Constraint violations are rejected and rolled back, never partially
        # applied.  They normally indicate a programming error, so no internals
        # leak into the response.
        return JSONResponse(
            status_code=409,
            content={
                "error": {
                    "code": "integrity_conflict",
                    "message": "the upload conflicts with stored data",
                    "details": None,
                }
            },
        )

    @contextmanager
    def _connection() -> Iterator[sqlite3.Connection]:
        conn = db.connect(
            resolved.db_path,
            busy_timeout_ms=resolved.sqlite_busy_timeout_ms,
        )
        try:
            yield conn
        finally:
            conn.close()

    def get_db(request: Request) -> Iterator[sqlite3.Connection]:
        with _connection() as conn:
            yield conn

    Db = Annotated[sqlite3.Connection, Depends(get_db)]

    @app.get("/healthz", tags=["system"], summary="健康检查")
    def healthz(conn: Db) -> dict:
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "version": __version__}

    @app.post(
        "/ports",
        tags=["ports"],
        status_code=201,
        summary="登记固定港区矩形范围（幂等）",
        description=(
            "同一 code 重复登记且内容一致时返回 200/created=false；"
            "内容不一致返回 409，避免悄悄改写事件口径。"
        ),
    )
    def register_port(body: PortIn, conn: Db) -> JSONResponse:
        min_lat, max_lat, min_lon, max_lon = body.e6_bounds()
        port, created = service.register_port(
            conn,
            code=body.code,
            name=body.name,
            min_lat_e6=min_lat,
            max_lat_e6=max_lat,
            min_lon_e6=min_lon,
            max_lon_e6=max_lon,
        )
        return JSONResponse(
            status_code=201 if created else 200,
            content={"port": port.as_dict(), "created": created},
        )

    @app.get(
        "/ports/{port_code}",
        tags=["ports"],
        summary="读取港区定义",
    )
    def read_port(port_code: str, conn: Db) -> dict:
        return {"port": service.get_port(conn, port_code).as_dict()}

    @app.post(
        "/ports/{port_code}/uploads",
        tags=["uploads"],
        summary="上传 AIS 点位 CSV（整批原子生效）",
        description=(
            "CSV 表头支持 vessel/船舶号/mmsi、time/时间、lat/纬度、lon/经度。"
            "时间可为 epoch 秒/毫秒或 ISO-8601。\n\n"
            "`Idempotency-Key` 可选；未提供时按内容 SHA-256 生成。"
            "同一 key 与同一内容重复上传返回原批次结果（replayed=true），"
            "不会重复写入点位或追发变更；同一 key 不同内容返回 409。"
        ),
        responses={
            200: {"description": "重复上传，返回原批次结果"},
            201: {"description": "新批次已原子提交"},
            409: {"description": "坐标冲突或幂等键被不同内容复用"},
            413: {"description": "文件或行数超限"},
        },
    )
    def upload_points(
        port_code: str,
        conn: Db,
        file: Annotated[UploadFile, File(description="UTF-8 CSV 文件")],
        idempotency_key: Annotated[
            str | None,
            Header(
                alias="Idempotency-Key",
                description="可选幂等键；省略时按内容哈希生成",
                max_length=200,
            ),
        ] = None,
    ) -> JSONResponse:
        # Sync endpoint on purpose: SQLite access and CSV parsing are blocking,
        # so FastAPI runs this in its worker threadpool instead of stalling the
        # event loop.
        try:
            content = file.file.read(resolved.max_upload_bytes + 1)
        finally:
            file.file.close()
        if len(content) > resolved.max_upload_bytes:
            raise AppError(
                413,
                "upload_too_large",
                f"upload exceeds {resolved.max_upload_bytes} bytes",
            )
        parsed = csv_ingest.parse_upload(
            content, max_rows=resolved.max_rows_per_upload
        )
        key = (idempotency_key or "").strip() or (
            "content:" + hashlib.sha256(content).hexdigest()
        )
        result, replayed = service.ingest(
            conn,
            port_code=port_code,
            upload_key=key,
            content=content,
            rows=parsed.rows,
            rows_received=parsed.rows_received,
            in_file_duplicates=parsed.in_file_duplicates,
        )
        return JSONResponse(
            status_code=200 if replayed else 201,
            content={"replayed": replayed, **result},
        )

    @app.get(
        "/ports/{port_code}/events",
        tags=["events"],
        summary="读取当前现役停留事件",
        description=(
            "返回现役事件快照，按 `event_id` 升序分页（`after_event_id` + "
            "`limit`）。`cursor` 与快照取自同一个 WAL 读快照，保存后用 "
            "`/changes?after=` 增量读取不会漏掉中间提交的变更。"
        ),
    )
    def read_events(
        port_code: str,
        conn: Db,
        vessel_id: str | None = Query(default=None, max_length=128),
        after_event_id: int = Query(default=0, ge=0),
        limit: int = Query(default=1000, ge=1, le=10000),
    ) -> dict:
        # Read the event page and the cursor from one WAL snapshot so a client
        # that saves the cursor never skips a change that happened between the
        # two statements.
        with db.read_snapshot(conn):
            port = service.get_port(conn, port_code)
            events, has_more = service.list_events(
                conn,
                port_code=port_code,
                vessel_id=vessel_id,
                after_event_id=after_event_id,
                limit=limit,
            )
            cursor = service.latest_cursor(conn, port_code=port_code)
        return {
            "port_id": port.id,
            "cursor": cursor,
            "count": len(events),
            "has_more": has_more,
            "events": events,
        }

    @app.get(
        "/ports/{port_code}/changes",
        tags=["changes"],
        summary="按游标顺序读取事件变更",
        description=(
            "严格按 `seq` 升序返回。`next_cursor` 是本页最后一条的 seq；"
            "没有新变更时原样返回传入的 `after`。用 `has_more` 继续翻页，"
            "支持进程重启后从任意已保存游标续读。\n\n"
            "`seq` 在全库单调递增，因此某港区的游标可能出现空隙（其它港区的"
            "变更占用了这些序号）；只要按 `seq > after` 读取即可，不要假设连续。"
        ),
    )
    def read_changes(
        port_code: str,
        conn: Db,
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        with db.read_snapshot(conn):
            return service.list_changes(
                conn, port_code=port_code, after=after, limit=limit
            )

    @app.post(
        "/ports/{port_code}/seals",
        tags=["seals"],
        status_code=201,
        summary="封存 UTC 结束水位（水位只前进，重复封存幂等）",
        description=(
            "封存一个 UTC 结束水位 `watermark`（ISO-8601，如 "
            "`2026-01-01T12:00:00Z`；也接受字段名 `watermark_time`）。\n\n"
            "**语义**\n"
            "- 交付口径：`end_time <= watermark` 的事件。响应返回 `seal`"
            "（含 `changes_cursor`）与 `frozen_event_count`。\n"
            "- 封存后仍可上传较新的观测；但任何时间 `<= watermark + 600s` "
            "的点都会使整批被拒（409 `point_at_or_before_watermark`）。\n"
            "- 已封存事件不会被延长、撤回或换号；越界批次整体回滚。\n"
            "- 水位只能前进：相同水位返回 200 与原封存；更低水位不生效，"
            "返回当前最新封存（`ignored=true`）。\n"
            "- 封存、事件、游标取自同一个 `BEGIN IMMEDIATE` 提交状态。\n\n"
            "**继续读取**：`GET /ports/{code}/seals/{seal_id}/events` 分页读取"
            "已封存事件；保存 `seal.changes_cursor` 后用 "
            "`/changes?after=<cursor>` 增量读取。响应中的 `resume` 给出两个"
            "可直接使用的地址。"
        ),
        responses={
            201: {"description": "新水位已封存"},
            200: {"description": "水位未前进，返回既有封存（幂等）"},
            404: {"description": "港区未登记"},
            422: {"description": "水位格式或范围无效"},
        },
    )
    def create_seal(port_code: str, body: SealIn, conn: Db) -> JSONResponse:
        try:
            watermark = body.epoch()
        except ValueError as exc:
            raise AppError(422, "invalid_watermark", str(exc)) from exc
        seal, created = service.seal_port(
            conn, port_code=port_code, watermark=watermark
        )
        payload = service.seal_response(conn, port_code=port_code, seal=seal)
        payload.update(
            {
                "created": created,
                "advanced": created,
                "requested_watermark": watermark,
                "requested_watermark_iso": format_epoch_iso(watermark),
                # A backwards request is accepted but does not move or replace
                # the watermark; the newest existing seal is returned.
                "ignored": (not created) and watermark != seal.watermark,
            }
        )
        return JSONResponse(status_code=201 if created else 200, content=payload)

    @app.get(
        "/ports/{port_code}/seals",
        tags=["seals"],
        summary="列出已封存水位（紧接上一次封存之后的增量可用它续读）",
        description=(
            "按水位升序返回该港区全部封存记录。每条含 `changes_cursor`，"
            "用 `/changes?after=<cursor>` 读取该封存之后的增量。"
        ),
    )
    def read_seals(port_code: str, conn: Db) -> dict:
        with db.read_snapshot(conn):
            port = service.get_port(conn, port_code)
            seals = service.list_seals(conn, port_code=port_code)
            latest = service.latest_seal(conn, port_id=port.id)
        return {
            "port_id": port.id,
            "count": len(seals),
            "latest": None if latest is None else latest.as_dict(),
            "seals": [seal.as_dict() for seal in seals],
        }

    @app.get(
        "/ports/{port_code}/seals/{seal_id}",
        tags=["seals"],
        summary="读取单个封存记录及其续读地址",
    )
    def read_seal(port_code: str, seal_id: int, conn: Db) -> dict:
        with db.read_snapshot(conn):
            seal = service.get_seal(conn, port_code=port_code, seal_id=seal_id)
            return service.seal_response(conn, port_code=port_code, seal=seal)

    @app.get(
        "/ports/{port_code}/seals/{seal_id}/events",
        tags=["seals"],
        summary="按已封存水位分页读取事件",
        description=(
            "只返回 `end_time <= seal.watermark` 的事件，按 `event_id` 升序分页"
            "（`after_event_id` + `limit`）。`cursor` 即该封存的 "
            "`changes_cursor`，与 `/changes?after=` 同源：保存它后继续读取"
            "增量不会漏掉封存之后提交的变更。\n\n"
            "`seal_id` 可写 `latest`，表示该港区当前最新封存水位。"
        ),
    )
    def read_sealed_events(
        port_code: str,
        seal_id: str,
        conn: Db,
        after_event_id: int = Query(default=0, ge=0),
        limit: int = Query(default=1000, ge=1, le=10000),
    ) -> dict:
        resolved_id: int | None
        if seal_id.strip().lower() == "latest":
            resolved_id = None
        else:
            try:
                resolved_id = int(seal_id)
            except ValueError as exc:
                raise AppError(
                    422,
                    "invalid_seal_id",
                    f"seal_id must be an integer or 'latest', got {seal_id!r}",
                ) from exc
            if resolved_id < 1:
                raise AppError(
                    422, "invalid_seal_id", "seal_id must be a positive integer"
                )
        with db.read_snapshot(conn):
            return service.list_sealed_events(
                conn,
                port_code=port_code,
                seal_id=resolved_id,
                after_event_id=after_event_id,
                limit=limit,
            )

    if resolved.demo_enabled:

        @app.post(
            "/demo/run",
            tags=["demo"],
            summary="演示迟到补点与撤回（可直接在 /docs 执行）",
            description=(
                "每次执行创建一个独立演示港区：先上传基线 CSV（同船两个事件"
                " + 另一艘船一个事件），记录事件编号与游标；再上传两个迟到点，"
                "使第一个事件起点前移。响应展示：旧事件被 retract、新事件以"
                "新编号 upsert、未受影响的其余事件保留原编号，以及可按游标"
                "读取的完整变更。"
            ),
        )
        def run_demo(conn: Db) -> dict:
            # One transaction for port registration, both uploads and all
            # reads, so a demo either fully succeeds or leaves nothing behind.
            with db.write_transaction(conn):
                return _run_demo(conn)

        @app.post(
            "/demo/seal/run",
            tags=["demo"],
            summary="演示封存水位、已封存事件分页与续读（可直接在 /docs 执行）",
            description=(
                "每次执行创建一个独立演示港区：上传基线事件后封存一个 UTC "
                "结束水位，分页读取只含已封存事件的快照与游标；随后尝试上传"
                "一个落在拒收带内的点位，展示整批 409 且游标不前进；最后展示"
                "较新观测仍可上传、水位重复封存是幂等 no-op。"
            ),
        )
        def run_seal_demo(conn: Db) -> dict:
            with db.write_transaction(conn):
                return _run_seal_demo(conn)

    return app


DEMO_PORT_BOUNDS = {
    "min_lat": 31.0,
    "max_lat": 31.4,
    "min_lon": 121.2,
    "max_lon": 121.8,
}


def _run_seal_demo(conn: sqlite3.Connection) -> dict:
    """Seal a watermark, page the frozen snapshot, then show rejection/resume."""
    port_code = "SEAL-" + uuid.uuid4().hex[:8].upper()
    min_lat, max_lat, min_lon, max_lon = PortIn(
        code=port_code, name="封存演示港区", **DEMO_PORT_BOUNDS
    ).e6_bounds()
    service.register_port(
        conn,
        code=port_code,
        name="封存演示港区",
        min_lat_e6=min_lat,
        max_lat_e6=max_lat,
        min_lon_e6=min_lon,
        max_lon_e6=max_lon,
    )

    seed_rows = [
        # Event A: 00:00 -> 01:00 (60 min), 10 minute spacing
        ("SEAL-01", "2026-01-01T00:00:00Z", 31.20, 121.50),
        ("SEAL-01", "2026-01-01T00:10:00Z", 31.201, 121.501),
        ("SEAL-01", "2026-01-01T00:20:00Z", 31.202, 121.502),
        ("SEAL-01", "2026-01-01T00:30:00Z", 31.203, 121.503),
        ("SEAL-01", "2026-01-01T00:40:00Z", 31.204, 121.504),
        ("SEAL-01", "2026-01-01T00:50:00Z", 31.205, 121.505),
        ("SEAL-01", "2026-01-01T01:00:00Z", 31.206, 121.506),
        # Event B: 01:20 -> 01:50 (30 min), ends after the watermark
        ("SEAL-01", "2026-01-01T01:20:00Z", 31.21, 121.51),
        ("SEAL-01", "2026-01-01T01:30:00Z", 31.211, 121.511),
        ("SEAL-01", "2026-01-01T01:40:00Z", 31.212, 121.512),
        ("SEAL-01", "2026-01-01T01:50:00Z", 31.213, 121.513),
        # Event C: 00:05 -> 00:35 (30 min)
        ("SEAL-02", "2026-01-01T00:05:00Z", 31.30, 121.60),
        ("SEAL-02", "2026-01-01T00:15:00Z", 31.301, 121.601),
        ("SEAL-02", "2026-01-01T00:25:00Z", 31.302, 121.602),
        ("SEAL-02", "2026-01-01T00:35:00Z", 31.303, 121.603),
    ]
    seed_csv = _demo_csv(seed_rows)
    parsed_seed = csv_ingest.parse_upload(seed_csv, max_rows=1000)
    service.ingest(
        conn,
        port_code=port_code,
        upload_key="seal-demo-seed-v1",
        content=seed_csv,
        rows=parsed_seed.rows,
        rows_received=parsed_seed.rows_received,
        in_file_duplicates=parsed_seed.in_file_duplicates,
    )

    watermark = _parse_utc_timestamp("2026-01-01T01:00:00Z")
    seal, created = service.seal_port(
        conn, port_code=port_code, watermark=watermark
    )
    sealed_page = service.list_sealed_events(
        conn, port_code=port_code, seal_id=seal.id, limit=1000
    )

    # A batch with one point inside the rejected band must fail as a whole.
    rejected_rows = [
        ("SEAL-01", "2026-01-01T02:10:00Z", 31.20, 121.50),
        ("SEAL-01", "2026-01-01T01:05:00Z", 31.20, 121.50),
    ]
    rejected_csv = _demo_csv(rejected_rows)
    parsed_rejected = csv_ingest.parse_upload(rejected_csv, max_rows=1000)
    conn.execute("SAVEPOINT seal_demo_reject")
    try:
        service.ingest(
            conn,
            port_code=port_code,
            upload_key="seal-demo-rejected-v1",
            content=rejected_csv,
            rows=parsed_rejected.rows,
            rows_received=parsed_rejected.rows_received,
            in_file_duplicates=parsed_rejected.in_file_duplicates,
        )
        rejected_response: dict | None = None
    except AppError as exc:
        conn.execute("ROLLBACK TO seal_demo_reject")
        rejected_response = {
            "status_code": exc.status_code,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
        }
    finally:
        conn.execute("RELEASE seal_demo_reject")

    # Points above the cutoff are still accepted; event B (not sealed) grows.
    newer_rows = [("SEAL-01", "2026-01-01T02:00:00Z", 31.214, 121.514)]
    newer_csv = _demo_csv(newer_rows)
    parsed_newer = csv_ingest.parse_upload(newer_csv, max_rows=1000)
    newer_batch, _ = service.ingest(
        conn,
        port_code=port_code,
        upload_key="seal-demo-newer-v1",
        content=newer_csv,
        rows=parsed_newer.rows,
        rows_received=parsed_newer.rows_received,
        in_file_duplicates=parsed_newer.in_file_duplicates,
    )
    changes_after_seal = service.list_changes(
        conn,
        port_code=port_code,
        after=seal.changes_cursor,
        limit=1000,
    )
    repeated, repeated_created = service.seal_port(
        conn, port_code=port_code, watermark=watermark
    )

    return {
        "demo_port": port_code,
        "narrative": [
            "1. 基线事件：SEAL-01 两个（00:00-01:00、01:20-01:50），"
            "SEAL-02 一个（00:05-00:35）。",
            "2. POST /ports/{code}/seals 封存 2026-01-01T01:00:00Z："
            "只交付 end_time <= 01:00 的 SEAL-01 事件与 SEAL-02 事件，"
            "响应含封存时的 changes_cursor。",
            "3. 封存后尝试上传含 01:05:00 点的批次（拒收带为 "
            "watermark + 600s = 01:10:00 以内）：整批 409，"
            "游标与事件均不变。",
            "4. 上传 02:10:00 的较新观测成功，只影响未封存的 SEAL-01 "
            "第二个事件。",
            "5. 重复封存同一水位返回 200，created=false，不产生新结果。",
            "6. 保存 seal.changes_cursor 后调用 /changes?after=<cursor> "
            "即可续读封存之后的增量。",
        ],
        "baseline_csv": seed_csv.decode("utf-8"),
        "seal": service.seal_response(
            conn, port_code=port_code, seal=seal
        ),
        "sealed_events_page": sealed_page,
        "rejected_upload": {
            "csv": rejected_csv.decode("utf-8"),
            "response": rejected_response,
        },
        "newer_upload": {
            "batch": newer_batch,
            "changes_after_seal_cursor": changes_after_seal,
        },
        "repeat_seal": {
            "created": repeated_created,
            "same_seal_id": repeated.id == seal.id,
            "changes_cursor": repeated.changes_cursor,
        },
    }


def _demo_csv(rows: list[tuple[str, str, float, float]]) -> bytes:
    lines = ["vessel,time,lat,lon"]
    for vessel, time_text, lat, lon in rows:
        lines.append(f"{vessel},{time_text},{lat},{lon}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _run_demo(conn: sqlite3.Connection) -> dict:
    port_code = "DEMO-" + uuid.uuid4().hex[:8].upper()
    min_lat, max_lat, min_lon, max_lon = PortIn(
        code=port_code, name="演示港区", **DEMO_PORT_BOUNDS
    ).e6_bounds()
    service.register_port(
        conn,
        code=port_code,
        name="演示港区",
        min_lat_e6=min_lat,
        max_lat_e6=max_lat,
        min_lon_e6=min_lon,
        max_lon_e6=max_lon,
    )

    seed_rows = [
        # DEMO-01 event A: 00:00 -> 01:00 (60 min)
        ("DEMO-01", "2026-01-01T00:00:00Z", 31.20, 121.50),
        ("DEMO-01", "2026-01-01T00:10:00Z", 31.201, 121.501),
        ("DEMO-01", "2026-01-01T00:20:00Z", 31.202, 121.502),
        ("DEMO-01", "2026-01-01T00:30:00Z", 31.203, 121.503),
        ("DEMO-01", "2026-01-01T00:40:00Z", 31.204, 121.504),
        ("DEMO-01", "2026-01-01T00:50:00Z", 31.205, 121.505),
        ("DEMO-01", "2026-01-01T01:00:00Z", 31.206, 121.506),
        # gap > 10 min => event B: 01:20 -> 01:50 (30 min)
        ("DEMO-01", "2026-01-01T01:20:00Z", 31.21, 121.51),
        ("DEMO-01", "2026-01-01T01:30:00Z", 31.211, 121.511),
        ("DEMO-01", "2026-01-01T01:40:00Z", 31.212, 121.512),
        ("DEMO-01", "2026-01-01T01:50:00Z", 31.213, 121.513),
        # DEMO-02 event C: 00:05 -> 00:35 (30 min)
        ("DEMO-02", "2026-01-01T00:05:00Z", 31.30, 121.60),
        ("DEMO-02", "2026-01-01T00:15:00Z", 31.301, 121.601),
        ("DEMO-02", "2026-01-01T00:25:00Z", 31.302, 121.602),
        ("DEMO-02", "2026-01-01T00:35:00Z", 31.303, 121.603),
    ]
    late_rows = [
        # Two late points before event A's old start; 23:55 -> 00:00 gap = 5 min,
        # so A becomes 23:50 -> 01:00. Event B and C are untouched.
        ("DEMO-01", "2025-12-31T23:50:00Z", 31.19, 121.49),
        ("DEMO-01", "2025-12-31T23:55:00Z", 31.191, 121.491),
    ]

    seed_csv = _demo_csv(seed_rows)
    late_csv = _demo_csv(late_rows)
    parsed_seed = csv_ingest.parse_upload(seed_csv, max_rows=1000)
    parsed_late = csv_ingest.parse_upload(late_csv, max_rows=1000)

    seed_batch, _ = service.ingest(
        conn,
        port_code=port_code,
        upload_key="demo-seed-v1",
        content=seed_csv,
        rows=parsed_seed.rows,
        rows_received=parsed_seed.rows_received,
        in_file_duplicates=parsed_seed.in_file_duplicates,
    )
    before, _ = service.list_events(conn, port_code=port_code)
    cursor_before = service.latest_cursor(conn, port_code=port_code)

    late_batch, _ = service.ingest(
        conn,
        port_code=port_code,
        upload_key="demo-late-v1",
        content=late_csv,
        rows=parsed_late.rows,
        rows_received=parsed_late.rows_received,
        in_file_duplicates=parsed_late.in_file_duplicates,
    )
    change_page = service.list_changes(
        conn, port_code=port_code, after=cursor_before, limit=1000
    )
    after, _ = service.list_events(conn, port_code=port_code)

    before_ids = {event["event_id"] for event in before}
    after_ids = {event["event_id"] for event in after}
    retracted = [
        c["event_id"] for c in change_page["changes"] if c["type"] == "retract"
    ]
    added = [
        c["event_id"] for c in change_page["changes"] if c["type"] == "upsert"
    ]
    unchanged = sorted(before_ids & after_ids)

    return {
        "demo_port": port_code,
        "narrative": [
            "1. 基线上传形成 DEMO-01 两个事件（00:00-01:00 与 01:20-01:50）"
            "及 DEMO-02 一个事件（00:05-00:35）。",
            "2. 迟到补点上传 2025-12-31 23:50/23:55 两个点：23:55 与 00:00 "
            "间隔 5 分钟、23:50 与 23:55 间隔 5 分钟，均不超过 10 分钟，"
            "因此并入 DEMO-01 的第一个事件。",
            "3. 该事件起点前移，旧编号被 retract，新编号被 upsert；"
            "DEMO-01 第二个事件与 DEMO-02 事件未变化，保留原编号。",
            "4. change 记录按 seq 游标升序返回，可据此增量消费。",
        ],
        "baseline_csv": seed_csv.decode("utf-8"),
        "late_csv": late_csv.decode("utf-8"),
        "baseline": {
            "batch": seed_batch,
            "cursor_before_late_upload": cursor_before,
            "events": before,
        },
        "late_upload": {
            "batch": late_batch,
            "changes_after_cursor": change_page,
        },
        "final_events": after,
        "summary": {
            "retracted_event_ids": retracted,
            "added_event_ids": added,
            "unchanged_retained_event_ids": unchanged,
        },
    }


app = create_app()
