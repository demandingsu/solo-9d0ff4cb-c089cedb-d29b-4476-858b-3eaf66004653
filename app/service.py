"""Ingest, event reconciliation and cursor-based change feed.

Consistency contract
--------------------
* Every upload is applied in exactly one SQLite ``BEGIN IMMEDIATE``
  transaction.  Raw points, current events and change-log entries therefore
  become visible together; a failure rolls all of them back and the change
  cursor does not advance.
* ``BEGIN IMMEDIATE`` serialises writers across processes/instances sharing the
  database file, so two interleaved uploads cannot lose updates.
* ``(port_id, upload_key)`` is unique and is checked inside the write
  transaction.  A replayed upload returns the original result and emits no new
  changes.  The same ``upload_key`` with different content is rejected.
* The change feed uses a monotonically increasing ``seq`` primary key as the
  cursor, so clients can page strictly forward and resume after restarts.
* A port may have a *watermark* (seal).  Points at or before
  ``watermark + LATE_POINT_GRACE_SECONDS`` are rejected in whole; events that
  end at or before the watermark are frozen: later uploads may not extend,
  retract, renumber or otherwise alter them.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from . import events as event_rules
from .csv_ingest import MAX_EPOCH as _MAX_EPOCH
from .csv_ingest import MIN_EPOCH as _MIN_EPOCH
from .csv_ingest import RawPointRow, format_epoch_iso
from .db import write_transaction
from .errors import AppError

# Points at or before ``watermark + this`` are rejected after a seal.  Ten
# minutes matches the maximum gap between two points of the same stay, so a
# point inside that band could still join a sealed event.
LATE_POINT_GRACE_SECONDS = 10 * 60


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Port:
    id: int
    code: str
    name: str
    min_lat_e6: int
    max_lat_e6: int
    min_lon_e6: int
    max_lon_e6: int

    def contains(self, lat_e6: int, lon_e6: int) -> bool:
        return (
            self.min_lat_e6 <= lat_e6 <= self.max_lat_e6
            and self.min_lon_e6 <= lon_e6 <= self.max_lon_e6
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "code": self.code,
            "name": self.name,
            "bounds": {
                "min_lat": self.min_lat_e6 / 1e6,
                "max_lat": self.max_lat_e6 / 1e6,
                "min_lon": self.min_lon_e6 / 1e6,
                "max_lon": self.max_lon_e6 / 1e6,
            },
        }


def _port_from_row(row: sqlite3.Row) -> Port:
    return Port(
        id=row["id"],
        code=row["code"],
        name=row["name"],
        min_lat_e6=row["min_lat_e6"],
        max_lat_e6=row["max_lat_e6"],
        min_lon_e6=row["min_lon_e6"],
        max_lon_e6=row["max_lon_e6"],
    )


def get_port(conn: sqlite3.Connection, code: str) -> Port:
    row = conn.execute(
        "SELECT * FROM ports WHERE code = ?", (code,)
    ).fetchone()
    if row is None:
        raise AppError(404, "port_not_found", f"port {code!r} is not registered")
    return _port_from_row(row)


@contextmanager
def _transaction_if_needed(
    conn: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Join an existing write transaction, or start one.

    Lets ``register_port``/``ingest`` be called standalone (own transaction) or
    from the demo handler, where both must commit or roll back together.
    """
    if conn.in_transaction:
        yield conn
        return
    with write_transaction(conn):
        yield conn


def register_port(
    conn: sqlite3.Connection,
    *,
    code: str,
    name: str,
    min_lat_e6: int,
    max_lat_e6: int,
    min_lon_e6: int,
    max_lon_e6: int,
) -> tuple[Port, bool]:
    """Idempotently register a rectangular port.  Returns (port, created)."""
    with _transaction_if_needed(conn):
        existing = conn.execute(
            "SELECT * FROM ports WHERE code = ?", (code,)
        ).fetchone()
        if existing is not None:
            same = (
                existing["name"] == name
                and existing["min_lat_e6"] == min_lat_e6
                and existing["max_lat_e6"] == max_lat_e6
                and existing["min_lon_e6"] == min_lon_e6
                and existing["max_lon_e6"] == max_lon_e6
            )
            if not same:
                raise AppError(
                    409,
                    "port_conflict",
                    f"port {code!r} already exists with a different definition",
                )
            return _port_from_row(existing), False
        cursor = conn.execute(
            """
            INSERT INTO ports
                (code, name, min_lat_e6, max_lat_e6, min_lon_e6, max_lon_e6,
                 created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (code, name, min_lat_e6, max_lat_e6, min_lon_e6, max_lon_e6,
             utc_now_iso()),
        )
        row = conn.execute(
            "SELECT * FROM ports WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
        return _port_from_row(row), True


def _batch_response(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM ingest_batches WHERE id = ?", (batch_id,)
    ).fetchone()
    change_rows = conn.execute(
        "SELECT change_type, event_id FROM event_changes WHERE batch_id = ? "
        "ORDER BY seq",
        (batch_id,),
    ).fetchall()
    first = conn.execute(
        "SELECT MIN(seq) AS s FROM event_changes WHERE batch_id = ?", (batch_id,)
    ).fetchone()["s"]
    last = conn.execute(
        "SELECT MAX(seq) AS s FROM event_changes WHERE batch_id = ?", (batch_id,)
    ).fetchone()["s"]
    return {
        "batch_id": row["id"],
        "port_id": row["port_id"],
        "upload_key": row["upload_key"],
        "content_sha256": row["content_sha256"],
        "received_at": row["received_at"],
        "rows_received": row["rows_received"],
        "rows_new": row["rows_new"],
        "rows_duplicate": row["rows_duplicate"],
        "rows_outside_port": row["rows_outside_port"],
        "latest_point_time": row["latest_point_time"],
        "event_changes": {
            "upserted": sum(1 for c in change_rows if c["change_type"] == "upsert"),
            "retracted": sum(
                1 for c in change_rows if c["change_type"] == "retract"
            ),
        },
        "change_cursor": {"from": first, "to": last},
    }


def _load_points_for_vessels(
    conn: sqlite3.Connection, port_id: int, vessel_ids: Iterable[str]
) -> dict[str, list[event_rules.Point]]:
    result: dict[str, list[event_rules.Point]] = {}
    for vessel_id in vessel_ids:
        rows = conn.execute(
            """
            SELECT id, vessel_id, event_time, lat_e6, lon_e6
            FROM raw_points
            WHERE port_id = ? AND vessel_id = ?
            ORDER BY event_time, id
            """,
            (port_id, vessel_id),
        ).fetchall()
        result[vessel_id] = [
            event_rules.Point(
                id=r["id"],
                vessel_id=r["vessel_id"],
                event_time=r["event_time"],
                lat_e6=r["lat_e6"],
                lon_e6=r["lon_e6"],
            )
            for r in rows
        ]
    return result


def _existing_events(
    conn: sqlite3.Connection, port_id: int, vessel_ids: Iterable[str]
) -> dict[str, list[sqlite3.Row]]:
    result: dict[str, list[sqlite3.Row]] = {}
    for vessel_id in vessel_ids:
        result[vessel_id] = conn.execute(
            """
            SELECT * FROM events
            WHERE port_id = ? AND vessel_id = ?
            ORDER BY start_time, id
            """,
            (port_id, vessel_id),
        ).fetchall()
    return result


def _event_matches_row(event: sqlite3.Row, stay: event_rules.Stay) -> bool:
    return (
        event["end_time"] == stay.end_time
        and event["start_lat_e6"] == stay.start_lat_e6
        and event["start_lon_e6"] == stay.start_lon_e6
        and event["end_lat_e6"] == stay.end_lat_e6
        and event["end_lon_e6"] == stay.end_lon_e6
        and event["point_count"] == stay.point_count
    )


def _append_change(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port_id: int,
    change_type: str,
    event_id: int,
    stay: event_rules.Stay,
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO event_changes
            (batch_id, port_id, change_type, event_id, vessel_id, start_time,
             end_time, start_lat_e6, start_lon_e6, end_lat_e6, end_lon_e6,
             point_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            port_id,
            change_type,
            event_id,
            stay.vessel_id,
            stay.start_time,
            stay.end_time,
            stay.start_lat_e6,
            stay.start_lon_e6,
            stay.end_lat_e6,
            stay.end_lon_e6,
            stay.point_count,
            created_at,
        ),
    )


def _retract_row(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port_id: int,
    event: sqlite3.Row,
    created_at: str,
) -> None:
    stay = event_rules.Stay(
        vessel_id=event["vessel_id"],
        start_time=event["start_time"],
        end_time=event["end_time"],
        start_lat_e6=event["start_lat_e6"],
        start_lon_e6=event["start_lon_e6"],
        end_lat_e6=event["end_lat_e6"],
        end_lon_e6=event["end_lon_e6"],
        point_count=event["point_count"],
    )
    _append_change(
        conn,
        batch_id=batch_id,
        port_id=port_id,
        change_type="retract",
        event_id=event["id"],
        stay=stay,
        created_at=created_at,
    )
    conn.execute("DELETE FROM events WHERE id = ?", (event["id"],))


def _reconcile_vessel(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    port: Port,
    vessel_id: str,
    points: list[event_rules.Point],
    existing: list[sqlite3.Row],
    created_at: str,
) -> None:
    stays = event_rules.derive_stays(points, port.contains)

    if len({stay.key for stay in stays}) != len(stays):
        raise RuntimeError("event derivation produced duplicate keys")
    old_by_key = {(row["vessel_id"], row["start_time"]): row for row in existing}
    new_by_key = {stay.key: stay for stay in stays}

    # 1. Retract events that no longer exist, or whose payload changed.
    kept_keys: set[tuple[str, int]] = set()
    for key, row in old_by_key.items():
        stay = new_by_key.get(key)
        if stay is not None and _event_matches_row(row, stay):
            kept_keys.add(key)
            continue
        _retract_row(
            conn,
            batch_id=batch_id,
            port_id=port.id,
            event=row,
            created_at=created_at,
        )

    # 2. Insert genuinely new events.  An event whose start moved arrived here
    #    because its old key was retracted above; a same-key payload change is
    #    represented as retract + insert so consumers see a full snapshot.
    for stay in stays:
        if stay.key in kept_keys:
            continue
        cursor = conn.execute(
            """
            INSERT INTO events
                (port_id, vessel_id, start_time, end_time, start_lat_e6,
                 start_lon_e6, end_lat_e6, end_lon_e6, point_count,
                 created_batch_id, updated_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                port.id,
                stay.vessel_id,
                stay.start_time,
                stay.end_time,
                stay.start_lat_e6,
                stay.start_lon_e6,
                stay.end_lat_e6,
                stay.end_lon_e6,
                stay.point_count,
                batch_id,
                batch_id,
            ),
        )
        _append_change(
            conn,
            batch_id=batch_id,
            port_id=port.id,
            change_type="upsert",
            event_id=cursor.lastrowid,
            stay=stay,
            created_at=created_at,
        )


@dataclass(frozen=True)
class Seal:
    id: int
    port_id: int
    watermark: int
    changes_cursor: int
    event_count: int
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "seal_id": self.id,
            "port_id": self.port_id,
            "watermark": self.watermark,
            "watermark_iso": format_epoch_iso(self.watermark),
            "late_point_cutoff": self.watermark + LATE_POINT_GRACE_SECONDS,
            "late_point_cutoff_iso": format_epoch_iso(
                self.watermark + LATE_POINT_GRACE_SECONDS
            ),
            "changes_cursor": self.changes_cursor,
            "event_count": self.event_count,
            "created_at": self.created_at,
        }


def _seal_from_row(row: sqlite3.Row) -> Seal:
    return Seal(
        id=row["id"],
        port_id=row["port_id"],
        watermark=row["watermark"],
        changes_cursor=row["changes_cursor"],
        event_count=row["event_count"],
        created_at=row["created_at"],
    )


def latest_seal(conn: sqlite3.Connection, *, port_id: int) -> Seal | None:
    """The highest committed watermark for a port (``None`` if never sealed)."""
    row = conn.execute(
        """
        SELECT * FROM seals
        WHERE port_id = ?
        ORDER BY watermark DESC, id DESC
        LIMIT 1
        """,
        (port_id,),
    ).fetchone()
    return None if row is None else _seal_from_row(row)


def get_seal(conn: sqlite3.Connection, *, port_code: str, seal_id: int) -> Seal:
    port = get_port(conn, port_code)
    row = conn.execute(
        "SELECT * FROM seals WHERE id = ? AND port_id = ?",
        (seal_id, port.id),
    ).fetchone()
    if row is None:
        raise AppError(
            404,
            "seal_not_found",
            f"seal {seal_id} does not exist for port {port_code!r}",
        )
    return _seal_from_row(row)


def list_seals(conn: sqlite3.Connection, *, port_code: str) -> list[Seal]:
    port = get_port(conn, port_code)
    rows = conn.execute(
        "SELECT * FROM seals WHERE port_id = ? ORDER BY watermark, id",
        (port.id,),
    ).fetchall()
    return [_seal_from_row(row) for row in rows]


def seal_response(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    seal: Seal,
) -> dict[str, Any]:
    """Seal payload shared by the seal POST and the read endpoints.

    ``event_count``/``frozen_event_count`` come from the seal row written in
    the same transaction as ``changes_cursor``; they are never recomputed from
    a later state, so a caller can rely on seal, delivered events and cursor
    describing one committed instant.
    """
    return {
        "seal": seal.as_dict(),
        "frozen_event_count": seal.event_count,
        "resume": {
            "events": (
                f"/ports/{port_code}/seals/{seal.id}/events"
                "?after_event_id=0&limit=1000"
            ),
            "changes": f"/ports/{port_code}/changes?after={seal.changes_cursor}",
        },
    }


def _check_sealed_points(
    conn: sqlite3.Connection, *, port_id: int, watermark: int, rows: list[RawPointRow]
) -> None:
    """Reject the whole batch if any point falls at/before the cutoff."""
    cutoff = watermark + LATE_POINT_GRACE_SECONDS
    late = [row for row in rows if row.event_time <= cutoff]
    if not late:
        return
    late.sort(key=lambda row: (row.event_time, row.vessel_id))
    first = late[0]
    raise AppError(
        409,
        "point_at_or_before_watermark",
        (
            "upload contains points at or before the sealed read watermark "
            f"plus {LATE_POINT_GRACE_SECONDS} s; the whole batch was rejected"
        ),
        {
            "watermark": watermark,
            "cutoff": cutoff,
            "cutoff_iso": format_epoch_iso(cutoff),
            "rejected_points": len(late),
            "first_rejected": {
                "vessel": first.vessel_id,
                "time": first.event_time,
                "row": first.row_number,
            },
        },
    )


def _frozen_events(
    conn: sqlite3.Connection,
    *,
    port_id: int,
    watermark: int,
    vessel_ids: Iterable[str] | None = None,
) -> dict[int, tuple[Any, ...]]:
    sql = "SELECT * FROM events WHERE port_id = ? AND end_time <= ?"
    args: list[Any] = [port_id, watermark]
    if vessel_ids is not None:
        ids = list(vessel_ids)
        if not ids:
            return {}
        sql += " AND vessel_id IN (" + ",".join("?" * len(ids)) + ")"
        args.extend(ids)
    rows = conn.execute(sql + " ORDER BY id", args).fetchall()
    return {row["id"]: _event_identity(row) for row in rows}


def _event_identity(row: sqlite3.Row) -> tuple[Any, ...]:
    return (
        row["vessel_id"],
        row["start_time"],
        row["end_time"],
        row["start_lat_e6"],
        row["start_lon_e6"],
        row["end_lat_e6"],
        row["end_lon_e6"],
        row["point_count"],
    )


def _assert_frozen_events_unchanged(
    conn: sqlite3.Connection,
    *,
    port_id: int,
    watermark: int,
    vessel_ids: Iterable[str],
    before: dict[int, tuple[Any, ...]],
) -> None:
    """Fail the batch if reconciliation touched a sealed event.

    Only vessels touched by this batch are reconciled, so only their sealed
    events can possibly move.  With points at/before ``watermark + grace``
    rejected this is a defensive invariant; raising rolls the batch back.
    """
    after = _frozen_events(
        conn, port_id=port_id, watermark=watermark, vessel_ids=vessel_ids
    )
    if after != before:
        raise AppError(
            409,
            "sealed_event_change_rejected",
            (
                "the batch would alter events sealed by the port watermark; "
                "it was rejected as a whole"
            ),
            {"watermark": watermark},
        )


def seal_port(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    watermark: int,
) -> tuple[Seal, bool]:
    """Seal a UTC read watermark for a port; returns (seal, replayed).

    One ``BEGIN IMMEDIATE`` transaction reads the frozen event count and the
    change cursor and writes the seal row, so the seal, the events it covers
    and ``changes_cursor`` always describe the same committed state.  A
    watermark that does not advance the newest seal is a no-op: the existing
    seal (for an identical watermark) or the current newest seal (for an older
    one) is returned without writing.
    """
    with _transaction_if_needed(conn):
        port = get_port(conn, port_code)
        if watermark > _MAX_EPOCH or watermark < _MIN_EPOCH:
            raise AppError(
                422,
                "watermark_out_of_range",
                f"watermark {watermark} is outside the supported range",
            )
        current = latest_seal(conn, port_id=port.id)
        if current is not None and watermark <= current.watermark:
            # Never move backwards and never mint a second result for the same
            # watermark: both are idempotent no-ops.
            return current, False

        event_count = conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE port_id = ? AND end_time <= ?",
            (port.id, watermark),
        ).fetchone()["n"]
        cursor = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS s FROM event_changes WHERE port_id = ?",
            (port.id,),
        ).fetchone()["s"]
        created_at = utc_now_iso()
        inserted = conn.execute(
            """
            INSERT INTO seals
                (port_id, watermark, changes_cursor, event_count, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (port.id, watermark, int(cursor), int(event_count), created_at),
        )
        return (
            Seal(
                id=int(inserted.lastrowid),
                port_id=port.id,
                watermark=watermark,
                changes_cursor=int(cursor),
                event_count=int(event_count),
                created_at=created_at,
            ),
            True,
        )


def list_sealed_events(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    seal_id: int | None = None,
    after_event_id: int = 0,
    limit: int = 1000,
) -> dict[str, Any]:
    """Page the events that were frozen by a seal (``end_time <= watermark``).

    Reads the seal and the page in one snapshot.  ``cursor`` is the seal's
    ``changes_cursor``; pairing the page with ``/changes?after=<cursor>``
    resumes the increment without a gap.
    """
    port = get_port(conn, port_code)
    if seal_id is None:
        seal = latest_seal(conn, port_id=port.id)
        if seal is None:
            raise AppError(
                409,
                "port_not_sealed",
                f"port {port_code!r} has not been sealed yet",
            )
    else:
        seal = get_seal(conn, port_code=port_code, seal_id=seal_id)

    rows = conn.execute(
        """
        SELECT * FROM events
        WHERE port_id = ? AND end_time <= ? AND id > ?
        ORDER BY id
        LIMIT ?
        """,
        (port.id, seal.watermark, after_event_id, limit + 1),
    ).fetchall()
    has_more = len(rows) > limit
    return {
        "port_id": port.id,
        "seal": seal.as_dict(),
        "cursor": seal.changes_cursor,
        "count": min(len(rows), limit),
        "has_more": has_more,
        "events": [_event_dict(row) for row in rows[:limit]],
    }


def ingest(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    upload_key: str,
    content: bytes,
    rows: list[RawPointRow],
    rows_received: int | None = None,
    in_file_duplicates: int = 0,
) -> tuple[dict[str, Any], bool]:
    """Apply an upload atomically.  Returns (response, replayed)."""
    content_sha = hashlib.sha256(content).hexdigest()

    with _transaction_if_needed(conn):
        port = get_port(conn, port_code)

        previous = conn.execute(
            "SELECT * FROM ingest_batches WHERE port_id = ? AND upload_key = ?",
            (port.id, upload_key),
        ).fetchone()
        if previous is not None:
            if previous["content_sha256"] != content_sha:
                raise AppError(
                    409,
                    "upload_key_reused",
                    (
                        f"upload_key {upload_key!r} was already used for "
                        "different content"
                    ),
                )
            # Replay returns the stored outcome even if a later seal would now
            # reject the same points: the batch was already accepted.
            return _batch_response(conn, previous["id"]), True

        seal = latest_seal(conn, port_id=port.id)
        latest_point_time = max((row.event_time for row in rows), default=None)
        touched_vessels: set[str] = set()
        frozen_before: dict[int, tuple[Any, ...]] | None = None
        if seal is not None:
            _check_sealed_points(
                conn, port_id=port.id, watermark=seal.watermark, rows=rows
            )
            # Snapshot only the vessels this batch can reconcile.
            touched_vessels = {row.vessel_id for row in rows}
            frozen_before = _frozen_events(
                conn,
                port_id=port.id,
                watermark=seal.watermark,
                vessel_ids=touched_vessels,
            )

        created_at = utc_now_iso()
        batch_cursor = conn.execute(
            """
            INSERT INTO ingest_batches
                (port_id, upload_key, content_sha256, rows_received, rows_new,
                 rows_duplicate, rows_outside_port, received_at,
                 latest_point_time)
            VALUES (?, ?, ?, ?, 0, 0, 0, ?, ?)
            """,
            (port.id, upload_key, content_sha,
             rows_received if rows_received is not None else len(rows),
             created_at, latest_point_time),
        )
        batch_id = int(batch_cursor.lastrowid)

        new_by_vessel: set[str] = set()
        duplicates = in_file_duplicates
        outside = 0
        inserted = 0
        for row in rows:
            # Out-of-port points are stored as raw observations too; they act
            # as exit markers and make later reprocessing deterministic.
            if not port.contains(row.lat_e6, row.lon_e6):
                outside += 1
            existing = conn.execute(
                """
                SELECT id, lat_e6, lon_e6
                FROM raw_points
                WHERE port_id = ? AND vessel_id = ? AND event_time = ?
                """,
                (port.id, row.vessel_id, row.event_time),
            ).fetchone()
            if existing is not None:
                if (existing["lat_e6"], existing["lon_e6"]) != (
                    row.lat_e6,
                    row.lon_e6,
                ):
                    raise AppError(
                        409,
                        "conflicting_coordinates",
                        (
                            f"stored point for vessel {row.vessel_id!r} at "
                            f"{row.event_time} has different coordinates"
                        ),
                        {
                            "vessel": row.vessel_id,
                            "time": row.event_time,
                            "row": row.row_number,
                        },
                    )
                duplicates += 1
                continue
            conn.execute(
                """
                INSERT INTO raw_points
                    (port_id, batch_id, vessel_id, event_time, lat_e6, lon_e6)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    port.id,
                    batch_id,
                    row.vessel_id,
                    row.event_time,
                    row.lat_e6,
                    row.lon_e6,
                ),
            )
            new_by_vessel.add(row.vessel_id)
            inserted += 1

        conn.execute(
            """
            UPDATE ingest_batches
            SET rows_new = ?, rows_duplicate = ?, rows_outside_port = ?
            WHERE id = ?
            """,
            (inserted, duplicates, outside, batch_id),
        )

        if new_by_vessel:
            points = _load_points_for_vessels(conn, port.id, sorted(new_by_vessel))
            existing_events = _existing_events(
                conn, port.id, sorted(new_by_vessel)
            )
            for vessel_id in sorted(new_by_vessel):
                _reconcile_vessel(
                    conn,
                    batch_id=batch_id,
                    port=port,
                    vessel_id=vessel_id,
                    points=points[vessel_id],
                    existing=existing_events[vessel_id],
                    created_at=created_at,
                )

        if frozen_before is not None:
            _assert_frozen_events_unchanged(
                conn,
                port_id=port.id,
                watermark=seal.watermark,
                vessel_ids=touched_vessels,
                before=frozen_before,
            )

        return _batch_response(conn, batch_id), False


def list_events(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    vessel_id: str | None = None,
    after_event_id: int = 0,
    limit: int = 1000,
) -> tuple[list[dict[str, Any]], bool]:
    """Return (events, has_more), paged by ascending event id."""
    port = get_port(conn, port_code)
    sql = "SELECT * FROM events WHERE port_id = ? AND id > ?"
    args: list[Any] = [port.id, after_event_id]
    if vessel_id is not None:
        sql += " AND vessel_id = ?"
        args.append(vessel_id)
    sql += " ORDER BY id LIMIT ?"
    args.append(limit + 1)
    rows = conn.execute(sql, args).fetchall()
    has_more = len(rows) > limit
    return [_event_dict(r) for r in rows[:limit]], has_more


def _event_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["id"],
        "port_id": row["port_id"],
        "vessel_id": row["vessel_id"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "duration_seconds": row["end_time"] - row["start_time"],
        "point_count": row["point_count"],
        "start": {"lat": row["start_lat_e6"] / 1e6, "lon": row["start_lon_e6"] / 1e6},
        "end": {"lat": row["end_lat_e6"] / 1e6, "lon": row["end_lon_e6"] / 1e6},
    }


def list_changes(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    after: int,
    limit: int,
) -> dict[str, Any]:
    port = get_port(conn, port_code)
    rows = conn.execute(
        """
        SELECT * FROM event_changes
        WHERE port_id = ? AND seq > ?
        ORDER BY seq
        LIMIT ?
        """,
        (port.id, after, limit + 1),
    ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    changes = [_change_dict(r) for r in rows]
    next_cursor = rows[-1]["seq"] if rows else after
    return {
        "port_id": port.id,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "changes": changes,
    }


def _change_dict(row: sqlite3.Row) -> dict[str, Any]:
    change: dict[str, Any] = {
        "cursor": row["seq"],
        "batch_id": row["batch_id"],
        "type": row["change_type"],
        "event_id": row["event_id"],
        "vessel_id": row["vessel_id"],
        "start_time": row["start_time"],
        "end_time": row["end_time"],
        "created_at": row["created_at"],
    }
    if row["change_type"] == "upsert":
        change["event"] = {
            "event_id": row["event_id"],
            "port_id": row["port_id"],
            "vessel_id": row["vessel_id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "duration_seconds": row["end_time"] - row["start_time"],
            "point_count": row["point_count"],
            "start": {
                "lat": row["start_lat_e6"] / 1e6,
                "lon": row["start_lon_e6"] / 1e6,
            },
            "end": {
                "lat": row["end_lat_e6"] / 1e6,
                "lon": row["end_lon_e6"] / 1e6,
            },
        }
    return change


def latest_cursor(conn: sqlite3.Connection, *, port_code: str) -> int:
    port = get_port(conn, port_code)
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS s FROM event_changes WHERE port_id = ?",
        (port.id,),
    ).fetchone()
    return int(row["s"])
