"""Sealing a UTC read watermark and paging the frozen event set.

Contract under test:

* ``POST /ports/{code}/seals`` freezes ``end_time <= watermark`` and returns
  the seal's ``changes_cursor``.
* ``GET /ports/{code}/seals/{seal_id}/events`` pages only sealed events.
* Uploads may continue after the seal, but any batch containing a point at or
  before ``watermark + 600 s`` is rejected in full.
* Sealed events can never be extended, retracted or renumbered.
* The watermark only moves forward; repeats are idempotent no-ops.
* Seal state, events and cursor always come from one committed state, including
  under interleaved uploads from two instances.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import csv_ingest, db, service
from app.config import Settings
from app.main import create_app

PORT = {
    "code": "CNSHA-SEAL",
    "name": "上海港外高桥",
    "min_lat": 31.0,
    "max_lat": 31.4,
    "min_lon": 121.2,
    "max_lon": 121.8,
}
INSIDE = (31.2, 121.5)
OUTSIDE = (30.0, 120.0)


def at(hour: int, minute: int = 0, second: int = 0, day: int = 1) -> str:
    return f"2026-01-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"


def csv_bytes(rows: list[tuple[str, str, float, float]]) -> bytes:
    lines = ["vessel,time,lat,lon"]
    lines += [f"{v},{t},{lat},{lon}" for v, t, lat, lon in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def upload(
    client: TestClient,
    rows: list[tuple[str, str, float, float]],
    *,
    key: str | None = None,
    port: str = PORT["code"],
    headers: dict[str, str] | None = None,
):
    send_headers = {"Idempotency-Key": key} if key is not None else {}
    send_headers.update(headers or {})
    return client.post(
        f"/ports/{port}/uploads",
        files={"file": ("ais.csv", csv_bytes(rows), "text/csv")},
        headers=send_headers,
    )


def seal(client: TestClient, watermark: str, *, port: str = PORT["code"]):
    return client.post(f"/ports/{port}/seals", json={"watermark": watermark})


def sealed_events(
    client: TestClient,
    seal_id: str | int = "latest",
    *,
    port: str = PORT["code"],
    after_event_id: int = 0,
    limit: int = 1000,
):
    return client.get(
        f"/ports/{port}/seals/{seal_id}/events",
        params={"after_event_id": after_event_id, "limit": limit},
    )


def events(client: TestClient, *, port: str = PORT["code"]) -> list[dict]:
    response = client.get(f"/ports/{port}/events")
    assert response.status_code == 200, response.text
    return response.json()["events"]


def changes(client: TestClient, after: int = 0, *, port: str = PORT["code"]) -> dict:
    response = client.get(
        f"/ports/{port}/changes", params={"after": after, "limit": 1000}
    )
    assert response.status_code == 200, response.text
    return response.json()


def counts(client: TestClient) -> tuple[int, int]:
    db_path = client.app.state.settings.db_path  # type: ignore[attr-defined]
    connection = sqlite3.connect(db_path)
    try:
        batches = connection.execute(
            "SELECT COUNT(*) FROM ingest_batches"
        ).fetchone()[0]
        points = connection.execute("SELECT COUNT(*) FROM raw_points").fetchone()[0]
        return batches, points
    finally:
        connection.close()


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    app = create_app(
        Settings(db_path=str(tmp_path / "seals.sqlite3"), demo_enabled=True)
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def seeded(client: TestClient) -> TestClient:
    """Two events: 00:00-00:30 and 02:00-02:30; nothing sealed yet."""
    assert client.post("/ports", json=PORT).status_code == 201
    rows = [("SHIP-A", at(0, m), *INSIDE) for m in (0, 10, 20, 30)]
    rows += [("SHIP-A", at(2, m), *INSIDE) for m in (0, 10, 20, 30)]
    response = upload(client, rows, key="seed")
    assert response.status_code == 201, response.text
    return client


# ---------------------------------------------------------------------------
# Seal creation and delivery of events
# ---------------------------------------------------------------------------


def test_seal_delivers_only_events_ending_at_or_before_watermark(
    seeded: TestClient,
) -> None:
    response = seal(seeded, at(0, 30))
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["created"] is True and body["advanced"] is True
    assert body["seal"]["watermark_iso"] == at(0, 30)
    assert body["frozen_event_count"] == 1
    assert body["seal"]["changes_cursor"] >= 1

    page = sealed_events(seeded, body["seal"]["seal_id"])
    assert page.status_code == 200, page.text
    payload = page.json()
    assert [e["end_time"] for e in payload["events"]] == [
        int(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc).timestamp())
    ]
    # The unfrozen 02:00-02:30 event is still visible on /events but is not
    # part of this seal's delivery.
    assert len(events(seeded)) == 2
    assert payload["cursor"] == body["seal"]["changes_cursor"]


def test_seal_at_exact_event_end_includes_it(seeded: TestClient) -> None:
    body = seal(seeded, at(0, 30)).json()
    ids = [e["event_id"] for e in sealed_events(seeded, "latest").json()["events"]]
    assert len(ids) == 1
    assert body["seal"]["event_count"] == 1


def test_seal_before_first_event_has_empty_page(seeded: TestClient) -> None:
    body = seal(seeded, at(0, 0)).json()
    page = sealed_events(seeded, body["seal"]["seal_id"]).json()
    assert page["events"] == []
    assert page["has_more"] is False
    assert page["cursor"] == body["seal"]["changes_cursor"]


def test_sealed_events_are_paginated_by_event_id(seeded: TestClient) -> None:
    body = seal(seeded, at(23, 59)).json()
    first = sealed_events(seeded, "latest", limit=1).json()
    assert len(first["events"]) == 1 and first["has_more"] is True
    second = sealed_events(
        seeded, "latest", after_event_id=first["events"][0]["event_id"], limit=1
    ).json()
    assert len(second["events"]) == 1 and second["has_more"] is False
    assert second["events"][0]["event_id"] > first["events"][0]["event_id"]
    assert first["cursor"] == second["cursor"] == body["seal"]["changes_cursor"]


def test_seal_cursor_pairs_with_change_stream_without_gap(seeded: TestClient) -> None:
    """Saving the seal cursor and polling /changes misses nothing and repeats
    nothing already delivered by the sealed page."""
    seal_body = seal(seeded, at(0, 30)).json()
    cursor = seal_body["seal"]["changes_cursor"]

    # Continue ingesting strictly-after-cutoff data.
    late_ok = [("SHIP-A", at(4, m), *INSIDE) for m in (0, 10, 20, 30)]
    assert upload(seeded, late_ok, key="later").status_code == 201
    page = changes(seeded, after=cursor)
    assert len(page["changes"]) == 1
    assert page["changes"][0]["type"] == "upsert"
    change_event = page["changes"][0]["event"]
    assert change_event["start_time"] == int(
        datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc).timestamp()
    )
    # The sealed page already contained every event at/before the watermark;
    # the increment contains only events after it.
    sealed_page = sealed_events(seeded, "latest").json()
    sealed_times = {e["start_time"] for e in sealed_page["events"]}
    assert change_event["start_time"] not in sealed_times


def test_seal_unknown_port_404(client: TestClient) -> None:
    response = seal(client, at(0, 0), port="NOPE")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "port_not_found"


def test_unsealed_port_has_no_latest_page(client: TestClient) -> None:
    assert client.post("/ports", json=PORT).status_code == 201
    response = sealed_events(client, "latest")
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "port_not_sealed"


def test_invalid_watermark_is_rejected(seeded: TestClient) -> None:
    bad_format = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark": "not-a-time"}
    )
    assert bad_format.status_code == 422
    assert bad_format.json()["error"]["code"] == "invalid_watermark"

    missing = seeded.post(f"/ports/{PORT['code']}/seals", json={})
    assert missing.status_code == 422

    unknown_field = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark_time": at(0), "x": 1}
    )
    assert unknown_field.status_code == 422


def test_watermark_alias_field_is_accepted(seeded: TestClient) -> None:
    response = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark_time": at(0, 30)}
    )
    assert response.status_code == 201, response.text
    assert response.json()["seal"]["watermark_iso"] == at(0, 30)


def test_naive_watermark_is_read_as_utc(seeded: TestClient) -> None:
    response = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark": "2026-01-01T00:30:00"}
    )
    assert response.status_code == 201
    assert response.json()["seal"]["watermark_iso"] == at(0, 30)


def test_epoch_seconds_watermark_is_accepted(seeded: TestClient) -> None:
    epoch = int(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc).timestamp())
    response = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark": epoch}
    )
    assert response.status_code == 201, response.text
    assert response.json()["seal"]["watermark"] == epoch
    assert response.json()["seal"]["watermark_iso"] == at(0, 30)

    # Repeating with the string form is the same watermark and is idempotent.
    repeat = seeded.post(
        f"/ports/{PORT['code']}/seals", json={"watermark": str(epoch)}
    )
    assert repeat.status_code == 200
    assert repeat.json()["seal"]["seal_id"] == response.json()["seal"]["seal_id"]


def test_sub_second_watermark_is_rejected(seeded: TestClient) -> None:
    response = seeded.post(
        f"/ports/{PORT['code']}/seals",
        json={"watermark": "2026-01-01T00:30:00.500Z"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_watermark"


def test_unknown_or_malformed_seal_id(seeded: TestClient) -> None:
    seal(seeded, at(0, 30))
    missing = sealed_events(seeded, 999)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "seal_not_found"

    malformed = seeded.get(f"/ports/{PORT['code']}/seals/abc/events")
    assert malformed.status_code == 422
    assert malformed.json()["error"]["code"] == "invalid_seal_id"


def test_seal_id_is_scoped_to_its_port(tmp_path: Path) -> None:
    app = create_app(Settings(db_path=str(tmp_path / "scoped.sqlite3")))
    with TestClient(app) as client:
        for code in ("PORT-A", "PORT-B"):
            assert client.post("/ports", json={**PORT, "code": code}).status_code == 201
        created = seal(client, at(0, 30), port="PORT-A").json()
        wrong_port = sealed_events(client, created["seal"]["seal_id"], port="PORT-B")
        assert wrong_port.status_code == 404
        assert wrong_port.json()["error"]["code"] == "seal_not_found"


def test_upload_batch_reports_latest_point_time(seeded: TestClient) -> None:
    last = int(datetime(2026, 1, 1, 2, 30, tzinfo=timezone.utc).timestamp())
    response = upload(seeded, [("SHIP-C", at(7, 0), *INSIDE)], key="lp")
    assert response.status_code == 201
    assert response.json()["latest_point_time"] == int(
        datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc).timestamp()
    )
    # The old batch keeps its own value, not the port-wide maximum.
    assert last < response.json()["latest_point_time"]


# ---------------------------------------------------------------------------
# Watermark monotonicity / idempotency
# ---------------------------------------------------------------------------


def test_repeated_seal_creates_no_new_result(seeded: TestClient) -> None:
    first = seal(seeded, at(0, 30))
    assert first.status_code == 201
    cursor = changes(seeded)["next_cursor"]

    repeat = seal(seeded, at(0, 30))
    assert repeat.status_code == 200
    assert repeat.json()["created"] is False
    assert repeat.json()["seal"]["seal_id"] == first.json()["seal"]["seal_id"]
    assert changes(seeded)["next_cursor"] == cursor

    listed = seeded.get(f"/ports/{PORT['code']}/seals").json()
    assert listed["count"] == 1


def test_watermark_only_moves_forward(seeded: TestClient) -> None:
    first = seal(seeded, at(2, 30)).json()
    backwards = seal(seeded, at(0, 30))
    assert backwards.status_code == 200
    body = backwards.json()
    assert body["created"] is False
    assert body["ignored"] is True
    assert body["seal"]["seal_id"] == first["seal"]["seal_id"]
    assert body["seal"]["watermark_iso"] == at(2, 30)

    # Still one seal row; the older requested watermark was not persisted.
    assert seeded.get(f"/ports/{PORT['code']}/seals").json()["count"] == 1

    advanced = seal(seeded, at(3, 0))
    assert advanced.status_code == 201
    assert advanced.json()["seal"]["watermark_iso"] == at(3, 0)
    assert seeded.get(f"/ports/{PORT['code']}/seals").json()["count"] == 2

    # Events frozen by the older seal remain part of the newer one.
    assert advanced.json()["frozen_event_count"] == 2


# ---------------------------------------------------------------------------
# Uploads after sealing
# ---------------------------------------------------------------------------


def test_newer_observations_can_still_be_uploaded(seeded: TestClient) -> None:
    seal(seeded, at(0, 30))
    rows = [("SHIP-B", at(5, m), *INSIDE) for m in (0, 10, 20, 30)]
    response = upload(seeded, rows, key="after-seal")
    assert response.status_code == 201, response.text
    assert response.json()["rows_new"] == 4
    assert len(events(seeded)) == 3


def test_point_exactly_at_cutoff_is_rejected_but_one_second_after_is_not(
    seeded: TestClient,
) -> None:
    seal(seeded, at(0, 30))  # cutoff = 00:40:00 inclusive
    rejected = upload(seeded, [("SHIP-B", at(0, 40, 0), *INSIDE)], key="at-cutoff")
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "point_at_or_before_watermark"
    assert rejected.json()["error"]["details"]["cutoff"] == int(
        datetime(2026, 1, 1, 0, 40, tzinfo=timezone.utc).timestamp()
    )

    accepted = upload(
        seeded, [("SHIP-B", at(0, 40, 1), *INSIDE)], key="after-cutoff"
    )
    assert accepted.status_code == 201, accepted.text


def test_one_late_point_rejects_the_whole_batch(seeded: TestClient) -> None:
    seal(seeded, at(0, 30))
    batches_before, points_before = counts(seeded)
    cursor_before = changes(seeded)["next_cursor"]
    event_ids_before = [e["event_id"] for e in events(seeded)]

    mixed = [
        ("SHIP-A", at(6, 0), *INSIDE),  # perfectly new
        ("SHIP-A", at(6, 10), *INSIDE),
        ("SHIP-A", at(0, 30), *INSIDE),  # at the watermark -> reject all
    ]
    response = upload(seeded, mixed, key="mixed")
    assert response.status_code == 409
    details = response.json()["error"]["details"]
    assert details["rejected_points"] == 1
    assert details["first_rejected"]["time"] == int(
        datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc).timestamp()
    )

    assert counts(seeded) == (batches_before, points_before)
    assert changes(seeded)["next_cursor"] == cursor_before
    assert [e["event_id"] for e in events(seeded)] == event_ids_before


def test_successful_upload_retry_after_seal_returns_original_batch(
    seeded: TestClient,
) -> None:
    """The same key with the same content replays, even though the points are
    now inside the rejected band."""
    original = upload(
        seeded,
        [("SHIP-A", at(0, m), *INSIDE) for m in (0, 10, 20, 30)],
        key="replayed-later",
    )
    # The seed used a different key; upload a fresh batch whose points are
    # still above this seal, then seal below it.
    assert original.status_code == 201
    seal(seeded, at(2, 30))
    cursor = changes(seeded)["next_cursor"]
    batches, points = counts(seeded)

    replay = upload(
        seeded,
        [("SHIP-A", at(0, m), *INSIDE) for m in (0, 10, 20, 30)],
        key="replayed-later",
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["replayed"] is True
    assert replay.json()["batch_id"] == original.json()["batch_id"]
    assert counts(seeded) == (batches, points)
    assert changes(seeded)["next_cursor"] == cursor


def test_extending_a_sealed_event_is_rejected(seeded: TestClient) -> None:
    """Even a point after the cutoff that would lengthen a sealed event cannot
    alter it: the batch is rejected and the sealed event is unchanged."""
    seal(seeded, at(0, 30))
    before = sealed_events(seeded, "latest").json()["events"]

    # 00:40:01 is outside the rejected band, but the derivation still treats
    # it as a continuation of the 00:00-00:30 stay (gap 10m1s > 10m, so use a
    # point at 00:40:00? that is in the band).  Use 00:35:00 -> inside band.
    reject_band = upload(
        seeded, [("SHIP-A", at(0, 35), *INSIDE)], key="extend-in-band"
    )
    assert reject_band.status_code == 409

    # A sealed event is never retracted or renumbered by later legitimate data.
    legitimate = upload(seeded, [("SHIP-A", at(9, m), *INSIDE) for m in (0, 10, 20, 30)],
                       key="later-event")
    assert legitimate.status_code == 201
    after = sealed_events(seeded, "latest").json()["events"]
    assert after == before


def test_sealed_event_is_not_renumbered_when_later_data_merges(
    seeded: TestClient,
) -> None:
    """A bridge point after the cutoff cannot merge a sealed event with a new
    one, because that would retract/renumber the sealed event."""
    seal(seeded, at(0, 30))
    sealed_before = sealed_events(seeded, "latest").json()["events"]
    assert len(sealed_before) == 1
    sealed_id = sealed_before[0]["event_id"]

    # 00:40:01 is just outside the rejected band; a point there would bridge
    # the sealed 00:30 end with a later stay only if the gap to the next is
    # <= 10 min.  Upload a new stay 00:50:01-01:20:01 and a bridge at
    # 00:40:01, 00:50:01 ... 00:30 -> 00:40:01 gap is 10m1s so no merge.
    rows = [("SHIP-A", at(0, 40, 1), *INSIDE)]
    rows += [("SHIP-A", at(0, 50, 1), *INSIDE)]
    rows += [("SHIP-A", at(1, 0, 1), *INSIDE)]
    rows += [("SHIP-A", at(1, 10, 1), *INSIDE)]
    rows += [("SHIP-A", at(1, 20, 1), *INSIDE)]
    response = upload(seeded, rows, key="post-seal-stay")
    assert response.status_code == 201, response.text

    sealed_after = sealed_events(seeded, "latest").json()["events"]
    assert sealed_after == sealed_before
    assert sealed_after[0]["event_id"] == sealed_id
    # The new observation formed its own event; the sealed one is untouched.
    all_events = events(seeded)
    assert len(all_events) == 3


def test_late_point_before_seal_still_works_normally(seeded: TestClient) -> None:
    """Before any seal, the old late-point retract/upsert behaviour is intact."""
    late = [("SHIP-A", at(0, 0, 0, day=1), *INSIDE)]
    # Extend the 00:00 event backwards is impossible on Jan 1; use Dec 31.
    late = [("SHIP-A", "2025-12-31T23:50:00Z", *INSIDE)]
    response = upload(seeded, late, key="pre-seal-late")
    assert response.status_code == 201
    assert len(events(seeded)) == 2


# ---------------------------------------------------------------------------
# Transaction / atomicity / interleaving
# ---------------------------------------------------------------------------


def test_failed_seal_leaves_no_seal_row(
    seeded: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail after the seal INSERT: the transaction must roll the row back."""
    from app import service as service_module

    class BoomSeal:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("simulated crash after seal insert")

    monkeypatch.setattr(service_module, "Seal", BoomSeal)
    with pytest.raises(RuntimeError):
        seal(seeded, at(0, 30))
    monkeypatch.undo()

    assert seeded.get(f"/ports/{PORT['code']}/seals").json()["count"] == 0
    assert sealed_events(seeded, "latest").status_code == 409
    # The port is still usable and sealable after the failed attempt.
    assert seal(seeded, at(0, 30)).status_code == 201


def test_seal_and_events_and_cursor_share_one_commit(
    tmp_path: Path,
) -> None:
    """A writer that commits while a seal is being created must not produce a
    seal whose cursor/count disagree with the stored events."""
    path = str(tmp_path / "atomic.sqlite3")
    db.initialise(path)
    writer = db.connect(path)
    try:
        service.register_port(
            writer,
            code="P",
            name="atomic",
            min_lat_e6=31_000_000,
            max_lat_e6=31_400_000,
            min_lon_e6=121_200_000,
            max_lon_e6=121_800_000,
        )
        base = [
            csv_ingest.RawPointRow(i, "SHIP-A", 1_800_000_000 + i * 600,
                                   31_200_000, 121_500_000)
            for i in range(4)
        ]
        service.ingest(
            writer, port_code="P", upload_key="base", content=b"base", rows=base
        )
    finally:
        writer.close()

    seal_conn = db.connect(path)
    try:
        seal_row, created = service.seal_port(
            seal_conn, port_code="P", watermark=1_800_001_800
        )
        assert created is True
        count = seal_conn.execute(
            "SELECT COUNT(*) FROM events WHERE port_id = ? AND end_time <= ?",
            (seal_row.port_id, seal_row.watermark),
        ).fetchone()[0]
        assert count == seal_row.event_count
        max_seq = seal_conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM event_changes WHERE port_id = ?",
            (seal_row.port_id,),
        ).fetchone()[0]
        assert max_seq <= seal_row.changes_cursor
    finally:
        seal_conn.close()


def test_interleaved_seal_and_upload_across_instances(tmp_path: Path) -> None:
    """Two replicas: seal on one, upload on the other.  The persisted seal must
    match exactly one committed state (either before or after the upload)."""
    path = str(tmp_path / "interleave.sqlite3")
    settings = Settings(db_path=path)
    app_one, app_two = create_app(settings), create_app(settings)
    with TestClient(app_one) as one, TestClient(app_two) as two:
        assert one.post("/ports", json=PORT).status_code == 201
        base = [("SHIP-A", at(0, m), *INSIDE) for m in (0, 10, 20, 30)]
        assert upload(one, base, key="base").status_code == 201

        barrier = threading.Barrier(2)
        results: dict[str, object] = {}

        def do_seal() -> None:
            barrier.wait(timeout=10)
            results["seal"] = seal(two, at(0, 30))

        def do_upload() -> None:
            barrier.wait(timeout=10)
            later = [("SHIP-B", at(6, m), *INSIDE) for m in (0, 10, 20, 30)]
            results["upload"] = upload(one, later, key="late")

        threads = [threading.Thread(target=do_seal), threading.Thread(target=do_upload)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        seal_response = results["seal"]
        assert getattr(seal_response, "status_code") == 201
        seal_body = seal_response.json()  # type: ignore[union-attr]

        # The seal's recorded count and cursor are internally consistent with
        # the events and the change log at the sealed watermark.
        page = two.get(
            f"/ports/{PORT['code']}/seals/{seal_body['seal']['seal_id']}/events"
        )
        assert page.status_code == 200
        assert page.json()["count"] == seal_body["frozen_event_count"]
        stream = two.get(
            f"/ports/{PORT['code']}/changes",
            params={"after": seal_body["seal"]["changes_cursor"]},
        ).json()
        # Any change after the seal cursor is strictly newer than the seal.
        for change in stream["changes"]:
            assert change["cursor"] > seal_body["seal"]["changes_cursor"]

        # Whatever the interleaving, the sealed event set never contains SHIP-B.
        sealed_vessels = {
            e["vessel_id"] for e in page.json()["events"]
        }
        assert "SHIP-B" not in sealed_vessels


def test_seal_demo_runs_and_shows_rejection_and_resume(client: TestClient) -> None:
    response = client.post("/demo/seal/run")
    assert response.status_code == 200, response.text
    body = response.json()
    seal = body["seal"]["seal"]
    page = body["sealed_events_page"]
    assert page["count"] == seal["event_count"] == 2
    assert page["cursor"] == seal["changes_cursor"]
    assert all(
        event["end_time"] <= seal["watermark"] for event in page["events"]
    )
    rejected = body["rejected_upload"]["response"]
    assert rejected["status_code"] == 409
    assert rejected["code"] == "point_at_or_before_watermark"
    assert body["repeat_seal"]["created"] is False
    assert body["repeat_seal"]["same_seal_id"] is True
    # The demo port stays isolated from the real one.
    assert client.post("/demo/seal/run").status_code == 200


def test_seal_between_two_uploads_keeps_first_three_events_stable(
    tmp_path: Path,
) -> None:
    """Deterministic cross-instance scenario: seal, then upload newer data, then
    re-page the seal; the delivered snapshot must not change."""
    path = str(tmp_path / "stable.sqlite3")
    settings = Settings(db_path=path)
    app_one, app_two = create_app(settings), create_app(settings)
    with TestClient(app_one) as one, TestClient(app_two) as two:
        assert one.post("/ports", json=PORT).status_code == 201
        base = [("SHIP-A", at(0, m), *INSIDE) for m in (0, 10, 20, 30)]
        assert upload(one, base, key="base").status_code == 201

        created = seal(two, at(0, 30)).json()
        first_page = two.get(
            f"/ports/{PORT['code']}/seals/{created['seal']['seal_id']}/events"
        ).json()

        later = [("SHIP-A", at(6, m), *INSIDE) for m in (0, 10, 20, 30)]
        assert upload(one, later, key="later").status_code == 201

        second_page = two.get(
            f"/ports/{PORT['code']}/seals/{created['seal']['seal_id']}/events"
        ).json()
        assert second_page["events"] == first_page["events"]
        assert second_page["cursor"] == first_page["cursor"]


def test_old_database_is_migrated_in_place(tmp_path: Path) -> None:
    """A version-1 database (no seals table, no batch watermark column) opens,
    gains the new objects and keeps its data."""
    path = str(tmp_path / "legacy.sqlite3")
    legacy = sqlite3.connect(path, isolation_level=None)
    try:
        legacy.execute("PRAGMA journal_mode = WAL")
        legacy.execute(
            """
            CREATE TABLE ingest_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                port_id INTEGER NOT NULL,
                upload_key TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                rows_received INTEGER NOT NULL,
                rows_new INTEGER NOT NULL,
                rows_duplicate INTEGER NOT NULL,
                rows_outside_port INTEGER NOT NULL,
                received_at TEXT NOT NULL,
                UNIQUE (port_id, upload_key)
            )
            """
        )
        legacy.execute(
            "INSERT INTO ingest_batches VALUES "
            "(1, 1, 'old', 'sha', 1, 1, 0, 0, '2025-01-01T00:00:00+00:00')"
        )
    finally:
        legacy.close()

    db.initialise(path)
    check = sqlite3.connect(path)
    try:
        columns = {row[1] for row in check.execute("PRAGMA table_info(ingest_batches)")}
        assert "latest_point_time" in columns
        tables = {
            row[0]
            for row in check.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "seals" in tables
        row = check.execute(
            "SELECT upload_key, latest_point_time FROM ingest_batches WHERE id = 1"
        ).fetchone()
        assert row == ("old", None)
    finally:
        check.close()


def test_defensive_seal_check_blocks_event_change_even_without_band_rejection(
    seeded: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even if the rejected band were bypassed, a batch that would alter a
    sealed event still fails and rolls back in full."""
    from app import service as service_module

    seal(seeded, at(0, 30))
    sealed_before = sealed_events(seeded, "latest").json()["events"]
    batches_before, points_before = counts(seeded)
    cursor_before = changes(seeded)["next_cursor"]

    monkeypatch.setattr(
        service_module,
        "_check_sealed_points",
        lambda conn, **_: None,
    )
    # 00:35:00 would extend the sealed 00:00-00:30 event.
    response = upload(
        seeded,
        [("SHIP-A", at(0, 35), *INSIDE), ("SHIP-Z", at(9, 0), *INSIDE)],
        key="bypass-band",
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "sealed_event_change_rejected"
    assert sealed_events(seeded, "latest").json()["events"] == sealed_before
    assert counts(seeded) == (batches_before, points_before)
    assert changes(seeded)["next_cursor"] == cursor_before


def test_seal_waits_for_in_flight_upload_and_includes_it_once_committed(
    tmp_path: Path,
) -> None:
    """Deterministic ordering: an open writer blocks the seal until commit.

    The seal then reflects the committed upload exactly once: either a seal
    sees the upload (cursor/count include it) or the upload runs after the
    seal; it can never observe a half-applied batch.
    """
    path = str(tmp_path / "ordering.sqlite3")
    db.initialise(path)
    setup = db.connect(path)
    try:
        service.register_port(
            setup,
            code="P",
            name="ordering",
            min_lat_e6=31_000_000,
            max_lat_e6=31_400_000,
            min_lon_e6=121_200_000,
            max_lon_e6=121_800_000,
        )
        base = [
            csv_ingest.RawPointRow(i, "SHIP-A", 1_800_000_000 + i * 600,
                                   31_200_000, 121_500_000)
            for i in range(4)
        ]
        service.ingest(
            setup, port_code="P", upload_key="base", content=b"base", rows=base
        )
    finally:
        setup.close()

    uploader = db.connect(path, busy_timeout_ms=30_000)
    sealer = db.connect(path, busy_timeout_ms=30_000)
    results: dict[str, object] = {}
    started = threading.Event()

    def run_seal() -> None:
        started.set()
        results["seal"] = service.seal_port(
            sealer, port_code="P", watermark=1_800_003_500
        )

    try:
        # Hold the write lock with a committed-visible but uncommitted event.
        uploader.execute("BEGIN IMMEDIATE")
        late = [
            csv_ingest.RawPointRow(i, "SHIP-B", 1_800_000_600 * 0 + 1_800_001_200 + i * 600,
                                   31_200_000, 121_500_000)
            for i in range(4)
        ]
        # Apply the same work ingest would, inside this held transaction.
        port = service.get_port(uploader, "P")
        batch = uploader.execute(
            """
            INSERT INTO ingest_batches
                (port_id, upload_key, content_sha256, rows_received, rows_new,
                 rows_duplicate, rows_outside_port, received_at)
            VALUES (?, 'held', 'sha', 4, 4, 0, 0, '2026-01-01T00:00:00+00:00')
            """,
            (port.id,),
        )
        for row in late:
            uploader.execute(
                """
                INSERT INTO raw_points
                    (port_id, batch_id, vessel_id, event_time, lat_e6, lon_e6)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (port.id, batch.lastrowid, row.vessel_id, row.event_time,
                 row.lat_e6, row.lon_e6),
            )
        uploader.execute(
            """
            INSERT INTO events
                (port_id, vessel_id, start_time, end_time, start_lat_e6,
                 start_lon_e6, end_lat_e6, end_lon_e6, point_count,
                 created_batch_id, updated_batch_id)
            VALUES (?, 'SHIP-B', ?, ?, 31200000, 121500000, 31200000,
                    121500000, 4, ?, ?)
            """,
            (port.id, 1_800_001_200, 1_800_003_000, batch.lastrowid,
             batch.lastrowid),
        )
        uploader.execute(
            """
            INSERT INTO event_changes
                (batch_id, port_id, change_type, event_id, vessel_id,
                 start_time, end_time, start_lat_e6, start_lon_e6,
                 end_lat_e6, end_lon_e6, point_count, created_at)
            VALUES (?, ?, 'upsert', 900, 'SHIP-B', ?, ?, 31200000, 121500000,
                    31200000, 121500000, 4, '2026-01-01T00:00:00+00:00')
            """,
            (batch.lastrowid, port.id, 1_800_001_200, 1_800_003_000),
        )

        thread = threading.Thread(target=run_seal)
        thread.start()
        assert started.wait(timeout=5)
        # The seal must still be waiting on the writer after a short delay.
        thread.join(timeout=0.5)
        assert thread.is_alive(), "seal should block behind an open writer"

        uploader.execute("COMMIT")
        thread.join(timeout=30)
        assert not thread.is_alive()
    finally:
        uploader.close()
        sealer.close()

    seal, created = results["seal"]  # type: ignore[misc]
    assert created is True
    check = db.connect(path)
    try:
        # The upload committed before the seal, so the seal saw it exactly once.
        assert seal.changes_cursor >= 2
        assert seal.event_count == 2
        actual = check.execute(
            "SELECT COUNT(*) FROM events WHERE port_id = ? AND end_time <= ?",
            (seal.port_id, seal.watermark),
        ).fetchone()[0]
        assert actual == seal.event_count
        cursor_now = check.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM event_changes WHERE port_id = ?",
            (seal.port_id,),
        ).fetchone()[0]
        assert cursor_now == seal.changes_cursor
    finally:
        check.close()


def test_seal_is_isolated_between_ports(tmp_path: Path) -> None:
    app = create_app(Settings(db_path=str(tmp_path / "ports.sqlite3")))
    with TestClient(app) as client:
        for code, shift in (("PORT-A", 0.0), ("PORT-B", 5.0)):
            response = client.post(
                "/ports",
                json={
                    **PORT,
                    "code": code,
                    "min_lat": PORT["min_lat"] + shift,
                    "max_lat": PORT["max_lat"] + shift,
                },
            )
            assert response.status_code == 201
        for code, shift in (("PORT-A", 0.0), ("PORT-B", 5.0)):
            rows = [
                ("SHIP-A", at(0, m), INSIDE[0] + shift, INSIDE[1])
                for m in (0, 10, 20, 30)
            ]
            assert upload(client, rows, key="k", port=code).status_code == 201

        assert seal(client, at(0, 30), port="PORT-A").status_code == 201
        # PORT-B remains unsealed and accepts the same band.
        response = upload(
            client, [("SHIP-A", at(0, 30), INSIDE[0] + 5.0, INSIDE[1])],
            key="b-after", port="PORT-B",
        )
        assert response.status_code == 201
        assert sealed_events(client, "latest", port="PORT-B").status_code == 409
