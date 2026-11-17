"""End-to-end seal/resume demo against a running service.

Usage:
    python scripts/demo_seal.py [BASE_URL]

Creates an isolated ``SEAL-SCRIPT-XXXXXX`` port, uploads two events, seals a
watermark, pages the sealed events, shows that a point inside the rejected band
is refused as a whole batch, that newer points are still accepted, and that a
repeated seal is an idempotent no-op.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
import uuid

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
PORT_CODE = "SEAL-SCRIPT-" + uuid.uuid4().hex[:6].upper()
BOUNDS = {"min_lat": 31.0, "max_lat": 31.4, "min_lon": 121.2, "max_lon": 121.8}
SEAL = "2026-01-01T01:00:00Z"


def request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:  # 409 responses carry the payload
        return {"http_status": exc.code, **json.loads(exc.read())}


def upload(rows: list[tuple[str, str, float, float]], key: str) -> dict:
    boundary = "----demo" + uuid.uuid4().hex
    csv_body = "vessel,time,lat,lon\n" + "\n".join(
        f"{vessel},{time},{lat},{lon}" for vessel, time, lat, lon in rows
    ) + "\n"
    payload = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="ais.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
        f"{csv_body}\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    req = urllib.request.Request(
        f"{BASE}/ports/{PORT_CODE}/uploads",
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Idempotency-Key": key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return {
                "http_status": response.status,
                **json.loads(response.read()),
            }
    except urllib.error.HTTPError as exc:
        return {"http_status": exc.code, **json.loads(exc.read())}


def main() -> None:
    request("POST", "/ports", {"code": PORT_CODE, "name": "封存演示", **BOUNDS})
    seed = [
        ("SHIP-A", f"2026-01-01T00:{minute:02d}:00Z", 31.2, 121.5)
        for minute in range(0, 60, 10)
    ]
    seed += [
        ("SHIP-A", f"2026-01-01T01:{minute:02d}:00Z", 31.201, 121.501)
        for minute in range(20, 60, 10)
    ]
    seed += [
        ("SHIP-B", "2026-01-01T00:05:00Z", 31.3, 121.6),
        ("SHIP-B", "2026-01-01T00:15:00Z", 31.3, 121.6),
        ("SHIP-B", "2026-01-01T00:25:00Z", 31.3, 121.6),
        ("SHIP-B", "2026-01-01T00:35:00Z", 31.3, 121.6),
    ]
    print("seed upload:", upload(seed, "seed")["rows_new"], "points")

    sealed = request(
        "POST", f"/ports/{PORT_CODE}/seals", {"watermark": SEAL}
    )
    print("\nseal:", json.dumps(sealed["seal"], ensure_ascii=False))
    print("frozen events:", sealed["frozen_event_count"])

    page = request(
        "GET",
        f"/ports/{PORT_CODE}/seals/{sealed['seal']['seal_id']}/events",
    )
    print("\nsealed page cursor:", page["cursor"])
    for event in page["events"]:
        print(
            f"  id={event['event_id']} {event['vessel_id']} "
            f"{event['start_time']}..{event['end_time']}"
        )

    rejected = upload(
        [
            ("SHIP-A", "2026-01-01T02:10:00Z", 31.2, 121.5),
            ("SHIP-A", "2026-01-01T01:05:00Z", 31.2, 121.5),
        ],
        "rejected",
    )
    print("\nband rejection:", rejected["http_status"], rejected["error"]["code"])

    accepted = upload([("SHIP-A", "2026-01-01T02:00:00Z", 31.202, 121.502)], "newer")
    print("newer upload:", accepted["http_status"], "rows_new:", accepted["rows_new"])

    after = request(
        "GET",
        f"/ports/{PORT_CODE}/changes?after={sealed['seal']['changes_cursor']}",
    )
    print(
        "changes since seal cursor:",
        [(c["type"], c["event_id"]) for c in after["changes"]],
    )

    repeat = request("POST", f"/ports/{PORT_CODE}/seals", {"watermark": SEAL})
    print(
        "repeat seal:",
        repeat.get("created"),
        "same seal_id:",
        repeat["seal"]["seal_id"] == sealed["seal"]["seal_id"],
    )


if __name__ == "__main__":
    main()
