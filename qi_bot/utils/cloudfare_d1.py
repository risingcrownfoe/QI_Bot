# qi_bot/utils/cloudfare_d1.py
"""Small helper to push a daily FoE snapshot into Cloudflare D1.

We talk directly to the D1 REST API (`/query` endpoint), so this works
from Render (or anywhere) without needing a Worker in front.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Mapping, Sequence

import requests

from qi_bot.config import settings

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - PY<3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

log = logging.getLogger("qi-bot")

TZ = ZoneInfo(settings.TIMEZONE)


@dataclass(frozen=True)
class D1Config:
    account_id: str
    database_id: str
    api_token: str

    @classmethod
    def from_env(cls) -> "D1Config":
        acc = os.getenv("CLOUDFLARE_ACCOUNT_ID")
        db = os.getenv("CLOUDFLARE_D1_DATABASE_ID")
        tok = os.getenv("CLOUDFLARE_D1_API_TOKEN") or os.getenv("CF_API_TOKEN")

        missing = [
            name
            for name, val in [
                ("CLOUDFLARE_ACCOUNT_ID", acc),
                ("CLOUDFLARE_D1_DATABASE_ID", db),
                ("CLOUDFLARE_D1_API_TOKEN/CF_API_TOKEN", tok),
            ]
            if not val
        ]

        if missing:
            raise RuntimeError(
                "Missing Cloudflare D1 env vars: " + ", ".join(missing)
            )

        return cls(account_id=acc or "", database_id=db or "", api_token=tok or "")


def _d1_base_url(cfg: D1Config) -> str:
    return (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{cfg.account_id}/d1/database/{cfg.database_id}"
    )


def d1_query(sql: str, params: Sequence[Any] | None = None) -> Mapping[str, Any]:
    """Execute a SQL statement via the D1 `/query` REST endpoint.

    If D1 returns an error, we raise RuntimeError with the detailed message
    from the API response so it is visible in Discord + Render logs.
    """
    cfg = D1Config.from_env()
    url = _d1_base_url(cfg) + "/query"

    body: dict[str, Any] = {"sql": sql}
    if params:
        # D1 REST API uses params as strings; SQLite will coerce types.
        body["params"] = ["" if p is None else str(p) for p in params]

    log.debug("[d1] POST %s payload=%s", url, json.dumps(body)[:500])

    try:
        r = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {cfg.api_token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=60,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to reach D1 API: {e}") from e

    text = r.text
    try:
        data = r.json()
    except Exception:
        # Non-JSON response; fall back to HTTP status and raw text
        if not r.ok:
            raise RuntimeError(
                f"D1 HTTP {r.status_code} error (non-JSON body): {text[:1000]}"
            )
        raise RuntimeError("D1 returned non-JSON response unexpectedly.")

    # If HTTP status not OK or D1 indicates failure, surface error details
    if not r.ok or not data.get("success", False):
        errors = data.get("errors") or data.get("messages") or []
        raise RuntimeError(
            f"D1 HTTP {r.status_code} error: {json.dumps(errors)[:1000]}"
        )

    return data


def insert_daily_snapshot(rows: List[Mapping[str, Any]]) -> dict[str, Any]:
    """Insert one daily snapshot plus all corresponding player_stats rows.

    Expects each row to have keys:
        - player_id
        - guild_id
        - era_nr
        - points
        - battles

    Returns a small dict with snapshot label, id and inserted row count.
    """
    if not rows:
        log.warning("[d1] No rows to insert; skipping snapshot.")
        return {"label": None, "snapshot_id": None, "rows_inserted": 0}

    now = datetime.now(TZ)
    ts = now.strftime("%Y%m%d_%H%M%S")
    label = f"daily_data_{ts}"
    captured_at = now.isoformat()

    log.info("[d1] Creating snapshot '%s' with %d rows", label, len(rows))

    # 1) Upsert snapshot row
    d1_query(
        """
        INSERT OR REPLACE INTO snapshots (label, captured_at)
        VALUES (?, ?);
        """,
        [label, captured_at],
    )

    # 2) Fetch its id
    res = d1_query("SELECT id FROM snapshots WHERE label = ?;", [label])
    try:
        snapshot_id = res["result"][0]["results"][0]["id"]
    except Exception as e:  # pragma: no cover - defensive
        raise RuntimeError(f"Could not read snapshot id from D1 response: {res}") from e

    # 3) Batch-insert all player_stats rows
    # Cloudflare D1 clearly enforces a stricter "max SQL variables" limit.
    # We insert 6 columns per row -> 6 params per row.
    # Use a *very* conservative batch size to stay well below any limit.
    COLS_PER_ROW = 6
    BATCH_SIZE = 10  # 10 * 6 = 60 SQL params per statement

    log.info(
        "[d1] Using batch size %d rows (max %d SQL params per statement)",
        BATCH_SIZE,
        BATCH_SIZE * COLS_PER_ROW,
    )

    total = 0

    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        if not chunk:
            continue

        placeholders = ", ".join(["(?, ?, ?, ?, ?, ?)"] * len(chunk))
        sql = (
            "INSERT INTO player_stats "
            "(snapshot_id, player_id, guild_id, era_nr, points, battles) "
            f"VALUES {placeholders};"
        )

        params: list[Any] = []
        for row in chunk:
            params.extend(
                [
                    snapshot_id,
                    row.get("player_id", 0),
                    row.get("guild_id", 0),
                    row.get("era_nr", 0),
                    row.get("points", 0),
                    row.get("battles", 0),
                ]
            )

        d1_query(sql, params)
        total += len(chunk)
        log.info("[d1] Inserted %d/%d player rows so far", total, len(rows))

    log.info(
        "[d1] ✅ Snapshot '%s' (id=%s) stored with %d rows",
        label,
        snapshot_id,
        total,
    )
    return {"label": label, "snapshot_id": snapshot_id, "rows_inserted": total}
