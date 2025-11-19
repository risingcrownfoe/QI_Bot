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
        - (optionally) player_name
        - (optionally) guild_name

    Behaviour:
        - Always upserts player_names / guild_names (INSERT OR IGNORE).
        - Inserts at most one snapshot per local day into `snapshots`.
        - If a snapshot for today already exists, no new snapshot or
          player_stats rows are inserted; only name mappings are updated.
    """

    def sql_int(v: Any) -> str:
        """Convert a value to a safe integer literal for SQL.

        All these columns are numeric in the schema and come from the FoE API.
        We coerce to int; on weird values we fall back to 0 or NULL.
        """
        if v is None:
            return "NULL"
        try:
            return str(int(v))
        except Exception:
            return "0"

    def sql_str(v: Any) -> str:
        """Convert a value to a safe TEXT literal for SQL."""
        if v is None:
            return "NULL"
        s = str(v)
        # Basic escaping: ' -> ''  (SQLite standard)
        s = s.replace("'", "''")
        return f"'{s}'"

    if not rows:
        log.warning("[d1] No rows to insert; skipping snapshot.")
        return {"label": None, "snapshot_id": None, "rows_inserted": 0}

    # Local "today" in game/timezone
    now = datetime.now(TZ)
    today_str = now.date().isoformat()

    # --- 1) Upsert player & guild name mappings (ALWAYS) -------------------

    player_names: dict[int, str] = {}
    guild_names: dict[int, str] = {}

    for row in rows:
        pid = row.get("player_id")
        pname = row.get("player_name")
        if pid and pname:
            try:
                player_names[int(pid)] = str(pname)
            except Exception:
                pass

        gid = row.get("guild_id")
        gname = row.get("guild_name")
        if gid and gname:
            try:
                guild_names[int(gid)] = str(gname)
            except Exception:
                pass

    # Insert-only behaviour: INSERT OR IGNORE so we don't rewrite old names.
    # (If you ever want renames, switch to ON CONFLICT(...) DO UPDATE.)
    if player_names:
        NAME_BATCH = 500
        ids = list(player_names.keys())
        for start_idx in range(0, len(ids), NAME_BATCH):
            chunk_ids = ids[start_idx : start_idx + NAME_BATCH]
            values_parts: list[str] = []
            for pid in chunk_ids:
                pname = player_names[pid]
                values_parts.append(
                    "("
                    f"{sql_int(pid)}, "
                    f"{sql_str(pname)}"
                    ")"
                )

            sql = (
                "INSERT OR IGNORE INTO player_names "
                "(player_id, player_name) VALUES\n"
                + ",\n".join(values_parts)
                + ";"
            )
            d1_query(sql)

    if guild_names:
        NAME_BATCH = 500
        ids = list(guild_names.keys())
        for start_idx in range(0, len(ids), NAME_BATCH):
            chunk_ids = ids[start_idx : start_idx + NAME_BATCH]
            values_parts: list[str] = []
            for gid in chunk_ids:
                gname = guild_names[gid]
                values_parts.append(
                    "("
                    f"{sql_int(gid)}, "
                    f"{sql_str(gname)}"
                    ")"
                )

            sql = (
                "INSERT OR IGNORE INTO guild_names "
                "(guild_id, guild_name) VALUES\n"
                + ",\n".join(values_parts)
                + ";"
            )
            d1_query(sql)

    # --- 2) Check if a snapshot for today already exists -------------------

    existing_snapshot: Mapping[str, Any] | None = None
    try:
        # captured_at is stored as ISO string, so substr(...,1,10) = 'YYYY-MM-DD'
        res_check = d1_query(
            """
            SELECT id, label, captured_at
            FROM snapshots
            WHERE substr(captured_at, 1, 10) = ?
            ORDER BY captured_at ASC
            LIMIT 1;
            """,
            [today_str],
        )
        stmts = res_check.get("result") or []
        if stmts:
            rows0 = stmts[0].get("results") or []
            if rows0:
                existing_snapshot = rows0[0]
    except Exception as e:
        log.warning("[d1] Could not check for existing daily snapshot: %s", e)

    if existing_snapshot is not None:
        log.info(
            "[d1] Snapshot already exists for today (id=%s, label='%s'); "
            "skipping player_stats insert.",
            existing_snapshot.get("id"),
            existing_snapshot.get("label"),
        )
        return {
            "label": existing_snapshot.get("label"),
            "snapshot_id": existing_snapshot.get("id"),
            "rows_inserted": 0,
            "skipped": True,
        }

    # --- 3) Create new snapshot row ---------------------------------------

    ts = now.strftime("%Y%m%d_%H%M%S")
    label = f"daily_data_{ts}"
    captured_at = now.isoformat()

    log.info("[d1] Creating snapshot '%s' with %d rows", label, len(rows))

    # Simple INSERT; we rely on the "one per day" guard above
    d1_query(
        """
        INSERT INTO snapshots (label, captured_at)
        VALUES (?, ?);
        """,
        [label, captured_at],
    )

    # --- 4) Fetch snapshot id ---------------------------------------------

    res = d1_query("SELECT id FROM snapshots WHERE label = ?;", [label])
    try:
        snapshot_id = res["result"][0]["results"][0]["id"]
    except Exception as e:  # pragma: no cover - defensive
        raise RuntimeError(
            f"Could not read snapshot id from D1 response: {res}"
        ) from e

    # --- 5) Batch-insert all player_stats rows using inline SQL literals ---

    # 6 numeric columns:
    #   snapshot_id, player_id, guild_id, era_nr, points, battles
    COLS_PER_ROW = 6
    BATCH_SIZE = 1000

    log.info(
        "[d1] Using batch size %d rows with inline literals (max ~%d values per statement)",
        BATCH_SIZE,
        BATCH_SIZE * COLS_PER_ROW,
    )

    total = 0

    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        if not chunk:
            continue

        values_parts: list[str] = []
        for row in chunk:
            values_parts.append(
                "("
                f"{sql_int(snapshot_id)}, "
                f"{sql_int(row.get('player_id'))}, "
                f"{sql_int(row.get('guild_id'))}, "
                f"{sql_int(row.get('era_nr'))}, "
                f"{sql_int(row.get('points'))}, "
                f"{sql_int(row.get('battles'))}"
                ")"
            )

        sql = (
            "INSERT INTO player_stats "
            "(snapshot_id, player_id, guild_id, era_nr, points, battles) VALUES\n"
            + ",\n".join(values_parts)
            + ";"
        )

        d1_query(sql)

        total += len(chunk)
        log.info("[d1] Inserted %d/%d player rows so far", total, len(rows))

    log.info(
        "[d1] ✅ Snapshot '%s' (id=%s) stored with %d rows",
        label,
        snapshot_id,
        total,
    )
    return {"label": label, "snapshot_id": snapshot_id, "rows_inserted": total}
