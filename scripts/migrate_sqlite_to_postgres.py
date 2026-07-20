#!/usr/bin/env python3
"""
Migrate legacy local data into Postgres.

What it migrates:
- quant-service SQLite:
  - tv_screeners
  - tv_screener_snapshots (payload JSON)
- ai-service local file config (model runtime profiles):
  - ~/.karios/ai-service.config.json (or $KARIOS_APP_DATA_DIR/ai-service.config.json)

Where it writes:
- Postgres (DATABASE_URL env var, same as data-sync-service):
  - tv_screeners
  - tv_screener_snapshots
  - ai_config_store (a simple backup table; ai-service does NOT read it yet)

Notes:
- This script is idempotent: it uses UPSERT (ON CONFLICT DO UPDATE).
- Secrets in ai-service config (API keys) will be copied into Postgres. Handle with care.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Tuple


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def default_sqlite_path() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "services" / "quant-service" / "karios.sqlite3"


def ai_config_path() -> Path:
    base = (os.getenv("KARIOS_APP_DATA_DIR") or "").strip()
    if base:
        return Path(base).expanduser() / "ai-service.config.json"
    return Path.home() / ".karios" / "ai-service.config.json"


def load_ai_config_store(p: Path) -> dict[str, Any] | None:
    if not p.exists():
        return None
    try:
        raw = p.read_text(encoding="utf-8")
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return None
        # v2 format
        if obj.get("version") == 2 and "profiles" in obj:
            return obj
        # v1 legacy single-config format => migrate to v2 (match ai-service behavior)
        if "provider" in obj and "modelId" in obj:
            return {
                "version": 2,
                "activeProfileId": "default",
                "profiles": [
                    {
                        "id": "default",
                        "name": "Default",
                        "provider": obj.get("provider"),
                        "modelId": obj.get("modelId"),
                        "openai": obj.get("openai"),
                        "google": obj.get("google"),
                        "ollama": obj.get("ollama"),
                    }
                ],
            }
        return None
    except Exception:
        return None


def iter_sqlite_rows(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> Iterable[tuple]:
    cur = conn.execute(sql, args)
    for row in cur.fetchall():
        yield row


@dataclass(frozen=True)
class TvScreenerRow:
    id: str
    name: str
    url: str
    enabled: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class TvSnapshotRow:
    id: str
    screener_id: str
    captured_at: str
    row_count: int
    payload: dict[str, Any]


def load_tv_from_sqlite(sqlite_path: Path) -> tuple[list[TvScreenerRow], list[TvSnapshotRow]]:
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")

    screeners: list[TvScreenerRow] = []
    snapshots: list[TvSnapshotRow] = []

    with sqlite3.connect(str(sqlite_path)) as conn:
        # Screeners
        try:
            for r in iter_sqlite_rows(
                conn,
                "SELECT id, name, url, enabled, created_at, updated_at FROM tv_screeners",
            ):
                screeners.append(
                    TvScreenerRow(
                        id=str(r[0]),
                        name=str(r[1] or "").strip() or "Untitled",
                        url=str(r[2] or "").strip(),
                        enabled=bool(int(r[3])) if r[3] is not None else True,
                        created_at=str(r[4] or now_iso()),
                        updated_at=str(r[5] or now_iso()),
                    )
                )
        except Exception:
            screeners = []

        # Snapshots
        try:
            for r in iter_sqlite_rows(
                conn,
                "SELECT id, screener_id, captured_at, row_count, rows_json FROM tv_screener_snapshots",
            ):
                raw = str(r[4] or "{}")
                try:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        payload = {}
                except Exception:
                    payload = {}
                snapshots.append(
                    TvSnapshotRow(
                        id=str(r[0]),
                        screener_id=str(r[1]),
                        captured_at=str(r[2] or ""),
                        row_count=int(r[3] or 0),
                        payload=payload,
                    )
                )
        except Exception:
            snapshots = []

    return screeners, snapshots


def ensure_pg_schema(conn: Any) -> None:
    # TradingView screeners & snapshots (aligned with data-sync-service schema)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_screeners (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            url         TEXT NOT NULL,
            enabled     BOOLEAN NOT NULL DEFAULT TRUE,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_screener_snapshots (
            id          TEXT PRIMARY KEY,
            screener_id TEXT NOT NULL REFERENCES tv_screeners(id) ON DELETE CASCADE,
            captured_at TEXT NOT NULL,
            row_count   INTEGER NOT NULL,
            payload     JSONB NOT NULL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tv_screeners_updated_at ON tv_screeners(updated_at DESC);")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tv_snapshots_screener_captured ON tv_screener_snapshots(screener_id, captured_at DESC);"
    )

    # AI config backup table (ai-service does not read it yet)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_config_store (
            id          TEXT PRIMARY KEY,
            store       JSONB NOT NULL,
            updated_at  TEXT NOT NULL
        );
        """
    )
    conn.commit()


def upsert_tv_screeners(conn: Any, rows: list[TvScreenerRow]) -> int:
    n = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO tv_screeners(id, name, url, enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                """,
                (r.id, r.name, r.url, r.enabled, r.created_at, r.updated_at),
            )
            n += 1
    conn.commit()
    return n


def upsert_tv_snapshots(conn: Any, rows: list[TvSnapshotRow]) -> int:
    n = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO tv_screener_snapshots(id, screener_id, captured_at, row_count, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    screener_id = EXCLUDED.screener_id,
                    captured_at = EXCLUDED.captured_at,
                    row_count = EXCLUDED.row_count,
                    payload = EXCLUDED.payload
                """,
                (r.id, r.screener_id, r.captured_at, int(r.row_count), json.dumps(r.payload, ensure_ascii=False)),
            )
            n += 1
    conn.commit()
    return n


def upsert_ai_config_store(conn: Any, store: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_config_store(id, store, updated_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (id) DO UPDATE SET
                store = EXCLUDED.store,
                updated_at = EXCLUDED.updated_at
            """,
            ("default", json.dumps(store, ensure_ascii=False), now_iso()),
        )
    conn.commit()


def main() -> int:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise SystemExit("Missing DATABASE_URL")

    # Lazy import so this script can be linted without psycopg stubs.
    import psycopg  # type: ignore[import-not-found]

    sqlite_env = (os.getenv("KARIOS_QUANT_SQLITE_PATH") or "").strip()
    sqlite_path = Path(sqlite_env).expanduser() if sqlite_env else default_sqlite_path()

    ai_env = (os.getenv("KARIOS_AI_CONFIG_PATH") or "").strip()
    ai_path = Path(ai_env).expanduser() if ai_env else ai_config_path()

    screeners, snapshots = load_tv_from_sqlite(sqlite_path)
    ai_store = load_ai_config_store(ai_path)

    print(f"[tv] sqlite={sqlite_path} screeners={len(screeners)} snapshots={len(snapshots)}")
    print(f"[ai] config={ai_path} present={'yes' if ai_store else 'no'}")

    with psycopg.connect(database_url, connect_timeout=5) as conn:
        ensure_pg_schema(conn)
        n1 = upsert_tv_screeners(conn, screeners)
        n2 = upsert_tv_snapshots(conn, snapshots)
        if ai_store is not None:
            upsert_ai_config_store(conn, ai_store)
        print(f"[pg] upserted screeners={n1} snapshots={n2} ai_config={'yes' if ai_store else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

