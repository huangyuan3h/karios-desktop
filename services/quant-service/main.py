from __future__ import annotations

import base64
import hashlib
import http.client
import json
import math
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from market.akshare_provider import (
    BarRow,
    StockRow,
    fetch_cn_a_chip_summary,
    fetch_cn_a_daily_bars,
    fetch_cn_a_fund_flow,
    fetch_cn_a_minute_bars,
    fetch_cn_a_spot,
    fetch_cn_concept_boards_spot,
    fetch_cn_concept_members,
    fetch_cn_failed_limitup_rate,
    fetch_cn_industry_boards_spot,
    fetch_cn_industry_fund_flow_eod,
    fetch_cn_industry_fund_flow_hist,
    fetch_cn_industry_members,
    fetch_cn_limitup_pool,
    fetch_cn_market_breadth_eod,
    fetch_cn_yesterday_limitup_premium,
    fetch_hk_daily_bars,
    fetch_hk_spot,
)
from tv.capture import capture_screener_over_cdp_sync
from tv.normalize import split_symbol_cell


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: int
    db_path: str


def load_config() -> ServerConfig:
    return ServerConfig(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "4320")),
        db_path=os.getenv("DATABASE_PATH", str(Path(__file__).with_name("karios.sqlite3"))),
    )


app = FastAPI(title="Karios Quant Service", version="0.1.0")

# Local desktop app: keep it permissive for v0.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _connect() -> sqlite3.Connection:
    default_db = str(Path(__file__).with_name("karios.sqlite3"))
    db_path = os.getenv("DATABASE_PATH", default_db)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_prompts (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          content TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_screeners (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          url TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tv_screener_snapshots (
          id TEXT PRIMARY KEY,
          screener_id TEXT NOT NULL,
          captured_at TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          headers_json TEXT NOT NULL,
          rows_json TEXT NOT NULL,
          FOREIGN KEY(screener_id) REFERENCES tv_screeners(id)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_stocks (
          symbol TEXT PRIMARY KEY,
          market TEXT NOT NULL,
          ticker TEXT NOT NULL,
          name TEXT NOT NULL,
          currency TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_quotes (
          symbol TEXT PRIMARY KEY,
          price TEXT,
          change_pct TEXT,
          volume TEXT,
          turnover TEXT,
          market_cap TEXT,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          FOREIGN KEY(symbol) REFERENCES market_stocks(symbol)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_bars (
          symbol TEXT NOT NULL,
          date TEXT NOT NULL,
          open TEXT,
          high TEXT,
          low TEXT,
          close TEXT,
          volume TEXT,
          amount TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(symbol, date),
          FOREIGN KEY(symbol) REFERENCES market_stocks(symbol)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_chips (
          symbol TEXT NOT NULL,
          date TEXT NOT NULL,
          profit_ratio TEXT,
          avg_cost TEXT,
          cost90_low TEXT,
          cost90_high TEXT,
          cost90_conc TEXT,
          cost70_low TEXT,
          cost70_high TEXT,
          cost70_conc TEXT,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          PRIMARY KEY(symbol, date),
          FOREIGN KEY(symbol) REFERENCES market_stocks(symbol)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_fund_flow (
          symbol TEXT NOT NULL,
          date TEXT NOT NULL,
          close TEXT,
          change_pct TEXT,
          main_net_amount TEXT,
          main_net_ratio TEXT,
          super_net_amount TEXT,
          super_net_ratio TEXT,
          large_net_amount TEXT,
          large_net_ratio TEXT,
          medium_net_amount TEXT,
          medium_net_ratio TEXT,
          small_net_amount TEXT,
          small_net_ratio TEXT,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          PRIMARY KEY(symbol, date),
          FOREIGN KEY(symbol) REFERENCES market_stocks(symbol)
        )
        """,
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cn_industry_fund_flow_daily (
          date TEXT NOT NULL,
          industry_code TEXT NOT NULL,
          industry_name TEXT NOT NULL,
          net_inflow REAL NOT NULL,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          PRIMARY KEY(date, industry_code)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_industry_fund_flow_date ON market_cn_industry_fund_flow_daily(date DESC)",
    )

    # --- CN market breadth & sentiment (v0) ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cn_sentiment_daily (
          date TEXT PRIMARY KEY,
          as_of_date TEXT NOT NULL,
          up_count INTEGER NOT NULL,
          down_count INTEGER NOT NULL,
          flat_count INTEGER NOT NULL,
          total_count INTEGER NOT NULL,
          up_down_ratio REAL NOT NULL,
          yesterday_limitup_premium REAL NOT NULL,
          failed_limitup_rate REAL NOT NULL,
          risk_mode TEXT NOT NULL,
          rules_json TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          raw_json TEXT NOT NULL
        )
        """,
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cn_sentiment_date ON market_cn_sentiment_daily(date DESC)")

    # --- Broker snapshots (v0) ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_accounts (
          id TEXT PRIMARY KEY,
          broker TEXT NOT NULL,
          title TEXT NOT NULL,
          account_masked TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_accounts_broker_updated ON broker_accounts(broker, updated_at DESC)",
    )

    # Consolidated broker account state (v0): keep a single up-to-date view per account.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_account_state (
          account_id TEXT PRIMARY KEY,
          broker TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          overview_json TEXT NOT NULL,
          positions_json TEXT NOT NULL,
          conditional_orders_json TEXT NOT NULL,
          trades_json TEXT NOT NULL,
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_account_state_broker_updated ON broker_account_state(broker, updated_at DESC)",
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_snapshots (
          id TEXT PRIMARY KEY,
          broker TEXT NOT NULL,
          captured_at TEXT NOT NULL,
          kind TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          image_path TEXT NOT NULL,
          extracted_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """,
    )
    # Add account_id column to existing DBs (SQLite has limited ALTER TABLE).
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(broker_snapshots)").fetchall()}
    if "account_id" not in cols:
        conn.execute("ALTER TABLE broker_snapshots ADD COLUMN account_id TEXT;")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_snapshots_broker_captured ON broker_snapshots(broker, captured_at DESC)",
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_broker_snapshots_broker_sha256 ON broker_snapshots(broker, sha256)",
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_broker_snapshots_broker_account_sha256 ON broker_snapshots(broker, account_id, sha256)",
    )

    # --- Strategy module (v0) ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_account_prompts (
          account_id TEXT PRIMARY KEY,
          strategy_prompt TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_reports (
          id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          date TEXT NOT NULL,
          created_at TEXT NOT NULL,
          model TEXT NOT NULL,
          input_snapshot_json TEXT NOT NULL,
          output_json TEXT NOT NULL,
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leader_stocks (
          id TEXT PRIMARY KEY,
          date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          market TEXT NOT NULL,
          ticker TEXT NOT NULL,
          name TEXT NOT NULL,
          entry_price REAL,
          score REAL,
          reason TEXT NOT NULL,
          why_bullets_json TEXT,
          expected_duration_days INTEGER,
          buy_zone_json TEXT,
          triggers_json TEXT,
          invalidation TEXT,
          target_price_json TEXT,
          probability INTEGER,
          source_signals_json TEXT NOT NULL,
          risk_points_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(date, symbol)
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leader_stock_scores (
          symbol TEXT PRIMARY KEY,
          live_score REAL NOT NULL,
          breakdown_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_rank_snapshots (
          id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          universe_version TEXT NOT NULL,
          created_at TEXT NOT NULL,
          output_json TEXT NOT NULL,
          UNIQUE(account_id, as_of_date, universe_version),
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_rank_snapshots_date ON cn_rank_snapshots(as_of_date DESC)",
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_intraday_rank_snapshots (
          id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          slot TEXT NOT NULL,
          universe_version TEXT NOT NULL,
          created_at TEXT NOT NULL,
          output_json TEXT NOT NULL,
          UNIQUE(account_id, as_of_ts, slot, universe_version),
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_intraday_rank_snapshots_trade_date ON cn_intraday_rank_snapshots(trade_date DESC)",
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_intraday_rank_snapshots_as_of_ts ON cn_intraday_rank_snapshots(as_of_ts DESC)",
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_intraday_observations (
          id TEXT PRIMARY KEY,
          trade_date TEXT NOT NULL,
          ts TEXT NOT NULL,
          kind TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_intraday_observations_trade_date ON cn_intraday_observations(trade_date DESC)",
    )
    # Optional minute bars cache (only for small candidate pools).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cn_minute_bars (
          symbol TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          interval TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          bars_json TEXT NOT NULL,
          PRIMARY KEY(symbol, trade_date, interval)
        )
        """,
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_mainline_snapshots (
          id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          universe_version TEXT NOT NULL,
          created_at TEXT NOT NULL,
          output_json TEXT NOT NULL,
          FOREIGN KEY(account_id) REFERENCES broker_accounts(id)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_mainline_snapshots_trade_date ON cn_mainline_snapshots(trade_date DESC)",
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_mainline_snapshots_as_of_ts ON cn_mainline_snapshots(as_of_ts DESC)",
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_mainline_snapshots_account_date ON cn_mainline_snapshots(account_id, trade_date DESC)",
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_theme_membership_cache (
          theme_key TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          members_json TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(theme_key, trade_date)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_theme_membership_cache_trade_date ON cn_theme_membership_cache(trade_date DESC)",
    )
    # Backward-compatible migration: add missing columns for existing DBs.
    try:
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(leader_stocks)").fetchall()}
        def _add(col: str, ddl: str) -> None:
            if col not in cols:
                conn.execute(f"ALTER TABLE leader_stocks ADD COLUMN {ddl}")
        _add("why_bullets_json", "why_bullets_json TEXT")
        _add("expected_duration_days", "expected_duration_days INTEGER")
        _add("buy_zone_json", "buy_zone_json TEXT")
        _add("triggers_json", "triggers_json TEXT")
        _add("invalidation", "invalidation TEXT")
        _add("target_price_json", "target_price_json TEXT")
        _add("probability", "probability INTEGER")
    except Exception:
        # Best-effort: do not block startup if schema probing fails.
        pass
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_reports_account_date ON strategy_reports(account_id, date)",
    )
    conn.commit()
    return conn


def get_setting(key: str) -> str | None:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])


def set_setting(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()


class PortfolioSnapshotResponse(BaseModel):
    ok: bool
    message: str


class SystemPromptResponse(BaseModel):
    value: str


class SystemPromptRequest(BaseModel):
    value: str


class SystemPromptPresetSummary(BaseModel):
    id: str
    title: str
    updatedAt: str


class ListSystemPromptPresetsResponse(BaseModel):
    items: list[SystemPromptPresetSummary]


class SystemPromptPresetDetail(BaseModel):
    id: str
    title: str
    content: str


class CreateSystemPromptPresetRequest(BaseModel):
    title: str
    content: str


class CreateSystemPromptPresetResponse(BaseModel):
    id: str


class UpdateSystemPromptPresetRequest(BaseModel):
    title: str
    content: str


class ActiveSystemPromptResponse(BaseModel):
    id: str | None
    title: str
    content: str


class SetActiveSystemPromptRequest(BaseModel):
    id: str | None


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.get("/portfolio/snapshot", response_model=PortfolioSnapshotResponse)
def portfolio_snapshot() -> PortfolioSnapshotResponse:
    return PortfolioSnapshotResponse(ok=True, message="Not implemented yet.")


@app.get("/settings/system-prompt", response_model=SystemPromptResponse)
def get_system_prompt() -> SystemPromptResponse:
    active = get_active_system_prompt()
    value = active.content if active else (get_setting("system_prompt") or "")
    return SystemPromptResponse(value=value)


@app.put("/settings/system-prompt")
def put_system_prompt(req: SystemPromptRequest) -> dict[str, bool]:
    # Backward compatible: if there's an active preset, update that preset's content.
    # Otherwise store the legacy single-value setting.
    active_id = get_setting("active_system_prompt_id")
    if active_id:
        updated = update_system_prompt_preset(active_id, title=None, content=req.value)
        if updated:
            return {"ok": True}
    set_setting("system_prompt", req.value)
    return {"ok": True}


def now_iso() -> str:
    # Use ISO 8601 for cross-language compatibility.
    return datetime.now(tz=UTC).isoformat()


_intraday_scheduler_started = False
_intraday_scheduler_lock = threading.Lock()


def _should_start_intraday_scheduler() -> bool:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False
    v = str(os.getenv("ENABLE_INTRADAY_RANK_SCHEDULER", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _run_intraday_scheduler_loop() -> None:
    """
    A lightweight in-process scheduler for desktop usage.
    It triggers intraday rank snapshots at configured clock times (Asia/Shanghai).
    """
    tz = ZoneInfo("Asia/Shanghai")
    # Target times (HH:MM, local time)
    targets = [
        ("09:15", "preopen_intent"),
        ("09:25", "opening_anchor"),
        ("10:25", "hourly_prep"),
        ("11:25", "hourly_prep"),
        ("13:55", "hourly_prep"),
        ("14:35", "hourly_prep"),
    ]
    fired: set[str] = set()
    while True:
        try:
            now_cn = datetime.now(tz=tz)
            trade_date = now_cn.strftime("%Y-%m-%d")
            hhmm = now_cn.strftime("%H:%M")
            for t, kind in targets:
                key = f"{trade_date}|{t}|{kind}"
                if key in fired:
                    continue
                # Fire within a small window to tolerate sleep jitter.
                if hhmm == t:
                    # Default account: first pingan account.
                    accs = list_broker_accounts(broker="pingan")
                    aid = accs[0].id if accs else ""
                    if aid:
                        try:
                            as_of_ts = now_iso()
                            slot = _infer_intraday_slot(now_cn)
                            # Observation first (best-effort).
                            _append_cn_intraday_observation(
                                trade_date=trade_date,
                                ts=as_of_ts,
                                kind=kind,
                                raw={"note": "scheduled", "slot": slot},
                            )
                            # Generate snapshot.
                            out = _intraday_rank_build_and_score(
                                account_id=aid,
                                as_of_ts=as_of_ts,
                                slot=slot,
                                limit=30,
                                universe_version="v0",
                            )
                            _upsert_cn_intraday_rank_snapshot(
                                account_id=aid,
                                as_of_ts=as_of_ts,
                                trade_date=str(out.get("tradeDate") or trade_date),
                                slot=str(out.get("slot") or slot),
                                universe_version="v0",
                                ts=as_of_ts,
                                output=out,
                            )
                            _prune_cn_intraday_rank_snapshots(account_id=aid, keep_days=10)
                        except Exception:
                            # Do not crash the scheduler loop.
                            pass
                    fired.add(key)
            # Reset fired set when a new trade_date begins.
            fired = {k for k in fired if k.startswith(f"{trade_date}|")}
        except Exception:
            pass
        time.sleep(20)


def _start_intraday_scheduler() -> None:
    global _intraday_scheduler_started
    if not _should_start_intraday_scheduler():
        return
    with _intraday_scheduler_lock:
        if _intraday_scheduler_started:
            return
        t = threading.Thread(target=_run_intraday_scheduler_loop, name="intraday-rank-scheduler", daemon=True)
        t.start()
        _intraday_scheduler_started = True


@app.on_event("startup")
def _on_startup() -> None:
    _start_intraday_scheduler()


def _finite_float(x: Any, default: float = 0.0) -> float:
    """
    Convert to float and sanitize NaN/Inf to a finite default.
    This prevents:
    - SQLite NOT NULL constraint failures when a provider returns NaN
    - JSON serialization crashes (some encoders reject NaN)
    """
    try:
        f = float(x)
        return f if math.isfinite(f) else float(default)
    except Exception:
        return float(default)


def _prune_cn_rank_snapshots(*, keep_days: int = 10) -> None:
    keep = max(1, min(int(keep_days), 60))
    with _connect() as conn:
        # Keep by date string ordering (YYYY-MM-DD).
        rows = conn.execute(
            "SELECT DISTINCT as_of_date FROM cn_rank_snapshots ORDER BY as_of_date DESC",
        ).fetchall()
        dates = [str(r[0]) for r in rows if r and r[0]]
        to_delete = dates[keep:]
        for d in to_delete:
            conn.execute("DELETE FROM cn_rank_snapshots WHERE as_of_date = ?", (d,))
        conn.commit()


def _get_cn_rank_snapshot(*, account_id: str, as_of_date: str, universe_version: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, created_at, output_json
            FROM cn_rank_snapshots
            WHERE account_id = ? AND as_of_date = ? AND universe_version = ?
            """,
            (account_id, as_of_date, universe_version),
        ).fetchone()
    if row is None:
        return None
    try:
        out = json.loads(str(row[2]) or "{}")
    except Exception:
        out = {}
    return {"id": str(row[0]), "createdAt": str(row[1]), "output": out}


def _upsert_cn_rank_snapshot(*, account_id: str, as_of_date: str, universe_version: str, ts: str, output: dict[str, Any]) -> str:
    snap_id = str(uuid.uuid4())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO cn_rank_snapshots(id, account_id, as_of_date, universe_version, created_at, output_json)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, as_of_date, universe_version) DO UPDATE SET
              id = excluded.id,
              created_at = excluded.created_at,
              output_json = excluded.output_json
            """,
            (snap_id, account_id, as_of_date, universe_version, ts, json.dumps(output or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()
    return snap_id


def _prune_cn_intraday_rank_snapshots(*, account_id: str, keep_days: int = 10) -> None:
    keep = max(1, min(int(keep_days), 60))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM cn_intraday_rank_snapshots
            WHERE account_id = ?
            ORDER BY trade_date DESC
            """,
            (account_id,),
        ).fetchall()
        dates = [str(r[0]) for r in rows if r and r[0]]
        to_delete = dates[keep:]
        for d in to_delete:
            conn.execute(
                "DELETE FROM cn_intraday_rank_snapshots WHERE account_id = ? AND trade_date = ?",
                (account_id, d),
            )
        conn.commit()


def _get_cn_intraday_rank_snapshot_latest(
    *,
    account_id: str,
    universe_version: str,
) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, created_at, output_json
            FROM cn_intraday_rank_snapshots
            WHERE account_id = ? AND universe_version = ?
            ORDER BY as_of_ts DESC
            LIMIT 1
            """,
            (account_id, universe_version),
        ).fetchone()
    if row is None:
        return None
    try:
        out = json.loads(str(row[2]) or "{}")
    except Exception:
        out = {}
    return {"id": str(row[0]), "createdAt": str(row[1]), "output": out}


def _get_cn_intraday_rank_snapshot_latest_for(
    *,
    account_id: str,
    trade_date: str,
    slot: str,
    universe_version: str,
) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, created_at, output_json
            FROM cn_intraday_rank_snapshots
            WHERE account_id = ? AND trade_date = ? AND slot = ? AND universe_version = ?
            ORDER BY as_of_ts DESC
            LIMIT 1
            """,
            (account_id, trade_date, slot, universe_version),
        ).fetchone()
    if row is None:
        return None
    try:
        out = json.loads(str(row[2]) or "{}")
    except Exception:
        out = {}
    return {"id": str(row[0]), "createdAt": str(row[1]), "output": out}


def _upsert_cn_intraday_rank_snapshot(
    *,
    account_id: str,
    as_of_ts: str,
    trade_date: str,
    slot: str,
    universe_version: str,
    ts: str,
    output: dict[str, Any],
) -> str:
    snap_id = str(uuid.uuid4())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO cn_intraday_rank_snapshots(
              id, account_id, as_of_ts, trade_date, slot, universe_version, created_at, output_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, as_of_ts, slot, universe_version) DO UPDATE SET
              id = excluded.id,
              trade_date = excluded.trade_date,
              created_at = excluded.created_at,
              output_json = excluded.output_json
            """,
            (
                snap_id,
                account_id,
                as_of_ts,
                trade_date,
                slot,
                universe_version,
                ts,
                json.dumps(output or {}, ensure_ascii=False, default=str),
            ),
        )
        conn.commit()
    return snap_id


def _append_cn_intraday_observation(*, trade_date: str, ts: str, kind: str, raw: dict[str, Any]) -> str:
    obs_id = str(uuid.uuid4())
    created_at = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO cn_intraday_observations(id, trade_date, ts, kind, raw_json, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (obs_id, trade_date, ts, kind, json.dumps(raw or {}, ensure_ascii=False, default=str), created_at),
        )
        conn.commit()
    return obs_id


def _list_cn_intraday_observations(*, trade_date: str) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, trade_date, ts, kind, raw_json, created_at
            FROM cn_intraday_observations
            WHERE trade_date = ?
            ORDER BY ts ASC
            """,
            (trade_date,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            raw = json.loads(str(r[4]) or "{}")
        except Exception:
            raw = {}
        out.append(
            {
                "id": str(r[0]),
                "tradeDate": str(r[1]),
                "ts": str(r[2]),
                "kind": str(r[3]),
                "raw": raw,
                "createdAt": str(r[5]),
            }
        )
    return out


def _get_cn_minute_bars_cached(*, symbol: str, trade_date: str, interval: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT updated_at, bars_json
            FROM market_cn_minute_bars
            WHERE symbol = ? AND trade_date = ? AND interval = ?
            """,
            (symbol, trade_date, interval),
        ).fetchone()
    if row is None:
        return None
    try:
        bars = json.loads(str(row[1]) or "[]")
    except Exception:
        bars = []
    return {"updatedAt": str(row[0]), "bars": bars if isinstance(bars, list) else []}


def _upsert_cn_minute_bars_cached(*, symbol: str, trade_date: str, interval: str, ts: str, bars: list[dict[str, Any]]) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO market_cn_minute_bars(symbol, trade_date, interval, updated_at, bars_json)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_date, interval) DO UPDATE SET
              updated_at = excluded.updated_at,
              bars_json = excluded.bars_json
            """,
            (symbol, trade_date, interval, ts, json.dumps(bars or [], ensure_ascii=False, default=str)),
        )
        conn.commit()


def _prune_cn_mainline_snapshots(*, account_id: str, keep_days: int = 10) -> None:
    keep = max(1, min(int(keep_days), 60))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM cn_mainline_snapshots
            WHERE account_id = ?
            ORDER BY trade_date DESC
            """,
            (account_id,),
        ).fetchall()
        dates = [str(r[0]) for r in rows if r and r[0]]
        to_delete = dates[keep:]
        for d in to_delete:
            conn.execute(
                "DELETE FROM cn_mainline_snapshots WHERE account_id = ? AND trade_date = ?",
                (account_id, d),
            )
        conn.commit()


def _get_cn_mainline_snapshot_latest(
    *,
    account_id: str,
    trade_date: str | None,
    universe_version: str,
) -> dict[str, Any] | None:
    with _connect() as conn:
        if trade_date:
            row = conn.execute(
                """
                SELECT id, created_at, output_json
                FROM cn_mainline_snapshots
                WHERE account_id = ? AND trade_date = ? AND universe_version = ?
                ORDER BY as_of_ts DESC
                LIMIT 1
                """,
                (account_id, trade_date, universe_version),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id, created_at, output_json
                FROM cn_mainline_snapshots
                WHERE account_id = ? AND universe_version = ?
                ORDER BY as_of_ts DESC
                LIMIT 1
                """,
                (account_id, universe_version),
            ).fetchone()
    if row is None:
        return None
    try:
        out = json.loads(str(row[2]) or "{}")
    except Exception:
        out = {}
    return {"id": str(row[0]), "createdAt": str(row[1]), "output": out}


def _insert_cn_mainline_snapshot(
    *,
    account_id: str,
    trade_date: str,
    as_of_ts: str,
    universe_version: str,
    ts: str,
    output: dict[str, Any],
) -> str:
    snap_id = str(uuid.uuid4())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO cn_mainline_snapshots(id, account_id, trade_date, as_of_ts, universe_version, created_at, output_json)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (snap_id, account_id, trade_date, as_of_ts, universe_version, ts, json.dumps(output or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()
    return snap_id


def _get_theme_members_cached(*, theme_key: str, trade_date: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT updated_at, members_json
            FROM cn_theme_membership_cache
            WHERE theme_key = ? AND trade_date = ?
            """,
            (theme_key, trade_date),
        ).fetchone()
    if row is None:
        return None
    try:
        members = json.loads(str(row[1]) or "[]")
    except Exception:
        members = []
    return {"updatedAt": str(row[0]), "members": members if isinstance(members, list) else []}


def _upsert_theme_members_cached(*, theme_key: str, trade_date: str, ts: str, members: list[str]) -> None:
    mem = [str(x).strip() for x in (members or []) if str(x).strip()]
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO cn_theme_membership_cache(theme_key, trade_date, members_json, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(theme_key, trade_date) DO UPDATE SET
              members_json = excluded.members_json,
              updated_at = excluded.updated_at
            """,
            (theme_key, trade_date, json.dumps(mem, ensure_ascii=False), ts),
        )
        conn.commit()


def _parse_data_url(data_url: str) -> tuple[str, bytes]:
    """
    Parse a data URL like 'data:image/png;base64,...' and return (mediaType, bytes).
    """
    m = re.match(r"^data:([^;]+);base64,(.+)$", (data_url or "").strip(), flags=re.IGNORECASE)
    if not m:
        raise ValueError("Invalid dataUrl")
    media_type = str(m.group(1)).strip().lower()
    b64 = m.group(2)
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception as e:
        raise ValueError("Invalid base64 dataUrl") from e
    return media_type, raw


def _sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _broker_data_dir() -> Path:
    # Store screenshots locally (not committed) for v0.
    d = Path(__file__).with_name("data").joinpath("broker")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_broker_image(*, broker: str, raw: bytes, media_type: str) -> str:
    ext = "png"
    if "jpeg" in media_type or "jpg" in media_type:
        ext = "jpg"
    elif "webp" in media_type:
        ext = "webp"
    elif "png" in media_type:
        ext = "png"

    sub = _broker_data_dir().joinpath(broker)
    sub.mkdir(parents=True, exist_ok=True)
    p = sub.joinpath(f"{uuid.uuid4()}.{ext}")
    p.write_bytes(raw)
    return str(p)


def _ai_service_base_url() -> str:
    return (os.getenv("AI_SERVICE_BASE_URL") or "http://127.0.0.1:4310").rstrip("/")


def _ai_extract_pingan_screenshot(*, image_data_url: str) -> dict[str, Any]:
    """
    Call ai-service to extract structured broker data from a Ping An Securities screenshot.
    """
    payload = json.dumps({"imageDataUrl": image_data_url}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/extract/broker/pingan",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def _seed_default_broker_account(broker: str) -> str:
    """
    Ensure a default account exists for the given broker and return its id.
    Backward compatible with older clients that didn't provide accountId.
    """
    b = (broker or "").strip().lower()
    if not b:
        b = "unknown"
    with _connect() as conn:
        row = conn.execute(
            "SELECT id FROM broker_accounts WHERE broker = ? ORDER BY updated_at DESC LIMIT 1",
            (b,),
        ).fetchone()
        if row is not None:
            return str(row[0])
        aid = str(uuid.uuid4())
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO broker_accounts(id, broker, title, account_masked, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (aid, b, "Default", None, ts, ts),
        )
        conn.commit()
        return aid


def _get_account_state_row(account_id: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT account_id, broker, updated_at, overview_json, positions_json, conditional_orders_json, trades_json
            FROM broker_account_state
            WHERE account_id = ?
            """,
            (account_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "accountId": str(row[0]),
            "broker": str(row[1]),
            "updatedAt": str(row[2]),
            "overview": json.loads(str(row[3]) or "{}"),
            "positions": json.loads(str(row[4]) or "[]"),
            "conditionalOrders": json.loads(str(row[5]) or "[]"),
            "trades": json.loads(str(row[6]) or "[]"),
        }


def _ensure_account_state(account_id: str, broker: str) -> None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT account_id FROM broker_account_state WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        if row is not None:
            return
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO broker_account_state(
              account_id, broker, updated_at, overview_json, positions_json, conditional_orders_json, trades_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                broker,
                ts,
                json.dumps({}, ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
            ),
        )
        conn.commit()


def _upsert_account_state(
    *,
    account_id: str,
    broker: str,
    updated_at: str,
    overview: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    conditional_orders: list[dict[str, Any]] | None = None,
    trades: list[dict[str, Any]] | None = None,
) -> None:
    _ensure_account_state(account_id, broker)
    with _connect() as conn:
        current = _get_account_state_row(account_id) or {
            "overview": {},
            "positions": [],
            "conditionalOrders": [],
            "trades": [],
        }
        next_overview = overview if overview is not None else (current.get("overview") or {})
        next_positions = positions if positions is not None else (current.get("positions") or [])
        next_orders = (
            conditional_orders
            if conditional_orders is not None
            else (current.get("conditionalOrders") or [])
        )
        next_trades = trades if trades is not None else (current.get("trades") or [])
        conn.execute(
            """
            UPDATE broker_account_state
            SET updated_at = ?, overview_json = ?, positions_json = ?, conditional_orders_json = ?, trades_json = ?
            WHERE account_id = ?
            """,
            (
                updated_at,
                json.dumps(next_overview, ensure_ascii=False),
                json.dumps(next_positions, ensure_ascii=False),
                json.dumps(next_orders, ensure_ascii=False),
                json.dumps(next_trades, ensure_ascii=False),
                account_id,
            ),
        )
        # Also bump broker_accounts.updated_at (for UX sorting).
        conn.execute("UPDATE broker_accounts SET updated_at = ? WHERE id = ?", (updated_at, account_id))
        conn.commit()


def _account_state_response(account_id: str) -> BrokerAccountStateResponse:
    row = _get_account_state_row(account_id)
    if row is None:
        # Best-effort init with empty state
        _ensure_account_state(account_id, "pingan")
        row = _get_account_state_row(account_id) or {
            "accountId": account_id,
            "broker": "pingan",
            "updatedAt": now_iso(),
            "overview": {},
            "positions": [],
            "conditionalOrders": [],
            "trades": [],
        }
    raw_positions = row.get("positions")
    positions: list[Any] = raw_positions if isinstance(raw_positions, list) else []

    raw_orders = row.get("conditionalOrders")
    orders: list[Any] = raw_orders if isinstance(raw_orders, list) else []

    raw_trades = row.get("trades")
    trades: list[Any] = raw_trades if isinstance(raw_trades, list) else []
    raw_overview = row.get("overview")
    overview_obj: dict[str, Any] = (
        {str(k): v for k, v in raw_overview.items()} if isinstance(raw_overview, dict) else {}
    )
    return BrokerAccountStateResponse(
        accountId=str(row["accountId"]),
        broker=str(row["broker"]),
        updatedAt=str(row["updatedAt"]),
        overview=overview_obj,
        positions=[x if isinstance(x, dict) else {"raw": x} for x in positions],
        conditionalOrders=[x if isinstance(x, dict) else {"raw": x} for x in orders],
        trades=[x if isinstance(x, dict) else {"raw": x} for x in trades],
        counts={"positions": len(positions), "conditionalOrders": len(orders), "trades": len(trades)},
    )


def _home_path(path: str) -> str:
    return str(Path(path).expanduser())


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _tcp_is_listening(host: str, port: int) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def _cdp_version(host: str, port: int) -> dict[str, str] | None:
    url = f"http://{host}:{port}/json/version"
    try:
        with urllib.request.urlopen(url, timeout=0.8) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except OSError:
        return None

    try:
        import json

        data = json.loads(raw)
        if isinstance(data, dict):
            return {k: str(v) for k, v in data.items()}
    except Exception:
        return None
    return None


TV_CDP_HOST = "127.0.0.1"
TV_CDP_PORT_DEFAULT = 9222
TV_USER_DATA_DIR_DEFAULT = "~/.karios/chrome-tv-cdp"
TV_PROFILE_DIR_DEFAULT = "Default"
TV_CHROME_BIN_DEFAULT = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
TV_CHROME_USER_DATA_DIR_DEFAULT = "~/Library/Application Support/Google/Chrome"
TV_BOOTSTRAP_PROFILE_DIR_DEFAULT = "Profile 1"


class TvChromeStartRequest(BaseModel):
    port: int = TV_CDP_PORT_DEFAULT
    userDataDir: str = TV_USER_DATA_DIR_DEFAULT
    profileDirectory: str = TV_PROFILE_DIR_DEFAULT
    chromeBin: str = TV_CHROME_BIN_DEFAULT
    headless: bool = False
    bootstrapFromChromeUserDataDir: str | None = None
    bootstrapFromProfileDirectory: str | None = None
    forceBootstrap: bool = False


class TvChromeStatusResponse(BaseModel):
    running: bool
    pid: int | None
    host: str
    port: int
    cdpOk: bool
    cdpVersion: dict[str, str] | None
    userDataDir: str
    profileDirectory: str
    headless: bool


def _get_tv_chrome_pid() -> int | None:
    raw = (get_setting("tv_chrome_pid") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _set_tv_chrome_pid(pid: int | None) -> None:
    set_setting("tv_chrome_pid", "" if pid is None else str(pid))


def _get_tv_cdp_port() -> int:
    raw = (get_setting("tv_cdp_port") or "").strip()
    if not raw:
        return TV_CDP_PORT_DEFAULT
    try:
        return int(raw)
    except ValueError:
        return TV_CDP_PORT_DEFAULT


def _set_tv_cdp_port(port: int) -> None:
    set_setting("tv_cdp_port", str(port))


def _get_tv_user_data_dir() -> str:
    return (get_setting("tv_user_data_dir") or TV_USER_DATA_DIR_DEFAULT).strip()


def _set_tv_user_data_dir(path: str) -> None:
    set_setting("tv_user_data_dir", path)


def _get_tv_profile_dir() -> str:
    return (get_setting("tv_profile_dir") or TV_PROFILE_DIR_DEFAULT).strip()


def _set_tv_profile_dir(profile_dir: str) -> None:
    set_setting("tv_profile_dir", profile_dir)


def _get_tv_headless() -> bool:
    raw = (get_setting("tv_headless") or "").strip().lower()
    if raw == "":
        # Default to silent background sync.
        return True
    return raw in {"1", "true", "yes", "y", "on"}


def _set_tv_headless(value: bool) -> None:
    set_setting("tv_headless", "1" if value else "0")


def _copy_chrome_profile(
    *,
    src_user_data_dir: str,
    src_profile_dir: str,
    dst_user_data_dir: str,
    dst_profile_dir: str,
    force: bool,
) -> None:
    """
    Copy an existing Chrome profile into a dedicated user-data-dir so that we can run
    Chrome with --remote-debugging-port (Chrome disallows remote debugging on the default dir).

    We intentionally skip heavy cache directories.
    """
    src_ud = Path(_home_path(src_user_data_dir))
    dst_ud = Path(_home_path(dst_user_data_dir))
    src_profile = src_ud / src_profile_dir
    dst_profile = dst_ud / dst_profile_dir

    if not src_ud.exists():
        raise HTTPException(status_code=400, detail=f"Source user-data-dir not found: {src_ud}")
    if not src_profile.exists():
        raise HTTPException(status_code=400, detail=f"Source profile not found: {src_profile}")

    dst_ud.mkdir(parents=True, exist_ok=True)

    # Copy Local State (contains encryption keys metadata for cookies).
    src_local_state = src_ud / "Local State"
    dst_local_state = dst_ud / "Local State"
    if src_local_state.exists() and (force or not dst_local_state.exists()):
        shutil.copy2(src_local_state, dst_local_state)

    if dst_profile.exists():
        if not force:
            return
        shutil.rmtree(dst_profile)

    def ignore(_dir: str, names: list[str]) -> set[str]:
        skip = {
            "Cache",
            "Code Cache",
            "GPUCache",
            "ShaderCache",
            "Media Cache",
            "GrShaderCache",
            "Crashpad",
            "SwReporter",
        }
        return {n for n in names if n in skip}

    shutil.copytree(src_profile, dst_profile, ignore=ignore)


def _get_tv_chrome_bin() -> str:
    return (get_setting("tv_chrome_bin") or TV_CHROME_BIN_DEFAULT).strip()


def _set_tv_chrome_bin(chrome_bin: str) -> None:
    set_setting("tv_chrome_bin", chrome_bin)


@app.get("/integrations/tradingview/status", response_model=TvChromeStatusResponse)
def tradingview_status() -> TvChromeStatusResponse:
    pid = _get_tv_chrome_pid()
    running = bool(pid and _pid_is_running(pid))
    port = _get_tv_cdp_port()
    user_data_dir = _get_tv_user_data_dir()
    profile_dir = _get_tv_profile_dir()
    headless = _get_tv_headless()
    cdp = _cdp_version(TV_CDP_HOST, port) if running else None
    cdp_ok = cdp is not None
    return TvChromeStatusResponse(
        running=running,
        pid=pid if running else None,
        host=TV_CDP_HOST,
        port=port,
        cdpOk=cdp_ok,
        cdpVersion=cdp,
        userDataDir=user_data_dir,
        profileDirectory=profile_dir,
        headless=headless,
    )


@app.post("/integrations/tradingview/chrome/start", response_model=TvChromeStatusResponse)
def tradingview_chrome_start(req: TvChromeStartRequest) -> TvChromeStatusResponse:
    # If a previous PID is stored but dead, clear it.
    pid = _get_tv_chrome_pid()
    if pid and not _pid_is_running(pid):
        _set_tv_chrome_pid(None)
        pid = None

    # Determine current stored config (if any) for restart decisions.
    current_port = _get_tv_cdp_port()
    current_user_data_dir = _home_path(_get_tv_user_data_dir())
    current_profile_dir = _get_tv_profile_dir()
    current_chrome_bin = _get_tv_chrome_bin()
    current_headless = _get_tv_headless()

    # Persist desired config.
    port = int(req.port)
    user_data_dir = _home_path(req.userDataDir)
    profile_dir = req.profileDirectory.strip() or TV_PROFILE_DIR_DEFAULT
    chrome_bin = req.chromeBin.strip() or TV_CHROME_BIN_DEFAULT
    headless = bool(req.headless)
    _set_tv_cdp_port(port)
    _set_tv_user_data_dir(user_data_dir)
    _set_tv_profile_dir(profile_dir)
    _set_tv_chrome_bin(chrome_bin)
    _set_tv_headless(headless)

    # If already running but config differs, restart so the new config takes effect.
    if pid and _pid_is_running(pid):
        changed = (
            current_port != port
            or current_user_data_dir != user_data_dir
            or current_profile_dir != profile_dir
            or current_chrome_bin != chrome_bin
            or current_headless != headless
        )
        if changed or req.forceBootstrap:
            tradingview_chrome_stop()
        else:
            return tradingview_status()

    # Fail fast if port is already taken.
    if _tcp_is_listening(TV_CDP_HOST, port):
        raise HTTPException(status_code=409, detail=f"Port {port} is already in use.")

    if not Path(chrome_bin).exists():
        raise HTTPException(status_code=400, detail=f"Chrome binary not found: {chrome_bin}")

    Path(user_data_dir).mkdir(parents=True, exist_ok=True)

    # Optional: bootstrap from an existing Chrome profile into the dedicated user-data-dir.
    # This enables "silent" headless syncing using the user's logged-in session.
    if req.bootstrapFromChromeUserDataDir and req.bootstrapFromProfileDirectory:
        # Persist bootstrap source for future auto-sync runs.
        set_setting("tv_bootstrap_src_user_data_dir", req.bootstrapFromChromeUserDataDir)
        set_setting("tv_bootstrap_src_profile_dir", req.bootstrapFromProfileDirectory)
        _copy_chrome_profile(
            src_user_data_dir=req.bootstrapFromChromeUserDataDir,
            src_profile_dir=req.bootstrapFromProfileDirectory,
            dst_user_data_dir=user_data_dir,
            dst_profile_dir=profile_dir,
            force=bool(req.forceBootstrap),
        )

    # Chrome requires a non-default user-data-dir for remote debugging.
    # We always use a dedicated directory to avoid interfering with the user's daily profile.
    args = [
        chrome_bin,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={profile_dir}",
        *(["--headless=new", "--disable-gpu", "--window-size=1280,820"] if headless else []),
        "--no-first-run",
        "--no-default-browser-check",
    ]

    try:
        proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to start Chrome: {e}") from e

    _set_tv_chrome_pid(proc.pid)

    # Best-effort: wait briefly for CDP to be ready.
    for _ in range(60):
        if _cdp_version(TV_CDP_HOST, port) is not None:
            break
        time.sleep(0.2)

    return tradingview_status()


@app.post("/integrations/tradingview/chrome/stop", response_model=TvChromeStatusResponse)
def tradingview_chrome_stop() -> TvChromeStatusResponse:
    pid = _get_tv_chrome_pid()
    port = _get_tv_cdp_port()
    if not pid:
        return tradingview_status()

    if not _pid_is_running(pid):
        _set_tv_chrome_pid(None)
        return tradingview_status()

    # Terminate the process group (Chrome spawns children).
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            _set_tv_chrome_pid(None)
            return tradingview_status()

    for _ in range(40):
        if not _pid_is_running(pid) and not _tcp_is_listening(TV_CDP_HOST, port):
            break
        time.sleep(0.2)

    if _pid_is_running(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    _set_tv_chrome_pid(None)
    return tradingview_status()


def _seed_default_tv_screeners() -> None:
    """
    Seed default TradingView screeners if the table is empty.
    URLs are configurable later via Settings UI.
    """
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(1) FROM tv_screeners").fetchone()
        count = int(row[0]) if row else 0
        if count > 0:
            return
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO tv_screeners(id, name, url, enabled, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "falcon",
                "Swing Falcon Filter",
                "https://www.tradingview.com/screener/TMcms1mM/",
                1,
                ts,
                ts,
            ),
        )
        conn.execute(
            """
            INSERT INTO tv_screeners(id, name, url, enabled, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                "blackhorse",
                "Black Horse Filter",
                "https://www.tradingview.com/screener/kBuKODpK/",
                1,
                ts,
                ts,
            ),
        )
        conn.commit()


class TvScreener(BaseModel):
    id: str
    name: str
    url: str
    enabled: bool
    updatedAt: str


class ListTvScreenersResponse(BaseModel):
    items: list[TvScreener]


class CreateTvScreenerRequest(BaseModel):
    name: str
    url: str
    enabled: bool = True


class CreateTvScreenerResponse(BaseModel):
    id: str


class UpdateTvScreenerRequest(BaseModel):
    name: str
    url: str
    enabled: bool


class TvScreenerSnapshotSummary(BaseModel):
    id: str
    screenerId: str
    capturedAt: str
    rowCount: int


class ListTvScreenerSnapshotsResponse(BaseModel):
    items: list[TvScreenerSnapshotSummary]


class TvScreenerHistoryCell(BaseModel):
    snapshotId: str
    capturedAt: str
    rowCount: int
    screenTitle: str | None = None
    filters: list[str] = []


class TvScreenerHistoryDayRow(BaseModel):
    date: str  # YYYY-MM-DD in Asia/Shanghai
    am: TvScreenerHistoryCell | None = None
    pm: TvScreenerHistoryCell | None = None


class TvScreenerHistoryResponse(BaseModel):
    screenerId: str
    screenerName: str
    days: int
    rows: list[TvScreenerHistoryDayRow]

class TvScreenerSnapshotDetail(BaseModel):
    id: str
    screenerId: str
    capturedAt: str
    rowCount: int
    screenTitle: str | None
    filters: list[str]
    url: str
    headers: list[str]
    rows: list[dict[str, str]]


class TvScreenerSyncResponse(BaseModel):
    snapshotId: str
    capturedAt: str
    rowCount: int


class BrokerImportImage(BaseModel):
    """
    Screenshot payload from the desktop UI. We use dataUrl to keep v0 simple.
    """

    id: str
    name: str
    mediaType: str
    dataUrl: str


class BrokerImportRequest(BaseModel):
    capturedAt: str | None = None
    accountId: str | None = None
    images: list[BrokerImportImage]


class BrokerSnapshotSummary(BaseModel):
    id: str
    broker: str
    accountId: str | None
    capturedAt: str
    kind: str
    createdAt: str


class BrokerSnapshotDetail(BrokerSnapshotSummary):
    imagePath: str
    extracted: dict[str, Any]


class BrokerImportResponse(BaseModel):
    ok: bool
    items: list[BrokerSnapshotSummary]


class BrokerAccountStateResponse(BaseModel):
    accountId: str
    broker: str
    updatedAt: str
    overview: dict[str, Any]
    positions: list[dict[str, Any]]
    conditionalOrders: list[dict[str, Any]]
    trades: list[dict[str, Any]]
    counts: dict[str, int]


class BrokerSyncRequest(BaseModel):
    capturedAt: str | None = None
    images: list[BrokerImportImage]


class DeleteBrokerConditionalOrderRequest(BaseModel):
    """
    Delete one conditional order row from the consolidated account state.
    The order does not have a stable id, so we match by a normalized signature.
    """

    order: dict[str, Any]


class BrokerAccountSummary(BaseModel):
    id: str
    broker: str
    title: str
    accountMasked: str | None
    updatedAt: str


class CreateBrokerAccountRequest(BaseModel):
    broker: str
    title: str
    accountMasked: str | None = None


class UpdateBrokerAccountRequest(BaseModel):
    title: str | None = None
    accountMasked: str | None = None


class MarketStatusResponse(BaseModel):
    stocks: int
    lastSyncAt: str | None


class MarketStockRow(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str
    price: str | None = None
    changePct: str | None = None
    volume: str | None = None
    turnover: str | None = None
    marketCap: str | None = None
    updatedAt: str


class MarketStocksResponse(BaseModel):
    items: list[MarketStockRow]
    total: int
    offset: int
    limit: int


class MarketBarsResponse(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str
    bars: list[dict[str, str]]


class MarketChipsResponse(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str
    items: list[dict[str, str]]


class MarketFundFlowResponse(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str
    items: list[dict[str, str]]


class IndustryFundFlowPoint(BaseModel):
    date: str  # YYYY-MM-DD
    netInflow: float  # CNY


class IndustryFundFlowRow(BaseModel):
    industryCode: str
    industryName: str
    netInflow: float  # CNY, asOfDate
    sum10d: float  # CNY
    series10d: list[IndustryFundFlowPoint]


class MarketCnIndustryFundFlowResponse(BaseModel):
    asOfDate: str  # YYYY-MM-DD
    days: int
    topN: int
    dates: list[str]  # actual dates used in aggregation (ascending)
    top: list[IndustryFundFlowRow]


class MarketCnIndustryFundFlowSyncRequest(BaseModel):
    date: str | None = None  # YYYY-MM-DD in Asia/Shanghai
    days: int = 10
    topN: int = 10  # also used for backfill hist
    force: bool = False


class MarketCnIndustryFundFlowSyncResponse(BaseModel):
    ok: bool
    asOfDate: str
    days: int
    rowsUpserted: int
    histRowsUpserted: int
    histFailures: int = 0
    message: str | None = None


# --- CN market breadth & sentiment (v0) ---
class MarketCnSentimentRow(BaseModel):
    date: str  # YYYY-MM-DD
    upCount: int
    downCount: int
    flatCount: int
    totalCount: int
    upDownRatio: float
    yesterdayLimitUpPremium: float  # percent, e.g. -1.2 means -1.2%
    failedLimitUpRate: float  # percent, e.g. 35.0
    riskMode: str  # normal | caution | no_new_positions
    rules: list[str] = []
    updatedAt: str


class MarketCnSentimentResponse(BaseModel):
    asOfDate: str
    days: int
    items: list[MarketCnSentimentRow]


class MarketCnSentimentSyncRequest(BaseModel):
    date: str | None = None
    force: bool = False


# --- CN 1-2D rank (rule+factor) (v0) ---
class RankNext2dGenerateRequest(BaseModel):
    accountId: str | None = None
    asOfDate: str | None = None  # YYYY-MM-DD
    force: bool = False
    limit: int = 30
    universeVersion: str = "v0"
    includeHoldings: bool = True


class RankItem(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    sector: str | None = None
    score: float
    probBand: str  # High | Medium | Low
    signals: list[str] = []
    breakdown: dict[str, float] = {}


class RankSnapshotResponse(BaseModel):
    id: str
    asOfDate: str
    accountId: str
    createdAt: str
    universeVersion: str
    riskMode: str | None = None
    items: list[RankItem] = []
    debug: dict[str, Any] | None = None


# --- CN intraday rank (DeltaT 1H) (v0) ---
class IntradayRankGenerateRequest(BaseModel):
    accountId: str | None = None
    asOfTs: str | None = None  # ISO timestamp, default=now
    slot: str | None = None  # auto if omitted
    force: bool = False
    limit: int = 30
    universeVersion: str = "v0"


class IntradayObservationRow(BaseModel):
    id: str
    tradeDate: str
    ts: str
    kind: str  # preopen_intent | opening_anchor | hourly_prep
    raw: dict[str, Any] = {}
    createdAt: str


class IntradayObservationsResponse(BaseModel):
    tradeDate: str
    items: list[IntradayObservationRow] = []


class IntradayRankItem(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    score: float
    probBand: str  # High | Medium | Low
    slot: str
    signals: list[str] = []
    factors: dict[str, float] = {}
    notes: str | None = None


class IntradayRankSnapshotResponse(BaseModel):
    id: str
    asOfTs: str
    tradeDate: str
    slot: str
    accountId: str
    createdAt: str
    universeVersion: str
    riskMode: str | None = None
    items: list[IntradayRankItem] = []
    observations: list[IntradayObservationRow] = []
    debug: dict[str, Any] | None = None


# --- Leaders mainline (industry+concept) (v0) ---
class MainlineTheme(BaseModel):
    kind: str  # industry | concept
    name: str
    compositeScore: float
    structureScore: float
    logicScore: float
    logicGrade: str | None = None  # S | A | B
    logicSummary: str | None = None
    leaderCandidate: dict[str, Any] | None = None
    topTickers: list[dict[str, Any]] = []
    followersCount: int = 0
    limitupCount: int = 0
    volSurge: float = 0.0
    todayStrength: float = 0.0
    ret3d: float = 0.0
    evidence: dict[str, Any] = {}
    decaySignals: list[str] = []


class MainlineSnapshotResponse(BaseModel):
    id: str
    tradeDate: str
    asOfTs: str
    accountId: str
    createdAt: str
    universeVersion: str
    riskMode: str | None = None
    selected: MainlineTheme | None = None
    themesTopK: list[MainlineTheme] = []
    debug: dict[str, Any] | None = None


class MainlineGenerateRequest(BaseModel):
    accountId: str | None = None
    tradeDate: str | None = None
    asOfTs: str | None = None
    universeVersion: str = "v0"
    force: bool = False
    topK: int = 3


# --- Strategy module (v0) ---
class StrategyAccountPromptResponse(BaseModel):
    accountId: str
    prompt: str
    updatedAt: str | None


class StrategyAccountPromptRequest(BaseModel):
    prompt: str


class StrategyDailyGenerateRequest(BaseModel):
    date: str | None = None  # YYYY-MM-DD, optional
    force: bool = False
    maxCandidates: int = 10
    # Context toggles (default ON). When OFF, the corresponding section is excluded or minimized
    # from the AI context to save tokens and isolate reasoning.
    includeAccountState: bool = True
    includeTradingView: bool = True
    includeIndustryFundFlow: bool = True
    includeMarketSentiment: bool = True
    includeLeaders: bool = True
    includeStocks: bool = True


class StrategyCandidate(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    score: float
    rank: int
    why: str


class StrategyLeader(BaseModel):
    symbol: str
    reason: str


class StrategyLevels(BaseModel):
    support: list[str] = []
    resistance: list[str] = []
    invalidations: list[str] = []


class StrategyOrder(BaseModel):
    """
    v0: keep orders as human-readable conditional-order recipes, so the UI can display it
    and the user can manually translate to broker conditional orders.
    """

    kind: str  # e.g. breakout_buy | pullback_buy | stop_loss | take_profit
    side: str  # buy | sell
    trigger: str  # e.g. "price >= 445.0"
    qty: str  # e.g. "1000 shares" or "10% equity"
    timeInForce: str | None = None  # day | gtc
    notes: str | None = None


class StrategyRecommendation(BaseModel):
    symbol: str
    ticker: str
    name: str
    thesis: str
    levels: StrategyLevels
    orders: list[StrategyOrder]
    positionSizing: str
    riskNotes: list[str] = []


class StrategyReportResponse(BaseModel):
    id: str
    date: str
    accountId: str
    accountTitle: str
    createdAt: str
    model: str
    markdown: str | None = None
    candidates: list[StrategyCandidate]
    leader: StrategyLeader
    recommendations: list[StrategyRecommendation]
    riskNotes: list[str] = []
    # Debugging / traceability
    inputSnapshot: dict[str, Any] | None = None
    raw: dict[str, Any] | None = None


class StrategyReportSummary(BaseModel):
    id: str
    date: str
    createdAt: str
    model: str
    hasMarkdown: bool = False


class ListStrategyReportsResponse(BaseModel):
    accountId: str
    days: int
    items: list[StrategyReportSummary]


# --- Leader stocks module (v0) ---
class LeaderDailyGenerateRequest(BaseModel):
    date: str | None = None  # YYYY-MM-DD
    force: bool = False
    maxCandidates: int = 20  # candidate universe cap from screener
    useMainline: bool = True
    mainlineTopK: int = 3


class LeaderPick(BaseModel):
    id: str
    date: str
    symbol: str
    market: str
    ticker: str
    name: str
    entryPrice: float | None = None
    score: float | None = None
    liveScore: float | None = None
    liveScoreUpdatedAt: str | None = None
    reason: str
    whyBullets: list[str] = []
    expectedDurationDays: int | None = None
    buyZone: dict[str, Any] = {}
    triggers: list[dict[str, Any]] = []
    invalidation: str | None = None
    targetPrice: dict[str, Any] = {}
    probability: int | None = None
    risks: list[str] = []
    sourceSignals: dict[str, Any] = {}
    riskPoints: list[str] = []
    createdAt: str
    # Computed metrics
    nowClose: float | None = None
    pctSinceEntry: float | None = None
    series: list[dict[str, Any]] = []  # [{date, close}]


class LeaderDailyResponse(BaseModel):
    date: str
    leaders: list[LeaderPick]
    debug: dict[str, Any] | None = None


class LeaderListResponse(BaseModel):
    days: int = 10
    dates: list[str]
    leaders: list[LeaderPick]


# --- Dashboard module (v0) ---
class DashboardSyncRequest(BaseModel):
    force: bool = True


class DashboardSyncStep(BaseModel):
    name: str
    ok: bool
    durationMs: int
    message: str | None = None
    meta: dict[str, Any] = {}


class DashboardScreenerSyncItem(BaseModel):
    id: str
    name: str
    ok: bool
    rowCount: int = 0
    capturedAt: str | None = None
    filtersCount: int = 0
    error: str | None = None


class DashboardScreenerSyncStatus(BaseModel):
    enabledCount: int
    syncedCount: int
    failed: list[DashboardScreenerSyncItem] = []
    missing: list[dict[str, str]] = []  # [{id,name,reason}]
    items: list[DashboardScreenerSyncItem] = []


class DashboardSyncResponse(BaseModel):
    ok: bool
    startedAt: str
    finishedAt: str
    steps: list[DashboardSyncStep]
    screener: DashboardScreenerSyncStatus


class DashboardAccountItem(BaseModel):
    id: str
    broker: str
    title: str
    accountMasked: str | None
    updatedAt: str


class DashboardAccountStateSummary(BaseModel):
    accountId: str
    broker: str
    updatedAt: str | None = None
    cashAvailable: str | None = None
    totalAssets: str | None = None
    positionsCount: int = 0
    conditionalOrdersCount: int = 0
    tradesCount: int = 0


class DashboardHoldingRow(BaseModel):
    ticker: str
    name: str | None = None
    symbol: str | None = None
    price: float | None = None
    weightPct: float | None = None
    pnlAmount: float | None = None
    # Keep raw fields for debug/compat if needed.
    qty: str | None = None
    cost: str | None = None
    pnl: str | None = None
    pnlPct: str | None = None


class DashboardLeadersSummary(BaseModel):
    latestDate: str | None = None
    latest: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []


class DashboardScreenerStatusRow(BaseModel):
    id: str
    name: str
    enabled: bool
    updatedAt: str | None = None
    capturedAt: str | None = None
    rowCount: int = 0
    filtersCount: int = 0


class DashboardSummaryResponse(BaseModel):
    asOfDate: str
    accounts: list[DashboardAccountItem]
    selectedAccountId: str | None = None
    accountState: DashboardAccountStateSummary | None = None
    holdings: list[DashboardHoldingRow] = []
    marketStatus: dict[str, Any] = {}
    industryFundFlow: dict[str, Any] = {}
    marketSentiment: dict[str, Any] = {}
    leaders: DashboardLeadersSummary = DashboardLeadersSummary()
    screeners: list[DashboardScreenerStatusRow] = []


@app.get("/integrations/tradingview/screeners", response_model=ListTvScreenersResponse)
def list_tv_screeners() -> ListTvScreenersResponse:
    _seed_default_tv_screeners()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, name, url, enabled, updated_at
            FROM tv_screeners
            ORDER BY updated_at DESC
            """,
        ).fetchall()
        items = [
            TvScreener(
                id=str(r[0]),
                name=str(r[1]),
                url=str(r[2]),
                enabled=bool(int(r[3])),
                updatedAt=str(r[4]),
            )
            for r in rows
        ]
        return ListTvScreenersResponse(items=items)


@app.post("/integrations/tradingview/screeners", response_model=CreateTvScreenerResponse)
def create_tv_screener(req: CreateTvScreenerRequest) -> CreateTvScreenerResponse:
    _seed_default_tv_screeners()
    screener_id = str(uuid.uuid4())
    ts = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO tv_screeners(id, name, url, enabled, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                screener_id,
                req.name.strip() or "Untitled",
                req.url.strip(),
                1 if req.enabled else 0,
                ts,
                ts,
            ),
        )
        conn.commit()
    return CreateTvScreenerResponse(id=screener_id)


@app.put("/integrations/tradingview/screeners/{screener_id}")
def update_tv_screener(screener_id: str, req: UpdateTvScreenerRequest) -> JSONResponse:
    _seed_default_tv_screeners()
    ts = now_iso()
    with _connect() as conn:
        cur = conn.execute(
            """
            UPDATE tv_screeners
            SET name = ?, url = ?, enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                req.name.strip() or "Untitled",
                req.url.strip(),
                1 if req.enabled else 0,
                ts,
                screener_id,
            ),
        )
        conn.commit()
        if (cur.rowcount or 0) == 0:
            return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
    return JSONResponse({"ok": True})


@app.delete("/integrations/tradingview/screeners/{screener_id}")
def delete_tv_screener(screener_id: str) -> JSONResponse:
    _seed_default_tv_screeners()
    with _connect() as conn:
        cur = conn.execute("DELETE FROM tv_screeners WHERE id = ?", (screener_id,))
        conn.commit()
        if (cur.rowcount or 0) == 0:
            return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
    return JSONResponse({"ok": True})


def _upsert_market_stock(conn: sqlite3.Connection, s: StockRow, ts: str) -> None:
    conn.execute(
        """
        INSERT INTO market_stocks(symbol, market, ticker, name, currency, updated_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
          market = excluded.market,
          ticker = excluded.ticker,
          name = excluded.name,
          currency = excluded.currency,
          updated_at = excluded.updated_at
        """,
        (s.symbol, s.market, s.ticker, s.name, s.currency, ts),
    )


def _upsert_market_quote(conn: sqlite3.Connection, s: StockRow, ts: str) -> None:
    raw_json = json.dumps(s.quote, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO market_quotes(
          symbol, price, change_pct, volume, turnover, market_cap, updated_at, raw_json
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
          price = excluded.price,
          change_pct = excluded.change_pct,
          volume = excluded.volume,
          turnover = excluded.turnover,
          market_cap = excluded.market_cap,
          updated_at = excluded.updated_at,
          raw_json = excluded.raw_json
        """,
        (
            s.symbol,
            s.quote.get("price"),
            s.quote.get("change_pct"),
            s.quote.get("volume"),
            s.quote.get("turnover"),
            s.quote.get("market_cap"),
            ts,
            raw_json,
        ),
    )


def _upsert_market_bars(conn: sqlite3.Connection, symbol: str, bars: list[BarRow], ts: str) -> None:
    for b in bars:
        conn.execute(
            """
            INSERT INTO market_bars(
              symbol, date, open, high, low, close, volume, amount, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
              open = excluded.open,
              high = excluded.high,
              low = excluded.low,
              close = excluded.close,
              volume = excluded.volume,
              amount = excluded.amount,
              updated_at = excluded.updated_at
            """,
            (
                symbol,
                b.date,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.amount,
                ts,
            ),
        )


def _upsert_market_chips(
    conn: sqlite3.Connection,
    symbol: str,
    items: list[dict[str, str]],
    ts: str,
) -> None:
    for it in items:
        raw = json.dumps(it, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO market_chips(
              symbol, date,
              profit_ratio, avg_cost,
              cost90_low, cost90_high, cost90_conc,
              cost70_low, cost70_high, cost70_conc,
              updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
              profit_ratio = excluded.profit_ratio,
              avg_cost = excluded.avg_cost,
              cost90_low = excluded.cost90_low,
              cost90_high = excluded.cost90_high,
              cost90_conc = excluded.cost90_conc,
              cost70_low = excluded.cost70_low,
              cost70_high = excluded.cost70_high,
              cost70_conc = excluded.cost70_conc,
              updated_at = excluded.updated_at,
              raw_json = excluded.raw_json
            """,
            (
                symbol,
                str(it.get("date") or ""),
                str(it.get("profitRatio") or ""),
                str(it.get("avgCost") or ""),
                str(it.get("cost90Low") or ""),
                str(it.get("cost90High") or ""),
                str(it.get("cost90Conc") or ""),
                str(it.get("cost70Low") or ""),
                str(it.get("cost70High") or ""),
                str(it.get("cost70Conc") or ""),
                ts,
                raw,
            ),
        )


def _upsert_market_fund_flow(
    conn: sqlite3.Connection,
    symbol: str,
    items: list[dict[str, str]],
    ts: str,
) -> None:
    for it in items:
        raw = json.dumps(it, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO market_fund_flow(
              symbol, date,
              close, change_pct,
              main_net_amount, main_net_ratio,
              super_net_amount, super_net_ratio,
              large_net_amount, large_net_ratio,
              medium_net_amount, medium_net_ratio,
              small_net_amount, small_net_ratio,
              updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET
              close = excluded.close,
              change_pct = excluded.change_pct,
              main_net_amount = excluded.main_net_amount,
              main_net_ratio = excluded.main_net_ratio,
              super_net_amount = excluded.super_net_amount,
              super_net_ratio = excluded.super_net_ratio,
              large_net_amount = excluded.large_net_amount,
              large_net_ratio = excluded.large_net_ratio,
              medium_net_amount = excluded.medium_net_amount,
              medium_net_ratio = excluded.medium_net_ratio,
              small_net_amount = excluded.small_net_amount,
              small_net_ratio = excluded.small_net_ratio,
              updated_at = excluded.updated_at,
              raw_json = excluded.raw_json
            """,
            (
                symbol,
                str(it.get("date") or ""),
                str(it.get("close") or ""),
                str(it.get("changePct") or ""),
                str(it.get("mainNetAmount") or ""),
                str(it.get("mainNetRatio") or ""),
                str(it.get("superNetAmount") or ""),
                str(it.get("superNetRatio") or ""),
                str(it.get("largeNetAmount") or ""),
                str(it.get("largeNetRatio") or ""),
                str(it.get("mediumNetAmount") or ""),
                str(it.get("mediumNetRatio") or ""),
                str(it.get("smallNetAmount") or ""),
                str(it.get("smallNetRatio") or ""),
                ts,
                raw,
            ),
        )


def _upsert_cn_industry_fund_flow_daily(
    conn: sqlite3.Connection,
    *,
    items: list[dict[str, Any]],
    ts: str,
) -> None:
    """
    Upsert CN industry fund flow rows into SQLite.

    Expected normalized input:
    - date (YYYY-MM-DD)
    - industry_code
    - industry_name
    - net_inflow (CNY)
    - raw (dict)
    """
    for it in items:
        d = str(it.get("date") or "").strip()
        code = str(it.get("industry_code") or "").strip()
        name = str(it.get("industry_name") or "").strip()
        if not d or not code or not name:
            continue
        net = float(it.get("net_inflow") or 0.0)
        # Some AkShare/Eastmoney rows contain datetime/date-like objects; serialize best-effort.
        raw = json.dumps(it.get("raw") or {}, ensure_ascii=False, default=str)
        conn.execute(
            """
            INSERT INTO market_cn_industry_fund_flow_daily(
              date, industry_code, industry_name, net_inflow, updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, industry_code) DO UPDATE SET
              industry_name = excluded.industry_name,
              net_inflow = excluded.net_inflow,
              updated_at = excluded.updated_at,
              raw_json = excluded.raw_json
            """,
            (d, code, name, net, ts, raw),
        )


@app.get("/market/status", response_model=MarketStatusResponse)
def market_status() -> MarketStatusResponse:
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(1) FROM market_stocks").fetchone()
        total = int(row[0]) if row else 0
    last = (get_setting("market_last_sync_at") or "").strip() or None
    return MarketStatusResponse(stocks=total, lastSyncAt=last)


@app.post("/market/sync")
def market_sync() -> JSONResponse:
    ts = now_iso()
    try:
        cn = fetch_cn_a_spot()
        hk = fetch_hk_spot()
    except RuntimeError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    with _connect() as conn:
        for s in cn + hk:
            _upsert_market_stock(conn, s, ts)
            _upsert_market_quote(conn, s, ts)
        conn.commit()

    set_setting("market_last_sync_at", ts)
    return JSONResponse({"ok": True, "stocks": len(cn) + len(hk), "syncedAt": ts})


@app.get("/market/stocks", response_model=MarketStocksResponse)
def market_list_stocks(
    market: str | None = None,
    q: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> MarketStocksResponse:
    market2 = (market or "").strip().upper()
    q2 = (q or "").strip()
    offset2 = max(0, int(offset))
    limit2 = max(1, min(int(limit), 200))

    where: list[str] = []
    params: list[Any] = []
    if market2 in {"CN", "HK"}:
        where.append("s.market = ?")
        params.append(market2)
    if q2:
        where.append("(s.ticker LIKE ? OR s.name LIKE ?)")
        params.extend([f"%{q2}%", f"%{q2}%"])
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with _connect() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(1) FROM market_stocks s {where_sql}",
            tuple(params),
        ).fetchone()
        total = int(total_row[0]) if total_row else 0

        rows = conn.execute(
            f"""
            SELECT
              s.symbol, s.market, s.ticker, s.name, s.currency, s.updated_at,
              q.price, q.change_pct, q.volume, q.turnover, q.market_cap
            FROM market_stocks s
            LEFT JOIN market_quotes q ON q.symbol = s.symbol
            {where_sql}
            ORDER BY s.market ASC, s.ticker ASC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [limit2, offset2]),
        ).fetchall()

    items = [
        MarketStockRow(
            symbol=str(r[0]),
            market=str(r[1]),
            ticker=str(r[2]),
            name=str(r[3]),
            currency=str(r[4]),
            updatedAt=str(r[5]),
            price=str(r[6]) if r[6] is not None else None,
            changePct=str(r[7]) if r[7] is not None else None,
            volume=str(r[8]) if r[8] is not None else None,
            turnover=str(r[9]) if r[9] is not None else None,
            marketCap=str(r[10]) if r[10] is not None else None,
        )
        for r in rows
    ]
    return MarketStocksResponse(items=items, total=total, offset=offset2, limit=limit2)


@app.get("/market/stocks/{symbol}/bars", response_model=MarketBarsResponse)
def market_stock_bars(symbol: str, days: int = 60, force: bool = False) -> MarketBarsResponse:
    days2 = max(10, min(int(days), 200))
    sym = symbol.strip()
    with _connect() as conn:
        row = conn.execute(
            "SELECT symbol, market, ticker, name, currency FROM market_stocks WHERE symbol = ?",
            (sym,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Not found")
        market = str(row[1])
        ticker = str(row[2])
        name = str(row[3])
        currency = str(row[4])

    # Load cached bars first.
    with _connect() as conn:
        cached = conn.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM market_bars
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()

    if force or len(cached) < days2:
        ts = now_iso()
        try:
            if market == "CN":
                bars = fetch_cn_a_daily_bars(ticker, days=days2)
            elif market == "HK":
                bars = fetch_hk_daily_bars(ticker, days=days2)
            else:
                raise HTTPException(status_code=400, detail="Unsupported market")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Bars fetch failed for {ticker}: {e}") from e
        with _connect() as conn:
            _upsert_market_bars(conn, sym, bars, ts)
            conn.commit()
        out = [
            {
                "date": b.date,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "amount": b.amount,
            }
            for b in bars
        ]
        return MarketBarsResponse(
            symbol=sym,
            market=market,
            ticker=ticker,
            name=name,
            currency=currency,
            bars=out,
        )

    out2 = [
        {
            "date": str(r[0]),
            "open": str(r[1] or ""),
            "high": str(r[2] or ""),
            "low": str(r[3] or ""),
            "close": str(r[4] or ""),
            "volume": str(r[5] or ""),
            "amount": str(r[6] or ""),
        }
        for r in reversed(cached)
    ]
    return MarketBarsResponse(
        symbol=sym,
        market=market,
        ticker=ticker,
        name=name,
        currency=currency,
        bars=out2,
    )


@app.get("/market/stocks/{symbol}/chips", response_model=MarketChipsResponse)
def market_stock_chips(symbol: str, days: int = 60, force: bool = False) -> MarketChipsResponse:
    days2 = max(10, min(int(days), 200))
    sym = symbol.strip()
    with _connect() as conn:
        row = conn.execute(
            "SELECT symbol, market, ticker, name, currency FROM market_stocks WHERE symbol = ?",
            (sym,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Not found")
        market = str(row[1])
        ticker = str(row[2])
        name = str(row[3])
        currency = str(row[4])

    if market != "CN":
        raise HTTPException(
            status_code=400,
            detail="Chip distribution is only supported for CN A-shares (v0).",
        )

    with _connect() as conn:
        cached = conn.execute(
            """
            SELECT raw_json
            FROM market_chips
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()
    if (not force) and len(cached) >= min(days2, 30):
        items = [json.loads(str(r[0])) for r in reversed(cached)]
        return MarketChipsResponse(
            symbol=sym,
            market=market,
            ticker=ticker,
            name=name,
            currency=currency,
            items=items,
        )

    ts = now_iso()
    try:
        items2 = fetch_cn_a_chip_summary(ticker, days=days2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chip fetch failed for {ticker}: {e}") from e
    with _connect() as conn:
        _upsert_market_chips(conn, sym, items2, ts)
        conn.commit()
    return MarketChipsResponse(
        symbol=sym,
        market=market,
        ticker=ticker,
        name=name,
        currency=currency,
        items=items2,
    )


@app.get("/market/stocks/{symbol}/fund-flow", response_model=MarketFundFlowResponse)
def market_stock_fund_flow(symbol: str, days: int = 60, force: bool = False) -> MarketFundFlowResponse:
    days2 = max(10, min(int(days), 200))
    sym = symbol.strip()
    with _connect() as conn:
        row = conn.execute(
            "SELECT symbol, market, ticker, name, currency FROM market_stocks WHERE symbol = ?",
            (sym,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Not found")
        market = str(row[1])
        ticker = str(row[2])
        name = str(row[3])
        currency = str(row[4])

    if market != "CN":
        raise HTTPException(
            status_code=400,
            detail="Fund flow distribution is only supported for CN A-shares (v0).",
        )

    with _connect() as conn:
        cached = conn.execute(
            """
            SELECT raw_json
            FROM market_fund_flow
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()
    if (not force) and len(cached) >= min(days2, 30):
        items = [json.loads(str(r[0])) for r in reversed(cached)]
        return MarketFundFlowResponse(
            symbol=sym,
            market=market,
            ticker=ticker,
            name=name,
            currency=currency,
            items=items,
        )

    ts = now_iso()
    try:
        items2 = fetch_cn_a_fund_flow(ticker, days=days2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fund flow fetch failed for {ticker}: {e}") from e
    with _connect() as conn:
        _upsert_market_fund_flow(conn, sym, items2, ts)
        conn.commit()
    return MarketFundFlowResponse(
        symbol=sym,
        market=market,
        ticker=ticker,
        name=name,
        currency=currency,
        items=items2,
    )


def _parse_yyyy_mm_dd(value: str) -> datetime | None:
    try:
        # Accept both full ISO and date-only.
        dt = datetime.fromisoformat(value.strip())
        return dt
    except Exception:
        try:
            return datetime.strptime(value.strip(), "%Y-%m-%d").replace(tzinfo=UTC)
        except Exception:
            return None


def _get_latest_cn_industry_fund_flow_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(date) FROM market_cn_industry_fund_flow_daily").fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])


def _industry_flow_signature(items: list[dict[str, Any]]) -> str:
    """
    Create a stable signature for an EOD snapshot to detect non-trading-day duplicates.
    We only use (industry_code, net_inflow) pairs and ignore raw_json.
    """
    pairs: list[tuple[str, float]] = []
    for it in items:
        code = str(it.get("industry_code") or "").strip()
        if not code:
            continue
        pairs.append((code, float(it.get("net_inflow") or 0.0)))
    pairs.sort(key=lambda x: x[0])
    s = json.dumps(pairs, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


@app.post(
    "/market/cn/industry-fund-flow/sync",
    response_model=MarketCnIndustryFundFlowSyncResponse,
)
def market_cn_industry_fund_flow_sync(
    req: MarketCnIndustryFundFlowSyncRequest,
) -> MarketCnIndustryFundFlowSyncResponse:
    """
    Sync CN industry fund flow into SQLite (DB-first cache for UI + strategy).

    Note: Data source is "latest snapshot" style. If you pass a historical date, we still
    label the snapshot using that date; for true backfill, rely on the hist backfill below.
    """
    as_of_str = (req.date or "").strip() or _today_cn_date_str()
    dt = _parse_yyyy_mm_dd(as_of_str)
    if dt is None:
        raise HTTPException(status_code=400, detail="Invalid date format (expected YYYY-MM-DD).")
    as_of = dt.date()

    days = max(1, min(int(req.days), 30))
    top_n = max(1, min(int(req.topN), 50))
    ts = now_iso()

    with _connect() as conn:
        if not req.force:
            row = conn.execute(
                "SELECT COUNT(1) FROM market_cn_industry_fund_flow_daily WHERE date = ?",
                (as_of.strftime("%Y-%m-%d"),),
            ).fetchone()
            if row and int(row[0] or 0) > 0:
                return MarketCnIndustryFundFlowSyncResponse(
                    ok=True,
                    asOfDate=as_of.strftime("%Y-%m-%d"),
                    days=days,
                    rowsUpserted=0,
                    histRowsUpserted=0,
                    message="Skipped (already cached). Use force=true to refresh.",
                )

    # Fetch latest snapshot
    try:
        items = fetch_cn_industry_fund_flow_eod(as_of)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Industry fund flow fetch failed: {e}") from e

    # If the market is closed (holiday/weekend), the provider may return the SAME snapshot as the
    # latest trading day. Detect it by comparing snapshot signatures and avoid storing a duplicate date.
    req_date = as_of.strftime("%Y-%m-%d")
    effective_date = req_date
    message: str | None = None
    with _connect() as conn:
        latest = _get_latest_cn_industry_fund_flow_date(conn)
        if latest and latest < req_date:
            prev_rows = conn.execute(
                "SELECT industry_code, net_inflow FROM market_cn_industry_fund_flow_daily WHERE date = ?",
                (latest,),
            ).fetchall()
            prev_items = [
                {"industry_code": str(r[0] or ""), "net_inflow": float(r[1] or 0.0)}
                for r in prev_rows
                if r and r[0]
            ]
            if prev_items and _industry_flow_signature(prev_items) == _industry_flow_signature(items):
                effective_date = latest
                # Clean up previously cached holiday rows (if any) to prevent duplicates.
                conn.execute("DELETE FROM market_cn_industry_fund_flow_daily WHERE date = ?", (req_date,))
                conn.commit()
                for it in items:
                    it["date"] = effective_date
                message = f"Market closed on {req_date}. Reused latest trading day snapshot: {effective_date}."

    # Upsert snapshot rows
    with _connect() as conn:
        _upsert_cn_industry_fund_flow_daily(conn, items=items, ts=ts)
        conn.commit()

    # Backfill hist for TopN industries (for sparkline + strategy context)
    hist_rows_upserted = 0
    hist_failures = 0
    try:
        top_items = sorted(items, key=lambda x: float(x.get("net_inflow") or 0.0), reverse=True)[:top_n]
        hist_upserts: list[dict[str, Any]] = []
        as_of_d = as_of.strftime("%Y-%m-%d")
        for it in top_items:
            name = str(it.get("industry_name") or "").strip()
            code = str(it.get("industry_code") or "").strip()
            if not name or not code:
                continue
            try:
                hist = fetch_cn_industry_fund_flow_hist(name, days=days)
            except Exception:
                hist_failures += 1
                continue
            for h in hist:
                d2 = str(h.get("date") or "").strip()
                if not d2:
                    continue
                # Do NOT overwrite the latest snapshot value for asOfDate with hist rows.
                if d2 == as_of_d:
                    continue
                hist_upserts.append(
                    {
                        "date": d2,
                        "industry_code": code,
                        "industry_name": name,
                        "net_inflow": float(h.get("net_inflow") or 0.0),
                        "raw": h.get("raw") or {},
                    }
                )
        with _connect() as conn:
            _upsert_cn_industry_fund_flow_daily(conn, items=hist_upserts, ts=ts)
            conn.commit()
        hist_rows_upserted = len(hist_upserts)
    except Exception:
        # Best-effort: do not fail the sync if hist backfill breaks.
        hist_rows_upserted = 0
        hist_failures = 0

    return MarketCnIndustryFundFlowSyncResponse(
        ok=True,
        asOfDate=effective_date,
        days=days,
        rowsUpserted=len(items),
        histRowsUpserted=hist_rows_upserted,
        histFailures=hist_failures,
        message=(
            message
            if message
            else (None if hist_failures == 0 else f"Hist backfill partial: {hist_failures} industries failed.")
        ),
    )


@app.get(
    "/market/cn/industry-fund-flow",
    response_model=MarketCnIndustryFundFlowResponse,
)
def market_cn_industry_fund_flow(
    days: int = 10,
    topN: int = 30,
    asOfDate: str | None = None,
) -> MarketCnIndustryFundFlowResponse:
    days2 = max(1, min(int(days), 30))
    top2 = max(1, min(int(topN), 100))
    with _connect() as conn:
        as_of = (asOfDate or "").strip() or (_get_latest_cn_industry_fund_flow_date(conn) or "")
        if not as_of:
            return MarketCnIndustryFundFlowResponse(
                asOfDate=_today_cn_date_str(),
                days=days2,
                topN=top2,
                dates=[],
                top=[],
            )

        # Load last N dates we have (descending), then reverse for series.
        date_rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM market_cn_industry_fund_flow_daily
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (as_of, days2),
        ).fetchall()
        dates_desc = [str(r[0]) for r in date_rows if r and r[0]]
        dates = list(reversed(dates_desc))
        if not dates:
            return MarketCnIndustryFundFlowResponse(asOfDate=as_of, days=days2, topN=top2, dates=[], top=[])

        # Load all rows for those dates
        placeholders = ",".join(["?"] * len(dates))
        rows = conn.execute(
            f"""
            SELECT date, industry_code, industry_name, net_inflow
            FROM market_cn_industry_fund_flow_daily
            WHERE date IN ({placeholders})
            """,
            tuple(dates),
        ).fetchall()

    # Aggregate per industry
    by_code: dict[str, dict[str, Any]] = {}
    for d, code, name, net in rows:
        code2 = str(code)
        d2 = str(d)
        name2 = str(name)
        net2 = float(net or 0.0)
        cur = by_code.get(code2)
        if cur is None:
            cur = {"industryCode": code2, "industryName": name2, "series": {}, "sum": 0.0}
            by_code[code2] = cur
        cur["industryName"] = name2 or cur["industryName"]
        cur["series"][d2] = net2
        cur["sum"] = float(cur["sum"]) + net2

    out_rows: list[IndustryFundFlowRow] = []
    for code, agg in by_code.items():
        series_map: dict[str, float] = agg.get("series") or {}
        series = [IndustryFundFlowPoint(date=d, netInflow=float(series_map.get(d, 0.0))) for d in dates]
        net_asof = float(series_map.get(as_of, 0.0))
        out_rows.append(
            IndustryFundFlowRow(
                industryCode=code,
                industryName=str(agg.get("industryName") or ""),
                netInflow=net_asof,
                sum10d=float(agg.get("sum") or 0.0),
                series10d=series,
            )
        )

    out_rows.sort(key=lambda r: r.netInflow, reverse=True)
    return MarketCnIndustryFundFlowResponse(
        asOfDate=as_of,
        days=days2,
        topN=top2,
        dates=dates,
        top=out_rows[:top2],
    )


def _compute_cn_sentiment_for_date(d: str) -> dict[str, Any]:
    """
    Compute CN A-share breadth & sentiment (simplified MVP) for the given date (YYYY-MM-DD).
    """
    ts = now_iso()
    as_of = d
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    raw: dict[str, Any] = {}
    errors: list[str] = []
    # 1) Breadth
    up = 0
    down = 0
    flat = 0
    ratio = 0.0
    try:
        breadth = fetch_cn_market_breadth_eod(dt)
        raw["breadth"] = breadth
        up = int(breadth.get("up_count") or 0)
        down = int(breadth.get("down_count") or 0)
        flat = int(breadth.get("flat_count") or 0)
        ratio = _finite_float(breadth.get("up_down_ratio"), 0.0)
    except Exception as e:
        errors.append(f"breadth_failed: {e}")
        raw["breadthError"] = str(e)
    # 2) Yesterday limit-up premium
    premium = 0.0
    try:
        premium_obj = fetch_cn_yesterday_limitup_premium(dt)
        raw["yesterdayLimitUpPremium"] = premium_obj
        premium_raw = premium_obj.get("premium")
        premium = _finite_float(premium_raw, 0.0)
        try:
            if premium_raw is not None and not math.isfinite(float(premium_raw)):
                errors.append("yesterday_limitup_premium_nan")
        except Exception:
            # ignore parse errors; _finite_float already sanitized it.
            pass
    except Exception as e:
        errors.append(f"yesterday_limitup_premium_failed: {e}")
        raw["yesterdayLimitUpPremiumError"] = str(e)
    # 3) Failed limit-up rate
    failed_rate = 0.0
    try:
        failed_obj = fetch_cn_failed_limitup_rate(dt)
        raw["failedLimitUpRate"] = failed_obj
        failed_raw = failed_obj.get("failed_rate")
        failed_rate = _finite_float(failed_raw, 0.0)
        try:
            if failed_raw is not None and not math.isfinite(float(failed_raw)):
                errors.append("failed_limitup_rate_nan")
        except Exception:
            pass
    except Exception as e:
        errors.append(f"failed_limitup_rate_failed: {e}")
        raw["failedLimitUpRateError"] = str(e)

    # Risk rules (MVP)
    rules: list[str] = []
    risk_mode = "normal"
    if premium < 0.0 and failed_rate > 30.0:
        risk_mode = "no_new_positions"
        rules.append("premium<0 && failedLimitUpRate>30 => no_new_positions")
    elif premium < 0.0 or failed_rate > 30.0:
        risk_mode = "caution"
        rules.append("premium<0 or failedLimitUpRate>30 => caution")
    if errors:
        # If any part failed, mark as caution so users don't blindly trust it.
        if risk_mode == "normal":
            risk_mode = "caution"
        rules.extend(errors[:3])

    return {
        "date": d,
        "asOfDate": as_of,
        "up": up,
        "down": down,
        "flat": flat,
        "ratio": ratio,
        "premium": premium,
        "failedRate": failed_rate,
        "riskMode": risk_mode,
        "rules": rules,
        "updatedAt": ts,
        "raw": raw,
    }


@app.post("/market/cn/sentiment/sync", response_model=MarketCnSentimentResponse)
def market_cn_sentiment_sync(req: MarketCnSentimentSyncRequest) -> MarketCnSentimentResponse:
    d = (req.date or "").strip() or _today_cn_date_str()
    # DB-first: return cached if exists and not forced.
    if not req.force:
        cached = _list_cn_sentiment_days(as_of_date=d, days=1)
        if cached and str(cached[-1].get("date") or "") == d:
            return MarketCnSentimentResponse(asOfDate=d, days=1, items=[MarketCnSentimentRow(**cached[-1])])

    try:
        out = _compute_cn_sentiment_for_date(d)
    except Exception as e:
        # Best-effort: still upsert a row with errors so UI can show what happened.
        out = {
            "date": d,
            "asOfDate": d,
            "up": 0,
            "down": 0,
            "flat": 0,
            "ratio": 0.0,
            "premium": 0.0,
            "failedRate": 0.0,
            "riskMode": "caution",
            "rules": [f"compute_failed: {e}"],
            "updatedAt": now_iso(),
            "raw": {"error": str(e)},
        }

    raw0 = out.get("raw")
    raw_dict: dict[str, Any] = raw0 if isinstance(raw0, dict) else {}
    rules2 = [str(x) for x in (out.get("rules") or [])]
    upsert_ok = False
    try:
        _upsert_cn_sentiment_daily(
            date=d,
            as_of_date=str(out["asOfDate"]),
            up=int(out["up"]),
            down=int(out["down"]),
            flat=int(out["flat"]),
            up_down_ratio=_finite_float(out.get("ratio"), 0.0),
            premium=_finite_float(out.get("premium"), 0.0),
            failed_rate=_finite_float(out.get("failedRate"), 0.0),
            risk_mode=str(out["riskMode"]),
            rules=rules2,
            updated_at=str(out["updatedAt"]),
            raw=raw_dict,
        )
        upsert_ok = True
    except Exception as e:
        # Never 500: return computed result and attach DB error in rules/raw for visibility.
        rules2 = [*rules2, f"upsert_failed: {e}"]
        raw_dict = {**raw_dict, "upsertError": str(e)}

    try:
        items = _list_cn_sentiment_days(as_of_date=d, days=1)
        if items and upsert_ok and str(items[-1].get("date") or "") == d:
            return MarketCnSentimentResponse(asOfDate=d, days=1, items=[MarketCnSentimentRow(**items[-1])])
    except Exception as e:
        rules2 = [*rules2, f"readback_failed: {e}"]
        raw_dict = {**raw_dict, "readbackError": str(e)}

    # Fallback: computed-only (not persisted).
    return MarketCnSentimentResponse(
        asOfDate=d,
        days=1,
        items=[
            MarketCnSentimentRow(
                date=str(out.get("date") or d),
                upCount=int(out.get("up") or 0),
                downCount=int(out.get("down") or 0),
                flatCount=int(out.get("flat") or 0),
                totalCount=int(int(out.get("up") or 0) + int(out.get("down") or 0) + int(out.get("flat") or 0)),
                upDownRatio=_finite_float(out.get("ratio"), 0.0),
                yesterdayLimitUpPremium=_finite_float(out.get("premium"), 0.0),
                failedLimitUpRate=_finite_float(out.get("failedRate"), 0.0),
                riskMode=str(out.get("riskMode") or "caution"),
                rules=rules2,
                updatedAt=str(out.get("updatedAt") or now_iso()),
            )
        ],
    )


@app.get("/market/cn/sentiment", response_model=MarketCnSentimentResponse)
def market_cn_sentiment(days: int = 10, asOfDate: str | None = None) -> MarketCnSentimentResponse:
    d = (asOfDate or "").strip() or _today_cn_date_str()
    items = _list_cn_sentiment_days(as_of_date=d, days=days)
    return MarketCnSentimentResponse(asOfDate=d, days=max(1, min(int(days), 30)), items=[MarketCnSentimentRow(**x) for x in items])


@app.get("/rank/cn/next2d", response_model=RankSnapshotResponse)
def rank_cn_next2d(
    accountId: str | None = None,
    limit: int = 30,
    asOfDate: str | None = None,
    universeVersion: str = "v0",
) -> RankSnapshotResponse:
    as_of = (asOfDate or "").strip() or _today_cn_date_str()
    # Default account: first pingan account.
    aid = (accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    cached = _get_cn_rank_snapshot(account_id=aid, as_of_date=as_of, universe_version=universeVersion)
    if cached is None:
        return RankSnapshotResponse(
            id="",
            asOfDate=as_of,
            accountId=aid,
            createdAt="",
            universeVersion=universeVersion,
            riskMode=None,
            items=[],
            debug={"status": "no_snapshot"},
        )
    out_raw = cached.get("output")
    out: dict[str, Any] = out_raw if isinstance(out_raw, dict) else {}
    items_raw = out.get("items")
    items0: list[Any] = items_raw if isinstance(items_raw, list) else []
    items = items0[: max(1, min(int(limit), 200))]
    return RankSnapshotResponse(
        id=str(cached.get("id") or ""),
        asOfDate=str(out.get("asOfDate") or as_of),
        accountId=aid,
        createdAt=str(cached.get("createdAt") or ""),
        universeVersion=str(out.get("universeVersion") or universeVersion),
        riskMode=str(out.get("riskMode") or "") or None,
        items=[RankItem(**x) for x in items if isinstance(x, dict)],
        debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
    )


@app.post("/rank/cn/next2d/generate", response_model=RankSnapshotResponse)
def rank_cn_next2d_generate(req: RankNext2dGenerateRequest) -> RankSnapshotResponse:
    as_of = (req.asOfDate or "").strip() or _today_cn_date_str()
    universe = (req.universeVersion or "").strip() or "v0"
    limit2 = max(1, min(int(req.limit), 200))

    # Default account: first pingan account.
    aid = (req.accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    cached = _get_cn_rank_snapshot(account_id=aid, as_of_date=as_of, universe_version=universe)
    if cached is not None and not req.force:
        out_raw = cached.get("output")
        out: dict[str, Any] = out_raw if isinstance(out_raw, dict) else {}
        items_raw = out.get("items")
        items0: list[Any] = items_raw if isinstance(items_raw, list) else []
        items = items0[:limit2]
        return RankSnapshotResponse(
            id=str(cached.get("id") or ""),
            asOfDate=str(out.get("asOfDate") or as_of),
            accountId=aid,
            createdAt=str(cached.get("createdAt") or ""),
            universeVersion=str(out.get("universeVersion") or universe),
            riskMode=str(out.get("riskMode") or "") or None,
            items=[RankItem(**x) for x in items if isinstance(x, dict)],
            debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
        )

    ts = now_iso()
    output = _rank_build_and_score(
        account_id=aid,
        as_of_date=as_of,
        limit=limit2,
        universe_version=universe,
        include_holdings=bool(req.includeHoldings),
    )
    snap_id = _upsert_cn_rank_snapshot(account_id=aid, as_of_date=as_of, universe_version=universe, ts=ts, output=output)
    _prune_cn_rank_snapshots(keep_days=10)
    out_items = output.get("items")
    items1: list[Any] = out_items if isinstance(out_items, list) else []
    items = items1[:limit2]
    return RankSnapshotResponse(
        id=snap_id,
        asOfDate=str(output.get("asOfDate") or as_of),
        accountId=aid,
        createdAt=ts,
        universeVersion=str(output.get("universeVersion") or universe),
        riskMode=str(output.get("riskMode") or "") or None,
        items=[RankItem(**x) for x in items if isinstance(x, dict)],
        debug=output.get("debug") if isinstance(output.get("debug"), dict) else None,
    )


@app.get("/rank/cn/intraday", response_model=IntradayRankSnapshotResponse)
def rank_cn_intraday(
    accountId: str | None = None,
    limit: int = 30,
    universeVersion: str = "v0",
) -> IntradayRankSnapshotResponse:
    universe = (universeVersion or "").strip() or "v0"
    limit2 = max(1, min(int(limit), 200))

    aid = (accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    cached = _get_cn_intraday_rank_snapshot_latest(account_id=aid, universe_version=universe)
    if cached is None:
        now_ts = now_iso()
        trade_date = _today_cn_date_str()
        return IntradayRankSnapshotResponse(
            id="",
            asOfTs=now_ts,
            tradeDate=trade_date,
            slot="",
            accountId=aid,
            createdAt="",
            universeVersion=universe,
            riskMode=None,
            items=[],
            observations=[],
            debug={"status": "no_snapshot"},
        )
    out_raw = cached.get("output")
    out: dict[str, Any] = out_raw if isinstance(out_raw, dict) else {}
    items_raw = out.get("items")
    items0: list[Any] = items_raw if isinstance(items_raw, list) else []
    items = items0[:limit2]
    obs_raw = out.get("observations")
    obs0: list[Any] = obs_raw if isinstance(obs_raw, list) else []
    obs_items = [IntradayObservationRow(**x) for x in obs0 if isinstance(x, dict)]
    return IntradayRankSnapshotResponse(
        id=str(cached.get("id") or ""),
        asOfTs=str(out.get("asOfTs") or ""),
        tradeDate=str(out.get("tradeDate") or ""),
        slot=str(out.get("slot") or ""),
        accountId=aid,
        createdAt=str(cached.get("createdAt") or ""),
        universeVersion=str(out.get("universeVersion") or universe),
        riskMode=str(out.get("riskMode") or "") or None,
        items=[IntradayRankItem(**x) for x in items if isinstance(x, dict)],
        observations=obs_items,
        debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
    )


@app.post("/rank/cn/intraday/generate", response_model=IntradayRankSnapshotResponse)
def rank_cn_intraday_generate(req: IntradayRankGenerateRequest) -> IntradayRankSnapshotResponse:
    universe = (req.universeVersion or "").strip() or "v0"
    limit2 = max(1, min(int(req.limit), 200))
    as_of_ts = (req.asOfTs or "").strip() or now_iso()

    aid = (req.accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    tz = ZoneInfo("Asia/Shanghai")
    try:
        dt = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
        dt_cn = dt.astimezone(tz)
    except Exception:
        dt_cn = datetime.now(tz=tz)
    trade_date = dt_cn.strftime("%Y-%m-%d")
    slot = (req.slot or "").strip() or _infer_intraday_slot(dt_cn)

    if not req.force:
        cached = _get_cn_intraday_rank_snapshot_latest_for(
            account_id=aid,
            trade_date=trade_date,
            slot=slot,
            universe_version=universe,
        )
        if cached is not None:
            out_raw = cached.get("output")
            out: dict[str, Any] = out_raw if isinstance(out_raw, dict) else {}
            items_raw = out.get("items")
            items0: list[Any] = items_raw if isinstance(items_raw, list) else []
            items = items0[:limit2]
            obs_raw = out.get("observations")
            obs0: list[Any] = obs_raw if isinstance(obs_raw, list) else []
            obs_items = [IntradayObservationRow(**x) for x in obs0 if isinstance(x, dict)]
            return IntradayRankSnapshotResponse(
                id=str(cached.get("id") or ""),
                asOfTs=str(out.get("asOfTs") or as_of_ts),
                tradeDate=str(out.get("tradeDate") or trade_date),
                slot=str(out.get("slot") or slot),
                accountId=aid,
                createdAt=str(cached.get("createdAt") or ""),
                universeVersion=str(out.get("universeVersion") or universe),
                riskMode=str(out.get("riskMode") or "") or None,
                items=[IntradayRankItem(**x) for x in items if isinstance(x, dict)],
                observations=obs_items,
                debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
            )

    ts = now_iso()
    output = _intraday_rank_build_and_score(
        account_id=aid,
        as_of_ts=as_of_ts,
        slot=slot,
        limit=limit2,
        universe_version=universe,
    )
    snap_id = _upsert_cn_intraday_rank_snapshot(
        account_id=aid,
        as_of_ts=as_of_ts,
        trade_date=str(output.get("tradeDate") or trade_date),
        slot=str(output.get("slot") or slot),
        universe_version=universe,
        ts=ts,
        output=output,
    )
    _prune_cn_intraday_rank_snapshots(account_id=aid, keep_days=10)

    out_items = output.get("items")
    items1: list[Any] = out_items if isinstance(out_items, list) else []
    items = items1[:limit2]
    out_obs = output.get("observations")
    obs1: list[Any] = out_obs if isinstance(out_obs, list) else []
    obs_items = [IntradayObservationRow(**x) for x in obs1 if isinstance(x, dict)]
    return IntradayRankSnapshotResponse(
        id=snap_id,
        asOfTs=str(output.get("asOfTs") or as_of_ts),
        tradeDate=str(output.get("tradeDate") or trade_date),
        slot=str(output.get("slot") or slot),
        accountId=aid,
        createdAt=ts,
        universeVersion=str(output.get("universeVersion") or universe),
        riskMode=str(output.get("riskMode") or "") or None,
        items=[IntradayRankItem(**x) for x in items if isinstance(x, dict)],
        observations=obs_items,
        debug=output.get("debug") if isinstance(output.get("debug"), dict) else None,
    )


@app.get("/rank/cn/intraday/observations", response_model=IntradayObservationsResponse)
def rank_cn_intraday_observations(date: str | None = None) -> IntradayObservationsResponse:
    trade_date = (date or "").strip() or _today_cn_date_str()
    items = _list_cn_intraday_observations(trade_date=trade_date)
    return IntradayObservationsResponse(tradeDate=trade_date, items=[IntradayObservationRow(**x) for x in items])


def _upsert_cn_sentiment_daily(
    *,
    date: str,
    as_of_date: str,
    up: int,
    down: int,
    flat: int,
    up_down_ratio: float,
    premium: float,
    failed_rate: float,
    risk_mode: str,
    rules: list[str],
    updated_at: str,
    raw: dict[str, Any],
) -> None:
    with _connect() as conn:
        total = max(0, int(up) + int(down) + int(flat))
        conn.execute(
            """
            INSERT INTO market_cn_sentiment_daily(
              date, as_of_date, up_count, down_count, flat_count, total_count,
              up_down_ratio, yesterday_limitup_premium, failed_limitup_rate,
              risk_mode, rules_json, updated_at, raw_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
              as_of_date = excluded.as_of_date,
              up_count = excluded.up_count,
              down_count = excluded.down_count,
              flat_count = excluded.flat_count,
              total_count = excluded.total_count,
              up_down_ratio = excluded.up_down_ratio,
              yesterday_limitup_premium = excluded.yesterday_limitup_premium,
              failed_limitup_rate = excluded.failed_limitup_rate,
              risk_mode = excluded.risk_mode,
              rules_json = excluded.rules_json,
              updated_at = excluded.updated_at,
              raw_json = excluded.raw_json
            """,
            (
                date,
                as_of_date,
                int(up),
                int(down),
                int(flat),
                int(total),
                float(up_down_ratio),
                float(premium),
                float(failed_rate),
                str(risk_mode),
                json.dumps(rules or [], ensure_ascii=False),
                updated_at,
                json.dumps(raw or {}, ensure_ascii=False, default=str),
            ),
        )
        conn.commit()


def _list_cn_sentiment_days(*, as_of_date: str, days: int) -> list[dict[str, Any]]:
    days2 = max(1, min(int(days), 30))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT date, up_count, down_count, flat_count, total_count,
                   up_down_ratio, yesterday_limitup_premium, failed_limitup_rate,
                   risk_mode, rules_json, updated_at
            FROM market_cn_sentiment_daily
            WHERE date <= ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (as_of_date, days2),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "date": str(r[0]),
                "upCount": int(r[1] or 0),
                "downCount": int(r[2] or 0),
                "flatCount": int(r[3] or 0),
                "totalCount": int(r[4] or 0),
                "upDownRatio": float(r[5] or 0.0),
                "yesterdayLimitUpPremium": float(r[6] or 0.0),
                "failedLimitUpRate": float(r[7] or 0.0),
                "riskMode": str(r[8] or "normal"),
                "rules": json.loads(str(r[9]) or "[]") if r[9] else [],
                "updatedAt": str(r[10] or ""),
            }
        )
    return list(reversed(out))


def _market_cn_industry_fund_flow_top_by_date(
    *, as_of_date: str, days: int = 10, top_k: int = 5
) -> dict[str, Any]:
    """
    Screenshot-style 'TopK × Date' matrix of industry NAMES (no numeric values).

    Returns:
      - asOfDate, days, topK
      - dates: last N dates we have (ASC)
      - ranks: [1..topK]
      - matrix: rows by rank, cols by date -> industryName
      - topByDate: [{date, top:[name1..nameK]}]
    """
    days2 = max(1, min(int(days), 30))
    topk2 = max(1, min(int(top_k), 10))
    with _connect() as conn:
        rows = conn.execute(
            """
            WITH dates AS (
              SELECT DISTINCT date
              FROM market_cn_industry_fund_flow_daily
              WHERE date <= ?
              ORDER BY date DESC
              LIMIT ?
            ),
            ranked AS (
              SELECT date, industry_name, net_inflow,
                     ROW_NUMBER() OVER (PARTITION BY date ORDER BY net_inflow DESC) AS rn
              FROM market_cn_industry_fund_flow_daily
              WHERE date IN (SELECT date FROM dates)
            )
            SELECT date, rn, industry_name
            FROM ranked
            WHERE rn <= ?
            ORDER BY date ASC, rn ASC
            """,
            (as_of_date, days2, topk2),
        ).fetchall()

    by_date: dict[str, list[str]] = {}
    for r in rows:
        d = str(r[0])
        rn = int(r[1])
        name = str(r[2] or "")
        if d not in by_date:
            by_date[d] = [""] * topk2
        if 1 <= rn <= topk2:
            by_date[d][rn - 1] = name

    dates = sorted(by_date.keys())
    top_by_date = [{"date": d, "top": by_date[d]} for d in dates]
    matrix: list[list[str]] = []
    for idx in range(topk2):
        matrix.append([by_date[d][idx] for d in dates])

    return {
        "asOfDate": as_of_date,
        "days": days2,
        "topK": topk2,
        "dates": dates,
        "ranks": list(range(1, topk2 + 1)),
        "matrix": matrix,
        "topByDate": top_by_date,
    }


def _upsert_leader_stocks(*, date: str, items: list[dict[str, Any]], ts: str) -> list[str]:
    """
    Upsert leaders for a date. Returns inserted/updated ids.
    """
    ids: list[str] = []
    with _connect() as conn:
        for it in items:
            rid = str(it.get("id") or uuid.uuid4())
            conn.execute(
                """
                INSERT INTO leader_stocks(
                  id, date, symbol, market, ticker, name,
                  entry_price, score, reason,
                  why_bullets_json, expected_duration_days, buy_zone_json, triggers_json, invalidation, target_price_json, probability,
                  source_signals_json, risk_points_json, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                  id = excluded.id,
                  market = excluded.market,
                  ticker = excluded.ticker,
                  name = excluded.name,
                  entry_price = excluded.entry_price,
                  score = excluded.score,
                  reason = excluded.reason,
                  why_bullets_json = excluded.why_bullets_json,
                  expected_duration_days = excluded.expected_duration_days,
                  buy_zone_json = excluded.buy_zone_json,
                  triggers_json = excluded.triggers_json,
                  invalidation = excluded.invalidation,
                  target_price_json = excluded.target_price_json,
                  probability = excluded.probability,
                  source_signals_json = excluded.source_signals_json,
                  risk_points_json = excluded.risk_points_json,
                  created_at = excluded.created_at
                """,
                (
                    rid,
                    date,
                    str(it.get("symbol") or ""),
                    str(it.get("market") or ""),
                    str(it.get("ticker") or ""),
                    str(it.get("name") or ""),
                    it.get("entryPrice"),
                    it.get("score"),
                    str(it.get("reason") or ""),
                    json.dumps(it.get("whyBullets") or [], ensure_ascii=False),
                    int(it.get("expectedDurationDays") or 0) or None,
                    json.dumps(it.get("buyZone") or {}, ensure_ascii=False),
                    json.dumps(it.get("triggers") or [], ensure_ascii=False),
                    str(it.get("invalidation") or "") or None,
                    json.dumps(it.get("targetPrice") or {}, ensure_ascii=False),
                    int(it.get("probability") or 0) or None,
                    json.dumps(it.get("sourceSignals") or {}, ensure_ascii=False),
                    json.dumps(it.get("risks") or it.get("riskPoints") or [], ensure_ascii=False),
                    ts,
                ),
            )
            ids.append(rid)
        conn.commit()
    return ids


def _list_leader_stocks(*, days: int = 10) -> tuple[list[str], list[dict[str, Any]]]:
    days2 = max(1, min(int(days), 30))
    with _connect() as conn:
        date_rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM leader_stocks
            ORDER BY date DESC
            LIMIT ?
            """,
            (days2,),
        ).fetchall()
        dates_desc = [str(r[0]) for r in date_rows if r and r[0]]
        dates = list(reversed(dates_desc))
        if not dates:
            return ([], [])
        placeholders = ",".join(["?"] * len(dates))
        rows = conn.execute(
            f"""
            SELECT id, date, symbol, market, ticker, name, entry_price, score, reason,
                   why_bullets_json, expected_duration_days, buy_zone_json, triggers_json, invalidation, target_price_json, probability,
                   source_signals_json, risk_points_json, created_at
            FROM leader_stocks
            WHERE date IN ({placeholders})
            ORDER BY date DESC, score DESC
            """,
            tuple(dates),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": str(r[0]),
                "date": str(r[1]),
                "symbol": str(r[2]),
                "market": str(r[3]),
                "ticker": str(r[4]),
                "name": str(r[5]),
                "entryPrice": float(r[6]) if r[6] is not None else None,
                "score": float(r[7]) if r[7] is not None else None,
                "reason": str(r[8] or ""),
                "whyBullets": json.loads(str(r[9]) or "[]") if r[9] else [],
                "expectedDurationDays": int(r[10]) if r[10] is not None else None,
                "buyZone": json.loads(str(r[11]) or "{}") if r[11] else {},
                "triggers": json.loads(str(r[12]) or "[]") if r[12] else [],
                "invalidation": str(r[13] or "") or None,
                "targetPrice": json.loads(str(r[14]) or "{}") if r[14] else {},
                "probability": int(r[15]) if r[15] is not None else None,
                "sourceSignals": json.loads(str(r[16]) or "{}") if r[16] else {},
                "riskPoints": json.loads(str(r[17]) or "[]") if r[17] else [],
                "createdAt": str(r[18]),
            }
        )
    return (dates, out)


def _prune_leader_stocks_keep_last_n_days(*, keep_days: int = 10) -> None:
    keep2 = max(1, min(int(keep_days), 60))
    with _connect() as conn:
        date_rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM leader_stocks
            ORDER BY date DESC
            LIMIT ?
            """,
            (keep2,),
        ).fetchall()
        keep_dates = [str(r[0]) for r in date_rows if r and r[0]]
        if not keep_dates:
            return
        placeholders = ",".join(["?"] * len(keep_dates))
        conn.execute(
            f"DELETE FROM leader_stocks WHERE date NOT IN ({placeholders})",
            tuple(keep_dates),
        )
        conn.commit()


def _delete_leader_stocks_for_date(date: str) -> None:
    d = (date or "").strip()
    if not d:
        return
    with _connect() as conn:
        conn.execute("DELETE FROM leader_stocks WHERE date = ?", (d,))
        conn.commit()


def _get_leader_live_scores(symbols: list[str]) -> dict[str, dict[str, Any]]:
    syms = [str(s).strip() for s in symbols if str(s).strip()]
    if not syms:
        return {}
    placeholders = ",".join(["?"] * len(syms))
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT symbol, live_score, breakdown_json, updated_at FROM leader_stock_scores WHERE symbol IN ({placeholders})",
            tuple(syms),
        ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        sym = str(r[0] or "")
        try:
            breakdown = json.loads(str(r[2]) or "{}")
        except Exception:
            breakdown = {}
        out[sym] = {"liveScore": float(r[1]) if r[1] is not None else None, "updatedAt": str(r[3] or "")}
        if isinstance(breakdown, dict):
            out[sym]["breakdown"] = breakdown
    return out


def _upsert_leader_live_score(*, symbol: str, live_score: float, breakdown: dict[str, Any], ts: str) -> None:
    sym = (symbol or "").strip()
    if not sym:
        return
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO leader_stock_scores(symbol, live_score, breakdown_json, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              live_score = excluded.live_score,
              breakdown_json = excluded.breakdown_json,
              updated_at = excluded.updated_at
            """,
            (sym, float(live_score), json.dumps(breakdown or {}, ensure_ascii=False), ts),
        )
        conn.commit()


def _compute_leader_live_score(
    *,
    market: str,
    feats: dict[str, Any],
    chips_summary: dict[str, Any] | None,
    ff_breakdown: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Deterministic "investability" score at current time.
    - Range: 0..100
    - Intended for cross-day comparison better than LLM daily scores.
    """
    last_close = _safe_float(feats.get("lastClose"))
    sma5 = _safe_float(feats.get("sma5"))
    sma10 = _safe_float(feats.get("sma10"))
    sma20 = _safe_float(feats.get("sma20"))
    high10 = _safe_float(feats.get("high10"))

    # Trend (0-40)
    trend = 0.0
    if sma20 > 0 and last_close > sma20:
        trend += 20.0
    elif last_close > 0:
        trend += 8.0
    if sma5 > 0 and sma10 > 0 and sma20 > 0 and (sma5 >= sma10 >= sma20):
        trend += 10.0
    if high10 > 0 and last_close >= high10 * 0.98:
        trend += 10.0
    trend = max(0.0, min(40.0, trend))

    # Flow (0-30) - CN only (HK gets neutral flow).
    flow = 0.0
    if market == "CN" and isinstance(ff_breakdown, dict):
        main_ratio = _safe_float(ff_breakdown.get("mainNetRatio"))
        super_ratio = _safe_float(ff_breakdown.get("superNetRatio"))
        large_ratio = _safe_float(ff_breakdown.get("largeNetRatio"))
        change_pct = _safe_float(ff_breakdown.get("changePct"))
        if main_ratio > 2:
            flow += 18.0
        elif main_ratio > 0:
            flow += 12.0
        else:
            flow += 4.0
        if (super_ratio + large_ratio) > 1.0:
            flow += 7.0
        elif (super_ratio + large_ratio) > 0.0:
            flow += 4.0
        if change_pct > 0:
            flow += 5.0
    else:
        flow += 12.0
    flow = max(0.0, min(30.0, flow))

    # Structure (0-20) - CN only (HK gets neutral structure).
    structure = 0.0
    if market == "CN" and isinstance(chips_summary, dict):
        pr = _safe_float(chips_summary.get("profitRatio"))
        avg_cost = _safe_float(chips_summary.get("avgCost"))
        conc70 = _safe_float(chips_summary.get("cost70Conc"))
        if pr >= 0.65:
            structure += 12.0
        elif pr >= 0.45:
            structure += 9.0
        else:
            structure += 6.0
        if avg_cost > 0 and last_close >= avg_cost:
            structure += 5.0
        elif avg_cost > 0:
            structure += 3.0
        if conc70 >= 0.08:
            structure += 3.0
        else:
            structure += 1.0
    else:
        structure += 10.0
    structure = max(0.0, min(20.0, structure))

    # Risk (0-10) - higher is better (lower risk).
    risk = 10.0
    ext = (last_close / sma20 - 1.0) if (sma20 > 0 and last_close > 0) else 0.0
    if ext > 0.15:
        risk -= 5.0
    elif ext > 0.10:
        risk -= 3.0
    if market == "CN" and isinstance(ff_breakdown, dict):
        main_ratio = _safe_float(ff_breakdown.get("mainNetRatio"))
        if main_ratio < 0:
            risk -= 3.0
    risk = max(0.0, min(10.0, risk))

    total = trend + flow + structure + risk
    total = max(0.0, min(100.0, total))
    return {
        "total": round(total, 2),
        "trend": round(trend, 2),
        "flow": round(flow, 2),
        "structure": round(structure, 2),
        "risk": round(risk, 2),
    }


def _refresh_leader_live_scores(*, symbols: list[str], ts: str, force_refresh_market: bool = False) -> None:
    # Refresh scores for a limited set of symbols (<= 30) to keep runtime bounded.
    syms: list[str] = []
    seen: set[str] = set()
    for s in symbols:
        sym = (s or "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        syms.append(sym)
        if len(syms) >= 30:
            break

    for sym in syms:
        try:
            # Determine market quickly
            with _connect() as conn:
                row = conn.execute(
                    "SELECT market, ticker FROM market_stocks WHERE symbol = ?",
                    (sym,),
                ).fetchone()
            market = str(row[0]) if row and row[0] else ("HK" if sym.startswith("HK:") else "CN")
            bars_cached = _load_cached_bars(sym, days=60)
            bars = bars_cached
            if force_refresh_market:
                try:
                    bars = market_stock_bars(sym, days=60, force=True).bars
                except Exception:
                    bars = bars_cached
            feats = _bars_features(bars or [])

            chips_summary: dict[str, Any] | None = None
            ff_breakdown: dict[str, Any] | None = None
            if market == "CN":
                try:
                    chips_items = market_stock_chips(sym, days=30, force=bool(force_refresh_market)).items
                    chips_last = chips_items[-1] if chips_items else {}
                    chips_summary = _chips_summary_last(chips_last)
                except Exception:
                    chips_summary = None
                try:
                    ff_items = market_stock_fund_flow(sym, days=30, force=bool(force_refresh_market)).items
                    ff_last = ff_items[-1] if ff_items else {}
                    ff_breakdown = _fund_flow_breakdown_last(ff_last)
                except Exception:
                    ff_breakdown = None

            breakdown = _compute_leader_live_score(
                market=market,
                feats=feats,
                chips_summary=chips_summary,
                ff_breakdown=ff_breakdown,
            )
            _upsert_leader_live_score(symbol=sym, live_score=float(breakdown.get("total") or 0.0), breakdown=breakdown, ts=ts)
        except Exception:
            continue


def _entry_close_for_date(symbol: str, date_str: str) -> float | None:
    """
    Prefer close on the exact date; if unavailable (e.g., intraday / holiday / data delay),
    fall back to the latest available close on or before the date.
    """
    bars = _load_cached_bars(symbol, days=180)
    if not bars:
        try:
            bars = market_stock_bars(symbol, days=180, force=True).bars
        except Exception:
            bars = []
    best_close: float | None = None
    best_date: str = ""
    for b in bars:
        d = str(b.get("date") or "")
        if not d or d > date_str:
            continue
        v = _safe_float(b.get("close"))
        if v <= 0:
            continue
        if d == date_str:
            return v
        if d > best_date:
            best_date = d
            best_close = v
    return best_close


def _bars_series_since(symbol: str, start_date: str, *, limit: int = 60) -> list[dict[str, Any]]:
    bars = _load_cached_bars(symbol, days=180)
    if not bars:
        try:
            bars = market_stock_bars(symbol, days=180).bars
        except Exception:
            bars = []
    out: list[dict[str, Any]] = []
    for b in bars:
        d = str(b.get("date") or "")
        if not d:
            continue
        if d < start_date:
            continue
        out.append({"date": d, "close": _safe_float(b.get("close"))})
    out.sort(key=lambda x: str(x.get("date") or ""))
    lim = max(1, int(limit))
    return out[-lim:]


def _get_tv_screener_row(screener_id: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, name, url, enabled
            FROM tv_screeners
            WHERE id = ?
            """,
            (screener_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": str(row[0]),
            "name": str(row[1]),
            "url": str(row[2]),
            "enabled": bool(int(row[3])),
        }


def _insert_tv_snapshot(
    *,
    screener_id: str,
    captured_at: str,
    url: str,
    screen_title: str | None,
    filters: list[str],
    headers: list[str],
    rows: list[dict[str, str]],
) -> str:
    snapshot_id = str(uuid.uuid4())
    payload = {
        "screenTitle": screen_title,
        "filters": [str(x) for x in (filters or []) if str(x).strip()],
        "url": url,
        "headers": headers,
        "rows": rows,
    }
    headers_json = json.dumps(headers, ensure_ascii=False)
    rows_json = json.dumps(payload, ensure_ascii=False)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO tv_screener_snapshots(
              id, screener_id, captured_at, row_count, headers_json, rows_json
            )
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, screener_id, captured_at, len(rows), headers_json, rows_json),
        )
        conn.commit()
    return snapshot_id


def _get_tv_snapshot(snapshot_id: str) -> TvScreenerSnapshotDetail | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, screener_id, captured_at, row_count, rows_json
            FROM tv_screener_snapshots
            WHERE id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row[4]))
        return TvScreenerSnapshotDetail(
            id=str(row[0]),
            screenerId=str(row[1]),
            capturedAt=str(row[2]),
            rowCount=int(row[3]),
            screenTitle=str(payload.get("screenTitle") or "") or None,
            filters=[str(x) for x in (payload.get("filters") or []) if str(x).strip()],
            url=str(payload.get("url") or ""),
            headers=[str(x) for x in payload.get("headers") or []],
            rows=[
                {str(k): str(v) for k, v in (r or {}).items()}
                for r in (payload.get("rows") or [])
            ],
        )


def _parse_iso_datetime(value: str) -> datetime | None:
    """
    Parse an ISO string used by our capture pipeline. Accepts 'Z' suffix.
    """
    s = (value or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _tv_local_date_and_slot(captured_at: str) -> tuple[str, str]:
    """
    Group snapshots by local date and slot (am/pm) in Asia/Shanghai.
    """
    dt = _parse_iso_datetime(captured_at)
    if dt is None:
        return _today_cn_date_str(), "unknown"
    try:
        dt2 = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    except Exception:
        dt2 = dt.astimezone(UTC)
    slot = "am" if dt2.hour < 12 else "pm"
    return dt2.date().isoformat(), slot


def _list_tv_snapshots_for_screener(
    screener_id: str,
    *,
    days: int,
) -> list[dict[str, Any]]:
    """
    DB-first: list snapshot rows for screener within last N days.
    Returns rows with minimal payload extraction (screenTitle/filters).
    """
    days2 = max(1, min(int(days), 60))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, screener_id, captured_at, row_count, rows_json
            FROM tv_screener_snapshots
            WHERE screener_id = ?
            ORDER BY captured_at DESC
            LIMIT 200
            """,
            (screener_id,),
        ).fetchall()

    out: list[dict[str, Any]] = []
    # Filter by local date window based on existing snapshots (stable for tests and UX).
    # Pick the latest N distinct local dates present in data.
    dates_desc: list[str] = []
    seen_dates: set[str] = set()
    for r in rows:
        captured_at = str(r[2])
        local_date, _slot = _tv_local_date_and_slot(captured_at)
        if not local_date:
            continue
        if local_date in seen_dates:
            continue
        seen_dates.add(local_date)
        dates_desc.append(local_date)
        if len(dates_desc) >= days2:
            break
    keep_dates: set[str] = set(dates_desc)

    for r in rows:
        sid = str(r[0])
        captured_at = str(r[2])
        local_date, _slot = _tv_local_date_and_slot(captured_at)
        if local_date not in keep_dates:
            continue
        try:
            payload = json.loads(str(r[4]) or "{}")
            screen_title = str(payload.get("screenTitle") or "") or None
            filters = payload.get("filters") or []
            filters2 = [str(x) for x in filters if str(x).strip()] if isinstance(filters, list) else []
        except Exception:
            screen_title = None
            filters2 = []
        out.append(
            {
                "snapshotId": sid,
                "screenerId": str(r[1]),
                "capturedAt": captured_at,
                "rowCount": int(r[3]),
                "screenTitle": screen_title,
                "filters": filters2,
            },
        )
    return out


@app.get(
    "/integrations/tradingview/screeners/{screener_id}/history",
    response_model=TvScreenerHistoryResponse,
)
def tv_screener_history(screener_id: str, days: int = 10) -> TvScreenerHistoryResponse:
    _seed_default_tv_screeners()
    s = _get_tv_screener_row(screener_id)
    if s is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    days2 = max(1, min(int(days), 30))

    items = _list_tv_snapshots_for_screener(screener_id, days=days2)
    # Build last N local dates based on available snapshots (DESC).
    dates_desc: list[str] = []
    seen_dates: set[str] = set()
    for it in items:
        local_date, _slot = _tv_local_date_and_slot(str(it.get("capturedAt") or ""))
        if not local_date or local_date in seen_dates:
            continue
        seen_dates.add(local_date)
        dates_desc.append(local_date)
        if len(dates_desc) >= days2:
            break
    # Fallback: still return an empty grid keyed by today if we have no snapshots.
    if not dates_desc:
        dates_desc = [_today_cn_date_str()]
    dates = list(reversed(dates_desc))
    by_date: dict[str, dict[str, TvScreenerHistoryCell]] = {d: {} for d in dates}

    for it in items:
        local_date, slot = _tv_local_date_and_slot(str(it.get("capturedAt") or ""))
        if local_date not in by_date:
            continue
        # Keep the latest snapshot per slot (items are sorted DESC by capturedAt).
        if slot not in {"am", "pm"}:
            continue
        if slot in by_date[local_date]:
            continue
        by_date[local_date][slot] = TvScreenerHistoryCell(
            snapshotId=str(it.get("snapshotId") or ""),
            capturedAt=str(it.get("capturedAt") or ""),
            rowCount=int(it.get("rowCount") or 0),
            screenTitle=str(it.get("screenTitle") or "") or None,
            filters=[str(x) for x in (it.get("filters") or []) if str(x).strip()],
        )

    rows_out: list[TvScreenerHistoryDayRow] = []
    for d in dates:
        cells = by_date.get(d) or {}
        rows_out.append(
            TvScreenerHistoryDayRow(
                date=d,
                am=cells.get("am"),
                pm=cells.get("pm"),
            ),
        )

    return TvScreenerHistoryResponse(
        screenerId=str(s["id"]),
        screenerName=str(s["name"]),
        days=days2,
        rows=rows_out,
    )

@app.post(
    "/integrations/tradingview/screeners/{screener_id}/sync",
    response_model=TvScreenerSyncResponse,
)
def sync_tv_screener(screener_id: str) -> TvScreenerSyncResponse:
    _seed_default_tv_screeners()
    screener = _get_tv_screener_row(screener_id)
    if screener is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    if not screener["enabled"]:
        raise HTTPException(status_code=409, detail="Screener is disabled")

    port = _get_tv_cdp_port()
    cdp = _cdp_version(TV_CDP_HOST, port)
    if cdp is None:
        # Auto-start a headless Chrome for silent sync. Settings is optional (for debugging).
        src_ud = (
            (get_setting("tv_bootstrap_src_user_data_dir") or "").strip()
            or os.getenv("TV_BOOTSTRAP_CHROME_USER_DATA_DIR", "").strip()
            or TV_CHROME_USER_DATA_DIR_DEFAULT
        )
        src_profile = (
            (get_setting("tv_bootstrap_src_profile_dir") or "").strip()
            or os.getenv("TV_BOOTSTRAP_PROFILE_DIR", "").strip()
            or TV_BOOTSTRAP_PROFILE_DIR_DEFAULT
        )
        # Use "Profile 1" naming by default, so bootstrap works out of the box for the user.
        desired_profile_dir = src_profile or TV_BOOTSTRAP_PROFILE_DIR_DEFAULT
        tradingview_chrome_start(
            TvChromeStartRequest(
                port=port,
                userDataDir=_get_tv_user_data_dir(),
                profileDirectory=desired_profile_dir,
                chromeBin=_get_tv_chrome_bin(),
                headless=True,
                bootstrapFromChromeUserDataDir=src_ud,
                bootstrapFromProfileDirectory=src_profile,
                forceBootstrap=False,
            ),
        )
        cdp = _cdp_version(TV_CDP_HOST, port)
        if cdp is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "CDP is not available. Auto-start failed. "
                    "Please ensure Chrome Profile 1 is logged in to TradingView, "
                    "or configure the bootstrap paths in Settings."
                ),
            )

    cdp_url = f"http://{TV_CDP_HOST}:{port}"
    try:
        result = capture_screener_over_cdp_sync(cdp_url=cdp_url, url=str(screener["url"]))
    except RuntimeError as e:
        msg = str(e)
        if "Cannot locate screener grid/table" in msg:
            raise HTTPException(status_code=409, detail=msg) from e
        raise HTTPException(status_code=500, detail=msg) from e

    snapshot_id = _insert_tv_snapshot(
        screener_id=screener_id,
        captured_at=result.captured_at,
        url=result.url,
        screen_title=result.screen_title,
        filters=result.filters,
        headers=result.headers,
        rows=result.rows,
    )
    set_setting("tv_last_sync_at", result.captured_at)
    set_setting("tv_last_sync_screener_id", screener_id)
    return TvScreenerSyncResponse(
        snapshotId=snapshot_id,
        capturedAt=result.captured_at,
        rowCount=len(result.rows),
    )


@app.get(
    "/integrations/tradingview/screeners/{screener_id}/snapshots",
    response_model=ListTvScreenerSnapshotsResponse,
)
def list_tv_screener_snapshots(
    screener_id: str,
    limit: int = 10,
) -> ListTvScreenerSnapshotsResponse:
    _seed_default_tv_screeners()
    if _get_tv_screener_row(screener_id) is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    limit2 = max(1, min(int(limit), 50))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, screener_id, captured_at, row_count
            FROM tv_screener_snapshots
            WHERE screener_id = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (screener_id, limit2),
        ).fetchall()
        items = [
            TvScreenerSnapshotSummary(
                id=str(r[0]),
                screenerId=str(r[1]),
                capturedAt=str(r[2]),
                rowCount=int(r[3]),
            )
            for r in rows
        ]
        return ListTvScreenerSnapshotsResponse(items=items)


@app.get(
    "/integrations/tradingview/snapshots/{snapshot_id}",
    response_model=TvScreenerSnapshotDetail,
)
def get_tv_screener_snapshot(snapshot_id: str) -> TvScreenerSnapshotDetail:
    snap = _get_tv_snapshot(snapshot_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="Not found")
    return snap


@app.get("/broker/pingan/snapshots", response_model=list[BrokerSnapshotSummary])
def list_pingan_broker_snapshots(limit: int = 20, accountId: str | None = None) -> list[BrokerSnapshotSummary]:
    """
    List imported Ping An Securities account screenshots.
    """
    account_id = (accountId or "").strip() or _seed_default_broker_account("pingan")
    return _list_broker_snapshots(broker="pingan", account_id=account_id, limit=limit)


@app.get("/broker/pingan/snapshots/{snapshot_id}", response_model=BrokerSnapshotDetail)
def get_pingan_broker_snapshot(snapshot_id: str) -> BrokerSnapshotDetail:
    snap = _get_broker_snapshot(snapshot_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="Not found")
    return snap


@app.get("/broker/accounts", response_model=list[BrokerAccountSummary])
def list_broker_accounts(broker: str | None = None) -> list[BrokerAccountSummary]:
    b = (broker or "").strip().lower()
    with _connect() as conn:
        if b:
            rows = conn.execute(
                """
                SELECT id, broker, title, account_masked, updated_at
                FROM broker_accounts
                WHERE broker = ?
                ORDER BY updated_at DESC
                """,
                (b,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, broker, title, account_masked, updated_at
                FROM broker_accounts
                ORDER BY updated_at DESC
                """,
            ).fetchall()
        return [
            BrokerAccountSummary(
                id=str(r[0]),
                broker=str(r[1]),
                title=str(r[2]),
                accountMasked=str(r[3]) if r[3] is not None else None,
                updatedAt=str(r[4]),
            )
            for r in rows
        ]


@app.post("/broker/accounts", response_model=BrokerAccountSummary)
def create_broker_account(req: CreateBrokerAccountRequest) -> BrokerAccountSummary:
    b = (req.broker or "").strip().lower()
    if not b:
        raise HTTPException(status_code=400, detail="broker is required")
    title = (req.title or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title is required")
    aid = str(uuid.uuid4())
    ts = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO broker_accounts(id, broker, title, account_masked, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (aid, b, title, (req.accountMasked or None), ts, ts),
        )
        conn.commit()
    return BrokerAccountSummary(
        id=aid,
        broker=b,
        title=title,
        accountMasked=req.accountMasked,
        updatedAt=ts,
    )


@app.put("/broker/accounts/{account_id}", response_model=dict[str, bool])
def update_broker_account(account_id: str, req: UpdateBrokerAccountRequest) -> dict[str, bool]:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    ts = now_iso()
    with _connect() as conn:
        row = conn.execute(
            "SELECT id FROM broker_accounts WHERE id = ?",
            (aid,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Not found")
        if req.title is not None:
            conn.execute("UPDATE broker_accounts SET title = ?, updated_at = ? WHERE id = ?", (req.title, ts, aid))
        if req.accountMasked is not None:
            conn.execute(
                "UPDATE broker_accounts SET account_masked = ?, updated_at = ? WHERE id = ?",
                (req.accountMasked, ts, aid),
            )
        conn.commit()
    return {"ok": True}


@app.delete("/broker/accounts/{account_id}", response_model=dict[str, bool])
def delete_broker_account(account_id: str) -> dict[str, bool]:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    with _connect() as conn:
        row = conn.execute("SELECT id FROM broker_accounts WHERE id = ?", (aid,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Not found")
        conn.execute("DELETE FROM broker_accounts WHERE id = ?", (aid,))
        conn.commit()
    return {"ok": True}


@app.get("/broker/pingan/snapshots/{snapshot_id}/image")
def get_pingan_broker_snapshot_image(snapshot_id: str) -> Response:
    snap = _get_broker_snapshot(snapshot_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="Not found")
    p = Path(snap.imagePath)
    if not p.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    ext = p.suffix.lower()
    media = "image/png"
    if ext in {".jpg", ".jpeg"}:
        media = "image/jpeg"
    elif ext == ".webp":
        media = "image/webp"
    return Response(content=p.read_bytes(), media_type=media)


@app.post("/broker/pingan/import", response_model=BrokerImportResponse)
def import_pingan_broker_screenshots(req: BrokerImportRequest) -> BrokerImportResponse:
    """
    Import one or more Ping An Securities screenshots.
    - Store the original image on disk
    - Run AI extraction (vision) to classify + parse content
    - Persist the extracted JSON into SQLite
    """
    captured_at = (req.capturedAt or now_iso()).strip() or now_iso()
    account_id = (req.accountId or "").strip() or _seed_default_broker_account("pingan")
    out: list[BrokerSnapshotSummary] = []

    for img in req.images:
        media_type, raw = _parse_data_url(img.dataUrl)
        sha = _sha256_hex(raw)

        # Dedupe first (by sha256) before writing duplicates to disk.
        with _connect() as conn:
            existing = conn.execute(
                """
                SELECT id, broker, account_id, captured_at, kind, created_at
                FROM broker_snapshots
                WHERE broker = ? AND account_id IS ? AND sha256 = ?
                """,
                ("pingan", account_id, sha),
            ).fetchone()
            if existing is not None:
                out.append(
                    BrokerSnapshotSummary(
                        id=str(existing[0]),
                        broker=str(existing[1]),
                        accountId=str(existing[2]) if existing[2] is not None else None,
                        capturedAt=str(existing[3]),
                        kind=str(existing[4]),
                        createdAt=str(existing[5]),
                    ),
                )
                continue

        image_path = _write_broker_image(broker="pingan", raw=raw, media_type=media_type)
        extracted = _ai_extract_pingan_screenshot(image_data_url=img.dataUrl)
        # Attach minimal metadata for debugging and UI display.
        if isinstance(extracted, dict):
            meta = extracted.get("__meta")
            meta2 = meta if isinstance(meta, dict) else {}
            meta2.update({"originalName": img.name, "mediaType": media_type})
            extracted["__meta"] = meta2
        kind = str((extracted or {}).get("kind") or "unknown")
        snapshot_id = _insert_broker_snapshot(
            broker="pingan",
            account_id=account_id,
            captured_at=captured_at,
            kind=kind,
            sha256=sha,
            image_path=image_path,
            extracted=extracted if isinstance(extracted, dict) else {"raw": extracted},
        )
        snap = _get_broker_snapshot(snapshot_id)
        if snap:
            out.append(
                BrokerSnapshotSummary(
                    id=snap.id,
                    broker=snap.broker,
                    accountId=snap.accountId,
                    capturedAt=snap.capturedAt,
                    kind=snap.kind,
                    createdAt=snap.createdAt,
                ),
            )

    return BrokerImportResponse(ok=True, items=out)


@app.get("/broker/pingan/accounts/{account_id}/state", response_model=BrokerAccountStateResponse)
def get_pingan_account_state(account_id: str) -> BrokerAccountStateResponse:
    """
    Get consolidated account state (overview/positions/conditional_orders/trades).
    This is the primary API for the UI and agent references.
    """
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    return _account_state_response(aid)


@app.post("/broker/pingan/accounts/{account_id}/sync", response_model=BrokerAccountStateResponse)
def sync_pingan_account_from_screenshots(
    account_id: str,
    req: BrokerSyncRequest,
) -> BrokerAccountStateResponse:
    """
    Sync the account state by analyzing screenshots. This does NOT persist per-import records
    (screenshots), it only updates the consolidated state and its updatedAt timestamp.
    """
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    captured_at = (req.capturedAt or now_iso()).strip() or now_iso()

    overview: dict[str, Any] | None = None

    saw_positions = False
    saw_orders = False
    saw_trades = False
    positions_acc: list[dict[str, Any]] = []
    orders_acc: list[dict[str, Any]] = []
    trades_acc: list[dict[str, Any]] = []

    def _dedupe(rows: list[dict[str, Any]], *, keys: list[str]) -> list[dict[str, Any]]:
        """
        Dedupe rows across multiple screenshots. We keep order and prefer earlier rows.
        """
        out_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for r in rows:
            base = {k: _norm_str(r.get(k)) for k in keys if k in r and _norm_str(r.get(k))}
            if not base:
                base = r
            sig = json.dumps(base, ensure_ascii=False, sort_keys=True)
            if sig in seen:
                continue
            seen.add(sig)
            out_rows.append(r)
        return out_rows

    for img in req.images:
        # We intentionally do NOT write images to disk in the state-first design.
        extracted = _ai_extract_pingan_screenshot(image_data_url=img.dataUrl)
        if not isinstance(extracted, dict):
            continue
        kind = str(extracted.get("kind") or "unknown")
        data = extracted.get("data")
        data2 = data if isinstance(data, dict) else {}

        # NOTE: A single screenshot can contain multiple sections (overview + positions, etc).
        # We therefore extract by data keys as well, not only by the classifier kind.
        if kind == "account_overview":
            overview = data2
        elif kind == "positions" and overview is None and any(
            k in data2 for k in ("totalAssets", "securitiesValue", "cashAvailable", "withdrawable")
        ):
            # Some models return kind=positions but include overview numbers; keep them if present.
            overview = data2

        ps = data2.get("positions")
        if isinstance(ps, list):
            saw_positions = True
            positions_acc.extend([p if isinstance(p, dict) else {"raw": p} for p in ps])

        os_ = data2.get("orders")
        if isinstance(os_, list):
            saw_orders = True
            orders_acc.extend([o if isinstance(o, dict) else {"raw": o} for o in os_])

        ts = data2.get("trades")
        if isinstance(ts, list):
            saw_trades = True
            trades_acc.extend([t if isinstance(t, dict) else {"raw": t} for t in ts])

    positions_out: list[dict[str, Any]] | None = None
    orders_out: list[dict[str, Any]] | None = None
    trades_out: list[dict[str, Any]] | None = None
    if saw_positions and positions_acc:
        positions_out = _dedupe(positions_acc, keys=["ticker", "Ticker", "symbol", "Symbol", "name", "Name"])
    if saw_orders and orders_acc:
        orders_out = _dedupe(
            orders_acc,
            keys=[
                "ticker",
                "Ticker",
                "symbol",
                "Symbol",
                "name",
                "Name",
                "side",
                "Side",
                "triggerCondition",
                "triggerValue",
                "qty",
                "quantity",
                "status",
                "validUntil",
            ],
        )
    if saw_trades and trades_acc:
        # User expectation: dedupe by time + ticker (code). Keep other fields as-is.
        trades_out = _dedupe(trades_acc, keys=["ticker", "Ticker", "symbol", "Symbol", "time", "date"])

    _upsert_account_state(
        account_id=aid,
        broker="pingan",
        updated_at=captured_at,
        overview=overview,
        positions=positions_out,
        conditional_orders=orders_out,
        trades=trades_out,
    )
    return _account_state_response(aid)


def _norm_str(v: Any) -> str:
    s = "" if v is None else str(v)
    s2 = re.sub(r"\s+", " ", s).strip()
    return s2


def _pick_first_str(obj: dict[str, Any], keys: list[str]) -> str:
    for k in keys:
        if k in obj:
            v = _norm_str(obj.get(k))
            if v:
                return v
    return ""


def _conditional_order_key(order: dict[str, Any]) -> str:
    """
    Build a stable signature for a conditional order row across OCR/model variants.
    """
    payload = {
        "ticker": _pick_first_str(order, ["ticker", "Ticker", "symbol", "Symbol", "代码"]),
        "side": _pick_first_str(order, ["side", "Side", "方向"]).lower(),
        "triggerCondition": _pick_first_str(order, ["triggerCondition", "condition", "触发条件"]),
        "triggerValue": _pick_first_str(order, ["triggerValue", "value", "触发价"]),
        "qty": _pick_first_str(order, ["qty", "quantity", "委托数量", "数量"]),
        "status": _pick_first_str(order, ["status", "Status", "状态"]),
        "validUntil": _pick_first_str(order, ["validUntil", "有效期"]),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@app.post(
    "/broker/pingan/accounts/{account_id}/state/conditional-orders/delete",
    response_model=BrokerAccountStateResponse,
)
def delete_pingan_account_conditional_order(
    account_id: str,
    req: DeleteBrokerConditionalOrderRequest,
) -> BrokerAccountStateResponse:
    """
    Manual adjustment: delete one conditional order row from the consolidated state.
    """
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    if not isinstance(req.order, dict) or not req.order:
        raise HTTPException(status_code=400, detail="order is required")

    row = _get_account_state_row(aid)
    if row is None:
        raise HTTPException(status_code=404, detail="Account state not found")

    target_key = _conditional_order_key(req.order)
    if not target_key or target_key == "{}":
        raise HTTPException(status_code=400, detail="order is invalid")

    raw_orders = row.get("conditionalOrders")
    orders: list[Any] = raw_orders if isinstance(raw_orders, list) else []

    kept: list[dict[str, Any]] = []
    removed = 0
    for o in orders:
        if isinstance(o, dict) and _conditional_order_key(o) == target_key:
            removed += 1
            continue
        kept.append(o if isinstance(o, dict) else {"raw": o})

    if removed == 0:
        raise HTTPException(status_code=404, detail="Conditional order not found")

    # Persist new state with fresh timestamp (manual edit).
    raw_positions = row.get("positions")
    positions: list[Any] = raw_positions if isinstance(raw_positions, list) else []
    raw_trades = row.get("trades")
    trades: list[Any] = raw_trades if isinstance(raw_trades, list) else []
    overview = row.get("overview") if isinstance(row.get("overview"), dict) else {}

    _upsert_account_state(
        account_id=aid,
        broker="pingan",
        updated_at=now_iso(),
        overview=overview,
        positions=[x if isinstance(x, dict) else {"raw": x} for x in positions],
        conditional_orders=kept,
        trades=[x if isinstance(x, dict) else {"raw": x} for x in trades],
    )
    return _account_state_response(aid)


def _today_cn_date_str() -> str:
    """
    Trading strategy is day-based and for CN/HK markets; use Asia/Shanghai calendar date.
    """
    try:
        tz = ZoneInfo("Asia/Shanghai")
        return datetime.now(tz=tz).date().isoformat()
    except Exception:
        return datetime.now(tz=UTC).date().isoformat()


def _get_broker_account_row(account_id: str) -> dict[str, str] | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, broker, title, account_masked, updated_at FROM broker_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": str(row[0]),
            "broker": str(row[1]),
            "title": str(row[2]),
            "accountMasked": str(row[3]) if row[3] is not None else "",
            "updatedAt": str(row[4]),
        }


def _get_strategy_prompt(account_id: str) -> tuple[str, str | None]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT strategy_prompt, updated_at FROM broker_account_prompts WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        if row is None:
            return "", None
        return str(row[0] or ""), str(row[1] or "") or None


def _set_strategy_prompt(account_id: str, prompt: str) -> StrategyAccountPromptResponse:
    ts = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO broker_account_prompts(account_id, strategy_prompt, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
              strategy_prompt = excluded.strategy_prompt,
              updated_at = excluded.updated_at
            """,
            (account_id, prompt, ts),
        )
        conn.commit()
    return StrategyAccountPromptResponse(accountId=account_id, prompt=prompt, updatedAt=ts)


@app.get("/strategy/accounts/{account_id}/prompt", response_model=StrategyAccountPromptResponse)
def get_strategy_account_prompt(account_id: str) -> StrategyAccountPromptResponse:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    if _get_broker_account_row(aid) is None:
        raise HTTPException(status_code=404, detail="Account not found")
    prompt, updated_at = _get_strategy_prompt(aid)
    return StrategyAccountPromptResponse(accountId=aid, prompt=prompt, updatedAt=updated_at)


@app.put("/strategy/accounts/{account_id}/prompt", response_model=StrategyAccountPromptResponse)
def put_strategy_account_prompt(
    account_id: str,
    req: StrategyAccountPromptRequest,
) -> StrategyAccountPromptResponse:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    if _get_broker_account_row(aid) is None:
        raise HTTPException(status_code=404, detail="Account not found")
    prompt = (req.prompt or "").strip()
    return _set_strategy_prompt(aid, prompt)


def _get_strategy_report_row(account_id: str, date: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, account_id, date, created_at, model, input_snapshot_json, output_json
            FROM strategy_reports
            WHERE account_id = ? AND date = ?
            """,
            (account_id, date),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": str(row[0]),
            "accountId": str(row[1]),
            "date": str(row[2]),
            "createdAt": str(row[3]),
            "model": str(row[4]),
            "inputSnapshot": json.loads(str(row[5]) or "{}"),
            "output": json.loads(str(row[6]) or "{}"),
        }


def _store_strategy_report(
    *,
    report_id: str,
    account_id: str,
    date: str,
    created_at: str,
    model: str,
    input_snapshot: dict[str, Any],
    output: dict[str, Any],
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO strategy_reports(id, account_id, date, created_at, model, input_snapshot_json, output_json)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, date) DO UPDATE SET
              id = excluded.id,
              created_at = excluded.created_at,
              model = excluded.model,
              input_snapshot_json = excluded.input_snapshot_json,
              output_json = excluded.output_json
            """,
            (
                report_id,
                account_id,
                date,
                created_at,
                model,
                json.dumps(input_snapshot, ensure_ascii=False),
                json.dumps(output, ensure_ascii=False),
            ),
        )
        conn.commit()


def _list_strategy_reports(*, account_id: str, days: int = 10) -> list[dict[str, Any]]:
    days2 = max(1, min(int(days), 60))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, date, created_at, model, output_json
            FROM strategy_reports
            WHERE account_id = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (account_id, days2),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            output = json.loads(str(r[4]) or "{}")
        except Exception:
            output = {}
        has_md = isinstance(output, dict) and bool(_norm_str(output.get("markdown") or "").strip())
        out.append({"id": str(r[0]), "date": str(r[1]), "createdAt": str(r[2]), "model": str(r[3]), "hasMarkdown": has_md})
    return out


def _prune_strategy_reports_keep_last_n_days(*, account_id: str, keep_days: int = 10) -> None:
    keep = max(1, min(int(keep_days), 60))
    with _connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT date FROM strategy_reports WHERE account_id = ? ORDER BY date DESC",
            (account_id,),
        ).fetchall()
        dates = [str(r[0]) for r in rows if r and r[0]]
        to_delete = dates[keep:]
        for d in to_delete:
            conn.execute(
                "DELETE FROM strategy_reports WHERE account_id = ? AND date = ?",
                (account_id, d),
            )
        conn.commit()


def _safe_float(v: Any) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return 0


def _strategy_report_response(
    *,
    report_id: str,
    date: str,
    account_id: str,
    account_title: str,
    created_at: str,
    model: str,
    output: dict[str, Any],
    input_snapshot: dict[str, Any] | None,
) -> StrategyReportResponse:
    markdown = _norm_str(output.get("markdown") or "") or None

    # Candidates
    raw_candidates = output.get("candidates")
    candidates_in: list[Any] = raw_candidates if isinstance(raw_candidates, list) else []
    candidates: list[StrategyCandidate] = []
    for i, c in enumerate(candidates_in[:5]):
        if not isinstance(c, dict):
            continue
        candidates.append(
            StrategyCandidate(
                symbol=_norm_str(c.get("symbol") or ""),
                market=_norm_str(c.get("market") or ""),
                ticker=_norm_str(c.get("ticker") or ""),
                name=_norm_str(c.get("name") or ""),
                score=_safe_float(c.get("score")),
                rank=_safe_int(c.get("rank")) or (i + 1),
                why=_norm_str(c.get("why") or ""),
            ),
        )

    # Leader
    leader_obj = output.get("leader") if isinstance(output.get("leader"), dict) else {}
    leader = StrategyLeader(
        symbol=_norm_str((leader_obj or {}).get("symbol") or ""),
        reason=_norm_str((leader_obj or {}).get("reason") or ""),
    )

    # Recommendations
    raw_recs = output.get("recommendations")
    recs_in: list[Any] = raw_recs if isinstance(raw_recs, list) else []
    recs: list[StrategyRecommendation] = []
    for r in recs_in[:3]:
        if not isinstance(r, dict):
            continue
        raw_levels = r.get("levels")
        levels_obj: dict[str, Any] = raw_levels if isinstance(raw_levels, dict) else {}
        raw_support = levels_obj.get("support")
        support_in: list[Any] = raw_support if isinstance(raw_support, list) else []
        raw_res = levels_obj.get("resistance")
        res_in: list[Any] = raw_res if isinstance(raw_res, list) else []
        raw_inv = levels_obj.get("invalidations")
        inv_in: list[Any] = raw_inv if isinstance(raw_inv, list) else []
        levels = StrategyLevels(
            support=[_norm_str(x) for x in support_in if _norm_str(x)],
            resistance=[_norm_str(x) for x in res_in if _norm_str(x)],
            invalidations=[_norm_str(x) for x in inv_in if _norm_str(x)],
        )
        raw_orders2 = r.get("orders")
        orders_in: list[Any] = raw_orders2 if isinstance(raw_orders2, list) else []
        orders: list[StrategyOrder] = []
        for o in orders_in:
            if not isinstance(o, dict):
                continue
            orders.append(
                StrategyOrder(
                    kind=_norm_str(o.get("kind") or ""),
                    side=_norm_str(o.get("side") or ""),
                    trigger=_norm_str(o.get("trigger") or ""),
                    qty=_norm_str(o.get("qty") or ""),
                    timeInForce=_norm_str(o.get("timeInForce") or "") or None,
                    notes=_norm_str(o.get("notes") or "") or None,
                ),
            )
        risk_notes = r.get("riskNotes")
        rn_in: list[Any] = risk_notes if isinstance(risk_notes, list) else []
        recs.append(
            StrategyRecommendation(
                symbol=_norm_str(r.get("symbol") or ""),
                ticker=_norm_str(r.get("ticker") or ""),
                name=_norm_str(r.get("name") or ""),
                thesis=_norm_str(r.get("thesis") or ""),
                levels=levels,
                orders=orders,
                positionSizing=_norm_str(r.get("positionSizing") or ""),
                riskNotes=[_norm_str(x) for x in rn_in if _norm_str(x)],
            ),
        )

    # Risk notes
    risk = output.get("riskNotes")
    risk_in: list[Any] = risk if isinstance(risk, list) else []
    risk_notes_out = [_norm_str(x) for x in risk_in if _norm_str(x)]

    return StrategyReportResponse(
        id=report_id,
        date=date,
        accountId=account_id,
        accountTitle=account_title,
        createdAt=created_at,
        model=model,
        markdown=markdown,
        candidates=candidates,
        leader=leader,
        recommendations=recs,
        riskNotes=risk_notes_out,
        inputSnapshot=input_snapshot,
        raw=output,
    )


def _latest_tv_snapshot_for_screener(screener_id: str) -> TvScreenerSnapshotDetail | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id
            FROM tv_screener_snapshots
            WHERE screener_id = ?
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            (screener_id,),
        ).fetchone()
        if row is None:
            return None
        return _get_tv_snapshot(str(row[0]))


def _list_enabled_tv_screeners(*, limit: int = 6) -> list[dict[str, Any]]:
    _seed_default_tv_screeners()
    limit2 = max(1, min(int(limit), 20))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, name, url, enabled, updated_at
            FROM tv_screeners
            WHERE enabled = 1
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit2,),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": str(r[0]),
                    "name": str(r[1]),
                    "url": str(r[2]),
                    "enabled": bool(r[3]),
                    "updatedAt": str(r[4]),
                }
            )
        return out


def _tv_latest_snapshot_meta(screener_id: str) -> dict[str, Any] | None:
    snap = _latest_tv_snapshot_for_screener(screener_id)
    if snap is None:
        return None
    return {
        "snapshotId": snap.id,
        "capturedAt": snap.capturedAt,
        "rowCount": snap.rowCount,
        "filtersCount": len(snap.filters or []),
        "filters": snap.filters or [],
    }


def _pick_tv_columns(headers: list[str]) -> list[str]:
    preferred = [
        "Ticker",
        "Name",
        "Symbol",
        "Price",
        "Change %",
        "Rel Volume",
        "Rel Volume 1W",
        "Market cap",
        "Sector",
        "Analyst Rating",
        "RSI (14)",
    ]
    seth = set(headers)
    picked = [h for h in preferred if h in seth]
    rest = [h for h in headers if h not in picked]
    return (picked + rest)[:8]


def _tv_snapshot_brief(snapshot_id: str, *, max_rows: int = 20) -> dict[str, Any]:
    snap = _get_tv_snapshot(snapshot_id)
    if snap is None:
        return {"snapshotId": snapshot_id, "status": "not_found"}
    cols = _pick_tv_columns(snap.headers)
    rows = snap.rows[: max(0, int(max_rows))]
    # Project only selected columns to reduce token usage.
    out_rows: list[dict[str, str]] = []
    for r in rows:
        rr: dict[str, str] = {}
        for c in cols:
            rr[c] = str(r.get(c) or "").replace("\n", " ").strip()
        out_rows.append(rr)
    return {
        "snapshotId": snap.id,
        "screenerId": snap.screenerId,
        "capturedAt": snap.capturedAt,
        "screenTitle": snap.screenTitle,
        "filters": snap.filters,
        "url": snap.url,
        "columns": cols,
        "rows": out_rows,
        "rowCount": snap.rowCount,
    }


def _infer_market_and_currency_from_tv_row(row: dict[str, Any]) -> tuple[str, str]:
    price = _norm_str(row.get("Price") or row.get("price") or "")
    if "HKD" in price:
        return "HK", "HKD"
    if "CNY" in price:
        return "CN", "CNY"
    ticker = _norm_str(row.get("Ticker") or "")
    # CN A-share tickers are 6 digits. Treat shorter numeric tickers as HK by default.
    if ticker.isdigit() and 0 < len(ticker) < 6:
        return "HK", "HKD"
    return "CN", "CNY"


def _extract_tv_candidates(snap: TvScreenerSnapshotDetail) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for r in snap.rows:
        # IMPORTANT: Keep original newlines in Symbol cell, otherwise we can't split ticker/name.
        sym_cell = str(r.get("Symbol") or r.get("Ticker") or r.get("代码") or "").strip()
        if not sym_cell:
            continue
        parts = split_symbol_cell(sym_cell)
        ticker = _norm_str(parts.get("Ticker") or "") or _norm_str(r.get("Ticker") or "")
        name = _norm_str(parts.get("Name") or "") or _norm_str(r.get("Name") or "")
        if not ticker:
            continue
        market, currency = _infer_market_and_currency_from_tv_row({**r, **parts})
        out.append(
            {
                "market": market,
                "currency": currency,
                "ticker": ticker,
                "name": name,
                "symbol": f"{market}:{ticker}",
            },
        )
    return out


def _ensure_market_stock_basic(
    *,
    symbol: str,
    market: str,
    ticker: str,
    name: str,
    currency: str,
) -> None:
    with _connect() as conn:
        row = conn.execute("SELECT symbol FROM market_stocks WHERE symbol = ?", (symbol,)).fetchone()
        if row is not None:
            return
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO market_stocks(symbol, market, ticker, name, currency, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (symbol, market, ticker, name or ticker, currency, ts),
        )
        conn.commit()


def _load_cached_bars(symbol: str, *, days: int) -> list[dict[str, str]]:
    """
    DB-first: load cached bars from SQLite. Returns bars in chronological order.
    """
    sym = symbol.strip()
    days2 = max(1, min(int(days), 200))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM market_bars
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()
    out = [
        {
            "date": str(r[0]),
            "open": str(r[1] or ""),
            "high": str(r[2] or ""),
            "low": str(r[3] or ""),
            "close": str(r[4] or ""),
            "volume": str(r[5] or ""),
            "amount": str(r[6] or ""),
        }
        for r in reversed(rows)
    ]
    return out


def _load_cached_chips(symbol: str, *, days: int) -> list[dict[str, str]]:
    """
    DB-first: load cached chip distribution rows from SQLite.
    """
    sym = symbol.strip()
    days2 = max(1, min(int(days), 200))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT raw_json
            FROM market_chips
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()
    out: list[dict[str, str]] = []
    for r in reversed(rows):
        try:
            obj = json.loads(str(r[0]) or "{}")
            if isinstance(obj, dict):
                out.append({str(k): str(v) for k, v in obj.items()})
        except Exception:
            continue
    return out


def _load_cached_fund_flow(symbol: str, *, days: int) -> list[dict[str, str]]:
    """
    DB-first: load cached fund flow distribution rows from SQLite.
    """
    sym = symbol.strip()
    days2 = max(1, min(int(days), 200))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT raw_json
            FROM market_fund_flow
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()
    out: list[dict[str, str]] = []
    for r in reversed(rows):
        try:
            obj = json.loads(str(r[0]) or "{}")
            if isinstance(obj, dict):
                out.append({str(k): str(v) for k, v in obj.items()})
        except Exception:
            continue
    return out


def _bars_features(bars: list[dict[str, str]]) -> dict[str, Any]:
    closes = []
    highs = []
    lows = []
    volumes = []
    for b in bars:
        closes.append(_safe_float(b.get("close")))
        highs.append(_safe_float(b.get("high")))
        lows.append(_safe_float(b.get("low")))
        volumes.append(_safe_float(b.get("volume")))
    last_close = closes[-1] if closes else 0.0

    def sma(xs: list[float], n: int) -> float:
        if len(xs) < n or n <= 0:
            return 0.0
        return sum(xs[-n:]) / float(n)

    return {
        "lastClose": last_close,
        "sma5": sma(closes, 5),
        "sma10": sma(closes, 10),
        "sma20": sma(closes, 20),
        "high10": max(highs[-10:]) if len(highs) >= 10 else (max(highs) if highs else 0.0),
        "low10": min(lows[-10:]) if len(lows) >= 10 else (min(lows) if lows else 0.0),
        "volSma10": sma(volumes, 10),
    }


def _rank_bars_metrics(bars: list[dict[str, str]]) -> dict[str, Any]:
    feats = _bars_features(bars)
    highs = [_safe_float(b.get("high")) for b in bars]
    closes = [_safe_float(b.get("close")) for b in bars]
    volumes = [_safe_float(b.get("volume")) for b in bars]
    amounts = [_safe_float(b.get("amount")) for b in bars]
    last_vol = volumes[-1] if volumes else 0.0
    last_amt = amounts[-1] if amounts else 0.0
    high20 = max(highs[-20:]) if len(highs) >= 20 else (max(highs) if highs else 0.0)
    low20 = min(highs[-20:]) if len(highs) >= 20 else (min(highs) if highs else 0.0)

    def sma(xs: list[float], n: int) -> float:
        if len(xs) < n or n <= 0:
            return 0.0
        return sum(xs[-n:]) / float(n)

    return {
        **feats,
        "lastVolume": last_vol,
        "lastAmount": last_amt,
        "high20": high20,
        "low20": low20,
        "volSma20": sma(volumes, 20),
        "close20": closes[-20:] if len(closes) >= 20 else closes,
    }


def _chips_summary_last(x: Any) -> dict[str, Any]:
    d = x if isinstance(x, dict) else {}
    return {
        "date": _norm_str(d.get("date") or ""),
        "profitRatio": d.get("profitRatio"),
        "avgCost": d.get("avgCost"),
        "cost90Low": d.get("cost90Low"),
        "cost90High": d.get("cost90High"),
        "cost90Conc": d.get("cost90Conc"),
        "cost70Low": d.get("cost70Low"),
        "cost70High": d.get("cost70High"),
        "cost70Conc": d.get("cost70Conc"),
    }


def _fund_flow_breakdown_last(x: Any) -> dict[str, Any]:
    d = x if isinstance(x, dict) else {}
    return {
        "date": _norm_str(d.get("date") or ""),
        "close": d.get("close"),
        "changePct": d.get("changePct"),
        # Bucket breakdown (small/medium/large/super/main) as requested.
        "mainNetAmount": d.get("mainNetAmount"),
        "mainNetRatio": d.get("mainNetRatio"),
        "superNetAmount": d.get("superNetAmount"),
        "superNetRatio": d.get("superNetRatio"),
        "largeNetAmount": d.get("largeNetAmount"),
        "largeNetRatio": d.get("largeNetRatio"),
        "mediumNetAmount": d.get("mediumNetAmount"),
        "mediumNetRatio": d.get("mediumNetRatio"),
        "smallNetAmount": d.get("smallNetAmount"),
        "smallNetRatio": d.get("smallNetRatio"),
    }


def _rank_prob_band(score: float) -> str:
    s = float(score or 0.0)
    if s >= 80.0:
        return "High"
    if s >= 65.0:
        return "Medium"
    return "Low"


def _rank_is_bad_cn_name(name: str) -> bool:
    raw = name or ""
    n = raw.upper()
    # Common CN A-share flags
    if n.startswith("*ST"):
        return True
    # "STxxxx" is common, but avoid false positives for English names like "StrongOne".
    if n.startswith("ST") and len(raw) >= 3:
        c3 = raw[2]
        if c3.isascii() and c3.isalpha():
            return False
        return True
    if " ST" in n:
        return True
    if ("退市" in raw) or ("退" in raw):
        return True
    return False


def _rank_extract_tv_pool(*, max_screeners: int = 20, max_rows: int = 120) -> list[dict[str, Any]]:
    """
    Build candidate pool from latest enabled TradingView snapshots.
    Includes best-effort 'sector' field if present in snapshot rows.
    """
    snaps: list[TvScreenerSnapshotDetail] = []
    for sc in _list_enabled_tv_screeners(limit=max_screeners):
        sid = _norm_str(sc.get("id") or "")
        if not sid:
            continue
        s = _latest_tv_snapshot_for_screener(sid)
        if s is not None:
            snaps.append(s)

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for snap in snaps:
        for r in snap.rows:
            sym_cell = str(r.get("Symbol") or r.get("Ticker") or r.get("代码") or "").strip()
            if not sym_cell:
                continue
            parts = split_symbol_cell(sym_cell)
            ticker = _norm_str(parts.get("Ticker") or "") or _norm_str(r.get("Ticker") or "")
            name = _norm_str(parts.get("Name") or "") or _norm_str(r.get("Name") or "")
            if not ticker:
                continue
            market, currency = _infer_market_and_currency_from_tv_row({**r, **parts})
            sym = f"{market}:{ticker}"
            if sym in seen:
                continue
            seen.add(sym)
            sector = _norm_str(r.get("Sector") or r.get("Industry") or r.get("行业") or r.get("板块") or "")
            out.append(
                {
                    "symbol": sym,
                    "market": market,
                    "currency": currency,
                    "ticker": ticker,
                    "name": name,
                    "sector": sector or None,
                    "isHolding": False,
                }
            )
            if len(out) >= max(1, min(int(max_rows), 500)):
                break
        if len(out) >= max(1, min(int(max_rows), 500)):
            break
    return out


def _rank_extract_holdings_pool(account_id: str) -> list[dict[str, Any]]:
    row = _get_account_state_row(account_id) or {}
    raw_positions = row.get("positions")
    pos_list: list[Any] = raw_positions if isinstance(raw_positions, list) else []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for p in pos_list:
        if not isinstance(p, dict):
            continue
        ticker = _norm_str(
            p.get("ticker")
            or p.get("Ticker")
            or p.get("symbol")
            or p.get("Symbol")
            or p.get("code")
            or p.get("证券代码")
            or p.get("股票代码")
            or ""
        )
        if not ticker:
            continue
        name = _norm_str(p.get("name") or p.get("Name") or "")
        market = "HK" if len(ticker) in (4, 5) else "CN"
        currency = "HKD" if market == "HK" else "CNY"
        sym = f"{market}:{ticker}"
        if sym in seen:
            continue
        seen.add(sym)
        out.append(
            {
                "symbol": sym,
                "market": market,
                "currency": currency,
                "ticker": ticker,
                "name": name,
                "sector": None,
                "isHolding": True,
            }
        )
    return out


def _rank_build_and_score(
    *,
    account_id: str,
    as_of_date: str,
    limit: int,
    universe_version: str,
    include_holdings: bool,
) -> dict[str, Any]:
    """
    Rank CN candidates for next 1-2 days using deterministic factors, DB-first only.
    No external sync is triggered here (expects Dashboard Sync all / manual sync to refresh caches).
    """
    # Risk context (latest 5D).
    risk_mode: str | None = None
    failed_rate = 0.0
    premium = 0.0
    try:
        items = _list_cn_sentiment_days(as_of_date=as_of_date, days=5)
        latest = items[-1] if items else {}
        risk_mode = _norm_str(latest.get("riskMode") or "") or None
        failed_rate = _finite_float(latest.get("failedLimitUpRate"), 0.0)
        premium = _finite_float(latest.get("yesterdayLimitUpPremium"), 0.0)
    except Exception:
        risk_mode = None

    risk_penalty = 0.0
    if risk_mode == "no_new_positions":
        risk_penalty -= 0.25
    elif risk_mode == "caution":
        risk_penalty -= 0.10
    if failed_rate > 50.0:
        risk_penalty -= 0.05
    if premium < 0.0:
        risk_penalty -= 0.05

    # Industry flow (names only) as a weak prior.
    hot_set: set[str] = set()
    try:
        mat = _market_cn_industry_fund_flow_top_by_date(as_of_date=as_of_date, days=10, top_k=5)
        top_by_date = mat.get("topByDate") if isinstance(mat, dict) else None
        if isinstance(top_by_date, list) and top_by_date:
            latest = top_by_date[-1] if isinstance(top_by_date[-1], dict) else {}
            tops = latest.get("topIndustries") if isinstance(latest, dict) else None
            if isinstance(tops, list):
                hot_set = {str(x) for x in tops if str(x).strip()}
    except Exception:
        hot_set = set()

    tv_pool = _rank_extract_tv_pool(max_screeners=20, max_rows=160)
    holdings_pool = _rank_extract_holdings_pool(account_id) if include_holdings else []
    # Merge: TV first, then holdings (ensure included).
    pool: list[dict[str, Any]] = []
    seen: set[str] = set()
    for it in tv_pool + holdings_pool:
        sym = _norm_str(it.get("symbol") or "")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        pool.append(it)

    weights = {
        "trend": 0.30,
        "breakout": 0.15,
        "volume": 0.10,
        "flow": 0.20,
        "chips": 0.10,
        "sectorHot": 0.15,
    }

    def clamp01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    scored: list[dict[str, Any]] = []
    dropped = {"badName": 0, "noBars": 0, "lowLiquidity": 0, "notMomentum": 0}
    for it in pool:
        sym = str(it.get("symbol") or "")
        market = str(it.get("market") or "CN")
        if market != "CN":
            continue
        ticker = str(it.get("ticker") or sym.split(":")[-1])
        name = str(it.get("name") or ticker)
        sector = it.get("sector") if isinstance(it.get("sector"), str) else None
        is_holding = bool(it.get("isHolding"))

        if (not is_holding) and _rank_is_bad_cn_name(name):
            dropped["badName"] += 1
            continue

        bars = _load_cached_bars(sym, days=60)
        if len(bars) < 15:
            if not is_holding:
                dropped["noBars"] += 1
                continue
        m = _rank_bars_metrics(bars)
        last_close = _finite_float(m.get("lastClose"), 0.0)
        sma5 = _finite_float(m.get("sma5"), 0.0)
        sma10 = _finite_float(m.get("sma10"), 0.0)
        sma20 = _finite_float(m.get("sma20"), 0.0)
        high20 = _finite_float(m.get("high20"), 0.0)
        vol_sma10 = _finite_float(m.get("volSma10"), 0.0)
        last_vol = _finite_float(m.get("lastVolume"), 0.0)
        last_amt = _finite_float(m.get("lastAmount"), 0.0)

        # Liquidity filter: amount >= 1e8 CNY (best-effort; holdings bypass).
        if (not is_holding) and last_amt > 0 and last_amt < 1e8:
            dropped["lowLiquidity"] += 1
            continue

        # Trend score.
        trend = 0.0
        if sma20 > 0 and last_close > sma20:
            trend += 0.45
        if sma5 > 0 and sma10 > 0 and sma20 > 0 and (sma5 >= sma10 >= sma20):
            trend += 0.45
        if sma5 > 0 and last_close >= sma5:
            trend += 0.10
        trend = clamp01(trend)

        # Breakout proximity: within 3% of 20D high.
        breakout = 0.0
        if high20 > 0 and last_close > 0:
            dist = (high20 - last_close) / high20
            breakout = clamp01(1.0 - dist / 0.03)

        # Volume expansion.
        rel_vol = (last_vol / vol_sma10) if (last_vol > 0 and vol_sma10 > 0) else 0.0
        volume = clamp01(rel_vol / 2.0)  # 2x volSma10 -> 1.0

        # Momentum filter (right-side): require trend + breakout + volume. Holdings bypass.
        if (not is_holding) and (trend < 0.55 or breakout < 0.20 or volume < 0.25):
            dropped["notMomentum"] += 1
            continue

        # Fund flow score (cached-only).
        ff_items = _load_cached_fund_flow(sym, days=30)
        ff_last = ff_items[-1] if ff_items else {}
        ff = _fund_flow_breakdown_last(ff_last)
        main_ratio = _finite_float(ff.get("mainNetRatio"), 0.0)
        super_ratio = _finite_float(ff.get("superNetRatio"), 0.0)
        large_ratio = _finite_float(ff.get("largeNetRatio"), 0.0)
        flow = 0.3
        if main_ratio > 2.0:
            flow = 1.0
        elif main_ratio > 0.0:
            flow = 0.75
        elif main_ratio < -1.0:
            flow = 0.10
        if (super_ratio + large_ratio) > 1.0:
            flow = clamp01(flow + 0.15)
        flow = clamp01(flow)

        # Chips score (cached-only).
        chips_items = _load_cached_chips(sym, days=30)
        chips_last = chips_items[-1] if chips_items else {}
        ch = _chips_summary_last(chips_last)
        pr = _finite_float(ch.get("profitRatio"), 0.0)
        avg_cost = _finite_float(ch.get("avgCost"), 0.0)
        chips = 0.30
        if pr >= 0.65:
            chips += 0.45
        elif pr >= 0.45:
            chips += 0.30
        if avg_cost > 0 and last_close >= avg_cost:
            chips += 0.25
        chips = clamp01(chips)

        # Sector hotness (weak prior).
        sector_hot = 0.0
        if sector and hot_set:
            sector_hot = 1.0 if sector in hot_set else 0.25
        elif hot_set:
            sector_hot = 0.10
        sector_hot = clamp01(sector_hot)

        breakdown = {
            "trend": round(trend, 4),
            "breakout": round(breakout, 4),
            "volume": round(volume, 4),
            "flow": round(flow, 4),
            "chips": round(chips, 4),
            "sectorHot": round(sector_hot, 4),
            "riskPenalty": round(float(risk_penalty), 4),
        }
        total = 0.0
        for k, w in weights.items():
            total += float(w) * float(breakdown.get(k) or 0.0)
        total = (total + risk_penalty) * 100.0
        total = max(0.0, min(100.0, total))

        signals: list[str] = []
        if breakout >= 0.8:
            signals.append("Near 20D high (breakout setup)")
        if trend >= 0.8:
            signals.append("MA uptrend (bullish alignment)")
        if volume >= 0.6:
            signals.append("Volume expansion")
        if main_ratio > 0:
            signals.append("Positive main fund flow")
        if pr >= 0.55:
            signals.append("High chip profit ratio")
        if sector_hot >= 0.9:
            signals.append("Hot sector")
        if risk_mode:
            signals.append(f"Risk mode: {risk_mode}")

        scored.append(
            {
                "symbol": sym,
                "market": market,
                "ticker": ticker,
                "name": name,
                "sector": sector,
                "score": round(total, 2),
                "probBand": _rank_prob_band(total),
                "signals": signals[:6],
                "breakdown": breakdown,
                "isHolding": is_holding,
            }
        )

    scored.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    top = scored[: max(1, min(int(limit), 200))]
    return {
        "asOfDate": as_of_date,
        "accountId": account_id,
        "universeVersion": universe_version,
        "riskMode": risk_mode,
        "items": top,
        "debug": {
            "poolSize": len(pool),
            "tvPool": len(tv_pool),
            "holdingsPool": len(holdings_pool),
            "scored": len(scored),
            "dropped": dropped,
        },
    }


def _parse_pct(v: Any) -> float:
    s = str(v or "").strip().replace("%", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_num(v: Any) -> float:
    s = str(v or "").strip().replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def _theme_key(kind: str, name: str) -> str:
    return f"{(kind or '').strip().lower()}:{(name or '').strip()}"


def _cn_trade_date_from_iso_ts(ts: str) -> str:
    tz = ZoneInfo("Asia/Shanghai")
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(tz).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now(tz=tz).strftime("%Y-%m-%d")


def _get_theme_members(
    *,
    kind: str,
    name: str,
    trade_date: str,
    force: bool,
    ttl_sec: int = 3600,
) -> tuple[list[str], dict[str, Any]]:
    """
    Resolve theme members (tickers) with a DB cache.
    """
    k = _theme_key(kind, name)
    cached = None if force else _get_theme_members_cached(theme_key=k, trade_date=trade_date)
    if cached is not None:
        try:
            updated = datetime.fromisoformat(str(cached.get("updatedAt") or "")).replace(tzinfo=UTC)
            age = (datetime.now(tz=UTC) - updated).total_seconds()
            if age <= float(ttl_sec):
                mem0 = cached.get("members") or []
                mem = [str(x).strip() for x in mem0 if str(x).strip()]
                return mem, {"cached": True, "ageSec": age}
        except Exception:
            pass

    try:
        if kind == "industry":
            members = fetch_cn_industry_members(name)
        elif kind == "concept":
            members = fetch_cn_concept_members(name)
        else:
            members = []
        mem = [str(x).strip() for x in (members or []) if str(x).strip()]
        ts = now_iso()
        _upsert_theme_members_cached(theme_key=k, trade_date=trade_date, ts=ts, members=mem)
        return mem, {"cached": False, "fetched": True, "count": len(mem)}
    except Exception as e:
        return [], {"cached": False, "error": str(e)}


def _infer_intraday_slot(dt_cn: datetime) -> str:
    """
    Slot boundaries (Asia/Shanghai):
      - 09:30-10:30 -> s1
      - 10:30-11:30 -> s2
      - 13:00-14:00 -> s3
      - 14:00-14:45 -> s4
    Outside these windows, map to the nearest next slot (for manual generation).
    """
    h = int(dt_cn.hour)
    m = int(dt_cn.minute)
    hm = h * 60 + m
    s1_start = 9 * 60 + 30
    s1_end = 10 * 60 + 30
    s2_end = 11 * 60 + 30
    s3_start = 13 * 60
    s3_end = 14 * 60
    s4_end = 14 * 60 + 45
    if s1_start <= hm < s1_end:
        return "0930_1030"
    if s1_end <= hm < s2_end:
        return "1030_1130"
    if s3_start <= hm < s3_end:
        return "1300_1400"
    if s3_end <= hm < s4_end:
        return "1400_1445"
    # Off-hours: best-effort mapping
    if hm < s1_start:
        return "0930_1030"
    if s2_end <= hm < s3_start:
        return "1300_1400"
    if hm >= s4_end:
        return "1400_1445"
    return "0930_1030"


def _intraday_prob_band(score: float) -> str:
    if score >= 80:
        return "High"
    if score >= 55:
        return "Medium"
    return "Low"


def _intraday_minute_features(bars: list[dict[str, Any]]) -> dict[str, float]:
    """
    Compute intraday features from minute bars (best-effort).
    Bars are expected to be in chronological order; if not, we sort by ts.
    """
    if not bars:
        return {
            "vwapAboveRatio": 0.0,
            "mom5": 0.0,
            "mom15": 0.0,
            "posMinutesRatio": 0.0,
            "pullbackRatio": 1.0,
            "closeNearHigh": 0.0,
            "lateRet15": 0.0,
            "lateRet30": 0.0,
            "lateVolSpike": 0.0,
        }
    # Sort by timestamp string.
    bs = sorted(bars, key=lambda x: str(x.get("ts") or ""))
    closes = [_parse_num(x.get("close")) for x in bs]
    highs = [_parse_num(x.get("high")) for x in bs]
    vols = [_parse_num(x.get("volume")) for x in bs]
    amts = [_parse_num(x.get("amount")) for x in bs]

    # VWAP (cumulative amount / cumulative volume); fallback to close if amount unavailable.
    cum_amt = 0.0
    cum_vol = 0.0
    above = 0
    for i in range(len(bs)):
        v = vols[i]
        a = amts[i]
        if v > 0:
            cum_vol += v
            cum_amt += a if a > 0 else (closes[i] * v)
        vwap = (cum_amt / cum_vol) if cum_vol > 0 else closes[i]
        if closes[i] >= vwap:
            above += 1
    vwap_above_ratio = float(above) / float(len(bs)) if bs else 0.0

    def _mom(n: int) -> float:
        if len(closes) <= n or closes[-1] <= 0 or closes[-1 - n] <= 0:
            return 0.0
        return (closes[-1] / closes[-1 - n] - 1.0) * 100.0

    mom5 = _mom(5)
    mom15 = _mom(15)

    pos_minutes = 0
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            pos_minutes += 1
    pos_minutes_ratio = float(pos_minutes) / float(max(1, len(closes) - 1))

    day_high = max(highs) if highs else 0.0
    last_close = closes[-1] if closes else 0.0
    close_near_high = 1.0 if (day_high > 0 and last_close >= day_high * 0.99) else 0.0

    # Pullback ratio: (day_high - last_close) / max(1e-9, day_high - first_close)
    first_close = closes[0] if closes else 0.0
    denom = max(1e-9, (day_high - first_close))
    pullback_ratio = (day_high - last_close) / denom if denom > 0 else 1.0
    pullback_ratio = max(0.0, min(1.0, pullback_ratio))

    # Late returns.
    late_ret15 = _mom(15)
    late_ret30 = _mom(30)

    # Late volume spike: last 15m volume / avg 15m block volume.
    block = 15
    blocks = []
    for i in range(0, len(vols), block):
        blocks.append(sum(vols[i : i + block]))
    last_block = blocks[-1] if blocks else 0.0
    avg_block = (sum(blocks[:-1]) / max(1, len(blocks) - 1)) if len(blocks) > 1 else (blocks[0] if blocks else 0.0)
    late_vol_spike = (last_block / avg_block) if (last_block > 0 and avg_block > 0) else 0.0

    return {
        "vwapAboveRatio": float(vwap_above_ratio),
        "mom5": float(mom5),
        "mom15": float(mom15),
        "posMinutesRatio": float(pos_minutes_ratio),
        "pullbackRatio": float(pullback_ratio),
        "closeNearHigh": float(close_near_high),
        "lateRet15": float(late_ret15),
        "lateRet30": float(late_ret30),
        "lateVolSpike": float(late_vol_spike),
    }


def _intraday_get_minute_bars(
    *,
    symbol: str,
    trade_date: str,
    interval: str,
    force: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    DB-first minute bars for a single CN symbol.
    Cache TTL is short because intraday data changes quickly.
    """
    ttl_sec = 90
    now_ts = now_iso()
    cached = None if force else _get_cn_minute_bars_cached(symbol=symbol, trade_date=trade_date, interval=interval)
    if cached is not None:
        try:
            updated = datetime.fromisoformat(str(cached.get("updatedAt") or "")).replace(tzinfo=UTC)
            age = (datetime.now(tz=UTC) - updated).total_seconds()
            if age <= ttl_sec:
                return list(cached.get("bars") or []), {"cached": True, "ageSec": age}
        except Exception:
            pass
    # Fetch and cache.
    ticker = symbol.split(":")[-1]
    try:
        bars = fetch_cn_a_minute_bars(ticker, trade_date=trade_date, interval=interval)
        if isinstance(bars, list):
            _upsert_cn_minute_bars_cached(symbol=symbol, trade_date=trade_date, interval=interval, ts=now_ts, bars=bars)
            return bars, {"cached": False, "fetched": True, "count": len(bars)}
    except Exception as e:
        return [], {"cached": False, "error": str(e)}
    return [], {"cached": False, "fetched": False}


def _intraday_rank_build_and_score(
    *,
    account_id: str,
    as_of_ts: str,
    slot: str,
    limit: int,
    universe_version: str,
) -> dict[str, Any]:
    """
    Intraday rank based on spot + minute bars (CN only), best-effort.
    """
    tz = ZoneInfo("Asia/Shanghai")
    try:
        dt = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
        dt_cn = dt.astimezone(tz)
    except Exception:
        dt_cn = datetime.now(tz=tz)
    trade_date = dt_cn.strftime("%Y-%m-%d")

    # Risk mode (reuse latest sentiment).
    risk_mode: str | None = None
    try:
        items = _list_cn_sentiment_days(as_of_date=trade_date, days=5)
        latest = items[-1] if items else {}
        risk_mode = _norm_str(latest.get("riskMode") or "") or None
    except Exception:
        risk_mode = None

    # Candidate pool: TV + holdings + spot movers.
    pool: list[dict[str, Any]] = []
    seen: set[str] = set()
    for it in _rank_extract_tv_pool(max_screeners=20, max_rows=120) + _rank_extract_holdings_pool(account_id):
        sym = _norm_str(it.get("symbol") or "")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        pool.append(it)
        if len(pool) >= 160:
            break

    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
    except Exception:
        spot_rows = []
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

    # Add spot movers (top by change_pct), capped.
    movers = sorted(spot_rows, key=lambda s: _parse_pct(s.quote.get("change_pct") or ""), reverse=True)[:120]
    for s in movers:
        sym = _norm_str(s.symbol)
        if not sym or sym in seen:
            continue
        seen.add(sym)
        pool.append({"symbol": sym, "market": "CN", "ticker": s.ticker, "name": s.name, "isHolding": False})
        if len(pool) >= 220:
            break

    def clamp01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    scored: list[dict[str, Any]] = []
    debug_fetch: dict[str, Any] = {"minuteBars": {"ok": 0, "err": 0}}
    for it in pool:
        sym = _norm_str(it.get("symbol") or "")
        if not sym.startswith("CN:"):
            continue
        ticker = _norm_str(it.get("ticker") or sym.split(":")[-1])
        name = _norm_str(it.get("name") or ticker)
        is_holding = bool(it.get("isHolding"))
        spot = spot_map.get(ticker)
        vol_ratio = _parse_num((spot.quote.get("vol_ratio") if spot else "") or 0.0)
        chg_pct = _parse_pct((spot.quote.get("change_pct") if spot else "") or 0.0)

        # Quick liquidity filter via turnover if available (holdings bypass).
        turnover = _parse_num((spot.quote.get("turnover") if spot else "") or 0.0)
        if (not is_holding) and turnover > 0 and turnover < 5e7:
            continue

        bars, meta = _intraday_get_minute_bars(symbol=sym, trade_date=trade_date, interval="1", force=False)
        if bars:
            debug_fetch["minuteBars"]["ok"] += 1
        else:
            debug_fetch["minuteBars"]["err"] += 1

        f = _intraday_minute_features(bars)
        # Build slot-specific factors and score.
        factors: dict[str, float] = {}
        signals: list[str] = []
        score01 = 0.0

        if slot == "0930_1030":
            # relVol proxy via spot vol_ratio (1->0.2, 5->1.0)
            relv = clamp01(vol_ratio / 5.0)
            mom = clamp01(max(0.0, f["mom15"]) / 5.0)
            vwap = clamp01(f["vwapAboveRatio"])
            factors = {"relVol": round(relv, 4), "mom15": round(mom, 4), "aboveVwap": round(vwap, 4)}
            score01 = 0.45 * relv + 0.35 * mom + 0.20 * vwap
            if vol_ratio >= 3:
                signals.append("High relative volume")
            if f["vwapAboveRatio"] >= 0.7:
                signals.append("Above VWAP")
            if chg_pct >= 2:
                signals.append("Early strength")
        elif slot == "1030_1130":
            support = clamp01(1.0 - f["pullbackRatio"])
            vwap_hold = clamp01(f["vwapAboveRatio"])
            factors = {"support": round(support, 4), "vwapHold": round(vwap_hold, 4)}
            score01 = 0.55 * support + 0.45 * vwap_hold
            if support >= 0.6:
                signals.append("Low pullback (strong support)")
        elif slot == "1300_1400":
            mom = clamp01(max(0.0, f["mom15"]) / 4.0)
            posm = clamp01(f["posMinutesRatio"])
            factors = {"mom15": round(mom, 4), "posMinutes": round(posm, 4)}
            score01 = 0.55 * mom + 0.45 * posm
            if mom >= 0.6:
                signals.append("Afternoon acceleration")
        else:  # 1400_1445
            late = clamp01(max(0.0, f["lateRet15"]) / 3.0)
            spike = clamp01(f["lateVolSpike"] / 2.0)
            near = clamp01(f["closeNearHigh"])
            factors = {"lateRet15": round(late, 4), "lateVolSpike": round(spike, 4), "closeNearHigh": round(near, 4)}
            score01 = 0.45 * late + 0.35 * spike + 0.20 * near
            if f["lateVolSpike"] >= 1.5:
                signals.append("Late volume spike")
            if f["closeNearHigh"] >= 1.0:
                signals.append("Close near day high")

        # Risk penalty.
        if risk_mode == "no_new_positions" and not is_holding:
            score01 *= 0.75
            signals.append("Risk mode: no_new_positions")
        elif risk_mode == "caution" and not is_holding:
            score01 *= 0.90
            signals.append("Risk mode: caution")

        score = max(0.0, min(100.0, score01 * 100.0))
        scored.append(
            {
                "symbol": sym,
                "market": "CN",
                "ticker": ticker,
                "name": name,
                "score": round(score, 2),
                "probBand": _intraday_prob_band(score),
                "slot": slot,
                "signals": signals[:6],
                "factors": factors,
                "notes": None,
                "isHolding": is_holding,
                "debug": {"spot": {"chgPct": chg_pct, "volRatio": vol_ratio}, "minuteMeta": meta},
            }
        )

    scored.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    top = scored[: max(1, min(int(limit), 200))]

    # Observations: store a small diagnostic snapshot.
    obs_raw = {
        "slot": slot,
        "topMovers": [
            {"ticker": s.ticker, "name": s.name, "chgPct": _parse_pct(s.quote.get("change_pct") or ""), "volRatio": _parse_num(s.quote.get("vol_ratio") or "")}
            for s in movers[:15]
        ],
    }
    _append_cn_intraday_observation(trade_date=trade_date, ts=as_of_ts, kind="hourly_prep", raw=obs_raw)
    obs_items = _list_cn_intraday_observations(trade_date=trade_date)

    return {
        "asOfTs": as_of_ts,
        "tradeDate": trade_date,
        "slot": slot,
        "accountId": account_id,
        "universeVersion": universe_version,
        "riskMode": risk_mode,
        "items": top,
        "observations": obs_items,
        "debug": {"poolSize": len(pool), "spotRows": len(spot_rows), "fetch": debug_fetch},
    }


def _mainline_step1_candidates(
    *,
    trade_date: str,
    force_membership: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Step1: identify candidate themes (industry+concept) using available signals:
      - today strength (spot change pct)
      - 3D return (from cached daily bars)
      - volume surge proxy (today turnover vs 5D avg amount)
      - limit-up count (theme members ∩ limit-up pool)
    """
    debug: dict[str, Any] = {"sources": {}, "errors": []}

    # Strong set: top movers by change pct with basic liquidity filters.
    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
        debug["sources"]["spot"] = len(spot_rows)
    except Exception as e:
        debug["errors"].append(f"spot_failed: {e}")
        spot_rows = []

    spot_cn = [s for s in spot_rows if s.market == "CN" and s.ticker]
    spot_cn_sorted = sorted(spot_cn, key=lambda s: _parse_pct(s.quote.get("change_pct") or ""), reverse=True)
    strong = []
    for s in spot_cn_sorted[:400]:
        turnover = _parse_num(s.quote.get("turnover") or "")
        vol_ratio = _parse_num(s.quote.get("vol_ratio") or "")
        # Loose filters: keep meaningful liquidity and attention.
        if turnover > 3e7 and (vol_ratio <= 0 or vol_ratio >= 1.2):
            strong.append(s)
    strong = strong[:200]
    strong_set = {s.ticker for s in strong if s.ticker}
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_cn if s.ticker}

    # Limit-up pool.
    limitups: list[dict[str, Any]] = []
    try:
        dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
        limitups = fetch_cn_limitup_pool(dt)
        debug["sources"]["limitups"] = len(limitups)
    except Exception as e:
        debug["errors"].append(f"limitup_pool_failed: {e}")
        limitups = []
    limitup_set = {str(x.get("ticker") or "").strip() for x in limitups if str(x.get("ticker") or "").strip()}

    # Theme name candidates.
    industry_names: list[str] = []
    concept_names: list[str] = []

    # Industry: from existing industry fund flow (DB) + spot rank (AkShare).
    try:
        mat = _market_cn_industry_fund_flow_top_by_date(as_of_date=trade_date, days=10, top_k=10)
        top_by_date = mat.get("topByDate") if isinstance(mat, dict) else None
        if isinstance(top_by_date, list) and top_by_date:
            latest = top_by_date[-1] if isinstance(top_by_date[-1], dict) else {}
            tops = latest.get("topIndustries") if isinstance(latest, dict) else None
            if isinstance(tops, list):
                for x in tops:
                    n = str(x).strip()
                    if n:
                        industry_names.append(n)
        debug["sources"]["industryFlowNames"] = len(industry_names)
    except Exception as e:
        debug["errors"].append(f"industry_flow_failed: {e}")

    try:
        boards = fetch_cn_industry_boards_spot()
        names = [str(x.get("name") or "").strip() for x in boards[:30]]
        industry_names.extend([n for n in names if n])
        debug["sources"]["industryBoards"] = len(boards)
    except Exception as e:
        debug["errors"].append(f"industry_boards_failed: {e}")

    try:
        boards = fetch_cn_concept_boards_spot()
        names = [str(x.get("name") or "").strip() for x in boards[:30]]
        concept_names.extend([n for n in names if n])
        debug["sources"]["conceptBoards"] = len(boards)
    except Exception as e:
        debug["errors"].append(f"concept_boards_failed: {e}")

    def _dedupe(xs: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for x in xs:
            k = x.strip()
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    industry_names = _dedupe(industry_names)[:25]
    concept_names = _dedupe(concept_names)[:25]

    # Compute features per theme on a bounded subset (intersection with strong movers if possible).
    def _ret3d_for_symbol(sym: str) -> float:
        bars = _load_cached_bars(sym, days=10)
        if len(bars) < 4:
            return 0.0
        try:
            c0 = _finite_float(bars[-4].get("close"), 0.0)
            c1 = _finite_float(bars[-1].get("close"), 0.0)
            if c0 > 0 and c1 > 0:
                return (c1 / c0 - 1.0) * 100.0
        except Exception:
            return 0.0
        return 0.0

    def _amount_5d_avg(sym: str) -> float:
        bars = _load_cached_bars(sym, days=10)
        vals = []
        for b in bars[-5:]:
            a = _finite_float(b.get("amount"), 0.0)
            if a > 0:
                vals.append(a)
        return float(sum(vals) / len(vals)) if vals else 0.0

    items: list[dict[str, Any]] = []
    membership_debug: dict[str, Any] = {"ok": 0, "err": 0}
    for kind, names in (("industry", industry_names), ("concept", concept_names)):
        for name in names:
            members, meta = _get_theme_members(kind=kind, name=name, trade_date=trade_date, force=force_membership)
            if members:
                membership_debug["ok"] += 1
            else:
                membership_debug["err"] += 1

            # Bound computation cost.
            mem_set = set(members)
            intersect = [t for t in members if t in strong_set]
            sample = intersect[:60] if intersect else list(mem_set)[:60]

            # Limit-up count.
            limitup_count = len([t for t in mem_set if t in limitup_set])

            # Followers (today > 5% or limit-up).
            followers = 0
            today_vals: list[float] = []
            turnover_sum = 0.0
            amt5_sum = 0.0
            ret3_vals: list[float] = []
            for t in sample:
                s = spot_map.get(t)
                if s is not None:
                    chg = _parse_pct(s.quote.get("change_pct") or "")
                    today_vals.append(chg)
                    if chg >= 5.0:
                        followers += 1
                    turnover_sum += _parse_num(s.quote.get("turnover") or "")
                if t in limitup_set:
                    followers += 1
                sym = f"CN:{t}"
                ret3_vals.append(_ret3d_for_symbol(sym))
                amt5_sum += _amount_5d_avg(sym)

            today_strength = float(sum(today_vals) / len(today_vals)) if today_vals else 0.0
            ret3d = float(sum(ret3_vals) / len(ret3_vals)) if ret3_vals else 0.0
            vol_surge = (turnover_sum / amt5_sum) if (turnover_sum > 0 and amt5_sum > 0) else 0.0

            items.append(
                {
                    "kind": kind,
                    "name": name,
                    "todayStrength": round(today_strength, 4),
                    "ret3d": round(ret3d, 4),
                    "volSurge": round(float(vol_surge), 4),
                    "limitupCount": int(limitup_count),
                    "followersCount": int(followers),
                    "membershipMeta": meta,
                    "sampleSize": int(len(sample)),
                }
            )

    debug["membership"] = membership_debug

    # Normalize and score for ranking.
    def _norm01(xs: list[float]) -> list[float]:
        if not xs:
            return []
        lo = min(xs)
        hi = max(xs)
        span = max(1e-9, hi - lo)
        return [(x - lo) / span for x in xs]

    today_list = [float(x.get("todayStrength") or 0.0) for x in items]
    ret3_list = [float(x.get("ret3d") or 0.0) for x in items]
    vol_list = [float(x.get("volSurge") or 0.0) for x in items]
    lu_list = [float(x.get("limitupCount") or 0.0) for x in items]

    today_n = _norm01(today_list)
    ret3_n = _norm01(ret3_list)
    vol_n = _norm01(vol_list)
    lu_n = _norm01(lu_list)

    scored: list[dict[str, Any]] = []
    for i, it in enumerate(items):
        # Step1 score: prioritize limit-up breadth and price action, then volume and 3D continuation.
        s = 0.0
        s += 0.35 * float(lu_n[i] if i < len(lu_n) else 0.0)
        s += 0.30 * float(today_n[i] if i < len(today_n) else 0.0)
        s += 0.20 * float(vol_n[i] if i < len(vol_n) else 0.0)
        s += 0.15 * float(ret3_n[i] if i < len(ret3_n) else 0.0)

        # Hard-ish filters aligned with your A/B/C/D idea; keep best-effort.
        limitup_ok = int(it.get("limitupCount") or 0) >= 3
        strength_ok = float(it.get("todayStrength") or 0.0) >= 2.0
        vol_ok = float(it.get("volSurge") or 0.0) >= 1.2
        if not (limitup_ok or (strength_ok and vol_ok)):
            continue

        scored.append({**it, "step1Score": round(s * 100.0, 2)})

    scored.sort(key=lambda x: float(x.get("step1Score") or 0.0), reverse=True)
    return scored, debug


def _mainline_step2_structure(
    *,
    trade_date: str,
    candidates: list[dict[str, Any]],
    force_membership: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Step2: structure analysis inside a theme:
      - leader_candidate selection
      - tiering completeness (followers)
      - linkage/consistency proxy using daily bars
    """
    debug: dict[str, Any] = {"errors": [], "themes": []}

    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
    except Exception:
        spot_rows = []
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

    limitup_set: set[str] = set()
    try:
        dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
        limitups = fetch_cn_limitup_pool(dt)
        limitup_set = {str(x.get("ticker") or "").strip() for x in limitups if str(x.get("ticker") or "").strip()}
    except Exception as e:
        debug["errors"].append(f"limitup_pool_failed: {e}")
        limitup_set = set()

    def clamp01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    def _ret_nd(sym: str, n: int) -> float:
        bars = _load_cached_bars(sym, days=max(10, n + 2))
        if len(bars) <= n:
            return 0.0
        c0 = _finite_float(bars[-1 - n].get("close"), 0.0)
        c1 = _finite_float(bars[-1].get("close"), 0.0)
        if c0 > 0 and c1 > 0:
            return (c1 / c0 - 1.0) * 100.0
        return 0.0

    def _returns_series(sym: str, n: int) -> list[float]:
        bars = _load_cached_bars(sym, days=max(20, n + 2))
        if len(bars) < (n + 1):
            return []
        closes = [_finite_float(b.get("close"), 0.0) for b in bars]
        rets: list[float] = []
        for i in range(len(closes) - n, len(closes)):
            if i <= 0:
                continue
            c0 = closes[i - 1]
            c1 = closes[i]
            if c0 > 0 and c1 > 0:
                rets.append(c1 / c0 - 1.0)
        return rets

    def _corr(a: list[float], b: list[float]) -> float:
        if len(a) != len(b) or len(a) < 3:
            return 0.0
        ma = sum(a) / len(a)
        mb = sum(b) / len(b)
        num = sum((a[i] - ma) * (b[i] - mb) for i in range(len(a)))
        da = math.sqrt(sum((x - ma) ** 2 for x in a))
        db = math.sqrt(sum((x - mb) ** 2 for x in b))
        if da <= 1e-9 or db <= 1e-9:
            return 0.0
        return float(num / (da * db))

    out: list[dict[str, Any]] = []
    for it in candidates:
        kind = str(it.get("kind") or "").strip()
        name = str(it.get("name") or "").strip()
        if not kind or not name:
            continue

        members, meta = _get_theme_members(kind=kind, name=name, trade_date=trade_date, force=force_membership)
        mem = [m for m in members if m]
        if not mem:
            out.append({**it, "structureScore": 0.0, "leaderCandidate": None, "structureDebug": {"members": 0, "meta": meta}})
            continue

        # Prefer evaluating a bounded set of active members.
        ranked = []
        for t in mem:
            s = spot_map.get(t)
            turnover = _parse_num(s.quote.get("turnover") or "") if s is not None else 0.0
            chg = _parse_pct(s.quote.get("change_pct") or "") if s is not None else 0.0
            vol_ratio = _parse_num(s.quote.get("vol_ratio") or "") if s is not None else 0.0
            ranked.append((t, turnover, chg, vol_ratio))
        ranked.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)
        sample = [x[0] for x in ranked[:40]]

        # Leader candidate: highest combination of 5D return + today strength + vol_ratio.
        best = None
        best_score = -1.0
        for t in sample:
            sym = f"CN:{t}"
            r5 = _ret_nd(sym, 5)
            s = spot_map.get(t)
            chg = _parse_pct(s.quote.get("change_pct") or "") if s is not None else 0.0
            vol_ratio = _parse_num(s.quote.get("vol_ratio") or "") if s is not None else 0.0
            # Map to 0..1 then weight.
            sc = 0.45 * clamp01(r5 / 15.0) + 0.35 * clamp01(chg / 8.0) + 0.20 * clamp01(vol_ratio / 5.0)
            if sc > best_score:
                best_score = sc
                best = t

        leader_candidate = None
        if best:
            s = spot_map.get(best)
            leader_candidate = {
                "symbol": f"CN:{best}",
                "ticker": best,
                "name": (s.name if s is not None else best),
                "todayChgPct": _parse_pct(s.quote.get("change_pct") or "") if s is not None else 0.0,
                "volRatio": _parse_num(s.quote.get("vol_ratio") or "") if s is not None else 0.0,
                "turnover": _parse_num(s.quote.get("turnover") or "") if s is not None else 0.0,
                "ret5d": round(_ret_nd(f"CN:{best}", 5), 4),
            }

        # Tiering: followers count + distribution.
        chgs = []
        followers = 0
        limitups = 0
        for t in sample:
            s = spot_map.get(t)
            chg = _parse_pct(s.quote.get("change_pct") or "") if s is not None else 0.0
            chgs.append(chg)
            if chg >= 5.0:
                followers += 1
            if t in limitup_set:
                limitups += 1
        chgs.sort(reverse=True)
        top1 = chgs[0] if chgs else 0.0
        top5 = chgs[4] if len(chgs) >= 5 else (chgs[-1] if chgs else 0.0)
        gap = max(0.0, top1 - top5)
        tiering = clamp01(min(1.0, followers / 6.0) * 0.65 + clamp01(gap / 6.0) * 0.35)

        # Linkage: corr between leader daily returns and average theme returns (sample-based).
        linkage = 0.0
        if best:
            lead_rets = _returns_series(f"CN:{best}", 5)
            if lead_rets:
                # Build average return series for top M sample members.
                m = 12
                series_list = []
                for t in sample[:m]:
                    rs = _returns_series(f"CN:{t}", 5)
                    if len(rs) == len(lead_rets):
                        series_list.append(rs)
                if series_list:
                    avg = []
                    for i2 in range(len(lead_rets)):
                        avg.append(float(sum(rs[i2] for rs in series_list) / len(series_list)))
                    linkage = clamp01((_corr(lead_rets, avg) + 1.0) / 2.0)

        # Leader strength uses best_score (0..1).
        leader_strength = clamp01(best_score if best_score > 0 else 0.0)

        structure01 = 0.40 * leader_strength + 0.35 * tiering + 0.25 * linkage
        structure_score = round(structure01 * 100.0, 2)

        out.append(
            {
                **it,
                "structureScore": structure_score,
                "leaderCandidate": leader_candidate,
                "followersCount": int(max(int(it.get("followersCount") or 0), followers)),
                "limitupCount": int(max(int(it.get("limitupCount") or 0), limitups)),
                "structureDebug": {
                    "members": len(mem),
                    "sample": len(sample),
                    "leaderStrength": round(leader_strength, 4),
                    "tiering": round(tiering, 4),
                    "linkage": round(linkage, 4),
                    "membershipMeta": meta,
                },
            }
        )
        debug["themes"].append({"kind": kind, "name": name, "members": len(mem), "sample": len(sample)})

    out.sort(key=lambda x: float(x.get("structureScore") or 0.0), reverse=True)
    return out, debug


def _ai_mainline_explain(*, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{_ai_service_base_url()}/mainline/explain"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _do() -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            raise OSError(
                f"ai-service HTTP {getattr(e, 'code', '?')} {getattr(e, 'reason', '')} while calling {url}: {err_body}"
            ) from e
        except http.client.RemoteDisconnected as e:
            raise OSError(f"ai-service disconnected while calling {url}: {e}") from e
        except urllib.error.URLError as e:
            raise OSError(f"ai-service URL error while calling {url}: {e}") from e
        except TimeoutError as e:
            raise OSError(f"ai-service timeout while calling {url}: {e}") from e

    try:
        return _do()
    except OSError as e:
        msg = str(e)
        if ("disconnected" in msg) or ("Connection reset" in msg) or ("timeout" in msg):
            time.sleep(0.25)
            return _do()
        raise


def _build_mainline_snapshot(
    *,
    account_id: str,
    as_of_ts: str,
    universe_version: str,
    force: bool,
    top_k: int,
) -> dict[str, Any]:
    trade_date = _cn_trade_date_from_iso_ts(as_of_ts)
    # Risk context (latest sentiment).
    risk_mode: str | None = None
    try:
        items = _list_cn_sentiment_days(as_of_date=trade_date, days=5)
        latest = items[-1] if items else {}
        risk_mode = _norm_str(latest.get("riskMode") or "") or None
    except Exception:
        risk_mode = None

    cands1, dbg1 = _mainline_step1_candidates(trade_date=trade_date, force_membership=force)
    # Bound: structure analysis only on top Step1 candidates.
    cands2, dbg2 = _mainline_step2_structure(trade_date=trade_date, candidates=cands1[:12], force_membership=force)

    # AI logic layer (best-effort).
    logic_map: dict[str, dict[str, Any]] = {}
    ai_error: str | None = None
    try:
        themes_payload = []
        for it in cands2[:8]:
            themes_payload.append(
                {
                    "kind": str(it.get("kind") or ""),
                    "name": str(it.get("name") or ""),
                    "evidence": {
                        "step1Score": it.get("step1Score"),
                        "todayStrength": it.get("todayStrength"),
                        "ret3d": it.get("ret3d"),
                        "volSurge": it.get("volSurge"),
                        "limitupCount": it.get("limitupCount"),
                        "followersCount": it.get("followersCount"),
                        "structureScore": it.get("structureScore"),
                        "leaderCandidate": it.get("leaderCandidate"),
                        "structureDebug": it.get("structureDebug"),
                        "riskMode": risk_mode,
                    },
                }
            )
        if themes_payload:
            resp = _ai_mainline_explain(payload={"date": trade_date, "themes": themes_payload, "context": {"riskMode": risk_mode}})
            lst = resp.get("themes")
            arr: list[Any] = lst if isinstance(lst, list) else []
            for x in arr:
                if not isinstance(x, dict):
                    continue
                k = _theme_key(str(x.get("kind") or ""), str(x.get("name") or ""))
                logic_map[k] = x
    except Exception as e:
        ai_error = str(e)
        logic_map = {}

    # Merge + composite decision.
    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
    except Exception:
        spot_rows = []
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

    def _top_tickers_for_theme(kind: str, name: str) -> list[dict[str, Any]]:
        try:
            members, _meta = _get_theme_members(kind=kind, name=name, trade_date=trade_date, force=False)
        except Exception:
            members = []
        rows = []
        for t in (members or [])[:800]:
            s = spot_map.get(str(t))
            chg = _parse_pct(s.quote.get("change_pct") or "") if s is not None else 0.0
            turnover = _parse_num(s.quote.get("turnover") or "") if s is not None else 0.0
            vol_ratio = _parse_num(s.quote.get("vol_ratio") or "") if s is not None else 0.0
            rows.append((str(t), chg, turnover, vol_ratio, (s.name if s is not None else "")))
        rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
        out = []
        for t, chg, turnover, vol_ratio, nm in rows[:12]:
            out.append(
                {
                    "symbol": f"CN:{t}",
                    "ticker": t,
                    "name": nm or t,
                    "chgPct": round(float(chg), 2),
                    "turnover": float(turnover),
                    "volRatio": round(float(vol_ratio), 2),
                }
            )
        return out

    merged: list[dict[str, Any]] = []
    for it in cands2:
        kind = str(it.get("kind") or "")
        name = str(it.get("name") or "")
        k = _theme_key(kind, name)
        logic = logic_map.get(k, {})
        logic_score = _finite_float(logic.get("logicScore"), _finite_float(it.get("structureScore"), 0.0))
        logic_grade = _norm_str(logic.get("logicGrade") or "") or None
        logic_summary = _norm_str(logic.get("logicSummary") or "") or None
        structure_score = _finite_float(it.get("structureScore"), 0.0)
        composite = 0.5 * float(structure_score) + 0.5 * float(logic_score)
        top_tickers = _top_tickers_for_theme(kind, name)
        merged.append(
            {
                "kind": kind,
                "name": name,
                "compositeScore": round(composite, 2),
                "structureScore": round(structure_score, 2),
                "logicScore": round(float(logic_score), 2),
                "logicGrade": logic_grade,
                "logicSummary": logic_summary,
                "leaderCandidate": it.get("leaderCandidate") if isinstance(it.get("leaderCandidate"), dict) else None,
                "topTickers": top_tickers,
                "followersCount": int(it.get("followersCount") or 0),
                "limitupCount": int(it.get("limitupCount") or 0),
                "volSurge": float(it.get("volSurge") or 0.0),
                "todayStrength": float(it.get("todayStrength") or 0.0),
                "ret3d": float(it.get("ret3d") or 0.0),
                "evidence": {
                    "step1": {k2: it.get(k2) for k2 in ("step1Score", "todayStrength", "ret3d", "volSurge", "limitupCount", "followersCount")},
                    "structure": it.get("structureDebug") if isinstance(it.get("structureDebug"), dict) else {},
                    "logic": logic if isinstance(logic, dict) else {},
                },
                "decaySignals": [],
            }
        )

    merged.sort(key=lambda x: float(x.get("compositeScore") or 0.0), reverse=True)
    themes_topk = merged[: max(1, min(int(top_k), 10))]

    selected: dict[str, Any] | None = None
    if themes_topk:
        top1 = themes_topk[0]
        top2 = themes_topk[1] if len(themes_topk) > 1 else None
        s1 = float(top1.get("compositeScore") or 0.0)
        s2 = float(top2.get("compositeScore") or 0.0) if top2 else 0.0
        if s1 >= 70.0 and ((s1 - s2) >= 5.0):
            selected = dict(top1)

    # Decay signals (lightweight).
    if selected and isinstance(selected.get("leaderCandidate"), dict):
        t = str((selected.get("leaderCandidate") or {}).get("ticker") or "").strip()
        if t:
            sym = f"CN:{t}"
            bars = _load_cached_bars(sym, days=10)
            if len(bars) >= 3:
                c2 = _finite_float(bars[-3].get("close"), 0.0)
                c1 = _finite_float(bars[-2].get("close"), 0.0)
                c0 = _finite_float(bars[-1].get("close"), 0.0)
                if c2 > 0 and c1 > 0 and c0 > 0:
                    r1 = c1 / c2 - 1.0
                    r0 = c0 / c1 - 1.0
                    if (r0 <= 0 and r1 <= 0) or (r0 <= -0.03):
                        selected["decaySignals"] = ["leader_weakening"]

    return {
        "tradeDate": trade_date,
        "asOfTs": as_of_ts,
        "accountId": account_id,
        "universeVersion": universe_version,
        "riskMode": risk_mode,
        "selected": selected,
        "themesTopK": themes_topk,
        "debug": {"step1": dbg1, "step2": dbg2, "aiError": ai_error},
    }


def _ai_strategy_daily(*, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/strategy/daily",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


def _ai_strategy_candidates(*, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{_ai_service_base_url()}/strategy/candidates"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _do() -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            raise OSError(
                f"ai-service HTTP {getattr(e, 'code', '?')} {getattr(e, 'reason', '')} while calling {url}: {err_body}"
            ) from e
        except http.client.RemoteDisconnected as e:
            raise OSError(f"ai-service disconnected while calling {url}: {e}") from e
        except urllib.error.URLError as e:
            raise OSError(f"ai-service URL error while calling {url}: {e}") from e
        except TimeoutError as e:
            raise OSError(f"ai-service timeout while calling {url}: {e}") from e

    try:
        return _do()
    except OSError as e:
        msg = str(e)
        if ("disconnected" in msg) or ("Connection reset" in msg) or ("timeout" in msg):
            time.sleep(0.25)
            return _do()
        raise


def _ai_leader_daily(*, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{_ai_service_base_url()}/leader/daily"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _do() -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            raise OSError(
                f"ai-service HTTP {getattr(e, 'code', '?')} {getattr(e, 'reason', '')} while calling {url}: {err_body}"
            ) from e
        except http.client.RemoteDisconnected as e:
            raise OSError(f"ai-service disconnected while calling {url}: {e}") from e
        except urllib.error.URLError as e:
            raise OSError(f"ai-service URL error while calling {url}: {e}") from e
        except TimeoutError as e:
            raise OSError(f"ai-service timeout while calling {url}: {e}") from e

    try:
        return _do()
    except OSError as e:
        msg = str(e)
        if ("disconnected" in msg) or ("Connection reset" in msg) or ("timeout" in msg):
            time.sleep(0.25)
            return _do()
        raise


def _ai_strategy_daily_markdown(*, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{_ai_service_base_url()}/strategy/daily-markdown"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _do() -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            raise OSError(
                f"ai-service HTTP {getattr(e, 'code', '?')} {getattr(e, 'reason', '')} while calling {url}: {err_body}"
            ) from e
        except http.client.RemoteDisconnected as e:
            raise OSError(f"ai-service disconnected while calling {url}: {e}") from e
        except urllib.error.URLError as e:
            raise OSError(f"ai-service URL error while calling {url}: {e}") from e
        except TimeoutError as e:
            raise OSError(f"ai-service timeout while calling {url}: {e}") from e

    # Retry once for transient disconnects (e.g. ai-service hot reload).
    try:
        return _do()
    except OSError as e:
        msg = str(e)
        if ("disconnected" in msg) or ("Connection reset" in msg) or ("timeout" in msg):
            time.sleep(0.25)
            return _do()
        raise


def _normalize_strategy_markdown(md: str) -> str:
    """
    Defensive Markdown normalization for LLM outputs:
    - Ensure headings (## ... etc) start on their own line.
    - Avoid touching fenced code blocks.
    """
    s = (md or "").replace("\r\n", "\n").replace("\r", "\n")
    if not s.strip():
        return ""

    parts = s.split("```")
    for i in range(0, len(parts), 2):  # outside code fences
        # If model puts analysis text on the same line as known headings, split it.
        # Example: "## 0 结果摘要 主线偏向..." -> "## 0 结果摘要\n\n主线偏向..."
        seg = re.sub(
            r"^(##\s*(?:0|1|2|3|4|5)\s*(?:结果摘要|资金板块|候选Top3|持仓计划|执行要点|条件单总表))\s+([^\n#].*)$",
            r"\1\n\n\2",
            parts[i],
            flags=re.MULTILINE,
        )
        # If a model puts '...## Heading' on the same line, split it.
        # Note: use lookahead so it also fixes cases without spaces (e.g. ')...## 1）').
        seg = re.sub(r"([^\n])(?=#{2,6}\s)", r"\1\n\n", seg)

        # Fix "one-line tables" so markdown renderers can parse them.
        seg = re.sub(r"\|\|\s*(?=[-:]{3,})", "|\n|", seg)  # header -> separator
        seg = re.sub(r"\|\|\s*(?=\d+\s*\|)", "|\n|", seg)  # separator -> data row (rank starts with number)
        seg = re.sub(r"\|\s+\|", "|\n|", seg)  # general row boundary as "| |"
        # Ensure table header starts on a new line.
        seg = re.sub(r"([^\n])\s*(\|[^\n]*\n\|\s*[-:]{3,}[^\n]*)", r"\1\n\n\2", seg)
        parts[i] = seg
    out = "```".join(parts)
    return out.strip() + "\n"


@app.get("/strategy/accounts/{account_id}/daily", response_model=StrategyReportResponse)
def get_strategy_daily_report(account_id: str, date: str | None = None) -> StrategyReportResponse:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    acct = _get_broker_account_row(aid)
    if acct is None:
        raise HTTPException(status_code=404, detail="Account not found")
    d = (date or "").strip() or _today_cn_date_str()
    row = _get_strategy_report_row(aid, d)
    if row is None:
        raise HTTPException(status_code=404, detail="Report not found")
    return _strategy_report_response(
        report_id=str(row["id"]),
        date=d,
        account_id=aid,
        account_title=str(acct["title"]),
        created_at=str(row["createdAt"]),
        model=str(row["model"]),
        output=row["output"] if isinstance(row.get("output"), dict) else {},
        input_snapshot=row["inputSnapshot"] if isinstance(row.get("inputSnapshot"), dict) else None,
    )


@app.get("/strategy/accounts/{account_id}/reports", response_model=ListStrategyReportsResponse)
def get_strategy_reports(account_id: str, days: int = 10) -> ListStrategyReportsResponse:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    if _get_broker_account_row(aid) is None:
        raise HTTPException(status_code=404, detail="Account not found")
    items = _list_strategy_reports(account_id=aid, days=days)
    return ListStrategyReportsResponse(
        accountId=aid,
        days=max(1, min(int(days), 60)),
        items=[StrategyReportSummary(**x) for x in items if isinstance(x, dict)],
    )


@app.post("/strategy/accounts/{account_id}/daily", response_model=StrategyReportResponse)
def generate_strategy_daily_report(account_id: str, req: StrategyDailyGenerateRequest) -> StrategyReportResponse:
    aid = (account_id or "").strip()
    if not aid:
        raise HTTPException(status_code=400, detail="account_id is required")
    acct = _get_broker_account_row(aid)
    if acct is None:
        raise HTTPException(status_code=404, detail="Account not found")

    d = (req.date or "").strip() or _today_cn_date_str()
    existing = _get_strategy_report_row(aid, d)
    if existing is not None and not req.force:
        return _strategy_report_response(
            report_id=str(existing["id"]),
            date=d,
            account_id=aid,
            account_title=str(acct["title"]),
            created_at=str(existing["createdAt"]),
            model=str(existing["model"]),
            output=existing["output"] if isinstance(existing.get("output"), dict) else {},
            input_snapshot=existing["inputSnapshot"] if isinstance(existing.get("inputSnapshot"), dict) else None,
        )

    # Context assembly (v0)
    strategy_prompt, _ = _get_strategy_prompt(aid)
    state_row = _get_account_state_row(aid) or {
        "overview": {},
        "positions": [],
        "conditionalOrders": [],
        "trades": [],
    }

    # Latest TradingView snapshots (all enabled screeners; capped).
    snaps: list[TvScreenerSnapshotDetail] = []
    tv_screeners_selected: list[dict[str, Any]] = []
    if req.includeTradingView:
        tv_screeners_selected = _list_enabled_tv_screeners(limit=6)
        for sc in tv_screeners_selected:
            sid = _norm_str(sc.get("id") or "")
            if not sid:
                continue
            s = _latest_tv_snapshot_for_screener(sid)
            if s is not None:
                snaps.append(s)

    # Include brief rows so debug can show tickers for newly-synced screeners.
    tv_latest: list[dict[str, Any]] = [_tv_snapshot_brief(s.id, max_rows=20) for s in snaps]

    # Candidate pool = union of TV rows, capped.
    pool: list[dict[str, str]] = []
    seen_sym: set[str] = set()
    if req.includeTradingView:
        for s in snaps:
            for c in _extract_tv_candidates(s):
                sym = c["symbol"]
                if sym in seen_sym:
                    continue
                seen_sym.add(sym)
                pool.append(c)
                if len(pool) >= max(1, min(int(req.maxCandidates), 20)):
                    break
            if len(pool) >= max(1, min(int(req.maxCandidates), 20)):
                break

    # Fallback candidate pool: include current holdings to ensure we can always generate a report.
    if req.includeAccountState:
        raw_positions = state_row.get("positions")
        pos_list: list[Any] = raw_positions if isinstance(raw_positions, list) else []
        for p in pos_list:
            if not isinstance(p, dict):
                continue
            ticker = _norm_str(p.get("ticker") or p.get("Ticker") or p.get("symbol") or p.get("Symbol") or "")
            if not ticker:
                continue
            name = _norm_str(p.get("name") or p.get("Name") or "")
            market = "HK" if (len(ticker) in (4, 5)) else "CN"
            currency = "HKD" if market == "HK" else "CNY"
            sym = f"{market}:{ticker}"
            if sym in seen_sym:
                continue
            seen_sym.add(sym)
            pool.append({"symbol": sym, "market": market, "currency": currency, "ticker": ticker, "name": name})
            if len(pool) >= max(1, min(int(req.maxCandidates), 20)):
                break

    # CN industry fund flow context: screenshot-style Top5×Date (names only), DB-first with best-effort sync.
    industry_flow_daily: dict[str, Any] = {"asOfDate": d, "days": 10, "topK": 5, "dates": [], "ranks": [], "matrix": [], "topByDate": []}
    industry_flow_error: str | None = None
    if req.includeIndustryFundFlow:
        try:
            industry_flow_daily = _market_cn_industry_fund_flow_top_by_date(as_of_date=d, days=10, top_k=5)
            if not (industry_flow_daily.get("dates") or []):
                try:
                    market_cn_industry_fund_flow_sync(
                        MarketCnIndustryFundFlowSyncRequest(date=d, days=10, topN=10, force=False)
                    )
                except Exception:
                    pass
                industry_flow_daily = _market_cn_industry_fund_flow_top_by_date(as_of_date=d, days=10, top_k=5)
        except Exception as e:
            industry_flow_error = str(e)

    # Leader stocks context: last 10 trading days leaders (DB-first), compact summary.
    leader_ctx: dict[str, Any] = {}
    if req.includeLeaders:
        try:
            leader_dates, leader_rows = _list_leader_stocks(days=10)
            latest_date = leader_dates[-1] if leader_dates else ""

            # Strategy context requirement:
            # - Include leaders from recent days (not only today)
            # - Cap to max 10 entries to control token size
            # - Prefer latest record per symbol (dedup by symbol)
            picked: list[dict[str, Any]] = []
            seen_leader_sym: set[str] = set()
            for r in leader_rows:
                sym = _norm_str(r.get("symbol") or "")
                if not sym or sym in seen_leader_sym:
                    continue
                seen_leader_sym.add(sym)
                picked.append(r)
                if len(picked) >= 10:
                    break

            leaders_out: list[dict[str, Any]] = []
            for r in picked:
                sym = _norm_str(r.get("symbol") or "")
                if not sym:
                    continue
                bars_resp = market_stock_bars(sym, days=60, force=True)
                bars = bars_resp.bars
                last_bar = bars[-1] if bars else {}
                chips_items: list[dict[str, str]] = []
                fund_flow_items: list[dict[str, str]] = []
                try:
                    chips_items = market_stock_chips(sym, days=30, force=True).items
                except Exception:
                    chips_items = []
                try:
                    fund_flow_items = market_stock_fund_flow(sym, days=30, force=True).items
                except Exception:
                    fund_flow_items = []
                chips_last = chips_items[-1] if chips_items else {}
                ff_last = fund_flow_items[-1] if fund_flow_items else {}
                feats = _bars_features(bars or [])
                live_breakdown = _compute_leader_live_score(
                    market=_norm_str(r.get("market") or ""),
                    feats=feats,
                    chips_summary=_chips_summary_last(chips_last),
                    ff_breakdown=_fund_flow_breakdown_last(ff_last),
                )
                # NOTE: Do NOT persist live score from Strategy context assembly.
                # Live score refresh should only happen on Leader "Generate today" or Dashboard "Sync all".
                ts2 = now_iso()
                entry = r.get("entryPrice")
                now_close = _safe_float(last_bar.get("close")) if isinstance(last_bar, dict) else None
                pct = ((float(now_close) - float(entry)) / float(entry)) if (now_close and entry) else None
                leaders_out.append(
                    {
                        "date": _norm_str(r.get("date") or ""),
                        "symbol": sym,
                        "ticker": _norm_str(r.get("ticker") or ""),
                        "name": _norm_str(r.get("name") or ""),
                        "score": r.get("score"),
                        "liveScore": live_breakdown.get("total"),
                        "liveScoreUpdatedAt": ts2,
                        "reason": _norm_str(r.get("reason") or ""),
                        "entryPrice": entry,
                        "nowClose": float(now_close) if now_close is not None else None,
                        "pctSinceEntry": float(pct) if pct is not None else None,
                        "current": {
                            "barDate": _norm_str(last_bar.get("date") if isinstance(last_bar, dict) else ""),
                            "close": last_bar.get("close") if isinstance(last_bar, dict) else None,
                            "volume": last_bar.get("volume") if isinstance(last_bar, dict) else None,
                            "amount": last_bar.get("amount") if isinstance(last_bar, dict) else None,
                        },
                        "chipsSummary": _chips_summary_last(chips_last),
                        "fundFlowBreakdown": _fund_flow_breakdown_last(ff_last),
                        "barsTail": bars[-6:] if bars else [],
                    }
                )

            leader_ctx = {"days": 10, "dates": leader_dates, "latestDate": latest_date, "leaders": leaders_out}
        except Exception as e:
            leader_ctx = {"days": 10, "dates": [], "leaders": [], "error": str(e)}

    # Stage 1: candidate selection WITHOUT per-stock deep context.
    sentiment_ctx: dict[str, Any] = {}
    if req.includeMarketSentiment:
        try:
            items = _list_cn_sentiment_days(as_of_date=d, days=5)
            latest = items[-1] if items else {}
            sentiment_ctx = {"asOfDate": d, "days": 5, "latest": latest, "items": items}
        except Exception as e:
            sentiment_ctx = {"asOfDate": d, "days": 5, "error": str(e), "items": []}
    base_snapshot: dict[str, Any] = {
        "date": d,
        "account": {
            "accountId": aid,
            "broker": "pingan",
            "accountTitle": acct["title"],
            "accountMasked": acct.get("accountMasked"),
        },
        "accountPrompt": strategy_prompt,
        "accountState": {} if not req.includeAccountState else state_row,
        "tradingView": {} if not req.includeTradingView else {"latest": tv_latest},
        "industryFundFlow": {}
        if not req.includeIndustryFundFlow
        else {"dailyTopInflow": industry_flow_daily, "error": industry_flow_error},
        "marketSentiment": {} if not req.includeMarketSentiment else sentiment_ctx,
        "leaderStocks": {} if not req.includeLeaders else leader_ctx,
        # Provide an explicit universe so stage 1 doesn't need to parse TV rows.
        "candidateUniverse": pool,
        # Stage 1 explicitly excludes deep context.
        "stocks": [],
    }

    stage1_req = {
        "date": d,
        "accountId": aid,
        "accountTitle": acct["title"],
        "accountPrompt": strategy_prompt,
        "context": base_snapshot,
    }

    stage1_resp: dict[str, Any] = {}
    stage1_candidates: list[dict[str, Any]] = []
    stage1_leader: dict[str, Any] = {"symbol": "", "reason": ""}
    stage1_error: str | None = None
    try:
        stage1_resp = _ai_strategy_candidates(payload=stage1_req)
        c_in = stage1_resp.get("candidates")
        stage1_candidates = c_in if isinstance(c_in, list) else []
        leader_in = stage1_resp.get("leader")
        stage1_leader = leader_in if isinstance(leader_in, dict) else stage1_leader
    except OSError as e:
        stage1_error = str(e)

    # Decide which symbols to fetch deep context for stage 2.
    selected_syms: list[str] = []
    for c in stage1_candidates[:5]:
        if not isinstance(c, dict):
            continue
        sym = _norm_str(c.get("symbol") or "")
        if sym and sym not in selected_syms:
            selected_syms.append(sym)
    if not selected_syms:
        # Fallback: pick from pool if stage1 failed/empty.
        for c in pool[:5]:
            sym = _norm_str(c.get("symbol") or "")
            if sym and sym not in selected_syms:
                selected_syms.append(sym)

    # Ensure market universe has selected symbols so we can fetch bars/chips/fund-flow.
    selected_meta: dict[str, dict[str, str]] = {c["symbol"]: c for c in pool if isinstance(c, dict) and _norm_str(c.get("symbol"))}
    for sym in selected_syms:
        meta = selected_meta.get(sym) or {}
        market = _norm_str(meta.get("market") or sym.split(":")[0] if ":" in sym else "CN")
        ticker = _norm_str(meta.get("ticker") or sym.split(":")[1] if ":" in sym else sym)
        currency = _norm_str(meta.get("currency") or ("HKD" if market == "HK" else "CNY"))
        name = _norm_str(meta.get("name") or ticker)
        _ensure_market_stock_basic(symbol=sym, market=market, ticker=ticker, name=name, currency=currency)

    # Stage 2: fetch deep context ONLY for selected symbols (if enabled).
    stock_context: list[dict[str, Any]] = []
    if req.includeStocks:
        for sym in selected_syms:
            meta = selected_meta.get(sym) or {}
            market = _norm_str(meta.get("market") or sym.split(":")[0] if ":" in sym else "CN")
            ticker = _norm_str(meta.get("ticker") or sym.split(":")[1] if ":" in sym else sym)
            currency = _norm_str(meta.get("currency") or ("HKD" if market == "HK" else "CNY"))
            name = _norm_str(meta.get("name") or "")

            bars_cached = _load_cached_bars(sym, days=60)
            bars = bars_cached
            bars_error: str | None = None
            bars_forced = True
            try:
                bars_resp = market_stock_bars(sym, days=60, force=True)
                bars = bars_resp.bars
            except Exception as e:
                # Fallback to cached bars if force refresh fails (AkShare may be flaky).
                bars = bars_cached
                bars_error = str(e)
            feats = _bars_features(bars)

            chips_cached = _load_cached_chips(sym, days=30)
            fund_flow_cached = _load_cached_fund_flow(sym, days=30)
            chips = chips_cached
            fund_flow = fund_flow_cached
            chips_error: str | None = None
            fund_flow_error: str | None = None
            try:
                chips = market_stock_chips(sym, days=30, force=True).items
            except Exception as e:
                chips = chips_cached
                chips_error = str(e)
            try:
                fund_flow = market_stock_fund_flow(sym, days=30, force=True).items
            except Exception as e:
                fund_flow = fund_flow_cached
                fund_flow_error = str(e)

            bars_tail = bars[-6:] if bars else []
            chips_tail = chips[-3:] if chips else []
            ff_tail = fund_flow[-5:] if fund_flow else []
            chips_last = chips_tail[-1] if chips_tail else {}
            ff_last = ff_tail[-1] if ff_tail else {}

            stock_context.append(
                {
                    "symbol": sym,
                    "market": market,
                    "ticker": ticker,
                    "name": name,
                    "currency": currency,
                    "deep": True,
                    "availability": {
                        "forced": True,
                        "barsCached": True if bars_cached else False,
                        "chipsCached": True if chips_cached else False,
                        "fundFlowCached": True if fund_flow_cached else False,
                        "barsForced": bars_forced,
                        "barsError": bars_error,
                        "chipsError": chips_error,
                        "fundFlowError": fund_flow_error,
                    },
                    "features": feats,
                    # Deep context guarantees:
                    "chipsSummary": _chips_summary_last(chips_last),
                    "fundFlowBreakdown": _fund_flow_breakdown_last(ff_last),
                    "barsTail": bars_tail,
                    "chipsTail": chips_tail,
                    "fundFlowTail": ff_tail,
                },
            )

    # Stage 2 snapshot: includes stage1 results + deep context for selected symbols.
    input_snapshot: dict[str, Any] = {
        "date": d,
        "account": {
            "accountId": aid,
            "broker": acct["broker"],
            "accountTitle": acct["title"],
            "accountMasked": acct.get("accountMasked") or "",
        },
        "accountPrompt": strategy_prompt,
        "accountState": {} if not req.includeAccountState else state_row,
        "tradingView": {} if not req.includeTradingView else {"latest": tv_latest},
        "industryFundFlow": {}
        if not req.includeIndustryFundFlow
        else {"dailyTopInflow": industry_flow_daily, "error": industry_flow_error},
        "marketSentiment": {} if not req.includeMarketSentiment else sentiment_ctx,
        "leaderStocks": {} if not req.includeLeaders else leader_ctx,
        "candidateUniverse": pool,
        "stage1": {"candidates": stage1_candidates[:5], "leader": stage1_leader, "error": stage1_error},
        "selectedSymbols": selected_syms,
        "stocks": [] if not req.includeStocks else stock_context,
    }

    # Call ai-service (stage 2 markdown)
    stage2_req = {
        "date": d,
        "accountId": aid,
        "accountTitle": acct["title"],
        "accountPrompt": strategy_prompt,
        "context": input_snapshot,
    }
    try:
        out = _ai_strategy_daily_markdown(payload=stage2_req)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"ai-service request failed: {e}") from e

    stage2_resp_raw: dict[str, Any] = out if isinstance(out, dict) else {"error": "Invalid strategy output", "raw": out}
    # Copy to avoid circular refs when attaching debug.
    output: dict[str, Any] = dict(stage2_resp_raw)
    # Normalize markdown for UI rendering (avoid headings on same line).
    if isinstance(output.get("markdown"), str):
        raw_md = output.get("markdown") or ""
        output.setdefault("markdownRaw", raw_md)
        output["markdown"] = _normalize_strategy_markdown(raw_md)
    # Two-stage debug info (stored in report.raw for UI debug)
    output["debug"] = {
        "stage1": {"request": stage1_req, "response": stage1_resp, "error": stage1_error},
        "stage2": {"request": stage2_req, "response": stage2_resp_raw},
    }
    model = _norm_str(output.get("model") or os.getenv("AI_MODEL") or "ai-service")

    report_id = str(uuid.uuid4())
    created_at = now_iso()
    _store_strategy_report(
        report_id=report_id,
        account_id=aid,
        date=d,
        created_at=created_at,
        model=model,
        input_snapshot=input_snapshot,
        output=output,
    )
    # Keep last 10 days of reports per account (best-effort).
    try:
        _prune_strategy_reports_keep_last_n_days(account_id=aid, keep_days=10)
    except Exception:
        pass
    return _strategy_report_response(
        report_id=report_id,
        date=d,
        account_id=aid,
        account_title=str(acct["title"]),
        created_at=created_at,
        model=model,
        output=output,
        input_snapshot=input_snapshot,
    )


@app.post("/leader/daily", response_model=LeaderDailyResponse)
def generate_leader_daily(req: LeaderDailyGenerateRequest) -> LeaderDailyResponse:
    d = (req.date or "").strip() or _today_cn_date_str()
    ts = now_iso()

    # If already generated for this date and not forced, return existing.
    if not req.force:
        dates, rows = _list_leader_stocks(days=10)
        existing = [r for r in rows if str(r.get("date") or "") == d]
        if existing:
            live_map = _get_leader_live_scores([_norm_str(r.get("symbol") or "") for r in existing if isinstance(r, dict)])
            leaders_out: list[LeaderPick] = []
            for r in existing:
                series = _bars_series_since(str(r["symbol"]), str(r["date"]), limit=60)
                now_close = series[-1]["close"] if series else None
                entry = r.get("entryPrice")
                pct = ((float(now_close) - float(entry)) / float(entry)) if (now_close and entry) else None
                live = live_map.get(_norm_str(r.get("symbol") or ""), {})
                src0 = r.get("sourceSignals")
                src = src0 if isinstance(src0, dict) else {}
                why0 = r.get("whyBullets")
                why = [str(x) for x in (why0 or [])] if isinstance(why0, list) else []
                bz0 = r.get("buyZone")
                bz = bz0 if isinstance(bz0, dict) else {}
                trg0 = r.get("triggers")
                trg = trg0 if isinstance(trg0, list) else []
                tp0 = r.get("targetPrice")
                tp = tp0 if isinstance(tp0, dict) else {}
                risks0 = r.get("riskPoints")
                risks = [str(x) for x in (risks0 or [])] if isinstance(risks0, list) else []
                leaders_out.append(
                    LeaderPick(
                        id=str(r["id"]),
                        date=str(r["date"]),
                        symbol=str(r["symbol"]),
                        market=str(r["market"]),
                        ticker=str(r["ticker"]),
                        name=str(r["name"]),
                        entryPrice=r.get("entryPrice"),
                        score=r.get("score"),
                        liveScore=live.get("liveScore"),
                        liveScoreUpdatedAt=str(live.get("updatedAt") or "") or None,
                        reason=str(r.get("reason") or ""),
                        whyBullets=why,
                        expectedDurationDays=r.get("expectedDurationDays"),
                        buyZone=bz,
                        triggers=trg,
                        invalidation=r.get("invalidation"),
                        targetPrice=tp,
                        probability=r.get("probability"),
                        risks=risks,
                        sourceSignals=src,
                        riskPoints=[str(x) for x in (r.get("riskPoints") or []) if str(x)],
                        createdAt=str(r.get("createdAt") or ""),
                        nowClose=float(now_close) if now_close is not None else None,
                        pctSinceEntry=float(pct) if pct is not None else None,
                        series=series,
                    )
                )
            return LeaderDailyResponse(date=d, leaders=leaders_out, debug=None)

    # Build TradingView latest snapshots (enabled screeners).
    snaps: list[TvScreenerSnapshotDetail] = []
    tv_screeners_selected = _list_enabled_tv_screeners(limit=6)
    for sc in tv_screeners_selected:
        sid = _norm_str(sc.get("id") or "")
        if not sid:
            continue
        s = _latest_tv_snapshot_for_screener(sid)
        if s is not None:
            snaps.append(s)
    tv_latest = [_tv_snapshot_brief(s.id, max_rows=20) for s in snaps]

    # Candidate universe from latest snapshots (TV).
    tv_pool: list[dict[str, str]] = []
    tv_seen: set[str] = set()
    tv_cap = max(1, min(int(req.maxCandidates), 120))
    for s in snaps:
        for c in _extract_tv_candidates(s):
            sym = c["symbol"]
            if sym in tv_seen:
                continue
            tv_seen.add(sym)
            tv_pool.append(c)
            if len(tv_pool) >= tv_cap:
                break
        if len(tv_pool) >= tv_cap:
            break

    # Build mainline snapshot (best-effort) and adjust candidate universe if a clear mainline exists.
    mainline_out: dict[str, Any] | None = None
    mainline_selected: dict[str, Any] | None = None
    aid_mainline = ""
    if bool(req.useMainline):
        try:
            accs = list_broker_accounts(broker="pingan")
            aid_mainline = accs[0].id if accs else ""
            if aid_mainline:
                topk = max(1, min(int(req.mainlineTopK), 10))
                mainline_out = _build_mainline_snapshot(
                    account_id=aid_mainline,
                    as_of_ts=ts,
                    universe_version="v0",
                    force=bool(req.force),
                    top_k=topk,
                )
                # Persist snapshot for UI/debug.
                _insert_cn_mainline_snapshot(
                    account_id=aid_mainline,
                    trade_date=str(mainline_out.get("tradeDate") or d),
                    as_of_ts=str(mainline_out.get("asOfTs") or ts),
                    universe_version=str(mainline_out.get("universeVersion") or "v0"),
                    ts=ts,
                    output=mainline_out,
                )
                _prune_cn_mainline_snapshots(account_id=aid_mainline, keep_days=10)
                sel = mainline_out.get("selected")
                mainline_selected = sel if isinstance(sel, dict) else None
        except Exception:
            mainline_out = None
            mainline_selected = None

    pool: list[dict[str, str]] = []
    seen: set[str] = set()
    if mainline_selected and aid_mainline:
        kind = str(mainline_selected.get("kind") or "").strip()
        name = str(mainline_selected.get("name") or "").strip()
        try:
            mem, _meta = _get_theme_members(kind=kind, name=name, trade_date=d, force=bool(req.force))
        except Exception:
            mem = []
        # Merge: theme members first, then TV, then holdings.
        spot_rows = []
        try:
            spot_rows = fetch_cn_a_spot()
        except Exception:
            spot_rows = []
        spot_map = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

        for t in (mem or [])[:300]:
            sym = f"CN:{t}"
            if sym in seen:
                continue
            seen.add(sym)
            s = spot_map.get(t)
            pool.append(
                {
                    "symbol": sym,
                    "market": "CN",
                    "currency": "CNY",
                    "ticker": t,
                    "name": (s.name if s is not None else t),
                }
            )
            if len(pool) >= tv_cap:
                break

        for c in tv_pool:
            sym = c["symbol"]
            if sym in seen:
                continue
            seen.add(sym)
            pool.append(c)
            if len(pool) >= tv_cap:
                break

        # Holdings (ensure included).
        try:
            holds = _rank_extract_holdings_pool(aid_mainline)
        except Exception:
            holds = []
        for h in holds:
            sym = _norm_str(h.get("symbol") or "")
            if not sym or sym in seen:
                continue
            seen.add(sym)
            pool.append(
                {
                    "symbol": sym,
                    "market": _norm_str(h.get("market") or "CN"),
                    "currency": _norm_str(h.get("currency") or "CNY"),
                    "ticker": _norm_str(h.get("ticker") or sym.split(":")[-1]),
                    "name": _norm_str(h.get("name") or ""),
                }
            )
            if len(pool) >= tv_cap:
                break
    else:
        pool = list(tv_pool)
        seen = set(tv_seen)

    # Industry flow matrix (names only).
    industry_daily = _market_cn_industry_fund_flow_top_by_date(as_of_date=d, days=10, top_k=5)

    # Leader history (last 10 trading days).
    _, hist_rows = _list_leader_stocks(days=10)
    leader_history = [
        {"date": str(r.get("date") or ""), "symbol": str(r.get("symbol") or ""), "score": r.get("score")}
        for r in hist_rows
    ]

    # Market per-stock summaries (compact) for candidate universe.
    market_ctx: list[dict[str, Any]] = []
    for c in pool[: max(1, min(int(req.maxCandidates), 20))]:
        sym = c["symbol"]
        _ensure_market_stock_basic(
            symbol=sym,
            market=c["market"],
            ticker=c["ticker"],
            name=c.get("name") or c["ticker"],
            currency=c["currency"],
        )
        bars_cached = _load_cached_bars(sym, days=60)
        bars = bars_cached
        try:
            bars = market_stock_bars(sym, days=60, force=True).bars
        except Exception:
            bars = bars_cached
        feats = _bars_features(bars)
        chips_cached = _load_cached_chips(sym, days=30)
        chips = chips_cached
        try:
            chips = market_stock_chips(sym, days=30, force=True).items
        except Exception:
            chips = chips_cached
        ff_cached = _load_cached_fund_flow(sym, days=30)
        ff = ff_cached
        try:
            ff = market_stock_fund_flow(sym, days=30, force=True).items
        except Exception:
            ff = ff_cached
        chips_tail = chips[-3:] if chips else []
        ff_tail = ff[-5:] if ff else []
        market_ctx.append(
            {
                "symbol": sym,
                "ticker": c["ticker"],
                "name": c.get("name") or "",
                "features": feats,
                "barsTail": (bars[-6:] if bars else []),
                "chipsSummary": _chips_summary_last(chips_tail[-1] if chips_tail else {}),
                "fundFlowBreakdown": _fund_flow_breakdown_last(ff_tail[-1] if ff_tail else {}),
            }
        )

    context = {
        "date": d,
        "tradingView": {"latest": tv_latest},
        "industryFundFlow": {"dailyTopInflow": industry_daily},
        "mainline": {"snapshot": mainline_out, "selected": mainline_selected},
        "candidateUniverse": pool,
        "market": market_ctx,
        "leaderHistory": leader_history,
    }
    stage_req = {"date": d, "context": context}
    stage_resp: dict[str, Any] = {}
    try:
        stage_resp = _ai_leader_daily(payload=stage_req)
    except OSError as e:
        stage_resp = {"date": d, "leaders": [], "error": str(e)}

    leaders_in = stage_resp.get("leaders")
    leaders_list: list[Any] = leaders_in if isinstance(leaders_in, list) else []
    picks: list[dict[str, Any]] = []
    for it in leaders_list[:2]:
        if not isinstance(it, dict):
            continue
        sym = _norm_str(it.get("symbol") or "")
        if not sym:
            continue
        # Ensure chosen symbol is within today universe.
        if sym not in seen:
            continue
        entry = _entry_close_for_date(sym, d)
        meta = next((x for x in pool if x.get("symbol") == sym), None) or {}
        picks.append(
            {
                "id": str(uuid.uuid4()),
                "symbol": sym,
                "market": _norm_str(it.get("market") or meta.get("market") or ""),
                "ticker": _norm_str(it.get("ticker") or meta.get("ticker") or ""),
                "name": _norm_str(it.get("name") or meta.get("name") or ""),
                "entryPrice": entry,
                "score": _safe_float(it.get("score")),
                "reason": _norm_str(it.get("reason") or ""),
                "whyBullets": it.get("whyBullets") if isinstance(it.get("whyBullets"), list) else [],
                "expectedDurationDays": int(it.get("expectedDurationDays") or 0) or None,
                "buyZone": it.get("buyZone") if isinstance(it.get("buyZone"), dict) else {},
                "triggers": it.get("triggers") if isinstance(it.get("triggers"), list) else [],
                "invalidation": _norm_str(it.get("invalidation") or "") or None,
                "targetPrice": it.get("targetPrice") if isinstance(it.get("targetPrice"), dict) else {},
                "probability": int(it.get("probability") or 0) or None,
                "sourceSignals": it.get("sourceSignals") if isinstance(it.get("sourceSignals"), dict) else {},
                "risks": it.get("risks") if isinstance(it.get("risks"), list) else [],
                "riskPoints": it.get("riskPoints") if isinstance(it.get("riskPoints"), list) else [],
            }
        )

    # IMPORTANT: If generating again for the same date (e.g., AM/PM runs),
    # keep at most 2 leaders per day by REPLACING the day's records.
    # Only delete existing rows if we have new picks (avoid losing previous leaders on AI failure).
    if req.force and picks:
        _delete_leader_stocks_for_date(d)

    _upsert_leader_stocks(date=d, items=picks, ts=ts)
    _prune_leader_stocks_keep_last_n_days(keep_days=10)

    # Refresh live score for all tracked leaders.
    # - When generating (force=true), we treat this as "refresh now" and force-refresh market data.
    # - Otherwise, keep it cached-only to reduce cost.
    try:
        _, rows2 = _list_leader_stocks(days=10)
        syms = [str(r.get("symbol") or "") for r in rows2 if isinstance(r, dict)]
        _refresh_leader_live_scores(symbols=syms, ts=ts, force_refresh_market=bool(req.force))
    except Exception:
        pass

    # Build response with computed series.
    _, saved_rows = _list_leader_stocks(days=10)
    today_rows = [r for r in saved_rows if str(r.get("date") or "") == d]
    live_map_today = _get_leader_live_scores([_norm_str(r.get("symbol") or "") for r in today_rows if isinstance(r, dict)])
    out: list[LeaderPick] = []
    for r in today_rows:
        series = _bars_series_since(str(r["symbol"]), str(r["date"]), limit=60)
        now_close = series[-1]["close"] if series else None
        entry = r.get("entryPrice")
        pct = ((float(now_close) - float(entry)) / float(entry)) if (now_close and entry) else None
        live = live_map_today.get(_norm_str(r.get("symbol") or ""), {})
        src0 = r.get("sourceSignals")
        src = src0 if isinstance(src0, dict) else {}
        why0 = r.get("whyBullets")
        why = [str(x) for x in (why0 or [])] if isinstance(why0, list) else []
        bz0 = r.get("buyZone")
        bz = bz0 if isinstance(bz0, dict) else {}
        trg0 = r.get("triggers")
        trg = trg0 if isinstance(trg0, list) else []
        tp0 = r.get("targetPrice")
        tp = tp0 if isinstance(tp0, dict) else {}
        risks0 = r.get("riskPoints")
        risks = [str(x) for x in (risks0 or [])] if isinstance(risks0, list) else []
        out.append(
            LeaderPick(
                id=str(r["id"]),
                date=str(r["date"]),
                symbol=str(r["symbol"]),
                market=str(r["market"]),
                ticker=str(r["ticker"]),
                name=str(r["name"]),
                entryPrice=r.get("entryPrice"),
                score=r.get("score"),
                liveScore=live.get("liveScore"),
                liveScoreUpdatedAt=str(live.get("updatedAt") or "") or None,
                reason=str(r.get("reason") or ""),
                whyBullets=why,
                expectedDurationDays=r.get("expectedDurationDays"),
                buyZone=bz,
                triggers=trg,
                invalidation=r.get("invalidation"),
                targetPrice=tp,
                probability=r.get("probability"),
                risks=risks,
                sourceSignals=src,
                riskPoints=[str(x) for x in (r.get("riskPoints") or []) if str(x)],
                createdAt=str(r.get("createdAt") or ""),
                nowClose=float(now_close) if now_close is not None else None,
                pctSinceEntry=float(pct) if pct is not None else None,
                series=series,
            )
        )

    return LeaderDailyResponse(date=d, leaders=out, debug={"request": stage_req, "response": stage_resp})


@app.get("/leader/mainline", response_model=MainlineSnapshotResponse)
def leader_mainline(
    accountId: str | None = None,
    tradeDate: str | None = None,
    universeVersion: str = "v0",
) -> MainlineSnapshotResponse:
    universe = (universeVersion or "").strip() or "v0"
    d = (tradeDate or "").strip() or None

    aid = (accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    cached = _get_cn_mainline_snapshot_latest(account_id=aid, trade_date=d, universe_version=universe)
    if cached is None:
        now_ts = now_iso()
        return MainlineSnapshotResponse(
            id="",
            tradeDate=d or _today_cn_date_str(),
            asOfTs=now_ts,
            accountId=aid,
            createdAt="",
            universeVersion=universe,
            riskMode=None,
            selected=None,
            themesTopK=[],
            debug={"status": "no_snapshot"},
        )
    out_raw = cached.get("output")
    out: dict[str, Any] = out_raw if isinstance(out_raw, dict) else {}
    themes_raw = out.get("themesTopK")
    themes0: list[Any] = themes_raw if isinstance(themes_raw, list) else []
    sel0 = out.get("selected")
    selected = MainlineTheme(**sel0) if isinstance(sel0, dict) else None
    return MainlineSnapshotResponse(
        id=str(cached.get("id") or ""),
        tradeDate=str(out.get("tradeDate") or d or _today_cn_date_str()),
        asOfTs=str(out.get("asOfTs") or ""),
        accountId=aid,
        createdAt=str(cached.get("createdAt") or ""),
        universeVersion=str(out.get("universeVersion") or universe),
        riskMode=str(out.get("riskMode") or "") or None,
        selected=selected,
        themesTopK=[MainlineTheme(**x) for x in themes0 if isinstance(x, dict)],
        debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
    )


@app.post("/leader/mainline/generate", response_model=MainlineSnapshotResponse)
def leader_mainline_generate(req: MainlineGenerateRequest) -> MainlineSnapshotResponse:
    universe = (req.universeVersion or "").strip() or "v0"
    top_k = max(1, min(int(req.topK), 10))
    as_of_ts = (req.asOfTs or "").strip() or now_iso()
    trade_date = (req.tradeDate or "").strip() or _cn_trade_date_from_iso_ts(as_of_ts)

    aid = (req.accountId or "").strip()
    if not aid:
        accs = list_broker_accounts(broker="pingan")
        aid = accs[0].id if accs else ""
    if not aid:
        raise HTTPException(status_code=400, detail="accountId is required")

    ts = now_iso()
    output = _build_mainline_snapshot(
        account_id=aid,
        as_of_ts=as_of_ts,
        universe_version=universe,
        force=bool(req.force),
        top_k=top_k,
    )
    snap_id = _insert_cn_mainline_snapshot(
        account_id=aid,
        trade_date=trade_date,
        as_of_ts=as_of_ts,
        universe_version=universe,
        ts=ts,
        output=output,
    )
    _prune_cn_mainline_snapshots(account_id=aid, keep_days=10)

    themes_raw = output.get("themesTopK")
    themes0: list[Any] = themes_raw if isinstance(themes_raw, list) else []
    sel0 = output.get("selected")
    selected = MainlineTheme(**sel0) if isinstance(sel0, dict) else None
    return MainlineSnapshotResponse(
        id=snap_id,
        tradeDate=str(output.get("tradeDate") or trade_date),
        asOfTs=str(output.get("asOfTs") or as_of_ts),
        accountId=aid,
        createdAt=ts,
        universeVersion=str(output.get("universeVersion") or universe),
        riskMode=str(output.get("riskMode") or "") or None,
        selected=selected,
        themesTopK=[MainlineTheme(**x) for x in themes0 if isinstance(x, dict)],
        debug=output.get("debug") if isinstance(output.get("debug"), dict) else None,
    )


@app.get("/leader", response_model=LeaderListResponse)
def list_leader_stocks(days: int = 10, force: bool = False) -> LeaderListResponse:
    dates, rows = _list_leader_stocks(days=days)
    # Optional: refresh latest market data for leader symbols so historical leaders' perf is up-to-date.
    # This can be expensive, so it is opt-in (used by UI refresh / chat reference).
    if force:
        # Deduplicate symbols and cap to keep the call cost bounded.
        syms: list[str] = []
        seen: set[str] = set()
        for r in rows:
            sym = _norm_str(r.get("symbol") or "")
            if not sym or sym in seen:
                continue
            seen.add(sym)
            syms.append(sym)
            if len(syms) >= 20:
                break
        for sym in syms:
            try:
                market_stock_bars(sym, days=60, force=True)
            except Exception:
                pass
        try:
            _refresh_leader_live_scores(symbols=syms, ts=now_iso(), force_refresh_market=True)
        except Exception:
            pass

    live_map = _get_leader_live_scores([_norm_str(r.get("symbol") or "") for r in rows if isinstance(r, dict)])
    out: list[LeaderPick] = []
    for r in rows:
        series = _bars_series_since(str(r["symbol"]), str(r["date"]), limit=60)
        now_close = series[-1]["close"] if series else None
        entry = r.get("entryPrice")
        pct = ((float(now_close) - float(entry)) / float(entry)) if (now_close and entry) else None
        live = live_map.get(_norm_str(r.get("symbol") or ""), {})
        src0 = r.get("sourceSignals")
        src = src0 if isinstance(src0, dict) else {}
        why0 = r.get("whyBullets")
        why = [str(x) for x in (why0 or [])] if isinstance(why0, list) else []
        bz0 = r.get("buyZone")
        bz = bz0 if isinstance(bz0, dict) else {}
        trg0 = r.get("triggers")
        trg = trg0 if isinstance(trg0, list) else []
        tp0 = r.get("targetPrice")
        tp = tp0 if isinstance(tp0, dict) else {}
        risks0 = r.get("riskPoints")
        risks = [str(x) for x in (risks0 or [])] if isinstance(risks0, list) else []
        out.append(
            LeaderPick(
                id=str(r["id"]),
                date=str(r["date"]),
                symbol=str(r["symbol"]),
                market=str(r["market"]),
                ticker=str(r["ticker"]),
                name=str(r["name"]),
                entryPrice=r.get("entryPrice"),
                score=r.get("score"),
                liveScore=live.get("liveScore"),
                liveScoreUpdatedAt=str(live.get("updatedAt") or "") or None,
                reason=str(r.get("reason") or ""),
                whyBullets=why,
                expectedDurationDays=r.get("expectedDurationDays"),
                buyZone=bz,
                triggers=trg,
                invalidation=r.get("invalidation"),
                targetPrice=tp,
                probability=r.get("probability"),
                risks=risks,
                sourceSignals=src,
                riskPoints=[str(x) for x in (r.get("riskPoints") or []) if str(x)],
                createdAt=str(r.get("createdAt") or ""),
                nowClose=float(now_close) if now_close is not None else None,
                pctSinceEntry=float(pct) if pct is not None else None,
                series=series,
            )
        )
    return LeaderListResponse(days=max(1, min(int(days), 30)), dates=dates, leaders=out)


@app.post("/dashboard/sync", response_model=DashboardSyncResponse)
def dashboard_sync(req: DashboardSyncRequest) -> DashboardSyncResponse:
    started_at = now_iso()
    t0 = time.perf_counter()
    steps: list[DashboardSyncStep] = []

    def step(name: str, fn) -> dict[str, Any]:
        st = time.perf_counter()
        ok = True
        msg: str | None = None
        meta: dict[str, Any] = {}
        try:
            out = fn()
            if isinstance(out, dict):
                meta = out
        except HTTPException as e:
            ok = False
            msg = str(e.detail)
        except Exception as e:
            ok = False
            msg = str(e)
        dur = int((time.perf_counter() - st) * 1000)
        steps.append(DashboardSyncStep(name=name, ok=ok, durationMs=dur, message=msg, meta=meta))
        return {"ok": ok, "message": msg, "meta": meta}

    # 1) Market sync (always refresh spot+quotes).
    def _sync_market() -> dict[str, Any]:
        resp = market_sync()
        body = bytes(resp.body).decode("utf-8", errors="replace") if hasattr(resp, "body") else ""
        try:
            j = json.loads(body) if body else {}
        except Exception:
            j = {}
        return {"response": j}

    step("market", _sync_market)

    # 2) Industry fund flow sync (force).
    def _sync_industry() -> dict[str, Any]:
        d = _today_cn_date_str()
        out = market_cn_industry_fund_flow_sync(MarketCnIndustryFundFlowSyncRequest(date=d, days=10, topN=10, force=True))
        return {"asOfDate": out.asOfDate, "rowsUpserted": out.rowsUpserted, "histRowsUpserted": out.histRowsUpserted, "message": out.message}

    step("industryFundFlow", _sync_industry)

    # 3) Market sentiment (force).
    def _sync_sentiment() -> dict[str, Any]:
        d = _today_cn_date_str()
        out = market_cn_sentiment_sync(MarketCnSentimentSyncRequest(date=d, force=True))
        last = out.items[-1].model_dump() if out.items else {}
        return {"asOfDate": out.asOfDate, "riskMode": str(last.get("riskMode") or ""), "premium": last.get("yesterdayLimitUpPremium"), "failedRate": last.get("failedLimitUpRate")}

    step("marketSentiment", _sync_sentiment)

    # 4) TradingView screeners (sync all enabled).
    screener_items: list[DashboardScreenerSyncItem] = []
    enabled = _list_enabled_tv_screeners(limit=50)
    for sc in enabled:
        sid = _norm_str(sc.get("id") or "")
        name = _norm_str(sc.get("name") or sid)
        if not sid:
            continue
        st = time.perf_counter()
        ok = True
        err: str | None = None
        captured_at: str | None = None
        row_count = 0
        filters_count = 0
        try:
            res = sync_tv_screener(sid)
            captured_at = res.capturedAt
            row_count = int(res.rowCount)
            meta = _tv_latest_snapshot_meta(sid) or {}
            filters_count = int(meta.get("filtersCount") or 0)
        except HTTPException as e:
            ok = False
            err = str(e.detail)
        except Exception as e:
            ok = False
            err = str(e)
        _dur = int((time.perf_counter() - st) * 1000)
        screener_items.append(
            DashboardScreenerSyncItem(
                id=sid,
                name=name,
                ok=ok,
                rowCount=row_count,
                capturedAt=captured_at,
                filtersCount=filters_count,
                error=err,
            )
        )
        # Attach per-screener details into the main step meta (aggregated later).
        _ = _dur

    failed = [it for it in screener_items if not it.ok]
    synced_count = len([it for it in screener_items if it.ok])

    # Coverage/missing check: ensure each enabled screener has a latest snapshot.
    missing: list[dict[str, str]] = []
    for sc in enabled:
        sid = _norm_str(sc.get("id") or "")
        name = _norm_str(sc.get("name") or sid)
        if not sid:
            continue
        meta2 = _tv_latest_snapshot_meta(sid)
        if meta2 is None:
            missing.append({"id": sid, "name": name, "reason": "No snapshots found"})
            continue
        if int(meta2.get("rowCount") or 0) <= 0:
            missing.append({"id": sid, "name": name, "reason": "RowCount=0 (grid not captured)"})

    # Record as a step for UI consistency.
    steps.append(
        DashboardSyncStep(
            name="screeners",
            ok=(len(failed) == 0 and len(missing) == 0),
            durationMs=0,
            message=None if (len(failed) == 0 and len(missing) == 0) else "Some screeners failed or missing",
            meta={
                "enabledCount": len(enabled),
                "syncedCount": synced_count,
                "failed": [it.model_dump() for it in failed],
                "missing": missing,
            },
        )
    )

    # 5) Leaders (force refresh) - run AFTER market/industry/sentiment/screeners so leader scoring can reference latest data.
    try:
        st = time.perf_counter()
        leaders_meta: dict[str, Any] = {}
        try:
            ls = list_leader_stocks(days=10, force=True)
            unique_syms = {str(x.symbol) for x in (ls.leaders or []) if getattr(x, "symbol", None)}
            leaders_meta = {
                "days": int(ls.days),
                "dates": len(ls.dates or []),
                "leaders": len(ls.leaders or []),
                "symbols": len(unique_syms),
            }
            steps.append(
                DashboardSyncStep(
                    name="leaders",
                    ok=True,
                    durationMs=int((time.perf_counter() - st) * 1000),
                    message=None,
                    meta=leaders_meta,
                )
            )
        except Exception as e:
            steps.append(
                DashboardSyncStep(
                    name="leaders",
                    ok=False,
                    durationMs=int((time.perf_counter() - st) * 1000),
                    message=str(e),
                    meta={},
                )
            )
    except Exception:
        # Best-effort: never fail the whole sync due to leaders.
        pass

    # 6) Mainline (force generate snapshot) - run last; does NOT generate leaders.
    # This is used for UI display and for Leaders "Generate today" candidate pool shaping.
    try:
        st = time.perf_counter()
        try:
            accs = list_broker_accounts(broker="pingan")
            aid = accs[0].id if accs else ""
            if not aid:
                raise RuntimeError("No broker account found (pingan).")
            as_of_ts = now_iso()
            out = _build_mainline_snapshot(
                account_id=aid,
                as_of_ts=as_of_ts,
                universe_version="v0",
                force=bool(req.force),
                top_k=3,
            )
            _insert_cn_mainline_snapshot(
                account_id=aid,
                trade_date=str(out.get("tradeDate") or _today_cn_date_str()),
                as_of_ts=str(out.get("asOfTs") or as_of_ts),
                universe_version=str(out.get("universeVersion") or "v0"),
                ts=as_of_ts,
                output=out,
            )
            _prune_cn_mainline_snapshots(account_id=aid, keep_days=10)
            sel = out.get("selected") if isinstance(out, dict) else None
            meta = {
                "tradeDate": str(out.get("tradeDate") or ""),
                "selected": ({"kind": str(sel.get("kind") or ""), "name": str(sel.get("name") or ""), "score": sel.get("compositeScore")} if isinstance(sel, dict) else None),
                "themes": len(out.get("themesTopK") or []) if isinstance(out.get("themesTopK"), list) else 0,
            }
            steps.append(
                DashboardSyncStep(
                    name="mainline",
                    ok=True,
                    durationMs=int((time.perf_counter() - st) * 1000),
                    message=None,
                    meta=meta,
                )
            )
        except Exception as e:
            steps.append(
                DashboardSyncStep(
                    name="mainline",
                    ok=False,
                    durationMs=int((time.perf_counter() - st) * 1000),
                    message=str(e),
                    meta={},
                )
            )
    except Exception:
        # Best-effort: never fail the whole sync due to mainline.
        pass

    finished_at = now_iso()
    ok_all = all(s.ok for s in steps)
    _total_ms = int((time.perf_counter() - t0) * 1000)
    # Ensure last step shows total duration if needed.
    if steps:
        steps[-1].durationMs = steps[-1].durationMs or _total_ms

    return DashboardSyncResponse(
        ok=ok_all,
        startedAt=started_at,
        finishedAt=finished_at,
        steps=steps,
        screener=DashboardScreenerSyncStatus(
            enabledCount=len(enabled),
            syncedCount=synced_count,
            failed=[DashboardScreenerSyncItem(**it.model_dump()) for it in failed],
            missing=missing,
            items=screener_items,
        ),
    )


@app.get("/dashboard/summary", response_model=DashboardSummaryResponse)
def dashboard_summary(accountId: str | None = None) -> DashboardSummaryResponse:
    as_of = _today_cn_date_str()
    # Accounts (pingan) + selected.
    accs = list_broker_accounts(broker="pingan")
    accounts_out = [
        DashboardAccountItem(
            id=a.id,
            broker=a.broker,
            title=a.title,
            accountMasked=a.accountMasked,
            updatedAt=a.updatedAt,
        )
        for a in accs
    ]
    selected_id = (accountId or "").strip() or (accounts_out[0].id if accounts_out else None)

    # Account state summary + holdings.
    state_sum: DashboardAccountStateSummary | None = None
    holdings_out: list[DashboardHoldingRow] = []
    if selected_id:
        st = _get_account_state_row(selected_id)
        if st and isinstance(st, dict):
            ov_raw = st.get("overview")
            ov: dict[str, Any] = ov_raw if isinstance(ov_raw, dict) else {}
            pos_raw = st.get("positions")
            positions: list[Any] = pos_raw if isinstance(pos_raw, list) else []
            orders_raw = st.get("conditionalOrders")
            orders: list[Any] = orders_raw if isinstance(orders_raw, list) else []
            trades_raw = st.get("trades")
            trades: list[Any] = trades_raw if isinstance(trades_raw, list) else []
            # Parse total assets for weight% calculation.
            total_assets_raw = ov.get("totalAssets") or ov.get("总资产") or ""
            total_assets_num = _safe_float(str(total_assets_raw).replace(",", "")) if total_assets_raw else 0.0
            state_sum = DashboardAccountStateSummary(
                accountId=selected_id,
                broker=str(st.get("broker") or "pingan"),
                updatedAt=str(st.get("updatedAt") or ""),
                cashAvailable=str(ov.get("cashAvailable") or ov.get("现金可用") or "") or None,
                totalAssets=str(ov.get("totalAssets") or ov.get("总资产") or "") or None,
                positionsCount=len(positions),
                conditionalOrdersCount=len(orders),
                tradesCount=len(trades),
            )
            for p in positions[:12]:
                if not isinstance(p, dict):
                    continue
                ticker_raw = (
                    p.get("ticker")
                    or p.get("Ticker")
                    or p.get("symbol")
                    or p.get("Symbol")
                    or p.get("code")
                    or p.get("Code")
                    or p.get("证券代码")
                    or p.get("股票代码")
                    or ""
                )
                ticker_s = _norm_str(ticker_raw or "")
                sym: str | None = None
                if ":" in ticker_s:
                    sym = ticker_s
                    ticker_s = ticker_s.split(":")[-1].strip()
                if ticker_s and sym is None:
                    mkt = "HK" if len(ticker_s) in (4, 5) else "CN"
                    sym = f"{mkt}:{ticker_s}"
                price_raw = p.get("price") or p.get("Price") or p.get("现价") or p.get("最新价") or ""
                cost_raw = p.get("cost") or p.get("Cost") or p.get("成本") or p.get("成本价") or ""
                qty_raw = p.get("qtyHeld") or p.get("qty") or p.get("Qty") or p.get("数量") or ""
                pnl_raw = p.get("pnl") or p.get("PnL") or p.get("盈亏") or p.get("浮动盈亏") or ""
                market_value_raw = (
                    p.get("marketValue")
                    or p.get("MarketValue")
                    or p.get("value")
                    or p.get("Value")
                    or p.get("市值")
                    or p.get("持仓市值")
                    or ""
                )

                price_num = _safe_float(str(price_raw).replace(",", "")) if price_raw else 0.0
                cost_num = _safe_float(str(cost_raw).replace(",", "")) if cost_raw else 0.0
                qty_num = _safe_float(str(qty_raw).replace(",", "")) if qty_raw else 0.0
                mv_num = _safe_float(str(market_value_raw).replace(",", "")) if market_value_raw else 0.0
                if mv_num <= 0.0 and price_num > 0.0 and qty_num > 0.0:
                    mv_num = price_num * qty_num
                weight_pct = (mv_num / total_assets_num * 100.0) if (mv_num > 0.0 and total_assets_num > 0.0) else None

                pnl_amount: float | None = None
                pnl_num = _safe_float(str(pnl_raw).replace(",", "")) if pnl_raw else 0.0
                if pnl_raw and pnl_num != 0.0:
                    pnl_amount = pnl_num
                elif price_num > 0.0 and cost_num > 0.0 and qty_num > 0.0:
                    pnl_amount = (price_num - cost_num) * qty_num
                holdings_out.append(
                    DashboardHoldingRow(
                        ticker=ticker_s,
                        symbol=sym,
                        name=_norm_str(p.get("name") or p.get("Name") or "") or None,
                        price=(price_num if price_num > 0.0 else None),
                        weightPct=weight_pct,
                        pnlAmount=pnl_amount,
                        qty=_norm_str(qty_raw) or None,
                        cost=_norm_str(cost_raw) or None,
                        pnl=_norm_str(pnl_raw) or None,
                        pnlPct=_norm_str(p.get("pnlPct") or p.get("PnLPct") or "") or None,
                    )
                )

    # Market status (counts + last sync).
    ms = market_status()
    market_status_out: dict[str, Any] = {
        "stocks": ms.stocks,
        "lastSyncAt": ms.lastSyncAt,
    }

    # Industry flow matrix (Top5×Date names only). No sync here; dashboard sync button is the source of truth.
    industry_daily = _market_cn_industry_fund_flow_top_by_date(as_of_date=as_of, days=5, top_k=5)
    # Industry flow (5D numeric): Top industries sorted by 5D sum, include daily net inflow series.
    industry_flow_5d: dict[str, Any] = {}
    try:
        ff = market_cn_industry_fund_flow(days=5, topN=30, asOfDate=as_of)
        rows_sorted = sorted((ff.top or []), key=lambda r: r.sum10d, reverse=True)[:10]
        industry_flow_5d = {
            "asOfDate": ff.asOfDate,
            "days": ff.days,
            "topN": 10,
            "dates": ff.dates,
            "top": [
                {
                    "industryCode": r.industryCode,
                    "industryName": r.industryName,
                    "sum5d": r.sum10d,
                    "netInflow": r.netInflow,
                    "series": [{"date": p.date, "netInflow": p.netInflow} for p in (r.series10d or [])],
                }
                for r in rows_sorted
            ],
        }
    except Exception:
        industry_flow_5d = {}

    # Market sentiment (last 5 days). No sync here; dashboard sync button is the source of truth.
    market_sentiment: dict[str, Any] = {}
    try:
        items = _list_cn_sentiment_days(as_of_date=as_of, days=5)
        market_sentiment = {
            "asOfDate": as_of,
            "days": 5,
            "items": items,
        }
    except Exception:
        market_sentiment = {}

    # Leaders summary: show latest leaders using cached market info (<=2), plus history list.
    # Do NOT force refresh here; Dashboard "Sync all" is the source of truth for refreshing leaders/liveScore.
    leaders_summary = DashboardLeadersSummary(latestDate=None, latest=[], history=[])
    try:
        leader_dates, leader_rows = _list_leader_stocks(days=10)
        latest_date = leader_dates[-1] if leader_dates else None
        leaders_summary.latestDate = latest_date
        live_map = _get_leader_live_scores([_norm_str(r.get("symbol") or "") for r in leader_rows if isinstance(r, dict)])
        # History (compact)
        leaders_summary.history = [
            {
                "date": _norm_str(r.get("date") or ""),
                "symbol": _norm_str(r.get("symbol") or ""),
                "ticker": _norm_str(r.get("ticker") or ""),
                "name": _norm_str(r.get("name") or ""),
                "score": r.get("score"),
            }
            for r in leader_rows[:20]
            if isinstance(r, dict)
        ]
        # Latest deep summary (cached)
        if latest_date:
            latest_rows = [r for r in leader_rows if _norm_str(r.get("date") or "") == latest_date][:2]
            latest_out: list[dict[str, Any]] = []
            for r in latest_rows:
                sym = _norm_str(r.get("symbol") or "")
                if not sym:
                    continue
                live = live_map.get(sym, {})
                bars_resp = market_stock_bars(sym, days=60, force=False)
                last_bar = (bars_resp.bars or [])[-1] if bars_resp.bars else {}
                chips_last: dict[str, str] = {}
                ff_last: dict[str, str] = {}
                try:
                    chips_items = market_stock_chips(sym, days=30, force=False).items
                    chips_last = chips_items[-1] if chips_items else {}
                except Exception:
                    chips_last = {}
                try:
                    ff_items = market_stock_fund_flow(sym, days=30, force=False).items
                    ff_last = ff_items[-1] if ff_items else {}
                except Exception:
                    ff_last = {}
                latest_out.append(
                    {
                        "date": latest_date,
                        "symbol": sym,
                        "ticker": _norm_str(r.get("ticker") or ""),
                        "name": _norm_str(r.get("name") or ""),
                        "score": r.get("score"),
                        "liveScore": live.get("liveScore"),
                        "liveScoreUpdatedAt": _norm_str(live.get("updatedAt") or "") or None,
                        "reason": _norm_str(r.get("reason") or ""),
                        "whyBullets": r.get("whyBullets") if isinstance(r.get("whyBullets"), list) else [],
                        "expectedDurationDays": r.get("expectedDurationDays"),
                        "buyZone": r.get("buyZone") if isinstance(r.get("buyZone"), dict) else {},
                        "triggers": r.get("triggers") if isinstance(r.get("triggers"), list) else [],
                        "invalidation": _norm_str(r.get("invalidation") or "") or None,
                        "targetPrice": r.get("targetPrice") if isinstance(r.get("targetPrice"), dict) else {},
                        "probability": r.get("probability"),
                        "risks": r.get("riskPoints") if isinstance(r.get("riskPoints"), list) else [],
                        "current": {
                            "barDate": _norm_str(last_bar.get("date") if isinstance(last_bar, dict) else ""),
                            "close": last_bar.get("close") if isinstance(last_bar, dict) else None,
                            "volume": last_bar.get("volume") if isinstance(last_bar, dict) else None,
                            "amount": last_bar.get("amount") if isinstance(last_bar, dict) else None,
                        },
                        "chipsSummary": _chips_summary_last(chips_last),
                        "fundFlowBreakdown": _fund_flow_breakdown_last(ff_last),
                    }
                )
            leaders_summary.latest = latest_out
    except Exception:
        pass

    # Screeners status: enabled screeners + latest snapshot meta.
    screeners = _list_enabled_tv_screeners(limit=50)
    screener_rows: list[DashboardScreenerStatusRow] = []
    for sc in screeners:
        sid = _norm_str(sc.get("id") or "")
        name = _norm_str(sc.get("name") or sid)
        meta = _tv_latest_snapshot_meta(sid) or {}
        screener_rows.append(
            DashboardScreenerStatusRow(
                id=sid,
                name=name,
                enabled=bool(sc.get("enabled")),
                updatedAt=_norm_str(sc.get("updatedAt") or "") or None,
                capturedAt=_norm_str(meta.get("capturedAt") or "") or None,
                rowCount=int(meta.get("rowCount") or 0),
                filtersCount=int(meta.get("filtersCount") or 0),
            )
        )

    return DashboardSummaryResponse(
        asOfDate=as_of,
        accounts=accounts_out,
        selectedAccountId=selected_id,
        accountState=state_sum,
        holdings=holdings_out,
        marketStatus=market_status_out,
        industryFundFlow={**industry_daily, "flow5d": industry_flow_5d},
        marketSentiment=market_sentiment,
        leaders=leaders_summary,
        screeners=screener_rows,
    )


def list_system_prompt_presets() -> list[SystemPromptPresetSummary]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT id, title, updated_at FROM system_prompts ORDER BY updated_at DESC",
        ).fetchall()
        return [
            SystemPromptPresetSummary(id=str(r[0]), title=str(r[1]), updatedAt=str(r[2]))
            for r in rows
        ]


def get_system_prompt_preset(preset_id: str) -> SystemPromptPresetDetail | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT id, title, content FROM system_prompts WHERE id = ?",
            (preset_id,),
        ).fetchone()
        if row is None:
            return None
        return SystemPromptPresetDetail(id=str(row[0]), title=str(row[1]), content=str(row[2]))


def create_system_prompt_preset(title: str, content: str) -> str:
    preset_id = str(uuid.uuid4())
    ts = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO system_prompts(id, title, content, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (preset_id, title, content, ts, ts),
        )
        conn.commit()
    return preset_id


def _insert_broker_snapshot(
    *,
    broker: str,
    account_id: str | None,
    captured_at: str,
    kind: str,
    sha256: str,
    image_path: str,
    extracted: dict[str, Any],
) -> str:
    """
    Insert a broker snapshot; dedupe by (broker, sha256).
    """
    with _connect() as conn:
        existing = conn.execute(
            "SELECT id FROM broker_snapshots WHERE broker = ? AND account_id IS ? AND sha256 = ?",
            (broker, account_id, sha256),
        ).fetchone()
        if existing is not None:
            return str(existing[0])

        snapshot_id = str(uuid.uuid4())
        ts = now_iso()
        conn.execute(
            """
            INSERT INTO broker_snapshots(
              id, broker, account_id, captured_at, kind, sha256, image_path, extracted_json, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                broker,
                account_id,
                captured_at,
                kind,
                sha256,
                image_path,
                json.dumps(extracted, ensure_ascii=False),
                ts,
            ),
        )
        conn.commit()
        return snapshot_id


def _get_broker_snapshot(snapshot_id: str) -> BrokerSnapshotDetail | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, broker, account_id, captured_at, kind, image_path, extracted_json, created_at
            FROM broker_snapshots
            WHERE id = ?
            """,
            (snapshot_id,),
        ).fetchone()
        if row is None:
            return None
        extracted = json.loads(str(row[6]) or "{}")
        return BrokerSnapshotDetail(
            id=str(row[0]),
            broker=str(row[1]),
            accountId=str(row[2]) if row[2] is not None else None,
            capturedAt=str(row[3]),
            kind=str(row[4]),
            imagePath=str(row[5]),
            extracted=extracted if isinstance(extracted, dict) else {"raw": extracted},
            createdAt=str(row[7]),
        )


def _list_broker_snapshots(
    *,
    broker: str,
    account_id: str | None,
    limit: int = 20,
) -> list[BrokerSnapshotSummary]:
    limit2 = max(1, min(int(limit), 100))
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, broker, account_id, captured_at, kind, created_at
            FROM broker_snapshots
            WHERE broker = ? AND account_id IS ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (broker, account_id, limit2),
        ).fetchall()
        return [
            BrokerSnapshotSummary(
                id=str(r[0]),
                broker=str(r[1]),
                accountId=str(r[2]) if r[2] is not None else None,
                capturedAt=str(r[3]),
                kind=str(r[4]),
                createdAt=str(r[5]),
            )
            for r in rows
        ]


def update_system_prompt_preset(
    preset_id: str,
    *,
    title: str | None,
    content: str | None,
) -> bool:
    existing = get_system_prompt_preset(preset_id)
    if existing is None:
        return False
    new_title = existing.title if title is None else title
    new_content = existing.content if content is None else content
    ts = now_iso()
    with _connect() as conn:
        conn.execute(
            """
            UPDATE system_prompts
            SET title = ?, content = ?, updated_at = ?
            WHERE id = ?
            """,
            (new_title, new_content, ts, preset_id),
        )
        conn.commit()
    return True


def delete_system_prompt_preset(preset_id: str) -> bool:
    with _connect() as conn:
        cur = conn.execute("DELETE FROM system_prompts WHERE id = ?", (preset_id,))
        conn.commit()
        return (cur.rowcount or 0) > 0


def get_active_system_prompt() -> SystemPromptPresetDetail | None:
    active_id = get_setting("active_system_prompt_id")
    if not active_id:
        return None
    return get_system_prompt_preset(active_id)


@app.get("/system-prompts", response_model=ListSystemPromptPresetsResponse)
def get_system_prompts() -> ListSystemPromptPresetsResponse:
    return ListSystemPromptPresetsResponse(items=list_system_prompt_presets())


@app.post("/system-prompts", response_model=CreateSystemPromptPresetResponse)
def post_system_prompt(req: CreateSystemPromptPresetRequest) -> CreateSystemPromptPresetResponse:
    preset_id = create_system_prompt_preset(req.title.strip() or "Untitled", req.content)
    # Newly created preset becomes active by default.
    set_setting("active_system_prompt_id", preset_id)
    return CreateSystemPromptPresetResponse(id=preset_id)


@app.get("/system-prompts/active", response_model=ActiveSystemPromptResponse)
def get_active_system_prompt_api() -> ActiveSystemPromptResponse:
    active = get_active_system_prompt()
    if active:
        return ActiveSystemPromptResponse(id=active.id, title=active.title, content=active.content)
    legacy = get_setting("system_prompt") or ""
    return ActiveSystemPromptResponse(id=None, title="Legacy", content=legacy)


@app.put("/system-prompts/active")
def put_active_system_prompt(req: SetActiveSystemPromptRequest) -> JSONResponse:
    preset_id = req.id
    if preset_id is None or preset_id == "":
        set_setting("active_system_prompt_id", "")
        return JSONResponse({"ok": True})
    if get_system_prompt_preset(preset_id) is None:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
    set_setting("active_system_prompt_id", preset_id)
    return JSONResponse({"ok": True})


@app.get("/system-prompts/{preset_id}", response_model=SystemPromptPresetDetail)
def get_system_prompt_preset_api(preset_id: str) -> SystemPromptPresetDetail:
    preset = get_system_prompt_preset(preset_id)
    if preset is None:
        raise HTTPException(status_code=404, detail="Not found")
    return preset


@app.put("/system-prompts/{preset_id}")
def put_system_prompt_preset(preset_id: str, req: UpdateSystemPromptPresetRequest) -> JSONResponse:
    ok = update_system_prompt_preset(
        preset_id,
        title=req.title.strip() or "Untitled",
        content=req.content,
    )
    if not ok:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
    return JSONResponse({"ok": True})


@app.delete("/system-prompts/{preset_id}")
def delete_system_prompt_api(preset_id: str) -> JSONResponse:
    deleted = delete_system_prompt_preset(preset_id)
    if not deleted:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
    active_id = get_setting("active_system_prompt_id")
    if active_id == preset_id:
        set_setting("active_system_prompt_id", "")
    return JSONResponse({"ok": True})

if __name__ == "__main__":
    import uvicorn

    config = load_config()
    uvicorn.run("main:app", host=config.host, port=config.port, reload=True)
