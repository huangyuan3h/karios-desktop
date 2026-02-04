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
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, cast
from zoneinfo import ZoneInfo

import duckdb
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query, Request
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
        db_path=os.getenv("DATABASE_PATH", str(Path(__file__).with_name("karios.duckdb"))),
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

# DuckDB schema initialization guard (avoid concurrent CREATE TABLE).
_schema_lock = threading.Lock()
_schema_initialized_paths: set[str] = set()


class DbLockedError(RuntimeError):
    pass


@app.exception_handler(DbLockedError)
def _handle_db_locked(_request: Request, exc: DbLockedError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "ok": False,
            "error": "Database is locked by another process. Close DBeaver or open the DB in read-only mode.",
            "detail": str(exc),
        },
    )


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
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
          rows_json TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
          id TEXT PRIMARY KEY,
          kind TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          duration_ms INTEGER,
          target_symbols INTEGER NOT NULL DEFAULT 0,
          ok_steps INTEGER NOT NULL DEFAULT 0,
          failed_steps INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          detail_json TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_run_steps (
          id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          step TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          duration_ms INTEGER,
          ok_count INTEGER,
          failed_count INTEGER,
          error TEXT,
          detail_json TEXT NOT NULL
        )
        """,
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_runs_trade_date ON sync_runs(trade_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_run_steps_run_id ON sync_run_steps(run_id)")
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
          raw_json TEXT NOT NULL
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
          PRIMARY KEY(symbol, date)
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
          PRIMARY KEY(symbol, date)
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
          PRIMARY KEY(symbol, date)
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
          market_turnover_cny REAL NOT NULL DEFAULT 0.0,
          market_volume REAL NOT NULL DEFAULT 0.0,
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
    # Add market turnover/volume columns to existing DBs.
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(market_cn_sentiment_daily)").fetchall()}
    if "market_turnover_cny" not in cols:
        conn.execute("ALTER TABLE market_cn_sentiment_daily ADD COLUMN market_turnover_cny REAL NOT NULL DEFAULT 0.0;")
    if "market_volume" not in cols:
        conn.execute("ALTER TABLE market_cn_sentiment_daily ADD COLUMN market_volume REAL NOT NULL DEFAULT 0.0;")
    # Defensive cleanup: remove any accidental test rows that may pollute the UI.
    # (Rules are stored as a JSON array string; production data must never include 'seed'.)
    try:
        conn.execute("DELETE FROM market_cn_sentiment_daily WHERE rules_json LIKE '%seed%'")
    except Exception:
        pass

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
          trades_json TEXT NOT NULL
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
          updated_at TEXT NOT NULL
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
          output_json TEXT NOT NULL
        )
        """,
    )
    # --- Trade journal module (v0) ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_journals (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          content_md TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
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
          UNIQUE(account_id, as_of_date, universe_version)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cn_rank_snapshots_date ON cn_rank_snapshots(as_of_date DESC)",
    )
    # --- Quant 2D rank learning loop (v0) ---
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quant_2d_rank_events (
          id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          ticker TEXT NOT NULL,
          name TEXT NOT NULL,
          buy_price REAL NOT NULL,
          buy_price_src TEXT NOT NULL,
          raw_score REAL NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(account_id, as_of_ts, symbol)
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_quant_2d_rank_events_date ON quant_2d_rank_events(as_of_date DESC)",
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quant_2d_outcomes (
          event_id TEXT PRIMARY KEY,
          account_id TEXT NOT NULL,
          as_of_ts TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          symbol TEXT NOT NULL,
          buy_price REAL NOT NULL,
          t1_date TEXT NOT NULL,
          t2_date TEXT NOT NULL,
          close_t1 REAL,
          close_t2 REAL,
          low_min REAL,
          ret2d_avg_pct REAL,
          dd2d_pct REAL,
          win INTEGER NOT NULL,
          labeled_at TEXT NOT NULL
        )
        """,
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_quant_2d_outcomes_date ON quant_2d_outcomes(as_of_date DESC)",
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quant_2d_calibration_cache (
          key TEXT PRIMARY KEY,
          updated_at TEXT NOT NULL,
          output_json TEXT NOT NULL
        )
        """,
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
          UNIQUE(account_id, as_of_ts, slot, universe_version)
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
          output_json TEXT NOT NULL
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


def _ensure_schema_once(conn: duckdb.DuckDBPyConnection, db_path: str) -> None:
    with _schema_lock:
        if db_path in _schema_initialized_paths:
            return
        _init_schema(conn)
        _schema_initialized_paths.add(db_path)


def _connect() -> duckdb.DuckDBPyConnection:
    default_db = str(Path(__file__).with_name("karios.duckdb"))
    db_path = os.getenv("DATABASE_PATH", default_db)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = duckdb.connect(db_path)
    except duckdb.IOException as e:
        msg = str(e)
        if "Could not set lock on file" in msg:
            raise DbLockedError(msg) from e
        raise
    except duckdb.BinderException as e:
        msg = str(e)
        if "Unique file handle conflict" in msg:
            raise DbLockedError(msg) from e
        raise
    _ensure_schema_once(conn, db_path)
    return conn


def get_setting(key: str) -> str | None:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])


def set_setting(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            """
            MERGE INTO settings AS t
            USING (SELECT ? AS key, ? AS value) AS s
            ON t.key = s.key
            WHEN MATCHED THEN UPDATE SET value = s.value
            WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)
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


_eod_scheduler: BackgroundScheduler | None = None
_eod_scheduler_started = False
_eod_scheduler_lock = threading.Lock()


def _should_start_eod_scheduler() -> bool:
    """
    Start the EOD sync scheduler for desktop usage.
    It is disabled automatically in pytest to keep tests deterministic.
    """
    if os.getenv("PYTEST_CURRENT_TEST"):
        return False
    v = str(os.getenv("ENABLE_EOD_SYNC_SCHEDULER", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _parse_hhmm(value: str, *, default: str) -> tuple[int, int]:
    s = (value or "").strip() or default
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    if not m:
        s = default
        m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    hh = int(m.group(1)) if m else 15
    mm = int(m.group(2)) if m else 15
    hh = max(0, min(hh, 23))
    mm = max(0, min(mm, 59))
    return hh, mm


def _start_eod_scheduler() -> None:
    global _eod_scheduler_started, _eod_scheduler
    if not _should_start_eod_scheduler():
        return
    with _eod_scheduler_lock:
        if _eod_scheduler_started and _eod_scheduler is not None:
            return

        tz_name = (os.getenv("EOD_SYNC_TZ", "") or "").strip() or "Asia/Shanghai"
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("Asia/Shanghai")
        hh, mm = _parse_hhmm(os.getenv("EOD_SYNC_TIME", "") or "", default="15:15")

        sched = BackgroundScheduler(timezone=tz)
        trigger = CronTrigger(hour=hh, minute=mm, timezone=tz)

        def _job() -> None:
            try:
                run_eod_sync(source="scheduler")
            except Exception:
                # Best-effort: never crash the scheduler thread.
                pass

        sched.add_job(
            _job,
            trigger=trigger,
            id="eod_sync_v0",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60 * 60,
        )
        sched.start()
        _eod_scheduler = sched
        _eod_scheduler_started = True


def _stop_eod_scheduler() -> None:
    global _eod_scheduler
    with _eod_scheduler_lock:
        if _eod_scheduler is None:
            return
        try:
            _eod_scheduler.shutdown(wait=False)
        except Exception:
            pass
        _eod_scheduler = None


@app.on_event("startup")
def _on_startup() -> None:
    _start_intraday_scheduler()
    _start_eod_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_eod_scheduler()


class SyncRunStepStatus(BaseModel):
    step: str
    status: str
    startedAt: str
    endedAt: str | None = None
    durationMs: int | None = None
    okCount: int | None = None
    failedCount: int | None = None
    error: str | None = None


class SyncRunStatus(BaseModel):
    id: str
    kind: str
    tradeDate: str
    status: str
    startedAt: str
    endedAt: str | None = None
    durationMs: int | None = None
    targetSymbols: int = 0
    okSteps: int = 0
    failedSteps: int = 0
    error: str | None = None
    detail: dict[str, Any] = {}
    steps: list[SyncRunStepStatus] = []


class SyncRunSummary(BaseModel):
    id: str
    kind: str
    tradeDate: str
    status: str
    startedAt: str
    endedAt: str | None = None
    durationMs: int | None = None
    targetSymbols: int = 0
    okSteps: int = 0
    failedSteps: int = 0
    error: str | None = None


class SyncRunsResponse(BaseModel):
    items: list[SyncRunSummary]
    total: int
    offset: int
    limit: int


class SyncStatusResponse(BaseModel):
    ok: bool = True
    lastRun: SyncRunStatus | None = None


class SyncTriggerRequest(BaseModel):
    force: bool = True
    symbols: list[str] | None = None


class SyncTriggerResponse(BaseModel):
    ok: bool
    runId: str
    status: str


_eod_sync_running = False
_eod_sync_running_lock = threading.Lock()


def _sync_now_ms() -> int:
    return int(time.time() * 1000.0)


def _sync_try_acquire() -> bool:
    global _eod_sync_running
    with _eod_sync_running_lock:
        if _eod_sync_running:
            return False
        _eod_sync_running = True
        return True


def _sync_release() -> None:
    global _eod_sync_running
    with _eod_sync_running_lock:
        _eod_sync_running = False


def _sync_run_insert(*, run_id: str, kind: str, trade_date: str, status: str, started_at: str, detail: dict[str, Any]) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sync_runs(
              id, kind, trade_date, status, started_at, ended_at, duration_ms,
              target_symbols, ok_steps, failed_steps, error, detail_json
            )
            VALUES(?, ?, ?, ?, ?, NULL, NULL, 0, 0, 0, NULL, ?)
            """,
            (run_id, kind, trade_date, status, started_at, json.dumps(detail or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()


def _sync_run_update(
    *,
    run_id: str,
    status: str,
    ended_at: str | None,
    duration_ms: int | None,
    target_symbols: int,
    ok_steps: int,
    failed_steps: int,
    error: str | None,
    detail: dict[str, Any],
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE sync_runs
            SET
              status = ?,
              ended_at = ?,
              duration_ms = ?,
              target_symbols = ?,
              ok_steps = ?,
              failed_steps = ?,
              error = ?,
              detail_json = ?
            WHERE id = ?
            """,
            (
                status,
                ended_at,
                duration_ms,
                int(target_symbols),
                int(ok_steps),
                int(failed_steps),
                error,
                json.dumps(detail or {}, ensure_ascii=False, default=str),
                run_id,
            ),
        )
        conn.commit()


def _sync_step_insert(
    *,
    step_id: str,
    run_id: str,
    step: str,
    status: str,
    started_at: str,
    detail: dict[str, Any],
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sync_run_steps(
              id, run_id, step, status, started_at, ended_at, duration_ms,
              ok_count, failed_count, error, detail_json
            )
            VALUES(?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?)
            """,
            (step_id, run_id, step, status, started_at, json.dumps(detail or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()


def _sync_step_update(
    *,
    step_id: str,
    status: str,
    ended_at: str,
    duration_ms: int,
    ok_count: int | None,
    failed_count: int | None,
    error: str | None,
    detail: dict[str, Any],
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE sync_run_steps
            SET
              status = ?,
              ended_at = ?,
              duration_ms = ?,
              ok_count = ?,
              failed_count = ?,
              error = ?,
              detail_json = ?
            WHERE id = ?
            """,
            (
                status,
                ended_at,
                int(duration_ms),
                ok_count,
                failed_count,
                error,
                json.dumps(detail or {}, ensure_ascii=False, default=str),
                step_id,
            ),
        )
        conn.commit()


def _sync_step_run(
    *,
    run_id: str,
    step: str,
    fn,
) -> tuple[bool, int | None, int | None, dict[str, Any], str | None]:
    step_id = str(uuid.uuid4())
    started_at = now_iso()
    t0 = _sync_now_ms()
    _sync_step_insert(step_id=step_id, run_id=run_id, step=step, status="running", started_at=started_at, detail={})
    try:
        ok_count, failed_count, detail = fn()
        ended_at = now_iso()
        dt = _sync_now_ms() - t0
        _sync_step_update(
            step_id=step_id,
            status="ok",
            ended_at=ended_at,
            duration_ms=dt,
            ok_count=ok_count,
            failed_count=failed_count,
            error=None,
            detail=detail,
        )
        return True, ok_count, failed_count, detail, None
    except Exception as e:
        ended_at = now_iso()
        dt = _sync_now_ms() - t0
        err = f"{type(e).__name__}: {repr(e)}"
        _sync_step_update(
            step_id=step_id,
            status="failed",
            ended_at=ended_at,
            duration_ms=dt,
            ok_count=None,
            failed_count=None,
            error=err,
            detail={"error": err},
        )
        return False, None, None, {"error": err}, err


def _collect_eod_targets(*, override_symbols: list[str] | None, limit: int = 200) -> list[dict[str, Any]]:
    """
    Build target symbol list for EOD sync.
    Priority:
    - explicit override (manual trigger)
    - latest enabled TradingView screeners (TV snapshot pool)
    - holdings pool (first pingan account)
    - fallback to local market universe cache (market_stocks)
    """
    if isinstance(override_symbols, list) and override_symbols:
        out0 = []
        for s in override_symbols:
            sym = str(s or "").strip().upper()
            if sym:
                out0.append({"symbol": sym})
        # Resolve via market cache when possible.
        basics = market_resolve_stocks(symbols=[x["symbol"] for x in out0])
        by_sym = {b.symbol: b for b in basics}
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for x in out0:
            sym = str(x["symbol"])
            if sym in seen:
                continue
            seen.add(sym)
            b = by_sym.get(sym)
            if b is not None:
                out.append(
                    {
                        "symbol": b.symbol,
                        "market": b.market,
                        "ticker": b.ticker,
                        "name": b.name,
                        "currency": b.currency,
                    }
                )
            else:
                # Best-effort: infer by ticker length.
                ticker = sym.split(":")[-1]
                market = "HK" if len(ticker) in (4, 5) else "CN"
                currency = "HKD" if market == "HK" else "CNY"
                out.append({"symbol": sym, "market": market, "ticker": ticker, "name": ticker, "currency": currency})
        return out[: max(1, min(int(limit), 500))]

    pool = _rank_extract_tv_pool(max_screeners=20, max_rows=300)
    accs = list_broker_accounts(broker="pingan")
    aid = accs[0].id if accs else ""
    pool2 = pool + (_rank_extract_holdings_pool(aid) if aid else [])
    if not pool2:
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, market, ticker, name, currency
                FROM market_stocks
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        pool2 = [
            {
                "symbol": str(r[0]),
                "market": str(r[1]),
                "ticker": str(r[2]),
                "name": str(r[3]),
                "currency": str(r[4]),
            }
            for r in rows
        ]

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for it in pool2:
        sym = str(it.get("symbol") or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(
            {
                "symbol": sym,
                "market": str(it.get("market") or "CN"),
                "ticker": str(it.get("ticker") or sym.split(":")[-1]),
                "name": str(it.get("name") or ""),
                "currency": str(it.get("currency") or ("HKD" if str(it.get("market") or "") == "HK" else "CNY")),
            }
        )
        if len(out) >= max(1, min(int(limit), 500)):
            break
    return out


def _ensure_market_stocks_basic_bulk(items: list[dict[str, Any]]) -> int:
    rows = [(str(it.get("symbol") or "").strip().upper(), it) for it in items]
    rows = [(s, it) for s, it in rows if s]
    if not rows:
        return 0
    syms = [s for s, _ in rows]
    placeholders = ",".join(["?"] * len(syms))
    with _connect() as conn:
        existing_rows = conn.execute(
            f"SELECT symbol FROM market_stocks WHERE symbol IN ({placeholders})",
            tuple(syms),
        ).fetchall()
        existing = {str(r[0]) for r in existing_rows if r and r[0]}
        ts = now_iso()
        inserted = 0
        for sym, it in rows:
            if sym in existing:
                continue
            market = str(it.get("market") or "CN").strip().upper() or "CN"
            ticker = str(it.get("ticker") or sym.split(":")[-1]).strip()
            name = str(it.get("name") or "").strip() or ticker
            currency = str(it.get("currency") or ("HKD" if market == "HK" else "CNY")).strip() or "CNY"
            conn.execute(
                """
                INSERT INTO market_stocks(symbol, market, ticker, name, currency, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (sym, market, ticker, name, currency, ts),
            )
            inserted += 1
        conn.commit()
    return inserted


def _call_with_retry(fn, *, tries: int = 3, base_sleep_s: float = 0.4, max_sleep_s: float = 2.0):
    """
    Best-effort retry wrapper for providers not protected by `_with_retry` yet.
    """
    tries2 = max(1, min(int(tries), 5))
    last: Exception | None = None
    for i in range(tries2):
        try:
            return fn()
        except Exception as e:
            last = e
            if i >= tries2 - 1:
                raise
            sleep_s = min(float(max_sleep_s), float(base_sleep_s) * (2**i))
            sleep_s = sleep_s * (0.7 + (time.time() % 1.0) * 0.6)
            time.sleep(max(0.0, sleep_s))
    if last is not None:
        raise last
    raise RuntimeError("Retry wrapper failed unexpectedly.")


def _batch_fetch_cn(
    items: list[dict[str, Any]],
    *,
    workers: int,
    fetch_one,
) -> tuple[int, int, list[tuple[str, list[dict[str, Any]]]]]:
    """
    Fetch per-ticker daily data in parallel, without sharing DB connections across threads.
    Returns (ok, failed, results) where results contains (symbol, items).
    """
    cn = [it for it in items if str(it.get("market") or "").upper() == "CN"]
    cn = [it for it in cn if str(it.get("ticker") or "").strip()]
    if not cn:
        return 0, 0, []
    ok = 0
    failed = 0
    results: list[tuple[str, list[dict[str, Any]]]] = []
    workers2 = max(1, min(int(workers), 16))
    with ThreadPoolExecutor(max_workers=workers2) as pool:
        futs = {}
        for it in cn:
            sym = str(it.get("symbol") or "").strip().upper()
            ticker = str(it.get("ticker") or "").strip()
            futs[pool.submit(fetch_one, sym, ticker)] = sym
        for fut in as_completed(futs):
            sym = futs[fut]
            try:
                items2 = fut.result()
                if isinstance(items2, list):
                    results.append((sym, items2))
                    ok += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
    return ok, failed, results


def _run_eod_sync_pipeline(
    *,
    run_id: str,
    source: str,
    started_at: str,
    trade_date: str,
    override_symbols: list[str] | None,
    force: bool,
    preinserted: bool,
) -> str:
    """
    EOD sync pipeline runner.
    - Uses a global acquire/release to prevent overlapping runs.
    - Records run-level and step-level progress to DuckDB.
    """
    if not _sync_try_acquire():
        if preinserted:
            _sync_run_update(
                run_id=run_id,
                status="skipped",
                ended_at=now_iso(),
                duration_ms=0,
                target_symbols=0,
                ok_steps=0,
                failed_steps=0,
                error="Already running.",
                detail={"source": source, "force": bool(force), "status": "skipped"},
            )
        return ""

    detail: dict[str, Any] = {"source": source, "force": bool(force)}
    if not preinserted:
        _sync_run_insert(run_id=run_id, kind="eod", trade_date=trade_date, status="running", started_at=started_at, detail=detail)
    else:
        with _connect() as conn:
            conn.execute("UPDATE sync_runs SET status = ? WHERE id = ?", ("running", run_id))
            conn.commit()

    t0 = _sync_now_ms()
    ok_steps = 0
    failed_steps = 0
    last_error: str | None = None
    try:
        targets = _collect_eod_targets(override_symbols=override_symbols, limit=200)
        _ensure_market_stocks_basic_bulk(targets)
        detail["targetSymbols"] = [str(x.get("symbol") or "") for x in targets if str(x.get("symbol") or "").strip()]

        def step_quotes():
            syms = {str(x.get("symbol") or "").strip().upper() for x in targets}
            need_cn = any(s.startswith("CN:") for s in syms)
            need_hk = any(s.startswith("HK:") for s in syms)
            ts = now_iso()
            cn_rows = fetch_cn_a_spot() if need_cn else []
            hk_rows = fetch_hk_spot() if need_hk else []
            spot_map = {str(s.symbol).strip().upper(): s for s in (cn_rows + hk_rows) if getattr(s, "symbol", "")}
            ok = 0
            skipped = 0
            with _connect() as conn:
                for sym in sorted(syms):
                    s = spot_map.get(sym)
                    if s is None:
                        skipped += 1
                        continue
                    _upsert_market_stock(conn, s, ts)
                    _upsert_market_quote(conn, s, ts)
                    ok += 1
                conn.commit()
            return ok, skipped, {"updated": ok, "skipped": skipped}

        def step_bars():
            syms = [str(x.get("symbol") or "").strip().upper() for x in targets]
            syms = [s for s in syms if s][:200]
            if not syms:
                return 0, 0, {"refreshed": 0, "failed": 0}
            body = BarsRefreshRequest(symbols=syms)
            out = market_stocks_bars_refresh(body)
            return int(out.refreshed), int(out.failed), {"refreshed": int(out.refreshed), "failed": int(out.failed)}

        def step_chips():
            def fetch_one(sym: str, ticker: str):
                return _call_with_retry(lambda: fetch_cn_a_chip_summary(ticker, days=60), tries=3)

            ok, failed, results = _batch_fetch_cn(targets, workers=6, fetch_one=fetch_one)
            ts = now_iso()
            with _connect() as conn:
                for sym, items2 in results:
                    _upsert_market_chips(conn, sym, items2, ts)
                conn.commit()
            return ok, failed, {"updated": ok, "failed": failed}

        def step_fund_flow():
            def fetch_one(sym: str, ticker: str):
                return _call_with_retry(lambda: fetch_cn_a_fund_flow(ticker, days=60), tries=3)

            ok, failed, results = _batch_fetch_cn(targets, workers=6, fetch_one=fetch_one)
            ts = now_iso()
            with _connect() as conn:
                for sym, items2 in results:
                    _upsert_market_fund_flow(conn, sym, items2, ts)
                conn.commit()
            return ok, failed, {"updated": ok, "failed": failed}

        def step_industry_flow():
            req = MarketCnIndustryFundFlowSyncRequest(date=trade_date, days=10, topN=5, force=bool(force))
            out = market_cn_industry_fund_flow_sync(req)
            return 1, 0, {"ok": bool(out.ok), "rowsUpserted": int(out.rowsUpserted), "histRowsUpserted": int(out.histRowsUpserted)}

        for step_name, fn in [
            ("quotes", step_quotes),
            ("bars", step_bars),
            ("chips", step_chips),
            ("fund_flow", step_fund_flow),
            ("industry_fund_flow", step_industry_flow),
        ]:
            ok_step, _ok_count, _failed_count, _detail, err = _sync_step_run(run_id=run_id, step=step_name, fn=fn)
            if ok_step:
                ok_steps += 1
            else:
                failed_steps += 1
                last_error = err or last_error

        ended_at = now_iso()
        duration_ms = _sync_now_ms() - t0
        status = "ok" if failed_steps == 0 else "partial"
        detail2 = dict(detail)
        detail2["okSteps"] = ok_steps
        detail2["failedSteps"] = failed_steps
        _sync_run_update(
            run_id=run_id,
            status=status,
            ended_at=ended_at,
            duration_ms=duration_ms,
            target_symbols=len(detail.get("targetSymbols") or []),
            ok_steps=ok_steps,
            failed_steps=failed_steps,
            error=last_error,
            detail=detail2,
        )
        set_setting("eod_last_sync_at", ended_at)
        set_setting("eod_last_sync_run_id", run_id)
        return run_id
    except Exception as e:
        ended_at = now_iso()
        duration_ms = _sync_now_ms() - t0
        err = f"{type(e).__name__}: {repr(e)}"
        _sync_run_update(
            run_id=run_id,
            status="failed",
            ended_at=ended_at,
            duration_ms=duration_ms,
            target_symbols=0,
            ok_steps=ok_steps,
            failed_steps=failed_steps,
            error=err,
            detail={"source": source, "force": bool(force), "error": err},
        )
        return ""
    finally:
        _sync_release()


def run_eod_sync(*, source: str, override_symbols: list[str] | None = None, force: bool = True) -> str:
    """
    Run end-of-day sync in-process (desktop usage).
    This is a best-effort pipeline: step failures do not abort subsequent steps.
    """
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    trade_date = _today_cn_date_str()
    return _run_eod_sync_pipeline(
        run_id=run_id,
        source=source,
        started_at=started_at,
        trade_date=trade_date,
        override_symbols=override_symbols,
        force=bool(force),
        preinserted=False,
    )


@app.get("/sync/status", response_model=SyncStatusResponse)
def sync_status() -> SyncStatusResponse:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, kind, trade_date, status, started_at, ended_at, duration_ms,
                   target_symbols, ok_steps, failed_steps, error, detail_json
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 1
            """,
        ).fetchone()
        if row is None:
            return SyncStatusResponse(ok=True, lastRun=None)
        run_id = str(row[0])
        try:
            detail = json.loads(str(row[11]) or "{}")
            detail2 = detail if isinstance(detail, dict) else {}
        except Exception:
            detail2 = {}
        steps_rows = conn.execute(
            """
            SELECT step, status, started_at, ended_at, duration_ms, ok_count, failed_count, error
            FROM sync_run_steps
            WHERE run_id = ?
            ORDER BY started_at ASC
            """,
            (run_id,),
        ).fetchall()
    steps = [
        SyncRunStepStatus(
            step=str(r[0]),
            status=str(r[1]),
            startedAt=str(r[2]),
            endedAt=str(r[3]) if r[3] is not None else None,
            durationMs=int(r[4]) if r[4] is not None else None,
            okCount=int(r[5]) if r[5] is not None else None,
            failedCount=int(r[6]) if r[6] is not None else None,
            error=str(r[7]) if r[7] is not None else None,
        )
        for r in steps_rows
    ]
    return SyncStatusResponse(
        ok=True,
        lastRun=SyncRunStatus(
            id=run_id,
            kind=str(row[1]),
            tradeDate=str(row[2]),
            status=str(row[3]),
            startedAt=str(row[4]),
            endedAt=str(row[5]) if row[5] is not None else None,
            durationMs=int(row[6]) if row[6] is not None else None,
            targetSymbols=int(row[7] or 0),
            okSteps=int(row[8] or 0),
            failedSteps=int(row[9] or 0),
            error=str(row[10]) if row[10] is not None else None,
            detail=detail2,
            steps=steps,
        ),
    )


@app.get("/sync/runs", response_model=SyncRunsResponse)
def sync_runs(limit: int = 20, offset: int = 0) -> SyncRunsResponse:
    limit2 = max(1, min(int(limit), 200))
    offset2 = max(0, int(offset))
    with _connect() as conn:
        total_row = conn.execute("SELECT COUNT(1) FROM sync_runs").fetchone()
        total = int(total_row[0]) if total_row else 0
        rows = conn.execute(
            """
            SELECT id, kind, trade_date, status, started_at, ended_at, duration_ms,
                   target_symbols, ok_steps, failed_steps, error
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit2, offset2),
        ).fetchall()
    items = [
        SyncRunSummary(
            id=str(r[0]),
            kind=str(r[1]),
            tradeDate=str(r[2]),
            status=str(r[3]),
            startedAt=str(r[4]),
            endedAt=str(r[5]) if r[5] is not None else None,
            durationMs=int(r[6]) if r[6] is not None else None,
            targetSymbols=int(r[7] or 0),
            okSteps=int(r[8] or 0),
            failedSteps=int(r[9] or 0),
            error=str(r[10]) if r[10] is not None else None,
        )
        for r in rows
    ]
    return SyncRunsResponse(items=items, total=total, offset=offset2, limit=limit2)


@app.post("/sync/trigger", response_model=SyncTriggerResponse)
def sync_trigger(req: SyncTriggerRequest) -> SyncTriggerResponse:
    # Manual trigger runs in background to avoid blocking API/UI.
    run_id = str(uuid.uuid4())
    started_at = now_iso()
    trade_date = _today_cn_date_str()
    detail = {"source": "manual", "force": bool(req.force), "status": "queued"}
    _sync_run_insert(run_id=run_id, kind="eod", trade_date=trade_date, status="queued", started_at=started_at, detail=detail)

    t = threading.Thread(
        target=_run_eod_sync_pipeline,
        kwargs={
            "run_id": run_id,
            "source": "manual",
            "started_at": started_at,
            "trade_date": trade_date,
            "override_symbols": req.symbols,
            "force": bool(req.force),
            "preinserted": True,
        },
        name="eod-sync-manual",
        daemon=True,
    )
    t.start()
    return SyncTriggerResponse(ok=True, runId=run_id, status="queued")

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
            MERGE INTO cn_rank_snapshots AS t
            USING (
              SELECT
                ? AS id,
                ? AS account_id,
                ? AS as_of_date,
                ? AS universe_version,
                ? AS created_at,
                ? AS output_json
            ) AS s
            ON t.account_id = s.account_id AND t.as_of_date = s.as_of_date AND t.universe_version = s.universe_version
            WHEN MATCHED THEN UPDATE SET
              id = s.id,
              created_at = s.created_at,
              output_json = s.output_json
            WHEN NOT MATCHED THEN INSERT (id, account_id, as_of_date, universe_version, created_at, output_json)
              VALUES (s.id, s.account_id, s.as_of_date, s.universe_version, s.created_at, s.output_json)
            """,
            (snap_id, account_id, as_of_date, universe_version, ts, json.dumps(output or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()
    return snap_id


def _get_market_bar_by_date(*, symbol: str, date: str) -> dict[str, Any] | None:
    sym = (symbol or "").strip()
    d = (date or "").strip()
    if not sym or not d:
        return None
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM market_bars
            WHERE symbol = ? AND date = ?
            """,
            (sym, d),
        ).fetchone()
    if row is None:
        return None
    return {
        "date": str(row[0]),
        "open": str(row[1] or ""),
        "high": str(row[2] or ""),
        "low": str(row[3] or ""),
        "close": str(row[4] or ""),
        "volume": str(row[5] or ""),
        "amount": str(row[6] or ""),
    }


def _cn_next_trade_dates(*, as_of_date: str, n: int) -> list[str]:
    """
    Best-effort CN trading day forward steps (weekday-only; no holiday calendar).
    """
    n2 = max(1, min(int(n), 10))
    try:
        d0 = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    except Exception:
        d0 = datetime.now(tz=UTC).date()
    out: list[str] = []
    cur = d0
    while len(out) < n2:
        cur = cur + timedelta(days=1)
        if cur.weekday() >= 5:
            continue
        out.append(cur.isoformat())
    return out


def _upsert_quant_2d_rank_events(
    *,
    account_id: str,
    as_of_ts: str,
    as_of_date: str,
    rows: list[dict[str, Any]],
) -> None:
    """
    Persist generated candidates (evidence + buy price) for later outcome labeling/calibration.
    """
    ts = now_iso()
    with _connect() as conn:
        for r in rows:
            sym = _norm_str(r.get("symbol") or "")
            ticker = _norm_str(r.get("ticker") or "")
            name = _norm_str(r.get("name") or "")
            if not sym or not ticker:
                continue
            ev = r.get("evidence") if isinstance(r.get("evidence"), dict) else {}
            buy_price = _finite_float(r.get("buyPrice"), 0.0)
            if buy_price <= 0:
                continue
            buy_src = _norm_str(r.get("buyPriceSrc") or "unknown") or "unknown"
            raw_score = _finite_float(r.get("rawScore"), 0.0)
            eid = str(uuid.uuid4())
            conn.execute(
                """
                MERGE INTO quant_2d_rank_events AS t
                USING (
                  SELECT
                    ? AS id,
                    ? AS account_id,
                    ? AS as_of_ts,
                    ? AS as_of_date,
                    ? AS symbol,
                    ? AS ticker,
                    ? AS name,
                    ? AS buy_price,
                    ? AS buy_price_src,
                    ? AS raw_score,
                    ? AS evidence_json,
                    ? AS created_at
                ) AS s
                ON t.account_id = s.account_id AND t.as_of_ts = s.as_of_ts AND t.symbol = s.symbol
                WHEN MATCHED THEN UPDATE SET
                  buy_price = s.buy_price,
                  buy_price_src = s.buy_price_src,
                  raw_score = s.raw_score,
                  evidence_json = s.evidence_json,
                  created_at = s.created_at
                WHEN NOT MATCHED THEN INSERT (
                  id, account_id, as_of_ts, as_of_date, symbol, ticker, name,
                  buy_price, buy_price_src, raw_score, evidence_json, created_at
                ) VALUES (
                  s.id, s.account_id, s.as_of_ts, s.as_of_date, s.symbol, s.ticker, s.name,
                  s.buy_price, s.buy_price_src, s.raw_score, s.evidence_json, s.created_at
                )
                """,
                (
                    eid,
                    account_id,
                    as_of_ts,
                    as_of_date,
                    sym,
                    ticker,
                    name,
                    float(buy_price),
                    buy_src,
                    float(raw_score),
                    json.dumps(ev or {}, ensure_ascii=False, default=str),
                    ts,
                ),
            )
        conn.commit()


def _label_quant_2d_outcomes_best_effort(*, account_id: str, as_of_date: str | None = None, limit: int = 500) -> dict[str, Any]:
    """
    Best-effort offline labeling for 2D outcomes based on cached daily bars.
    Outcome metric: average return of next 2 trading days' closes relative to buy_price.
    """
    lim = max(1, min(int(limit), 5000))
    where = ""
    args: list[Any] = [account_id]
    if as_of_date:
        where = " AND e.as_of_date = ?"
        args.append(str(as_of_date))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT e.id, e.as_of_ts, e.as_of_date, e.symbol, e.buy_price
            FROM quant_2d_rank_events e
            LEFT JOIN quant_2d_outcomes o ON o.event_id = e.id
            WHERE e.account_id = ?{where} AND o.event_id IS NULL
            ORDER BY e.as_of_ts ASC
            LIMIT ?
            """,
            (*args, lim),
        ).fetchall()

    labeled = 0
    skipped = 0
    for r in rows:
        event_id = str(r[0])
        ts = str(r[1])
        d0 = str(r[2])
        sym = str(r[3])
        buy = float(r[4] or 0.0)
        if not sym or buy <= 0:
            skipped += 1
            continue
        t12 = _cn_next_trade_dates(as_of_date=d0, n=2)
        t1 = t12[0] if len(t12) >= 1 else ""
        t2 = t12[1] if len(t12) >= 2 else ""
        if not t1 or not t2:
            skipped += 1
            continue
        b1 = _get_market_bar_by_date(symbol=sym, date=t1)
        b2 = _get_market_bar_by_date(symbol=sym, date=t2)
        if b1 is None or b2 is None:
            skipped += 1
            continue
        c1 = _finite_float(b1.get("close"), 0.0)
        c2 = _finite_float(b2.get("close"), 0.0)
        l1 = _finite_float(b1.get("low"), 0.0)
        l2 = _finite_float(b2.get("low"), 0.0)
        if c1 <= 0 or c2 <= 0:
            skipped += 1
            continue
        ret_avg = ((c1 + c2) / 2.0) / buy - 1.0
        low_min = min([x for x in [l1, l2] if x > 0] or [0.0])
        dd = (low_min / buy - 1.0) if (low_min > 0 and buy > 0) else 0.0
        win = 1 if ret_avg > 0 else 0
        labeled_at = now_iso()
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO quant_2d_outcomes(
                  event_id, account_id, as_of_ts, as_of_date, symbol, buy_price,
                  t1_date, t2_date, close_t1, close_t2, low_min,
                  ret2d_avg_pct, dd2d_pct, win, labeled_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    account_id,
                    ts,
                    d0,
                    sym,
                    float(buy),
                    t1,
                    t2,
                    float(c1),
                    float(c2),
                    float(low_min),
                    float(ret_avg * 100.0),
                    float(dd * 100.0),
                    int(win),
                    labeled_at,
                ),
            )
            conn.commit()
        labeled += 1
    return {"unlabeled": len(rows), "labeled": labeled, "skipped": skipped}


def _get_quant_2d_calibration_cached(*, key: str) -> dict[str, Any] | None:
    k = (key or "").strip()
    if not k:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT updated_at, output_json FROM quant_2d_calibration_cache WHERE key = ?",
            (k,),
        ).fetchone()
    if row is None:
        return None
    try:
        out = json.loads(str(row[1]) or "{}")
    except Exception:
        out = {}
    return {"updatedAt": str(row[0]), "output": out}


def _upsert_quant_2d_calibration_cached(*, key: str, ts: str, output: dict[str, Any]) -> None:
    k = (key or "").strip()
    if not k:
        return
    with _connect() as conn:
        conn.execute(
            """
            MERGE INTO quant_2d_calibration_cache AS t
            USING (SELECT ? AS key, ? AS updated_at, ? AS output_json) AS s
            ON t.key = s.key
            WHEN MATCHED THEN UPDATE SET
              updated_at = s.updated_at,
              output_json = s.output_json
            WHEN NOT MATCHED THEN INSERT (key, updated_at, output_json)
              VALUES (s.key, s.updated_at, s.output_json)
            """,
            (k, ts, json.dumps(output or {}, ensure_ascii=False, default=str)),
        )
        conn.commit()


def _build_quant_2d_calibration(
    *,
    account_id: str,
    buckets: int = 20,
    lookback_days: int = 180,
) -> dict[str, Any]:
    b = max(5, min(int(buckets), 50))
    days = max(10, min(int(lookback_days), 720))
    # Simple lookback by as_of_date string ordering (YYYY-MM-DD).
    cutoff = (datetime.now(tz=UTC).date() - timedelta(days=days)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT e.raw_score, o.win, o.ret2d_avg_pct, o.dd2d_pct
            FROM quant_2d_outcomes o
            JOIN quant_2d_rank_events e ON e.id = o.event_id
            WHERE o.account_id = ? AND o.as_of_date >= ?
            """,
            (account_id, cutoff),
        ).fetchall()

    pts: list[tuple[float, int, float, float]] = []
    for r in rows:
        raw = float(r[0] or 0.0)
        win = int(r[1] or 0)
        ret = float(r[2] or 0.0)
        dd = float(r[3] or 0.0)
        pts.append((raw, win, ret, dd))
    pts.sort(key=lambda x: x[0])
    if not pts:
        return {"buckets": b, "n": 0, "items": []}

    # Equal-frequency buckets.
    n = len(pts)
    items: list[dict[str, Any]] = []
    for i in range(b):
        lo_idx = int(i * n / b)
        hi_idx = int((i + 1) * n / b)
        seg = pts[lo_idx:hi_idx] if hi_idx > lo_idx else []
        if not seg:
            continue
        raws = [x[0] for x in seg]
        wins = [x[1] for x in seg]
        rets = [x[2] for x in seg]
        dds = [x[3] for x in seg]
        prob = float(sum(wins)) / float(len(wins)) if wins else 0.0
        ev = float(sum(rets)) / float(len(rets)) if rets else 0.0
        dd_mean = float(sum(dds)) / float(len(dds)) if dds else 0.0
        # Conservative tail return (10th percentile).
        rets_sorted = sorted(rets)
        p10 = rets_sorted[int(0.10 * (len(rets_sorted) - 1))] if len(rets_sorted) >= 2 else (rets_sorted[0] if rets_sorted else 0.0)
        items.append(
            {
                "minRawScore": float(min(raws)),
                "maxRawScore": float(max(raws)),
                "n": int(len(seg)),
                "probWin": float(prob),  # 0..1
                "ev2dPct": float(ev),
                "p10Ret2dPct": float(p10),
                "dd2dPct": float(dd_mean),
            }
        )
    return {"buckets": b, "n": n, "items": items}


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
            MERGE INTO cn_intraday_rank_snapshots AS t
            USING (
              SELECT
                ? AS id,
                ? AS account_id,
                ? AS as_of_ts,
                ? AS trade_date,
                ? AS slot,
                ? AS universe_version,
                ? AS created_at,
                ? AS output_json
            ) AS s
            ON t.account_id = s.account_id AND t.as_of_ts = s.as_of_ts AND t.slot = s.slot AND t.universe_version = s.universe_version
            WHEN MATCHED THEN UPDATE SET
              id = s.id,
              trade_date = s.trade_date,
              created_at = s.created_at,
              output_json = s.output_json
            WHEN NOT MATCHED THEN INSERT (
              id, account_id, as_of_ts, trade_date, slot, universe_version, created_at, output_json
            ) VALUES (
              s.id, s.account_id, s.as_of_ts, s.trade_date, s.slot, s.universe_version, s.created_at, s.output_json
            )
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
            MERGE INTO market_cn_minute_bars AS t
            USING (
              SELECT
                ? AS symbol,
                ? AS trade_date,
                ? AS interval,
                ? AS updated_at,
                ? AS bars_json
            ) AS s
            ON t.symbol = s.symbol AND t.trade_date = s.trade_date AND t.interval = s.interval
            WHEN MATCHED THEN UPDATE SET
              updated_at = s.updated_at,
              bars_json = s.bars_json
            WHEN NOT MATCHED THEN INSERT (symbol, trade_date, interval, updated_at, bars_json)
              VALUES (s.symbol, s.trade_date, s.interval, s.updated_at, s.bars_json)
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
            MERGE INTO cn_theme_membership_cache AS t
            USING (SELECT ? AS theme_key, ? AS trade_date, ? AS members_json, ? AS updated_at) AS s
            ON t.theme_key = s.theme_key AND t.trade_date = s.trade_date
            WHEN MATCHED THEN UPDATE SET
              members_json = s.members_json,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (theme_key, trade_date, members_json, updated_at)
              VALUES (s.theme_key, s.trade_date, s.members_json, s.updated_at)
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


_GLOBAL_QUANT_ACCOUNT_ID = "global"


def _global_quant_account_id() -> str:
    """
    Quant/Rank is global and MUST NOT depend on any broker account.
    DB schemas require an account_id (FK), so we use a stable internal account id.
    """
    ts = now_iso()
    with _connect() as conn:
        row = conn.execute("SELECT id FROM broker_accounts WHERE id = ?", (_GLOBAL_QUANT_ACCOUNT_ID,)).fetchone()
        if row is not None:
            return str(row[0])
        conn.execute(
            """
            INSERT INTO broker_accounts(id, broker, title, account_masked, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (_GLOBAL_QUANT_ACCOUNT_ID, "system", "Global", None, ts, ts),
        )
        conn.commit()
    return _GLOBAL_QUANT_ACCOUNT_ID


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


class MarketStockBasicRow(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str


class MarketBarsResponse(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    currency: str
    bars: list[dict[str, str]]


class BarsRefreshRequest(BaseModel):
    symbols: list[str] = []


class BarsRefreshResponse(BaseModel):
    refreshed: int
    failed: int


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
    marketTurnoverCny: float = 0.0
    marketVolume: float = 0.0
    yesterdayLimitUpPremium: float  # percent, e.g. -1.2 means -1.2%
    failedLimitUpRate: float  # percent, e.g. 35.0
    riskMode: str  # no_new_positions | caution | normal | hot | euphoric
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
    includeHoldings: bool = False


class RankItem(BaseModel):
    symbol: str
    market: str
    ticker: str
    name: str
    sector: str | None = None
    # Final decision score (0-100): higher means "more likely to profit within ~2 trading days".
    score: float
    # Calibrated metrics (best-effort).
    probProfit2d: float | None = None  # 0-100 (%)
    ev2dPct: float | None = None  # percent
    dd2dPct: float | None = None  # percent (<=0 means drawdown)
    confidence: str | None = None  # High | Medium | Low
    # Snapshot metadata.
    buyPrice: float | None = None
    buyPriceSrc: str | None = None  # spot | bars_close | unknown
    # Explanations (best-effort).
    whyBullets: list[str] = []
    # Debug fields.
    rawScore: float | None = None
    probBand: str | None = None  # High | Medium | Low (derived)
    signals: list[str] = []
    breakdown: dict[str, float] = {}


class RankSnapshotResponse(BaseModel):
    id: str
    asOfTs: str | None = None
    asOfDate: str
    accountId: str
    createdAt: str
    universeVersion: str
    riskMode: str | None = None
    objective: str | None = None
    horizon: str | None = None
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


# --- CN morning radar (09-10) (v0) ---
class MorningRadarTheme(BaseModel):
    kind: str  # industry | concept
    name: str
    score: float
    todayStrength: float = 0.0
    volSurge: float = 0.0
    limitupCount: int = 0
    followersCount: int = 0
    topTickers: list[dict[str, Any]] = []


class MorningRadarResponse(BaseModel):
    asOfTs: str
    tradeDate: str
    accountId: str
    universeVersion: str
    themes: list[MorningRadarTheme] = []
    debug: dict[str, Any] | None = None


class MorningRadarGenerateRequest(BaseModel):
    accountId: str | None = None
    asOfTs: str | None = None
    universeVersion: str = "v0"
    topK: int = 3
    perTheme: int = 3


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
    includeMainline: bool = True
    includeStocks: bool = True
    includeQuant2d: bool = False
    includeWatchlist: bool = False
    # Client-provided local watchlist snapshot (small JSON). When includeWatchlist is off, ignored.
    watchlist: dict[str, Any] | None = None


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


# --- Trade journal module (v0) ---
class TradeJournal(BaseModel):
    id: str
    title: str
    contentMd: str
    createdAt: str
    updatedAt: str


class TradeJournalCreateRequest(BaseModel):
    title: str | None = None
    contentMd: str = ""


class TradeJournalUpdateRequest(BaseModel):
    title: str | None = None
    contentMd: str | None = None


class ListTradeJournalsResponse(BaseModel):
    total: int
    items: list[TradeJournal]


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
    # Today's change percent (best-effort) computed from the latest 2 daily closes: (c0/c-1 - 1) * 100.
    todayChangePct: float | None = None  # percent, e.g. 3.21 means +3.21%
    # Trend series for UI sparkline (best-effort): last N daily closes.
    trendSeries: list[dict[str, Any]] = []  # [{date, close}]


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
    asOfDate: str  # Trading date (YYYY-MM-DD)
    asOfTs: str  # Last dashboard summary refresh timestamp (ISO, with time)
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
        # DuckDB does not reliably expose sqlite-style cursor.rowcount across versions.
        # Check existence first to keep stable 404 semantics.
        exists = conn.execute("SELECT 1 FROM tv_screeners WHERE id = ? LIMIT 1", (screener_id,)).fetchone()
        if exists is None:
            return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
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
    return JSONResponse({"ok": True})


@app.delete("/integrations/tradingview/screeners/{screener_id}")
def delete_tv_screener(screener_id: str) -> JSONResponse:
    _seed_default_tv_screeners()
    with _connect() as conn:
        exists = conn.execute("SELECT 1 FROM tv_screeners WHERE id = ? LIMIT 1", (screener_id,)).fetchone()
        if exists is None:
            return JSONResponse({"ok": False, "error": "Not found"}, status_code=404)
        conn.execute("DELETE FROM tv_screeners WHERE id = ?", (screener_id,))
        conn.commit()
    return JSONResponse({"ok": True})


def _upsert_market_stock(conn: duckdb.DuckDBPyConnection, s: StockRow, ts: str) -> None:
    conn.execute(
        """
        MERGE INTO market_stocks AS t
        USING (
          SELECT
            ? AS symbol,
            ? AS market,
            ? AS ticker,
            ? AS name,
            ? AS currency,
            ? AS updated_at
        ) AS s
        ON t.symbol = s.symbol
        WHEN MATCHED THEN UPDATE SET
          market = s.market,
          ticker = s.ticker,
          name = s.name,
          currency = s.currency,
          updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (symbol, market, ticker, name, currency, updated_at)
          VALUES (s.symbol, s.market, s.ticker, s.name, s.currency, s.updated_at)
        """,
        (s.symbol, s.market, s.ticker, s.name, s.currency, ts),
    )


def _upsert_market_quote(conn: duckdb.DuckDBPyConnection, s: StockRow, ts: str) -> None:
    raw_json = json.dumps(s.quote, ensure_ascii=False)
    conn.execute(
        """
        MERGE INTO market_quotes AS t
        USING (
          SELECT
            ? AS symbol,
            ? AS price,
            ? AS change_pct,
            ? AS volume,
            ? AS turnover,
            ? AS market_cap,
            ? AS updated_at,
            ? AS raw_json
        ) AS s
        ON t.symbol = s.symbol
        WHEN MATCHED THEN UPDATE SET
          price = s.price,
          change_pct = s.change_pct,
          volume = s.volume,
          turnover = s.turnover,
          market_cap = s.market_cap,
          updated_at = s.updated_at,
          raw_json = s.raw_json
        WHEN NOT MATCHED THEN INSERT (
          symbol, price, change_pct, volume, turnover, market_cap, updated_at, raw_json
        ) VALUES (
          s.symbol, s.price, s.change_pct, s.volume, s.turnover, s.market_cap, s.updated_at, s.raw_json
        )
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


def _upsert_market_bars(conn: duckdb.DuckDBPyConnection, symbol: str, bars: list[BarRow], ts: str) -> None:
    for b in bars:
        conn.execute(
            """
            MERGE INTO market_bars AS t
            USING (
              SELECT
                ? AS symbol,
                ? AS date,
                ? AS open,
                ? AS high,
                ? AS low,
                ? AS close,
                ? AS volume,
                ? AS amount,
                ? AS updated_at
            ) AS s
            ON t.symbol = s.symbol AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              open = s.open,
              high = s.high,
              low = s.low,
              close = s.close,
              volume = s.volume,
              amount = s.amount,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              symbol, date, open, high, low, close, volume, amount, updated_at
            ) VALUES (
              s.symbol, s.date, s.open, s.high, s.low, s.close, s.volume, s.amount, s.updated_at
            )
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
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    items: list[dict[str, str]],
    ts: str,
) -> None:
    for it in items:
        raw = json.dumps(it, ensure_ascii=False)
        conn.execute(
            """
            MERGE INTO market_chips AS t
            USING (
              SELECT
                ? AS symbol,
                ? AS date,
                ? AS profit_ratio,
                ? AS avg_cost,
                ? AS cost90_low,
                ? AS cost90_high,
                ? AS cost90_conc,
                ? AS cost70_low,
                ? AS cost70_high,
                ? AS cost70_conc,
                ? AS updated_at,
                ? AS raw_json
            ) AS s
            ON t.symbol = s.symbol AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              profit_ratio = s.profit_ratio,
              avg_cost = s.avg_cost,
              cost90_low = s.cost90_low,
              cost90_high = s.cost90_high,
              cost90_conc = s.cost90_conc,
              cost70_low = s.cost70_low,
              cost70_high = s.cost70_high,
              cost70_conc = s.cost70_conc,
              updated_at = s.updated_at,
              raw_json = s.raw_json
            WHEN NOT MATCHED THEN INSERT (
              symbol, date,
              profit_ratio, avg_cost,
              cost90_low, cost90_high, cost90_conc,
              cost70_low, cost70_high, cost70_conc,
              updated_at, raw_json
            ) VALUES (
              s.symbol, s.date,
              s.profit_ratio, s.avg_cost,
              s.cost90_low, s.cost90_high, s.cost90_conc,
              s.cost70_low, s.cost70_high, s.cost70_conc,
              s.updated_at, s.raw_json
            )
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
    conn: duckdb.DuckDBPyConnection,
    symbol: str,
    items: list[dict[str, str]],
    ts: str,
) -> None:
    for it in items:
        raw = json.dumps(it, ensure_ascii=False)
        conn.execute(
            """
            MERGE INTO market_fund_flow AS t
            USING (
              SELECT
                ? AS symbol,
                ? AS date,
                ? AS close,
                ? AS change_pct,
                ? AS main_net_amount,
                ? AS main_net_ratio,
                ? AS super_net_amount,
                ? AS super_net_ratio,
                ? AS large_net_amount,
                ? AS large_net_ratio,
                ? AS medium_net_amount,
                ? AS medium_net_ratio,
                ? AS small_net_amount,
                ? AS small_net_ratio,
                ? AS updated_at,
                ? AS raw_json
            ) AS s
            ON t.symbol = s.symbol AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              close = s.close,
              change_pct = s.change_pct,
              main_net_amount = s.main_net_amount,
              main_net_ratio = s.main_net_ratio,
              super_net_amount = s.super_net_amount,
              super_net_ratio = s.super_net_ratio,
              large_net_amount = s.large_net_amount,
              large_net_ratio = s.large_net_ratio,
              medium_net_amount = s.medium_net_amount,
              medium_net_ratio = s.medium_net_ratio,
              small_net_amount = s.small_net_amount,
              small_net_ratio = s.small_net_ratio,
              updated_at = s.updated_at,
              raw_json = s.raw_json
            WHEN NOT MATCHED THEN INSERT (
              symbol, date,
              close, change_pct,
              main_net_amount, main_net_ratio,
              super_net_amount, super_net_ratio,
              large_net_amount, large_net_ratio,
              medium_net_amount, medium_net_ratio,
              small_net_amount, small_net_ratio,
              updated_at, raw_json
            ) VALUES (
              s.symbol, s.date,
              s.close, s.change_pct,
              s.main_net_amount, s.main_net_ratio,
              s.super_net_amount, s.super_net_ratio,
              s.large_net_amount, s.large_net_ratio,
              s.medium_net_amount, s.medium_net_ratio,
              s.small_net_amount, s.small_net_ratio,
              s.updated_at, s.raw_json
            )
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
    conn: duckdb.DuckDBPyConnection,
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
            MERGE INTO market_cn_industry_fund_flow_daily AS t
            USING (
              SELECT
                ? AS date,
                ? AS industry_code,
                ? AS industry_name,
                ? AS net_inflow,
                ? AS updated_at,
                ? AS raw_json
            ) AS s
            ON t.date = s.date AND t.industry_code = s.industry_code
            WHEN MATCHED THEN UPDATE SET
              industry_name = s.industry_name,
              net_inflow = s.net_inflow,
              updated_at = s.updated_at,
              raw_json = s.raw_json
            WHEN NOT MATCHED THEN INSERT (
              date, industry_code, industry_name, net_inflow, updated_at, raw_json
            ) VALUES (
              s.date, s.industry_code, s.industry_name, s.net_inflow, s.updated_at, s.raw_json
            )
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
    hk_error: str | None = None
    try:
        # CN is required for the core market universe.
        cn = fetch_cn_a_spot()
    except Exception as e:
        # Upstream data sources can occasionally return HTML/captcha or abort connections.
        # If we already have a cached market universe, keep the app usable by falling back to cache.
        err = f"{type(e).__name__}: {repr(e)}"
        with _connect() as conn:
            row = conn.execute("SELECT COUNT(1) FROM market_stocks").fetchone()
            cached_total = int(row[0]) if row else 0
        if cached_total > 0:
            # Do not update lastSyncAt on skipped runs; the UI can still surface the error.
            return JSONResponse(
                {
                    "ok": True,
                    "stocks": cached_total,
                    "syncedAt": ts,
                    "skipped": True,
                    "error": err,
                    "hkOk": None,
                    "hkError": None,
                }
            )
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    try:
        # HK is optional; failures should not block CN market sync.
        hk = fetch_hk_spot()
    except Exception as e:
        hk = []
        hk_error = f"{type(e).__name__}: {repr(e)}"

    with _connect() as conn:
        # Cleanup legacy tickers inserted by fallback providers (e.g. "sz000001"/"sh600000").
        # These cause duplicate symbols and skew market breadth counts.
        try:
            bad_syms = conn.execute(
                """
                SELECT symbol
                FROM market_stocks
                WHERE market = 'CN'
                  AND (
                    LENGTH(ticker) != 6
                    OR ticker GLOB '*[^0-9]*'
                  )
                """,
            ).fetchall()
            bad = [str(r[0]) for r in bad_syms if r and r[0]]
            if bad:
                placeholders = ",".join(["?"] * len(bad))
                conn.execute(f"DELETE FROM market_quotes WHERE symbol IN ({placeholders})", tuple(bad))
                conn.execute(f"DELETE FROM market_stocks WHERE symbol IN ({placeholders})", tuple(bad))
        except Exception:
            pass
        for s in cn + hk:
            _upsert_market_stock(conn, s, ts)
            _upsert_market_quote(conn, s, ts)
        conn.commit()

    set_setting("market_last_sync_at", ts)
    return JSONResponse(
        {
            "ok": True,
            "stocks": len(cn) + len(hk),
            "syncedAt": ts,
            "hkOk": hk_error is None,
            "hkError": hk_error,
        }
    )


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


@app.get("/market/stocks/resolve", response_model=list[MarketStockBasicRow])
def market_resolve_stocks(symbols: Annotated[list[str] | None, Query()] = None) -> list[MarketStockBasicRow]:
    """
    Resolve stock basic info by symbol, using the local market universe cache.
    This is a DB-first helper for UI modules (e.g. Watchlist).
    """
    syms0 = symbols if isinstance(symbols, list) else []
    syms = [str(s or "").strip() for s in syms0]
    syms = [s for s in syms if s]
    if not syms:
        return []
    if len(syms) > 200:
        syms = syms[:200]

    placeholders = ",".join(["?"] * len(syms))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT symbol, market, ticker, name, currency
            FROM market_stocks
            WHERE symbol IN ({placeholders})
            """,
            tuple(syms),
        ).fetchall()

    by_sym: dict[str, MarketStockBasicRow] = {}
    for r in rows:
        sym = str(r[0])
        by_sym[sym] = MarketStockBasicRow(
            symbol=sym,
            market=str(r[1]),
            ticker=str(r[2]),
            name=str(r[3]),
            currency=str(r[4]),
        )

    # Preserve request order; drop unknown symbols.
    return [by_sym[s] for s in syms if s in by_sym]


class TrendOkChecks(BaseModel):
    emaOrder: bool | None = None  # EMA(5) > EMA(20) > EMA(60)
    macdPositive: bool | None = None  # macdLine > 0
    macdHistExpanding: bool | None = None  # last 4 days: >=2 day-over-day increases
    closeNear20dHigh: bool | None = None  # close >= 0.95 * high20
    rsiInRange: bool | None = None  # 50 <= rsi14 <= 75
    volumeSurge: bool | None = None  # avgVol5 > 1.0 * avgVol30 OR close >= high20 (缩量上涨也认可)


class TrendOkValues(BaseModel):
    close: float | None = None
    ema5: float | None = None
    ema20: float | None = None
    ema60: float | None = None
    macd: float | None = None  # macdLine
    macdSignal: float | None = None
    macdHist: float | None = None
    macdHist4: list[float] = []  # last 4 histogram values (oldest -> newest)
    rsi14: float | None = None
    high20: float | None = None
    avgVol5: float | None = None
    avgVol30: float | None = None


class TrendOkResult(BaseModel):
    symbol: str
    name: str | None = None
    asOfDate: str | None = None
    trendOk: bool | None = None
    score: float | None = None  # 0+ (can exceed 100), formula-based (no LLM)
    scoreParts: dict[str, float] = {}  # points breakdown (positive parts and penalties)
    stopLossPrice: float | None = None
    stopLossParts: dict[str, Any] = {}
    buyMode: str | None = None  # A_pullback | B_momentum | none
    buyAction: str | None = None  # wait | buy | add | avoid
    buyZoneLow: float | None = None
    buyZoneHigh: float | None = None
    buyRefPrice: float | None = None
    buyWhy: str | None = None
    buyChecks: dict[str, Any] = {}
    checks: TrendOkChecks = TrendOkChecks()
    values: TrendOkValues = TrendOkValues()
    missingData: list[str] = []


def _ema(values: list[float], period: int) -> list[float]:
    """
    Exponential Moving Average (EMA).
    Returns an EMA series aligned with input values.
    """
    if period <= 0 or not values:
        return []
    alpha = 2.0 / (float(period) + 1.0)
    out: list[float] = []
    prev = values[0]
    out.append(prev)
    for v in values[1:]:
        prev = alpha * v + (1.0 - alpha) * prev
        out.append(prev)
    return out


def _rsi(values: list[float], period: int = 14) -> list[float]:
    """
    Relative Strength Index (RSI) using Wilder's smoothing.
    Returns RSI series aligned with input values; first values may be 0.0 due to warm-up.
    """
    if period <= 0 or len(values) < 2:
        return []
    gains: list[float] = [0.0]
    losses: list[float] = [0.0]
    for i in range(1, len(values)):
        chg = values[i] - values[i - 1]
        gains.append(max(0.0, chg))
        losses.append(max(0.0, -chg))
    # Wilder smoothing
    avg_gain = 0.0
    avg_loss = 0.0
    out: list[float] = [0.0] * len(values)
    for i in range(1, len(values)):
        if i <= period:
            # build initial average using simple mean for first 'period' changes
            avg_gain = sum(gains[1 : i + 1]) / max(1.0, float(i))
            avg_loss = sum(losses[1 : i + 1]) / max(1.0, float(i))
        else:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / float(period)
            avg_loss = (avg_loss * (period - 1) + losses[i]) / float(period)
        if avg_loss <= 0.0:
            out[i] = 100.0 if avg_gain > 0.0 else 50.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[list[float], list[float], list[float]]:
    """
    MACD (Moving Average Convergence Divergence).
    Returns (macdLine, signalLine, histogram) series aligned with input values.
    """
    if not values:
        return ([], [], [])
    ema_fast = _ema(values, fast)
    ema_slow = _ema(values, slow)
    macd_line = [a - b for a, b in zip(ema_fast, ema_slow, strict=True)]
    signal_line = _ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, signal_line, strict=True)]
    return (macd_line, signal_line, hist)


def _atr14(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    """
    ATR(period) using True Range + Wilder smoothing.
    Returns latest ATR value.
    """
    if period <= 0:
        return None
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    tr: list[float] = []
    for i in range(1, n):
        h = highs[i]
        low = lows[i]
        pc = closes[i - 1]
        tr_i = max(h - low, abs(h - pc), abs(low - pc))
        tr.append(tr_i)
    if len(tr) < period:
        return None
    atr = sum(tr[:period]) / float(period)
    for x in tr[period:]:
        atr = (atr * (period - 1) + x) / float(period)
    return atr if math.isfinite(atr) else None


def _parse_float_safe(v: Any) -> float | None:
    try:
        if v is None:
            return None
        n = float(v)
        return n if math.isfinite(n) else None
    except Exception:
        return None


def _market_stock_trendok_one(
    *,
    symbol: str,
    name: str | None,
    bars: list[tuple[str, str | None, str | None, str | None, str | None, str | None]],
) -> TrendOkResult:
    """
    Compute TrendOK for one CN symbol from daily bars.
    bars: list of (date, open, high, low, close, volume) ordered by date ASC.
    """
    res = TrendOkResult(symbol=symbol, name=name)
    if not symbol.startswith("CN:"):
        res.missingData.append("unsupported_market")
        return res

    closes: list[float] = []
    vols: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    opens: list[float] = []
    dates: list[str] = []
    for d, o, h, low, c, v in bars:
        c2 = _parse_float_safe(c)
        v2 = _parse_float_safe(v)
        h2 = _parse_float_safe(h)
        l2 = _parse_float_safe(low)
        o2 = _parse_float_safe(o)
        if c2 is None:
            continue
        closes.append(c2)
        vols.append(v2 if v2 is not None else 0.0)
        highs.append(h2 if h2 is not None else c2)
        lows.append(l2 if l2 is not None else c2)
        opens.append(o2 if o2 is not None else c2)
        dates.append(str(d))

    if not closes:
        res.missingData.append("no_bars")
        return res
    res.asOfDate = dates[-1]
    res.values.close = closes[-1]

    if len(closes) < 60:
        res.missingData.append("bars_lt_60")
        # still compute whatever is possible for display/debug

    ema5s = _ema(closes, 5)
    ema20s = _ema(closes, 20)
    ema60s = _ema(closes, 60)
    if ema5s and ema20s and ema60s:
        res.values.ema5 = ema5s[-1]
        res.values.ema20 = ema20s[-1]
        res.values.ema60 = ema60s[-1]
        res.checks.emaOrder = bool(ema5s[-1] > ema20s[-1] > ema60s[-1])

    macd_line, sig_line, hist = _macd(closes, 12, 26, 9)
    if macd_line and sig_line and hist:
        res.values.macd = macd_line[-1]
        res.values.macdSignal = sig_line[-1]
        res.values.macdHist = hist[-1]
        res.checks.macdPositive = bool(macd_line[-1] > 0.0)
        if len(hist) >= 4:
            # Use the last 4 histogram values and count day-over-day expansions.
            # Condition (bullish expansion):
            # - Latest histogram must be > 0
            # - In the last 3 comparisons, at least 2 are increasing,
            #   counting only the positive part (negative values are treated as 0).
            h = hist[-4:]
            res.values.macdHist4 = [float(x) for x in h]
            hpos = [max(0.0, float(x)) for x in h]
            inc = 0
            if hpos[1] > hpos[0]:
                inc += 1
            if hpos[2] > hpos[1]:
                inc += 1
            if hpos[3] > hpos[2]:
                inc += 1
            res.checks.macdHistExpanding = bool(hpos[3] > 0.0 and inc >= 2)

    rsi14s = _rsi(closes, 14)
    if rsi14s:
        res.values.rsi14 = rsi14s[-1]
        res.checks.rsiInRange = bool(50.0 <= rsi14s[-1] <= 85.0)

    if len(closes) >= 20:
        high20 = max(closes[-20:])
        res.values.high20 = high20
        res.checks.closeNear20dHigh = bool(closes[-1] >= 0.95 * high20)

    if len(vols) >= 30:
        avg5 = sum(vols[-5:]) / 5.0
        avg30 = sum(vols[-30:]) / 30.0
        res.values.avgVol5 = avg5
        res.values.avgVol30 = avg30
        # Volume surge: avgVol5 > avgVol30 OR close >= high20 (缩量上涨也认可，龙头股筹码锁定后缩量上涨是强势)
        volume_surge_by_ratio = bool(avg5 > 1.0 * avg30) if avg30 > 0 else bool(avg5 > 0)
        # If close >= high20 (new 20-day high), accept even without volume surge (light selling pressure is healthy)
        close_at_new_high = False
        if res.values.high20 is not None and len(closes) > 0:
            close_at_new_high = bool(closes[-1] >= float(res.values.high20))
        res.checks.volumeSurge = volume_surge_by_ratio or close_at_new_high

    # ---------- Score (0+), formula-based (CN daily; no LLM) ----------
    # Goal: next 1-2 trading days action score; prefer strong trend + momentum + volume confirmation with limited risk.
    # Score can exceed 100 when multiple bonuses apply (e.g., new high + high momentum + high elasticity).
    try:
        def _clip01(x: float) -> float:
            return 0.0 if x <= 0.0 else 1.0 if x >= 1.0 else x

        parts: dict[str, float] = {}

        # If core indicators are missing, keep score as None (UI shows —).
        if (
            res.values.close is None
            or res.values.ema5 is None
            or res.values.ema20 is None
            or res.values.ema60 is None
            or res.values.high20 is None
            or res.values.rsi14 is None
            or res.values.avgVol5 is None
            or res.values.avgVol30 is None
            or res.values.macd is None
            or not res.values.macdHist4
        ):
            res.score = None
        else:
            close = float(res.values.close)
            ema5 = float(res.values.ema5)
            ema20 = float(res.values.ema20)
            ema60 = float(res.values.ema60)
            high20 = float(res.values.high20)
            rsi14 = float(res.values.rsi14)
            avg5 = float(res.values.avgVol5)
            avg30 = float(res.values.avgVol30)
            macd_last = float(res.values.macd)
            h4 = [float(x) for x in (res.values.macdHist4 or [])]

            # Subscores in [0,1]
            # 1) Trend / EMA (0.25): partial credit for partial alignment.
            ema_pairs = 0
            if ema5 > ema20:
                ema_pairs += 1
            if ema20 > ema60:
                ema_pairs += 1
            s_ema = float(ema_pairs) / 2.0

            # 2) Momentum / MACD (0.20): require macdLine>0; positive-part expansions in last 4 hist values.
            hpos = [max(0.0, x) for x in h4] if len(h4) == 4 else [0.0, 0.0, 0.0, 0.0]
            inc = 0
            if hpos[1] > hpos[0]:
                inc += 1
            if hpos[2] > hpos[1]:
                inc += 1
            if hpos[3] > hpos[2]:
                inc += 1
            # Add "absolute strength" gate: tiny histogram changes near zero should not score high.
            # Use a normalized threshold (hist/close) so the gate scales across price levels.
            # Example: for close~100, 0.0005*close ~= 0.05 (matches the suggested 0.05 scale).
            hist_min = 0.0005 * close if close > 0 else 0.0
            has_hist_strength = bool(hpos[3] >= hist_min and hpos[3] > 0.0)
            s_hist = (float(inc) / 3.0) if has_hist_strength else 0.0
            s_macd = 0.0 if macd_last <= 0.0 else _clip01(0.5 + 0.5 * s_hist)

            # 3) Near breakout (0.20): close/high20 in [0.85,0.95] -> [0,1]
            # Use 20D highest HIGH (not just close) for better breakout semantics.
            high20_high = max(highs[-20:]) if len(highs) >= 20 else high20
            ratio_hi = close / high20_high if high20_high > 0 else 0.0
            s_break = _clip01((ratio_hi - 0.85) / 0.10)
            # Bonus for true new high (stronger than "near high"): +3 points.
            bonus_new_high = 3.0 if (high20_high > 0 and close >= high20_high) else 0.0

            # 4) RSI quality (0.15): triangular preference within [50,75], peak at 62.5
            # For RSI > 75: no penalty (keep score), and add High Momentum Bonus if volume surges.
            if 50.0 <= rsi14 <= 75.0:
                s_rsi = _clip01(1.0 - (abs(rsi14 - 62.5) / 12.5))
            elif rsi14 > 75.0:
                # RSI > 75: keep peak score (no penalty), treat as strong momentum
                s_rsi = 1.0
            else:
                s_rsi = 0.0

            # 5) Volume confirmation (0.20): avg5/avg30 in [1.0,1.3] -> [0,1]
            ratio_vol = (avg5 / avg30) if avg30 > 0 else (1.0 if avg5 > 0 else 0.0)
            s_vol = _clip01((ratio_vol - 1.0) / 0.30)

            w_ema, w_macd, w_break, w_rsi, w_vol = 0.25, 0.20, 0.20, 0.15, 0.20
            pts_ema = 100.0 * w_ema * s_ema
            pts_macd = 100.0 * w_macd * s_macd
            pts_break = 100.0 * w_break * s_break
            pts_rsi = 100.0 * w_rsi * s_rsi
            pts_vol = 100.0 * w_vol * s_vol
            parts["ema"] = round(pts_ema, 3)
            parts["macd"] = round(pts_macd, 3)
            parts["breakout"] = round(pts_break, 3)
            parts["rsi"] = round(pts_rsi, 3)
            parts["volume"] = round(pts_vol, 3)
            if bonus_new_high > 0.0:
                parts["bonus_new_high20"] = round(bonus_new_high, 3)

            # High Momentum Bonus: RSI > 75 and volume surge (ratio_vol > 1.2)
            bonus_high_momentum = 0.0
            if rsi14 > 75.0 and ratio_vol > 1.2:
                bonus_high_momentum = 5.0
                parts["bonus_high_momentum"] = round(bonus_high_momentum, 3)

            # Risk penalties (points, negative): volatility + below EMA20
            # ATR relative scoring: high volatility is bonus when trend is up, penalty when trend is down.
            penalty = 0.0
            atr_bonus = 0.0
            atr14 = _atr14(highs, lows, closes, 14)
            if atr14 is not None and close > 0:
                atr_ratio = float(atr14) / float(close)
                # Check if trend is up: Close > EMA20 and MACD expanding
                trend_up = bool(close > ema20 and res.checks.macdHistExpanding is True)
                if trend_up:
                    # High volatility = high elasticity (good for tech/AI stocks like CPO)
                    # Map ATR ratio [0.015, 0.05] -> [0, 10] bonus points
                    atr_bonus = _clip01((atr_ratio - 0.015) / 0.035) * 10.0
                    parts["bonus_volatility_atr_high_elasticity"] = round(atr_bonus, 3)
                else:
                    # Trend down: high volatility is risky, penalize
                    p_vol = _clip01((atr_ratio - 0.015) / 0.035) * 10.0
                    penalty += p_vol
                    parts["penalty_volatility_atr"] = -round(p_vol, 3)
            # Below EMA20: 5% below -> 10 points
            if ema20 > 0 and close < ema20:
                dd = (ema20 - close) / ema20
                p_below = _clip01(dd / 0.05) * 10.0
                penalty += p_below
                parts["penalty_below_ema20"] = -round(p_below, 3)

            total = pts_ema + pts_macd + pts_break + pts_rsi + pts_vol + bonus_new_high + bonus_high_momentum + atr_bonus - penalty
            # Remove 100 cap: score can exceed 100 when multiple bonuses apply (e.g., new high + high momentum + high elasticity).
            total2 = max(0.0, total)  # Only enforce minimum 0, allow scores > 100
            res.score = round(total2, 3)
            res.scoreParts = parts
    except Exception:
        # Keep score optional; never break endpoint for score computation failures.
        res.score = None

    # ---------- StopLoss (CN daily), formula-based (no LLM) ----------
    # stop_loss = max(final_support - atr_k*ATR14, current*(1-max_loss_pct))
    try:
        stop_parts: dict[str, Any] = {}
        current = float(closes[-1])
        stop_parts["current_price"] = round(current, 6)

        if not lows or res.values.ema20 is None:
            res.stopLossPrice = None
            res.missingData.append("stoploss_missing_inputs")
        else:
            swing_low = min(lows[-10:]) if len(lows) >= 10 else min(lows)
            if len(lows) >= 20:
                # Exclude last 5 days if possible
                platform_slice = lows[-20:-5] if len(lows) >= 25 else lows[: max(0, len(lows) - 5)]
                platform_low = min(platform_slice) if platform_slice else swing_low
            else:
                platform_low = min(lows[: max(0, len(lows) - 5)]) if len(lows) > 5 else swing_low

            ema20 = float(res.values.ema20)
            structural_support = max(swing_low, platform_low, ema20)
            stop_parts["swing_low_10d"] = round(swing_low, 6)
            stop_parts["platform_low_20d_excl_5d"] = round(platform_low, 6)
            stop_parts["ema20"] = round(ema20, 6)
            stop_parts["structural_support"] = round(structural_support, 6)

            # Optional chip support: use cached chips avgCost as a support proxy if below current.
            chip_support: float | None = None
            try:
                chips_items = _load_cached_chips(symbol, days=30)
                chips_last = chips_items[-1] if chips_items else {}
                ch = _chips_summary_last(chips_last)
                avg_cost = _safe_float(ch.get("avgCost"))
                if avg_cost is not None and avg_cost < current:
                    chip_support = float(avg_cost)
                    stop_parts["chip_support_avgCost"] = round(chip_support, 6)
            except Exception:
                chip_support = None

            final_support = structural_support
            if chip_support is not None:
                final_support = max(final_support, chip_support * 0.99)
            stop_parts["final_support"] = round(final_support, 6)

            # Exit-now overrides:
            # 1) Trend structure break: EMA5 < EMA20 OR close < EMA20 => exit immediately (stop = current)
            exit_now = False
            exit_reasons: list[str] = []
            exit_check_ema5_lt_ema20 = False
            exit_check_close_lt_ema20 = False
            exit_check_mom_exhaust = False
            exit_check_vol_dry = False
            if res.values.ema5 is not None and res.values.ema20 is not None:
                if float(res.values.ema5) < float(res.values.ema20):
                    exit_now = True
                    exit_check_ema5_lt_ema20 = True
                    exit_reasons.append("trend_structure_break:ema5_below_ema20")
            if res.values.ema20 is not None and current < float(res.values.ema20):
                exit_now = True
                exit_check_close_lt_ema20 = True
                exit_reasons.append("trend_structure_break:close_below_ema20")

            # 2) Momentum exhaustion: MACD hist shrinks 3 days then turns negative + volume dries up
            # Define shrink as: hist[-4] > hist[-3] > hist[-2] > 0 and hist[-1] < 0
            # Volume dries up as: avgVol5 < avgVol30
            # Also add a warning case: shrinks 3 days but NOT negative yet => suggest sell half.
            warn_reduce_half = False
            warn_reasons: list[str] = []
            if res.values.avgVol5 is not None and res.values.avgVol30 is not None:
                avg5v = float(res.values.avgVol5)
                avg30v = float(res.values.avgVol30)
                if len(hist) >= 4:
                    h = [float(x) for x in hist[-4:]]
                    shrink_then_flip = (h[0] > h[1] > h[2] > 0.0) and (h[3] < 0.0)
                    vol_dry = avg30v > 0.0 and (avg5v < avg30v)
                    exit_check_vol_dry = bool(vol_dry)
                    if shrink_then_flip and vol_dry:
                        exit_now = True
                        exit_check_mom_exhaust = True
                        exit_reasons.append("momentum_exhaustion:hist_shrink3_flip_negative_and_volume_dry")
                    # Warning: histogram still positive but momentum is weakening.
                    # Relaxed rule: in last 3 comparisons, at least 2 are shrinking, and last hist > 0.
                    if not shrink_then_flip:
                        shrink_cnt = 0
                        if h[1] < h[0]:
                            shrink_cnt += 1
                        if h[2] < h[1]:
                            shrink_cnt += 1
                        if h[3] < h[2]:
                            shrink_cnt += 1
                        stop_parts["warn_hist4"] = [round(x, 6) for x in h]
                        stop_parts["warn_hist_shrink_cnt_3"] = shrink_cnt
                        if avg30v > 0:
                            stop_parts["warn_vol_ratio_5_30"] = round(avg5v / avg30v, 6)
                        if h[3] > 0.0 and shrink_cnt >= 2:
                            if vol_dry:
                                warn_reduce_half = True
                                warn_reasons.append("momentum_warning:hist_shrinking_and_volume_dry")
                            else:
                                warn_reduce_half = True
                                warn_reasons.append("momentum_warning:hist_shrinking")
            else:
                # If volume averages are unavailable, still warn based on MACD histogram shrinking (best-effort).
                if len(hist) >= 4:
                    h = [float(x) for x in hist[-4:]]
                    shrink_cnt = 0
                    if h[1] < h[0]:
                        shrink_cnt += 1
                    if h[2] < h[1]:
                        shrink_cnt += 1
                    if h[3] < h[2]:
                        shrink_cnt += 1
                    stop_parts["warn_hist4"] = [round(x, 6) for x in h]
                    stop_parts["warn_hist_shrink_cnt_3"] = shrink_cnt
                    stop_parts["warn_vol_ratio_5_30"] = None
                    if h[3] > 0.0 and shrink_cnt >= 2:
                        warn_reduce_half = True
                        warn_reasons.append("momentum_warning:hist_shrinking_volume_unknown")

            stop_parts["exit_now"] = bool(exit_now)
            stop_parts["exit_reasons"] = exit_reasons
            stop_parts["exit_check_ema5_lt_ema20"] = bool(exit_check_ema5_lt_ema20)
            stop_parts["exit_check_close_lt_ema20"] = bool(exit_check_close_lt_ema20)
            stop_parts["exit_check_momentum_exhaustion"] = bool(exit_check_mom_exhaust)
            stop_parts["exit_check_volume_dry"] = bool(exit_check_vol_dry)
            stop_parts["warn_reduce_half"] = bool(warn_reduce_half)
            stop_parts["warn_reasons"] = warn_reasons
            if warn_reduce_half:
                stop_parts["warn_display"] = "警告：MACD柱缩小但未转负，建议至少卖出一半"

            if exit_now:
                # immediate exit: stop at current price
                res.stopLossPrice = round(current, 6)
                stop_parts["final_stop_loss"] = round(current, 6)
                stop_parts["exit_display"] = "立刻离场"
                res.stopLossParts = stop_parts
                # Skip normal stop-loss calculation (but continue to compute trendOk decision below).
                # (No further stop-loss parts are needed in this branch.)
                pass
            else:

                # Volatility bin: std(returns[-20:])
                vol_std20: float | None = None
                if len(closes) >= 21:
                    rets_sl: list[float] = []
                    for i in range(-20, 0):
                        c0 = closes[i - 1]
                        c1 = closes[i]
                        if c0 > 0:
                            rets_sl.append((c1 / c0) - 1.0)
                    if len(rets_sl) >= 10:
                        mu = sum(rets_sl) / float(len(rets_sl))
                        var = sum((r - mu) ** 2 for r in rets_sl) / float(len(rets_sl))
                        vol_std20 = math.sqrt(max(0.0, var))
                stop_parts["vol_std20"] = round(vol_std20, 6) if vol_std20 is not None else None

                if vol_std20 is None:
                    atr_k = 1.2
                    max_loss_pct = 0.08
                    vol_bin = "unknown"
                elif vol_std20 <= 0.02:
                    atr_k = 1.1
                    max_loss_pct = 0.06
                    vol_bin = "low"
                elif vol_std20 <= 0.04:
                    atr_k = 1.2
                    max_loss_pct = 0.08
                    vol_bin = "mid"
                else:
                    atr_k = 1.4
                    max_loss_pct = 0.10
                    vol_bin = "high"
                stop_parts["vol_bin"] = vol_bin
                stop_parts["atr_k"] = atr_k
                stop_parts["max_loss_pct"] = max_loss_pct

                atr14 = _atr14(highs, lows, closes, 14)
                if atr14 is None:
                    res.stopLossPrice = None
                    res.missingData.append("atr14_unavailable")
                else:
                    buffer = atr_k * atr14
                    hard_stop = current * (1.0 - max_loss_pct)
                    stop_loss_support = final_support - buffer
                    final_stop = max(stop_loss_support, hard_stop)
                    final_stop = min(final_stop, current)  # never above current
                    stop_parts["atr14"] = round(atr14, 6)
                    stop_parts["buffer"] = round(buffer, 6)
                    stop_parts["hard_stop"] = round(hard_stop, 6)
                    stop_parts["stop_loss_support_minus_buffer"] = round(stop_loss_support, 6)
                    stop_parts["final_stop_loss"] = round(final_stop, 6)
                    res.stopLossPrice = round(final_stop, 6)
                    res.stopLossParts = stop_parts
    except Exception:
        res.stopLossPrice = None

    # ---------- Buy (CN daily), deterministic (no LLM) ----------
    # Unified two-mode right-side system:
    # - Mode A: breakout + pullback
    # - Mode B: momentum new-high
    try:
        buy_checks: dict[str, Any] = {}
        buy_mode: str = "none"
        buy_action: str = "wait"
        buy_zone_low: float | None = None
        buy_zone_high: float | None = None
        buy_why: str | None = None

        if bool(res.stopLossParts.get("exit_now")):
            buy_mode = "none"
            buy_action = "avoid"
            buy_why = "风险：立刻离场信号触发，禁止买入"
        else:
            n = len(closes)
            if n >= 26 and len(opens) == n and len(highs) == n and len(lows) == n and len(vols) == n:
                close = closes[-1]
                vol = vols[-1]
                vol_prev = vols[-2] if n >= 2 else vol

                vol_sma20 = (sum(vols[-21:-1]) / 20.0) if n >= 21 else None
                buy_checks["vol_sma20"] = round(vol_sma20, 6) if vol_sma20 is not None else None

                ema20_rising = False
                if ema20s and len(ema20s) >= 2:
                    ema20_rising = bool(ema20s[-1] > ema20s[-2])
                macd_hist_now = float(hist[-1]) if hist else 0.0
                in_trend = bool(
                    res.values.ema20 is not None
                    and close > float(res.values.ema20)
                    and ema20_rising
                    and macd_hist_now > 0.0
                )
                buy_checks["in_trend"] = in_trend
                buy_checks["ema20_rising"] = ema20_rising
                buy_checks["macd_hist_now"] = round(macd_hist_now, 6)

                if in_trend:
                    buy_mode = "B_momentum"
                    prev10_high = max(highs[-11:-1]) if n >= 11 else max(highs[:-1])
                    new_high = bool(close > prev10_high)
                    vol_ok = bool(vol_sma20 is not None and vol > vol_sma20 * 1.2)
                    macd_inc = bool(len(hist) >= 2 and float(hist[-1]) > float(hist[-2]))
                    rsi_ok = bool(res.values.rsi14 is not None and float(res.values.rsi14) < 80.0)
                    buy_checks["b_prev10_high"] = round(prev10_high, 6)
                    buy_checks["b_new_high"] = new_high
                    buy_checks["b_vol_ok"] = vol_ok
                    buy_checks["b_macd_inc"] = macd_inc
                    buy_checks["b_rsi_ok"] = rsi_ok

                    buy_zone_low = float(prev10_high)
                    buy_zone_high = float(prev10_high) * 1.02
                    if new_high and vol_ok and macd_inc and rsi_ok:
                        buy_action = "buy"
                        buy_why = "模式B：趋势中创10日新高，放量且动能增强"
                    else:
                        buy_action = "wait"
                        buy_why = "模式B：趋势中，等待新高+放量/动能确认"
                else:
                    buy_mode = "A_pullback"
                    breakout_idx: int | None = None
                    breakout_level: float | None = None
                    # Search last 1..5 days for breakout day (exclude today)
                    for k in range(1, min(6, n)):
                        di = n - 1 - k
                        if di < 21:
                            continue
                        level = max(highs[di - 20 : di])
                        vol_ma = sum(vols[di - 20 : di]) / 20.0
                        is_breakout = bool(closes[di] > level and vols[di] > vol_ma * 1.2)
                        if is_breakout:
                            breakout_idx = di
                            breakout_level = level
                            break
                    in_pullback_window = breakout_idx is not None
                    buy_checks["a_in_pullback_window"] = in_pullback_window
                    buy_checks["a_breakout_idx"] = breakout_idx
                    buy_checks["a_breakout_level"] = round(breakout_level, 6) if breakout_level is not None else None

                    ema20_now = float(res.values.ema20) if res.values.ema20 is not None else None
                    low10 = min(lows[-10:]) if n >= 10 else min(lows)
                    support = max(low10, ema20_now) if ema20_now is not None else low10
                    buy_checks["a_support"] = round(support, 6)

                    if breakout_level is not None and ema20_now is not None:
                        pullback_signal = (
                            (lows[-1] <= breakout_level * 1.01)
                            and (close >= support * 0.99)
                            and (vol < vol_prev)
                            and (closes[-1] > opens[-1])
                        )
                        buy_checks["a_pullback_signal"] = bool(pullback_signal)
                        buy_zone_low = max(support * 0.99, breakout_level * 0.99)
                        buy_zone_high = breakout_level * 1.01
                        if in_pullback_window and pullback_signal:
                            buy_action = "buy"
                            buy_why = "模式A：突破后回踩到支撑区，缩量止跌"
                        elif in_pullback_window:
                            buy_action = "wait"
                            buy_why = "模式A：回踩窗口内，等待缩量止跌"
                        else:
                            buy_action = "wait"
                            buy_why = "模式A：未在回踩窗口"
                    else:
                        buy_action = "wait"
                        buy_why = "模式A：数据不足（需要≥20日平台/EMA）"
            else:
                buy_mode = "none"
                buy_action = "wait"
                buy_why = "数据不足（需要至少26日K线）"

        res.buyMode = buy_mode
        res.buyAction = buy_action
        res.buyZoneLow = round(buy_zone_low, 6) if buy_zone_low is not None else None
        res.buyZoneHigh = round(buy_zone_high, 6) if buy_zone_high is not None else None
        res.buyRefPrice = round(float(closes[-1]), 6) if closes else None
        res.buyWhy = buy_why
        res.buyChecks = buy_checks
    except Exception:
        res.buyMode = None
        res.buyAction = None

    # Decide final TrendOK: require all checks to be True; if any required check is None, return None.
    required = [
        res.checks.emaOrder,
        res.checks.macdPositive,
        res.checks.macdHistExpanding,
        res.checks.closeNear20dHigh,
        res.checks.rsiInRange,
        res.checks.volumeSurge,
    ]
    if any(x is None for x in required):
        res.trendOk = None
        res.missingData.append("insufficient_indicators")
    else:
        res.trendOk = bool(all(bool(x) for x in required))
    return res


@app.get("/market/stocks/trendok", response_model=list[TrendOkResult])
def market_stocks_trendok(
    symbols: Annotated[list[str] | None, Query()] = None,
    refresh: bool = False,
) -> list[TrendOkResult]:
    """
    Batch TrendOK evaluation for Watchlist (CN daily only).
    Uses DB-cached daily bars and does NOT trigger external fetches.
    """
    syms0 = symbols if isinstance(symbols, list) else []
    syms = [str(s or "").strip().upper() for s in syms0]
    syms = [s for s in syms if s]
    if not syms:
        return []
    if len(syms) > 200:
        syms = syms[:200]

    # Load names (best-effort).
    by_name: dict[str, str] = {}
    placeholders = ",".join(["?"] * len(syms))
    with _connect() as conn:
        for r in conn.execute(
            f"SELECT symbol, name FROM market_stocks WHERE symbol IN ({placeholders})",
            tuple(syms),
        ).fetchall():
            by_name[str(r[0])] = str(r[1])

    # Optional: refresh cached daily bars before computing indicators (parallel, spot cache shared).
    if refresh:
        _BARS_REFRESH_WORKERS = 4
        with ThreadPoolExecutor(max_workers=_BARS_REFRESH_WORKERS) as pool:
            futures = {
                pool.submit(market_stock_bars, sym, days=120, force=False): sym for sym in syms
            }
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    pass

    out: list[TrendOkResult] = []
    with _connect() as conn:
        for sym in syms:
            # Pull the most recent 120 daily bars for indicators.
            rows = conn.execute(
                """
                SELECT date, open, high, low, close, volume
                FROM market_bars
                WHERE symbol = ?
                ORDER BY date ASC
                """,
                (sym,),
            ).fetchall()
            # Keep only the tail for performance.
            tail = rows[-120:] if len(rows) > 120 else rows
            bars = [
                (
                    str(r[0]),
                    str(r[1]) if r[1] is not None else None,
                    str(r[2]) if r[2] is not None else None,
                    str(r[3]) if r[3] is not None else None,
                    str(r[4]) if r[4] is not None else None,
                    str(r[5]) if r[5] is not None else None,
                )
                for r in tail
            ]
            out.append(_market_stock_trendok_one(symbol=sym, name=by_name.get(sym), bars=bars))
    return out


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

    def _latest_expected_daily_bar_date(mkt: str) -> str:
        """
        Best-effort expected latest completed daily bar date (YYYY-MM-DD).

        We intentionally avoid complex holiday calendars; we only handle weekends.
        This is used to decide whether DB cache is stale and should be refreshed even
        when we already have enough cached rows.
        """
        if mkt == "HK":
            tz = ZoneInfo("Asia/Hong_Kong")
        else:
            tz = ZoneInfo("Asia/Shanghai")
        now_local = datetime.now(tz=tz).date()
        # Weekend -> move back to Friday.
        d0 = now_local
        while d0.weekday() >= 5:
            d0 = d0 - timedelta(days=1)
        return d0.strftime("%Y-%m-%d")

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

    def _resp_from_cached(rows: list[tuple[Any, ...]]) -> MarketBarsResponse:
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
            for r in reversed(rows)
        ]
        return MarketBarsResponse(
            symbol=sym,
            market=market,
            ticker=ticker,
            name=name,
            currency=currency,
            bars=out2,
        )

    # Auto-refresh if cache is stale (even if we already have enough rows).
    cached_last = str(cached[0][0]) if cached else ""
    expected_last = _latest_expected_daily_bar_date(market)
    cache_stale = bool(cached_last and cached_last < expected_last)

    if force or len(cached) < days2 or cache_stale:
        ts = now_iso()
        try:
            # Upstream endpoints can be flaky (e.g. remote disconnect). Retry once, then fall back to cache if available.
            last_err: Exception | None = None
            for attempt in range(2):
                try:
                    if market == "CN":
                        bars = fetch_cn_a_daily_bars(ticker, days=days2)
                    elif market == "HK":
                        bars = fetch_hk_daily_bars(ticker, days=days2)
                    else:
                        raise HTTPException(status_code=400, detail="Unsupported market")
                    last_err = None
                    break
                except HTTPException:
                    raise
                except Exception as e:
                    last_err = e
                    if attempt == 0:
                        time.sleep(0.4)
                        continue
            if last_err is not None:
                if cached:
                    # Graceful degrade: return cached bars so UI remains usable.
                    return _resp_from_cached(cached)
                raise HTTPException(status_code=500, detail=f"Bars fetch failed for {ticker}: {last_err}") from last_err
        except HTTPException:
            raise
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

    return _resp_from_cached(cached)


@app.post("/market/stocks/bars/refresh", response_model=BarsRefreshResponse)
def market_stocks_bars_refresh(body: BarsRefreshRequest) -> BarsRefreshResponse:
    """
    Batch refresh daily bars for given symbols (force fetch). Uses shared spot cache
    and parallel workers so Watchlist can refresh in one call instead of N sequential GETs.
    """
    syms = [str(s or "").strip().upper() for s in (body.symbols or []) if str(s or "").strip()]
    syms = syms[:200]
    if not syms:
        return BarsRefreshResponse(refreshed=0, failed=0)
    refreshed = 0
    failed = 0
    _BARS_REFRESH_WORKERS = 4
    with ThreadPoolExecutor(max_workers=_BARS_REFRESH_WORKERS) as pool:
        futures = {
            pool.submit(market_stock_bars, sym, days=120, force=True): sym for sym in syms
        }
        for fut in as_completed(futures):
            try:
                fut.result()
                refreshed += 1
            except Exception:
                failed += 1
    return BarsRefreshResponse(refreshed=refreshed, failed=failed)


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
            SELECT date, raw_json
            FROM market_chips
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()

    # Auto-refresh if cache is stale (chips are daily; refresh when latest cached date lags).
    cached_last = str(cached[0][0]) if cached else ""
    expected_last = _today_cn_date_str()
    cache_stale = bool(cached_last and cached_last < expected_last)

    if (not force) and (not cache_stale) and len(cached) >= min(days2, 30):
        items = [json.loads(str(r[1])) for r in reversed(cached)]
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
            SELECT date, raw_json
            FROM market_fund_flow
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (sym, days2),
        ).fetchall()

    # Auto-refresh if cache is stale (fund flow is daily; refresh when latest cached date lags).
    cached_last = str(cached[0][0]) if cached else ""
    expected_last = _today_cn_date_str()
    cache_stale = bool(cached_last and cached_last < expected_last)

    if (not force) and (not cache_stale) and len(cached) >= min(days2, 30):
        items = [json.loads(str(r[1])) for r in reversed(cached)]
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


def _get_latest_cn_industry_fund_flow_date(conn: duckdb.DuckDBPyConnection) -> str | None:
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
                hist = fetch_cn_industry_fund_flow_hist(name, industry_code=code, days=days)
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

    # Aggregate per industry.
    #
    # IMPORTANT: Use industry NAME as the primary key to avoid historical code mismatches.
    # Different providers/versions may emit different `industry_code` values for the same
    # industry (e.g. hashed codes vs BKxxxx), which would otherwise fragment the time series
    # and cause the UI to show zeros (and "Top outflow" to be empty).
    by_name: dict[str, dict[str, Any]] = {}
    for d, code, name, net in rows:
        d2 = str(d)
        name2 = _norm_str(name)
        if not name2:
            continue
        code2 = _norm_str(code)
        net2 = float(net or 0.0)
        cur = by_name.get(name2)
        if cur is None:
            cur = {"industryCode": code2, "industryName": name2, "series": {}, "sum": 0.0}
            by_name[name2] = cur
        # Prefer a BK-style code when available.
        if (not str(cur.get("industryCode") or "").strip()) or (str(code2).startswith("BK")):
            if code2:
                cur["industryCode"] = code2
        cur["series"][d2] = net2
        cur["sum"] = float(cur["sum"]) + net2

    out_rows: list[IndustryFundFlowRow] = []
    for name, agg in by_name.items():
        series_map: dict[str, float] = agg.get("series") or {}
        series = [IndustryFundFlowPoint(date=d, netInflow=float(series_map.get(d, 0.0))) for d in dates]
        net_asof = float(series_map.get(as_of, 0.0))
        out_rows.append(
            IndustryFundFlowRow(
                industryCode=str(agg.get("industryCode") or ""),
                industryName=str(agg.get("industryName") or name),
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
    market_turnover_cny = 0.0
    market_volume = 0.0

    def _finite_float0(v: Any) -> float:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            try:
                return float(v) if math.isfinite(float(v)) else 0.0
            except Exception:
                return 0.0
        s = str(v).strip().replace(",", "").replace("%", "")
        if not s or s in ("-", "—", "N/A", "None"):
            return 0.0
        try:
            f = float(s)
            return f if math.isfinite(f) else 0.0
        except Exception:
            return 0.0

    def _breadth_from_market_cache() -> dict[str, Any] | None:
        """
        Prefer local market cache to avoid upstream blocks/captcha.
        Uses `market_quotes.change_pct` for CN stocks to compute breadth and sums `turnover`/`volume`.
        """
        try:
            with _connect() as conn:
                rows = conn.execute(
                    """
                    SELECT s.ticker, q.change_pct, q.turnover, q.volume
                    FROM market_stocks s
                    JOIN market_quotes q ON q.symbol = s.symbol
                    WHERE s.market = 'CN'
                    """,
                ).fetchall()
        except Exception:
            return None
        if not rows:
            return None
        up2 = 0
        down2 = 0
        flat2 = 0
        turn2 = 0.0
        vol2 = 0.0
        for ticker, chg_s, turn_s, vol_s in rows:
            t = str(ticker or "").strip()
            # Only count CN equities (exclude legacy prefixed tickers and non-6-digit entries).
            if len(t) != 6 or (not t.isdigit()):
                continue

            # Parse change_pct: treat non-numeric as missing (do NOT count as flat).
            chg_raw = str(chg_s or "").strip().replace(",", "").replace("%", "")
            chg_val: float | None = None
            if chg_raw and chg_raw not in ("-", "—", "N/A", "None"):
                try:
                    f = float(chg_raw)
                    chg_val = f if math.isfinite(f) else None
                except Exception:
                    chg_val = None

            if chg_val is not None:
                if chg_val > 0:
                    up2 += 1
                elif chg_val < 0:
                    down2 += 1
                else:
                    flat2 += 1

            # Turnover/volume: best-effort sum regardless of change_pct parse success.
            turn2 += _finite_float0(turn_s)
            vol2 += _finite_float0(vol_s)
        total2 = up2 + down2 + flat2
        if total2 <= 0:
            return None
        ratio2 = float(up2) / float(down2) if down2 > 0 else float(up2)
        return {
            "date": d,
            "up_count": up2,
            "down_count": down2,
            "flat_count": flat2,
            "total_count": total2,
            "up_down_ratio": ratio2,
            "total_turnover_cny": turn2,
            "total_volume": vol2,
            "raw": {"source": "db_market_quotes", "rows": len(rows)},
        }
    try:
        # Prefer local cache for TODAY; for historical dates fall back to provider.
        breadth = _breadth_from_market_cache() if d == _today_cn_date_str() else None
        if breadth is None:
            breadth = fetch_cn_market_breadth_eod(dt)
        raw["breadth"] = breadth
        up = int(breadth.get("up_count") or 0)
        down = int(breadth.get("down_count") or 0)
        flat = int(breadth.get("flat_count") or 0)
        ratio = _finite_float(breadth.get("up_down_ratio"), 0.0)
        market_turnover_cny = _finite_float(breadth.get("total_turnover_cny"), 0.0)
        market_volume = _finite_float(breadth.get("total_volume"), 0.0)
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

    # Risk rules (5-zone MVP):
    # no_new_positions (risk-off) -> caution -> normal -> hot -> euphoric
    rules: list[str] = []
    risk_mode = "normal"

    # Note: marketTurnoverCny is best-effort, computed from spot snapshot (same source as breadth).
    turnover_high = market_turnover_cny >= 1.5e12  # ~1.5T CNY
    turnover_hot = market_turnover_cny >= 1.8e12
    turnover_euphoric = market_turnover_cny >= 2.5e12
    breadth_good = ratio >= 1.2
    breadth_hot = ratio >= 1.5
    breadth_euphoric = ratio >= 2.0
    premium_good = premium >= 0.0
    premium_hot = premium >= 0.5
    premium_euphoric = premium >= 3.0

    # Bullish override: high activity + breadth + positive premium should not be forced into caution.
    bullish_override = turnover_high and breadth_good and premium_good

    # 1) Bullish tiers first (so we can output hot/euphoric).
    if turnover_euphoric and breadth_euphoric and premium_euphoric and failed_rate <= 35.0:
        risk_mode = "euphoric"
        rules.append("euphoric(turnover>=2.5T && breadth>=2.0 && premium>=3.0 && failed<=35)")
    elif turnover_hot and breadth_hot and premium_hot and failed_rate <= 50.0:
        risk_mode = "hot"
        rules.append("hot(turnover>=1.8T && breadth>=1.5 && premium>=0.5 && failed<=50)")
    else:
        # 2) Bearish / risk-off gates.
        if premium < 0.0 and failed_rate >= 70.0:
            risk_mode = "no_new_positions"
            rules.append("premium<0 && failedLimitUpRate>=70 => no_new_positions")
        elif failed_rate >= 70.0:
            risk_mode = "caution"
            rules.append("failedLimitUpRate>=70 => caution")
        elif premium < 0.0:
            risk_mode = "caution"
            rules.append("premium<0 => caution")

        # 3) Override: allow normal in strong markets even if failed rate is noisy.
        if risk_mode in ("caution", "no_new_positions") and bullish_override and failed_rate <= 85.0:
            risk_mode = "normal"
            rules.append("bullish_override(turnover_high && breadth_ratio>=1.2 && premium>=0)")
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
        "marketTurnoverCny": market_turnover_cny,
        "marketVolume": market_volume,
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
        # Prefer returning cached values over overwriting the DB with zeros.
        cached2 = _list_cn_sentiment_days(as_of_date=d, days=1)
        if cached2:
            last = cached2[-1]
            last_rules = (last.get("rules") if isinstance(last.get("rules"), list) else []) or []
            return MarketCnSentimentResponse(
                asOfDate=d,
                days=1,
                items=[
                    MarketCnSentimentRow(
                        **{
                            **last,
                            "rules": [*last_rules, f"stale_sync_failed: {type(e).__name__}: {e}"],
                        }
                    )
                ],
            )
        # Fallback: computed-only (not persisted).
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
            "rules": [f"compute_failed: {type(e).__name__}: {e}"],
            "updatedAt": now_iso(),
            "raw": {"error": str(e)},
        }

    rules_raw = out.get("rules") or []
    rules_list = [str(x) for x in rules_raw] if isinstance(rules_raw, list) else [str(rules_raw)]

    breadth_failed = any(("breadth_failed" in r) for r in rules_list)
    compute_failed = any(("compute_failed" in r) for r in rules_list)
    premium_failed = any(("yesterday_limitup_premium_failed" in r) for r in rules_list)
    failed_rate_failed = any(("failed_limitup_rate_failed" in r) for r in rules_list)

    # If breadth computation failed, do NOT overwrite the DB with zeros.
    # Premium/failed-rate failures are treated as partial failures: we can still update breadth
    # metrics and keep other fields from cache.
    if breadth_failed or compute_failed:
        # Clean up placeholder rows written by older versions (all-zero rows caused by upstream failures).
        try:
            dt0 = datetime.strptime(d, "%Y-%m-%d").date()
            with _connect() as conn:
                for back in range(0, 8):  # last 7 days + today
                    dd = (dt0 - timedelta(days=back)).strftime("%Y-%m-%d")
                    conn.execute(
                        """
                        DELETE FROM market_cn_sentiment_daily
                        WHERE date = ?
                          AND total_count = 0
                          AND up_down_ratio = 0.0
                          AND market_turnover_cny = 0.0
                          AND market_volume = 0.0
                          AND yesterday_limitup_premium = 0.0
                        """,
                        (dd,),
                    )
                conn.commit()
        except Exception:
            # Best-effort cleanup: ignore failures.
            pass
        cached3 = _list_cn_sentiment_days(as_of_date=d, days=1)
        if cached3:
            last = cached3[-1]
            last_date = str(last.get("date") or "")
            last_rules = (last.get("rules") if isinstance(last.get("rules"), list) else []) or []
            # Return cached data and surface why it is stale.
            return MarketCnSentimentResponse(
                asOfDate=d,
                days=1,
                items=[
                    MarketCnSentimentRow(
                        **{
                            **last,
                            "rules": [
                                *last_rules,
                                f"stale_upstream_failed: requested={d} latest={last_date}",
                                *rules_list[:3],
                            ],
                        }
                    )
                ],
            )

    # Partial failures: keep last known premium/failed-rate instead of writing zeros.
    if premium_failed or failed_rate_failed:
        cached4 = _list_cn_sentiment_days(as_of_date=d, days=1)
        if cached4:
            last = cached4[-1]
            last_date = str(last.get("date") or "")
            if premium_failed:
                out["premium"] = float(last.get("yesterdayLimitUpPremium") or 0.0)
                rules_list = [
                    r for r in rules_list if "yesterday_limitup_premium_failed" not in r
                ] + [f"premium_stale_from: {last_date}"]
            if failed_rate_failed:
                out["failedRate"] = float(last.get("failedLimitUpRate") or 0.0)
                rules_list = [
                    r for r in rules_list if "failed_limitup_rate_failed" not in r
                ] + [f"failed_rate_stale_from: {last_date}"]

    raw0 = out.get("raw")
    raw_dict: dict[str, Any] = raw0 if isinstance(raw0, dict) else {}
    rules2 = rules_list
    upsert_ok = False
    try:
        _upsert_cn_sentiment_daily(
            date=d,
            as_of_date=str(out["asOfDate"]),
            up=int(out["up"]),
            down=int(out["down"]),
            flat=int(out["flat"]),
            up_down_ratio=_finite_float(out.get("ratio"), 0.0),
            market_turnover_cny=_finite_float(out.get("marketTurnoverCny"), 0.0),
            market_volume=_finite_float(out.get("marketVolume"), 0.0),
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
                marketTurnoverCny=_finite_float(out.get("marketTurnoverCny"), 0.0),
                marketVolume=_finite_float(out.get("marketVolume"), 0.0),
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
    # Quant is global: ignore accountId and use a stable internal account id for caching.
    aid = _global_quant_account_id()

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
        asOfTs=str(out.get("asOfTs") or "") or None,
        asOfDate=str(out.get("asOfDate") or as_of),
        accountId=aid,
        createdAt=str(cached.get("createdAt") or ""),
        universeVersion=str(out.get("universeVersion") or universeVersion),
        riskMode=str(out.get("riskMode") or "") or None,
        objective=str(out.get("objective") or "") or None,
        horizon=str(out.get("horizon") or "") or None,
        items=[RankItem(**x) for x in items if isinstance(x, dict)],
        debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
    )


@app.post("/rank/cn/next2d/generate", response_model=RankSnapshotResponse)
def rank_cn_next2d_generate(req: RankNext2dGenerateRequest) -> RankSnapshotResponse:
    as_of = (req.asOfDate or "").strip() or _today_cn_date_str()
    universe = (req.universeVersion or "").strip() or "v0"
    limit2 = max(1, min(int(req.limit), 200))

    # Quant is global: ignore accountId and use a stable internal account id for caching/learning.
    aid = _global_quant_account_id()

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
    # Run best-effort outcome labeling to keep calibration fresh.
    try:
        _label_quant_2d_outcomes_best_effort(account_id=aid, limit=500)
    except Exception:
        pass

    # Build a larger raw universe for learning; still return only `limit`.
    internal_limit = max(limit2, 80)
    raw_out = _rank_build_and_score(
        account_id=aid,
        as_of_date=as_of,
        limit=internal_limit,
        universe_version=universe,
        include_holdings=False,
    )

    # Calibration (bucketed, cached).
    calib_key = f"v0:{aid}:quant2d:bucket20"
    calib_out: dict[str, Any] = {}
    cached_cal = _get_quant_2d_calibration_cached(key=calib_key)
    use_cache = False
    if cached_cal is not None:
        try:
            updated = datetime.fromisoformat(str(cached_cal.get("updatedAt") or "")).replace(tzinfo=UTC)
            age = (datetime.now(tz=UTC) - updated).total_seconds()
            if age <= 6 * 3600:
                out0 = cached_cal.get("output")
                calib_out = out0 if isinstance(out0, dict) else {}
                use_cache = True
        except Exception:
            use_cache = False
    if not use_cache:
        calib_out = _build_quant_2d_calibration(account_id=aid, buckets=20, lookback_days=180)
        try:
            _upsert_quant_2d_calibration_cached(key=calib_key, ts=ts, output=calib_out)
        except Exception:
            pass

    items_raw = raw_out.get("items")
    raw_items: list[Any] = items_raw if isinstance(items_raw, list) else []

    # Persist rank events for learning (cap).
    try:
        ev_rows: list[dict[str, Any]] = []
        for r in raw_items[:80]:
            if not isinstance(r, dict):
                continue
            evidence0 = r.get("evidence")
            ev = cast(dict[str, Any], evidence0) if isinstance(evidence0, dict) else {}
            ev_rows.append(
                {
                    "symbol": r.get("symbol"),
                    "ticker": r.get("ticker"),
                    "name": r.get("name"),
                    "buyPrice": ev.get("buyPrice"),
                    "buyPriceSrc": ev.get("buyPriceSrc"),
                    "rawScore": r.get("rawScore"),
                    "evidence": ev,
                }
            )
        _upsert_quant_2d_rank_events(account_id=aid, as_of_ts=str(raw_out.get("asOfTs") or ts), as_of_date=as_of, rows=ev_rows)
    except Exception:
        pass

    # Apply calibration and compute base decision score (before LLM rerank).
    final_items: list[dict[str, Any]] = []
    evidence_by_symbol: dict[str, dict[str, Any]] = {}
    calib_n_total = int((calib_out.get("n") or 0) if isinstance(calib_out, dict) else 0)
    calib_ready = calib_n_total >= 60 and isinstance(calib_out.get("items"), list) and bool(calib_out.get("items"))
    for r in raw_items:
        if not isinstance(r, dict):
            continue
        raw_score = _finite_float(r.get("rawScore"), _finite_float(r.get("score"), 0.0))
        evidence0 = r.get("evidence")
        ev = cast(dict[str, Any], evidence0) if isinstance(evidence0, dict) else {}
        sym = _norm_str(r.get("symbol") or "")
        if sym and isinstance(ev, dict):
            evidence_by_symbol[sym] = ev
        why = _quant2d_why_from_evidence(ev)

        # Bootstrap: if calibration is not ready, use deterministic rawScore as score.
        if not calib_ready:
            score = max(0.0, min(100.0, float(raw_score)))
            prob: float | None = None
            ev2: float | None = None
            dd2: float | None = None
            p10 = 0.0
            n = 0
            prob_band = _rank_prob_band(float(raw_score))
            conf = "Low"
        else:
            bkt = _quant2d_find_bucket(calib_out, raw_score)
            if bkt is None:
                # Calibration exists but this score fell outside buckets; fall back to raw score.
                score = max(0.0, min(100.0, float(raw_score)))
                prob = None
                ev2 = None
                dd2 = None
                p10 = 0.0
                n = 0
                prob_band = _rank_prob_band(float(raw_score))
                conf = "Low"
            else:
                prob = _finite_float(bkt.get("probWin"), 0.0) * 100.0
                ev2 = _finite_float(bkt.get("ev2dPct"), 0.0)
                p10 = _finite_float(bkt.get("p10Ret2dPct"), 0.0)
                dd2 = _finite_float(bkt.get("dd2dPct"), 0.0)
                n = int(bkt.get("n") or 0)
                conf = _quant2d_confidence(n)
                score = _quant2d_decision_score(prob_profit_pct=prob, ev2d_pct=ev2, p10_ret2d_pct=p10, dd2d_pct=dd2)
                prob_band = _quant2d_prob_band(prob)
        final_items.append(
            {
                "symbol": r.get("symbol"),
                "market": r.get("market"),
                "ticker": r.get("ticker"),
                "name": r.get("name"),
                "sector": r.get("sector"),
                "score": round(float(score), 2),
                "probProfit2d": (round(float(prob), 2) if isinstance(prob, (int, float)) else None),
                "ev2dPct": (round(float(ev2), 3) if isinstance(ev2, (int, float)) else None),
                "dd2dPct": (round(float(dd2), 3) if isinstance(dd2, (int, float)) else None),
                "confidence": conf,
                "buyPrice": _finite_float(ev.get("buyPrice"), 0.0) or None,
                "buyPriceSrc": _norm_str(ev.get("buyPriceSrc") or "") or None,
                "whyBullets": why,
                "rawScore": round(float(raw_score), 2),
                "probBand": prob_band,
                "signals": r.get("signals") if isinstance(r.get("signals"), list) else [],
                "breakdown": r.get("breakdown") if isinstance(r.get("breakdown"), dict) else {},
            }
        )

    # LLM rerank + explain (best-effort): only adjust TopK candidates, and only when evidenceRefs are valid.
    llm_meta: dict[str, Any] = {"ok": False}
    try:
        top_for_llm = sorted(final_items, key=lambda x: float(x.get("score") or 0.0), reverse=True)[:12]
        payload = {
            "asOfTs": str(raw_out.get("asOfTs") or ts),
            "asOfDate": as_of,
            "horizon": "2d",
            "objective": "profit_probability",
            "candidates": [
                {
                    "symbol": _norm_str(x.get("symbol") or ""),
                    "ticker": _norm_str(x.get("ticker") or ""),
                    "name": _norm_str(x.get("name") or ""),
                    "evidence": evidence_by_symbol.get(_norm_str(x.get("symbol") or ""), {}),
                }
                for x in top_for_llm
                if _norm_str(x.get("symbol") or "")
            ],
            "context": {
                "riskMode": raw_out.get("riskMode"),
                "calibrationN": int((calib_out.get("n") or 0) if isinstance(calib_out, dict) else 0),
                "asOfDate": as_of,
            },
        }
        resp = _ai_quant_rank_explain(payload=payload)
        items_in = resp.get("items") if isinstance(resp, dict) else None
        items_llm: list[Any] = items_in if isinstance(items_in, list) else []
        adj_by_sym: dict[str, dict[str, Any]] = {}
        for it in items_llm:
            if not isinstance(it, dict):
                continue
            sym = _norm_str(it.get("symbol") or "")
            if not sym:
                continue
            adj_by_sym[sym] = it
        applied = 0
        for x in final_items:
            sym = _norm_str(x.get("symbol") or "")
            if not sym:
                continue
            if sym not in adj_by_sym:
                continue
            ev = evidence_by_symbol.get(sym) or {}
            it = adj_by_sym[sym]
            adj = _finite_float(it.get("llmScoreAdj"), 0.0)
            # Validate why bullets.
            why_in = it.get("whyBullets")
            why_out: list[str] = []
            if isinstance(why_in, list):
                for b in why_in:
                    if not isinstance(b, dict):
                        continue
                    txt = _norm_str(b.get("text") or "")
                    refs0 = b.get("evidenceRefs")
                    refs: list[str] = [str(r).strip() for r in refs0] if isinstance(refs0, list) else []
                    if not txt or not refs:
                        continue
                    ok = True
                    for ref in refs[:4]:
                        if _get_by_dot_path(ev, ref) is None:
                            ok = False
                            break
                    if ok:
                        why_out.append(txt)
                    if len(why_out) >= 5:
                        break
            if why_out:
                x["whyBullets"] = why_out
                # Apply small adjustment ONLY when we have evidence-backed bullets.
                x["score"] = round(
                    max(0.0, min(100.0, float(x.get("score") or 0.0) + float(adj))),
                    2,
                )
                applied += 1
        llm_meta = {
            "ok": True,
            "applied": applied,
            "model": resp.get("model") if isinstance(resp, dict) else None,
        }
    except Exception as e:
        llm_meta = {"ok": False, "error": str(e)}

    final_items.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    final_items = final_items[:limit2]

    output = {
        "asOfTs": str(raw_out.get("asOfTs") or ts),
        "asOfDate": as_of,
        "objective": "profit_probability_2d",
        "horizon": "avg_close_t1_t2",
        "accountId": aid,
        "universeVersion": universe,
        "riskMode": raw_out.get("riskMode"),
        "items": final_items,
        "debug": {
            "raw": raw_out.get("debug") if isinstance(raw_out.get("debug"), dict) else {},
            "calibrationKey": calib_key,
            "calibrationCached": bool(use_cache),
            "calibrationN": int((calib_out.get("n") or 0) if isinstance(calib_out, dict) else 0),
            "calibrationBuckets": len(calib_out.get("items") or []) if isinstance(calib_out.get("items"), list) else 0,
            "calibrationReady": bool(calib_ready),
            "llm": llm_meta,
        },
    }

    snap_id = _upsert_cn_rank_snapshot(account_id=aid, as_of_date=as_of, universe_version=universe, ts=ts, output=output)
    _prune_cn_rank_snapshots(keep_days=10)
    return RankSnapshotResponse(
        id=snap_id,
        asOfTs=str(output.get("asOfTs") or "") or None,
        asOfDate=str(output.get("asOfDate") or as_of),
        accountId=aid,
        createdAt=ts,
        universeVersion=str(output.get("universeVersion") or universe),
        riskMode=str(output.get("riskMode") or "") or None,
        objective=str(output.get("objective") or "") or None,
        horizon=str(output.get("horizon") or "") or None,
        items=[RankItem(**x) for x in final_items if isinstance(x, dict)],
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

    # Quant is global: ignore accountId and use a stable internal account id for caching.
    aid = _global_quant_account_id()

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

    # Quant is global: ignore accountId and use a stable internal account id for caching.
    aid = _global_quant_account_id()

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


def _quant_morning_radar_build(
    *,
    account_id: str,
    as_of_ts: str,
    universe_version: str,
    top_k: int = 3,
    per_theme: int = 3,
) -> dict[str, Any]:
    tz = ZoneInfo("Asia/Shanghai")
    try:
        dt = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
        dt_cn = dt.astimezone(tz)
    except Exception:
        dt_cn = datetime.now(tz=tz)
    trade_date = dt_cn.strftime("%Y-%m-%d")

    # Spot snapshot for representative stocks.
    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
    except Exception:
        spot_rows = []
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

    themes, dbg = _mainline_step1_candidates(trade_date=trade_date, force_membership=False)
    topk = (themes or [])[: max(1, min(int(top_k), 10))]

    out_themes: list[dict[str, Any]] = []
    for t in topk:
        if not isinstance(t, dict):
            continue
        kind = _norm_str(t.get("kind") or "")
        name = _norm_str(t.get("name") or "")
        if not kind or not name:
            continue
        mem, _meta = _get_theme_members(kind=kind, name=name, trade_date=trade_date, force=False)
        # Pick representative stocks from members using spot change_pct.
        picks: list[dict[str, Any]] = []
        for code in (mem or [])[:300]:
            s = spot_map.get(code)
            if s is None:
                continue
            picks.append(
                {
                    "symbol": s.symbol,
                    "ticker": s.ticker,
                    "name": s.name,
                    "chgPct": _parse_pct(s.quote.get("change_pct") or ""),
                    "volRatio": _parse_num(s.quote.get("vol_ratio") or ""),
                    "turnover": _parse_num(s.quote.get("turnover") or ""),
                }
            )
        picks.sort(key=lambda x: float(x.get("chgPct") or 0.0), reverse=True)
        out_themes.append(
            {
                "kind": kind,
                "name": name,
                "score": _finite_float(t.get("step1Score"), 0.0),
                "todayStrength": _finite_float(t.get("todayStrength"), 0.0),
                "volSurge": _finite_float(t.get("volSurge"), 0.0),
                "limitupCount": int(t.get("limitupCount") or 0),
                "followersCount": int(t.get("followersCount") or 0),
                "topTickers": picks[: max(1, min(int(per_theme), 10))],
            }
        )

    return {
        "asOfTs": as_of_ts,
        "tradeDate": trade_date,
        "accountId": account_id,
        "universeVersion": universe_version,
        "themes": out_themes,
        "debug": {"step1": dbg, "spotRows": len(spot_rows)},
    }


@app.post("/rank/cn/morning/generate", response_model=MorningRadarResponse)
def rank_cn_morning_generate(req: MorningRadarGenerateRequest) -> MorningRadarResponse:
    universe = (req.universeVersion or "").strip() or "v0"
    # Quant is global: ignore accountId and use a stable internal account id for caching.
    aid = _global_quant_account_id()
    ts = (req.asOfTs or "").strip() or now_iso()
    out = _quant_morning_radar_build(
        account_id=aid,
        as_of_ts=ts,
        universe_version=universe,
        top_k=int(req.topK),
        per_theme=int(req.perTheme),
    )
    themes_raw = out.get("themes")
    themes0: list[Any] = themes_raw if isinstance(themes_raw, list) else []
    return MorningRadarResponse(
        asOfTs=str(out.get("asOfTs") or ts),
        tradeDate=str(out.get("tradeDate") or _today_cn_date_str()),
        accountId=aid,
        universeVersion=universe,
        themes=[MorningRadarTheme(**x) for x in themes0 if isinstance(x, dict)],
        debug=out.get("debug") if isinstance(out.get("debug"), dict) else None,
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
    market_turnover_cny: float = 0.0,
    market_volume: float = 0.0,
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
            MERGE INTO market_cn_sentiment_daily AS t
            USING (
              SELECT
                ? AS date,
                ? AS as_of_date,
                ? AS up_count,
                ? AS down_count,
                ? AS flat_count,
                ? AS total_count,
                ? AS up_down_ratio,
                ? AS market_turnover_cny,
                ? AS market_volume,
                ? AS yesterday_limitup_premium,
                ? AS failed_limitup_rate,
                ? AS risk_mode,
                ? AS rules_json,
                ? AS updated_at,
                ? AS raw_json
            ) AS s
            ON t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              as_of_date = s.as_of_date,
              up_count = s.up_count,
              down_count = s.down_count,
              flat_count = s.flat_count,
              total_count = s.total_count,
              up_down_ratio = s.up_down_ratio,
              market_turnover_cny = s.market_turnover_cny,
              market_volume = s.market_volume,
              yesterday_limitup_premium = s.yesterday_limitup_premium,
              failed_limitup_rate = s.failed_limitup_rate,
              risk_mode = s.risk_mode,
              rules_json = s.rules_json,
              updated_at = s.updated_at,
              raw_json = s.raw_json
            WHEN NOT MATCHED THEN INSERT (
              date, as_of_date, up_count, down_count, flat_count, total_count,
              up_down_ratio, market_turnover_cny, market_volume,
              yesterday_limitup_premium, failed_limitup_rate,
              risk_mode, rules_json, updated_at, raw_json
            ) VALUES (
              s.date, s.as_of_date, s.up_count, s.down_count, s.flat_count, s.total_count,
              s.up_down_ratio, s.market_turnover_cny, s.market_volume,
              s.yesterday_limitup_premium, s.failed_limitup_rate,
              s.risk_mode, s.rules_json, s.updated_at, s.raw_json
            )
            """,
            (
                date,
                as_of_date,
                int(up),
                int(down),
                int(flat),
                int(total),
                float(up_down_ratio),
                float(market_turnover_cny),
                float(market_volume),
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
                   up_down_ratio, market_turnover_cny, market_volume,
                   yesterday_limitup_premium, failed_limitup_rate,
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
                "marketTurnoverCny": float(r[6] or 0.0),
                "marketVolume": float(r[7] or 0.0),
                "yesterdayLimitUpPremium": float(r[8] or 0.0),
                "failedLimitUpRate": float(r[9] or 0.0),
                "riskMode": str(r[10] or "normal"),
                "rules": json.loads(str(r[11]) or "[]") if r[11] else [],
                "updatedAt": str(r[12] or ""),
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
                MERGE INTO leader_stocks AS t
                USING (
                  SELECT
                    ? AS id,
                    ? AS date,
                    ? AS symbol,
                    ? AS market,
                    ? AS ticker,
                    ? AS name,
                    ? AS entry_price,
                    ? AS score,
                    ? AS reason,
                    ? AS why_bullets_json,
                    ? AS expected_duration_days,
                    ? AS buy_zone_json,
                    ? AS triggers_json,
                    ? AS invalidation,
                    ? AS target_price_json,
                    ? AS probability,
                    ? AS source_signals_json,
                    ? AS risk_points_json,
                    ? AS created_at
                ) AS s
                ON t.date = s.date AND t.symbol = s.symbol
                WHEN MATCHED THEN UPDATE SET
                  id = s.id,
                  market = s.market,
                  ticker = s.ticker,
                  name = s.name,
                  entry_price = s.entry_price,
                  score = s.score,
                  reason = s.reason,
                  why_bullets_json = s.why_bullets_json,
                  expected_duration_days = s.expected_duration_days,
                  buy_zone_json = s.buy_zone_json,
                  triggers_json = s.triggers_json,
                  invalidation = s.invalidation,
                  target_price_json = s.target_price_json,
                  probability = s.probability,
                  source_signals_json = s.source_signals_json,
                  risk_points_json = s.risk_points_json,
                  created_at = s.created_at
                WHEN NOT MATCHED THEN INSERT (
                  id, date, symbol, market, ticker, name,
                  entry_price, score, reason,
                  why_bullets_json, expected_duration_days, buy_zone_json, triggers_json, invalidation, target_price_json, probability,
                  source_signals_json, risk_points_json, created_at
                ) VALUES (
                  s.id, s.date, s.symbol, s.market, s.ticker, s.name,
                  s.entry_price, s.score, s.reason,
                  s.why_bullets_json, s.expected_duration_days, s.buy_zone_json, s.triggers_json, s.invalidation, s.target_price_json, s.probability,
                  s.source_signals_json, s.risk_points_json, s.created_at
                )
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
            MERGE INTO leader_stock_scores AS t
            USING (SELECT ? AS symbol, ? AS live_score, ? AS breakdown_json, ? AS updated_at) AS s
            ON t.symbol = s.symbol
            WHEN MATCHED THEN UPDATE SET
              live_score = s.live_score,
              breakdown_json = s.breakdown_json,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (symbol, live_score, breakdown_json, updated_at)
              VALUES (s.symbol, s.live_score, s.breakdown_json, s.updated_at)
            """,
            (sym, float(live_score), json.dumps(breakdown or {}, ensure_ascii=False), ts),
        )
        conn.commit()


def _compute_leader_live_score(
    *,
    market: str,
    feats: dict[str, Any],
    bars: list[dict[str, Any]] | None = None,
    chips_summary: dict[str, Any] | None,
    ff_breakdown: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Probability-weighted expected profitability score for the next ~2 trading days.
    - Range: 0..100 (higher => better expected edge)
    - Emphasizes win probability, while penalizing expected drawdown.
    - Uses recent daily bars (kNN-like similarity on price features) when available.
    - Falls back to a deterministic investability score when data is insufficient.
    """
    def clamp01(x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    last_close = _safe_float(feats.get("lastClose"))
    sma5 = _safe_float(feats.get("sma5"))
    sma10 = _safe_float(feats.get("sma10"))
    sma20 = _safe_float(feats.get("sma20"))
    high10 = _safe_float(feats.get("high10"))

    # --- Legacy deterministic investability breakdown (kept for explainability) ---
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
    main_ratio = 0.0
    super_ratio = 0.0
    large_ratio = 0.0
    change_pct = 0.0
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
    pr = 0.0
    avg_cost = 0.0
    conc70 = 0.0
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
        if main_ratio < 0:
            risk -= 3.0
    risk = max(0.0, min(10.0, risk))

    investability_total = max(0.0, min(100.0, trend + flow + structure + risk))

    # --- 2D profitability stats from recent bars (kNN-like similarity) ---
    p_win = 0.5
    ev_pct = 0.0
    dd_pct = 0.0
    samples = 0
    k = 0
    used_model = "fallback_investability"

    def _closes_from_bars(bs: list[dict[str, Any]]) -> list[float]:
        out: list[float] = []
        for b in bs:
            c = _finite_float((b or {}).get("close"), 0.0)
            if c > 0:
                out.append(float(c))
        return out

    def _sma(cl: list[float], end_idx: int, n: int) -> float:
        if end_idx + 1 < n:
            return 0.0
        seg = cl[end_idx + 1 - n : end_idx + 1]
        return float(sum(seg) / len(seg)) if seg else 0.0

    def _high(cl: list[float], end_idx: int, n: int) -> float:
        if end_idx < 0:
            return 0.0
        start = max(0, end_idx + 1 - n)
        seg = cl[start : end_idx + 1]
        return float(max(seg)) if seg else 0.0

    def _std(xs: list[float]) -> float:
        if not xs:
            return 0.0
        m = sum(xs) / len(xs)
        v = sum((x - m) ** 2 for x in xs) / len(xs)
        return float(math.sqrt(max(0.0, v)))

    bs = bars if isinstance(bars, list) else None
    if bs and len(bs) >= 35:
        closes = _closes_from_bars(bs[-220:])
        if len(closes) >= 35:
            # Build historical feature vectors and future labels.
            xs: list[list[float]] = []
            ys_ret: list[float] = []
            ys_dd: list[float] = []
            # Need at least 20 bars for MA features and +2 for label.
            for i in range(20, len(closes) - 2):
                c = closes[i]
                c1 = closes[i - 1]
                c3 = closes[i - 3] if i >= 3 else c1
                if c <= 0 or c1 <= 0 or c3 <= 0:
                    continue
                ret1 = c / c1 - 1.0
                ret3 = c / c3 - 1.0
                ma5 = _sma(closes, i, 5)
                ma20 = _sma(closes, i, 20)
                ma_gap = (ma5 / ma20 - 1.0) if (ma5 > 0 and ma20 > 0) else 0.0
                hi10 = _high(closes, i, 10)
                dist_hi10 = (c / hi10 - 1.0) if hi10 > 0 else 0.0
                # Volatility: std of last 10 1D returns.
                r10 = []
                for j in range(max(1, i - 9), i + 1):
                    if closes[j - 1] > 0 and closes[j] > 0:
                        r10.append(closes[j] / closes[j - 1] - 1.0)
                vol10 = _std(r10)

                # Labels: 2D forward return and worst close drawdown within 2D.
                fut2 = closes[i + 2] / c - 1.0
                low2 = min(closes[i + 1], closes[i + 2]) / c - 1.0
                dd2 = abs(min(0.0, low2))

                xs.append([ret1, ret3, ma_gap, dist_hi10, vol10])
                ys_ret.append(float(fut2))
                ys_dd.append(float(dd2))

            # Current feature vector.
            i0 = len(closes) - 1
            if len(xs) >= 25 and i0 >= 20:
                c = closes[i0]
                c1 = closes[i0 - 1]
                c3 = closes[i0 - 3] if i0 >= 3 else c1
                ret1_0 = c / c1 - 1.0 if (c > 0 and c1 > 0) else 0.0
                ret3_0 = c / c3 - 1.0 if (c > 0 and c3 > 0) else 0.0
                ma5_0 = _sma(closes, i0, 5)
                ma20_0 = _sma(closes, i0, 20)
                ma_gap_0 = (ma5_0 / ma20_0 - 1.0) if (ma5_0 > 0 and ma20_0 > 0) else 0.0
                hi10_0 = _high(closes, i0, 10)
                dist_hi10_0 = (c / hi10_0 - 1.0) if hi10_0 > 0 else 0.0
                r10_0 = []
                for j in range(max(1, i0 - 9), i0 + 1):
                    if closes[j - 1] > 0 and closes[j] > 0:
                        r10_0.append(closes[j] / closes[j - 1] - 1.0)
                vol10_0 = _std(r10_0)
                x0 = [ret1_0, ret3_0, ma_gap_0, dist_hi10_0, vol10_0]

                # Standardize by historical mean/std per feature.
                cols = list(zip(*xs, strict=False))
                means = [float(sum(col) / len(col)) for col in cols]
                stds = [max(1e-9, _std(list(col))) for col in cols]

                def _z(v: list[float]) -> list[float]:
                    return [(v[i] - means[i]) / stds[i] for i in range(len(v))]

                xz = [_z(v) for v in xs]
                x0z = _z(x0)

                # Nearest neighbors.
                dists = []
                for i, v in enumerate(xz):
                    d = 0.0
                    for j in range(len(v)):
                        dv = v[j] - x0z[j]
                        d += dv * dv
                    dists.append((d, i))
                dists.sort(key=lambda t: t[0])
                k = max(15, min(35, int(len(dists) * 0.15)))
                idxs = [i for _d, i in dists[:k]]
                samples = len(idxs)
                if samples >= 15:
                    wins = [ys_ret[i] for i in idxs if ys_ret[i] > 0]
                    losses = [ys_ret[i] for i in idxs if ys_ret[i] <= 0]
                    p_win = float(len(wins) / samples) if samples > 0 else 0.5
                    e_win = float(sum(wins) / len(wins)) if wins else 0.0
                    e_loss = float(sum(abs(x) for x in losses) / len(losses)) if losses else 0.0
                    ev = p_win * e_win - (1.0 - p_win) * e_loss
                    dd = float(sum(ys_dd[i] for i in idxs) / samples) if samples > 0 else 0.0

                    # Adjust P using latest flow/chips proxies (small bounded nudges).
                    p_adj = p_win
                    if market == "CN":
                        if main_ratio > 2:
                            p_adj += 0.03
                        elif main_ratio > 0:
                            p_adj += 0.015
                        elif main_ratio < 0:
                            p_adj -= 0.02
                        if pr >= 0.65:
                            p_adj += 0.02
                        elif pr >= 0.45:
                            p_adj += 0.01
                        elif pr > 0:
                            p_adj -= 0.01
                    # Penalize excessive extension slightly.
                    if ext > 0.15:
                        p_adj -= 0.02
                    p_adj = max(0.05, min(0.95, p_adj))

                    p_win = p_adj
                    ev_pct = float(ev * 100.0)
                    dd_pct = float(dd * 100.0)
                    used_model = "knn_2d_edge"

    # Combine into final live score (probability weighted, with drawdown penalty).
    # Probability has higher weight by design.
    edge = 1.8 * (p_win * 100.0 - 50.0) + 0.8 * ev_pct - 0.6 * dd_pct
    total = 50.0 + edge

    if used_model == "fallback_investability":
        total = investability_total
    total = max(0.0, min(100.0, float(total)))

    return {
        "total": round(total, 2),
        "model": used_model,
        # Profitability view (2D horizon)
        "pWin2d": round(float(p_win), 4),
        "ev2dPct": round(float(ev_pct), 4),
        "dd2dPct": round(float(dd_pct), 4),
        "samples": int(samples),
        "k": int(k),
        # Legacy investability view (for explanation/debug)
        "investabilityTotal": round(float(investability_total), 2),
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
                bars=bars if isinstance(bars, list) else None,
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


def _bars_series_since_cached(symbol: str, start_date: str, *, limit: int = 60) -> list[dict[str, Any]]:
    """
    Cached-only daily close series (no external fetch).
    Used by UI endpoints to avoid triggering AkShare on refresh.
    """
    bars = _load_cached_bars(symbol, days=240)
    out: list[dict[str, Any]] = []
    for b in bars:
        d = str(b.get("date") or "")
        if not d or d < start_date:
            continue
        out.append({"date": d, "close": _safe_float(b.get("close"))})
    out.sort(key=lambda x: str(x.get("date") or ""))
    lim = max(1, int(limit))
    return out[-lim:]


def _bars_series_last_cached(symbol: str, *, limit: int = 20) -> list[dict[str, Any]]:
    """
    Cached-only last N daily closes (no external fetch).
    """
    bars = _load_cached_bars(symbol, days=240)
    out: list[dict[str, Any]] = []
    for b in bars:
        d = str(b.get("date") or "")
        if not d:
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
                WHERE broker = ? AND account_id IS NOT DISTINCT FROM ? AND sha256 = ?
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
            MERGE INTO broker_account_prompts AS t
            USING (SELECT ? AS account_id, ? AS strategy_prompt, ? AS updated_at) AS s
            ON t.account_id = s.account_id
            WHEN MATCHED THEN UPDATE SET
              strategy_prompt = s.strategy_prompt,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (account_id, strategy_prompt, updated_at)
              VALUES (s.account_id, s.strategy_prompt, s.updated_at)
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
            MERGE INTO strategy_reports AS t
            USING (
              SELECT
                ? AS id,
                ? AS account_id,
                ? AS date,
                ? AS created_at,
                ? AS model,
                ? AS input_snapshot_json,
                ? AS output_json
            ) AS s
            ON t.account_id = s.account_id AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
              id = s.id,
              created_at = s.created_at,
              model = s.model,
              input_snapshot_json = s.input_snapshot_json,
              output_json = s.output_json
            WHEN NOT MATCHED THEN INSERT (id, account_id, date, created_at, model, input_snapshot_json, output_json)
              VALUES (s.id, s.account_id, s.date, s.created_at, s.model, s.input_snapshot_json, s.output_json)
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

    # Best-effort spot snapshot for buyPrice/evidence (does not affect DB-first bars/chips/flow scoring).
    spot_rows: list[StockRow] = []
    try:
        spot_rows = fetch_cn_a_spot()
    except Exception:
        spot_rows = []
    spot_map: dict[str, StockRow] = {s.ticker: s for s in spot_rows if s.market == "CN" and s.ticker}

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

        spot = spot_map.get(ticker)
        buy_price = _finite_float((spot.quote.get("price") if spot else None), 0.0) if spot else 0.0
        buy_src = "spot" if buy_price > 0 else "bars_close"
        if buy_price <= 0:
            buy_price = last_close
            buy_src = "bars_close" if buy_price > 0 else "unknown"

        evidence = {
            "asOfDate": as_of_date,
            "symbol": sym,
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "riskMode": risk_mode,
            "buyPrice": buy_price,
            "buyPriceSrc": buy_src,
            "spot": (
                {
                    "price": spot.quote.get("price"),
                    "chgPct": spot.quote.get("change_pct"),
                    "volRatio": spot.quote.get("vol_ratio"),
                    "turnover": spot.quote.get("turnover"),
                }
                if spot
                else {}
            ),
            "bars": {
                "lastClose": last_close,
                "sma5": sma5,
                "sma10": sma10,
                "sma20": sma20,
                "high20": high20,
                "lastAmount": last_amt,
                "lastVolume": last_vol,
                "relVol": rel_vol,
            },
            "fundFlow": {
                "mainNetRatio": main_ratio,
                "superNetRatio": super_ratio,
                "largeNetRatio": large_ratio,
            },
            "chips": {
                "profitRatio": pr,
                "avgCost": avg_cost,
            },
            "breakdown": breakdown,
        }

        scored.append(
            {
                "symbol": sym,
                "market": market,
                "ticker": ticker,
                "name": name,
                "sector": sector,
                # rawScore is the deterministic factor score; final score will be calibrated in the API layer.
                "rawScore": round(total, 2),
                "score": round(total, 2),
                "probBand": _rank_prob_band(total),
                "signals": signals[:6],
                "breakdown": breakdown,
                "buyPrice": float(buy_price) if buy_price > 0 else None,
                "buyPriceSrc": buy_src,
                "evidence": evidence,
                "isHolding": is_holding,
            }
        )

    scored.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    top = scored[: max(1, min(int(limit), 200))]
    return {
        "asOfDate": as_of_date,
        "asOfTs": now_iso(),
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
            "spotRows": len(spot_rows),
        },
    }


def _quant2d_confidence(n: int) -> str:
    if int(n) >= 200:
        return "High"
    if int(n) >= 60:
        return "Medium"
    return "Low"


def _quant2d_prob_band(prob_pct: float) -> str:
    p = float(prob_pct or 0.0)
    if p >= 70.0:
        return "High"
    if p >= 55.0:
        return "Medium"
    return "Low"


def _quant2d_find_bucket(calib: dict[str, Any], raw_score: float) -> dict[str, Any] | None:
    items = calib.get("items") if isinstance(calib, dict) else None
    if not isinstance(items, list) or not items:
        return None
    s = float(raw_score or 0.0)
    for it in items:
        if not isinstance(it, dict):
            continue
        lo = _finite_float(it.get("minRawScore"), -1e9)
        hi = _finite_float(it.get("maxRawScore"), 1e9)
        if lo <= s <= hi:
            return it
    # Fallback: nearest by min/max distance.
    best = None
    best_dist = 1e18
    for it in items:
        if not isinstance(it, dict):
            continue
        lo = _finite_float(it.get("minRawScore"), 0.0)
        hi = _finite_float(it.get("maxRawScore"), 0.0)
        dist = min(abs(s - lo), abs(s - hi))
        if dist < best_dist:
            best_dist = dist
            best = it
    return best if isinstance(best, dict) else None


def _quant2d_decision_score(
    *,
    prob_profit_pct: float,
    ev2d_pct: float,
    p10_ret2d_pct: float,
    dd2d_pct: float,
) -> float:
    """
    Decision score (0-100) that strongly prioritizes win probability,
    while penalizing downside tails and drawdown (prefers 'high prob small win, low prob small loss').
    """
    p = max(0.0, min(100.0, float(prob_profit_pct or 0.0)))
    ev = float(ev2d_pct or 0.0)
    p10 = float(p10_ret2d_pct or 0.0)
    dd = float(dd2d_pct or 0.0)  # <=0 for drawdown

    # Cap EV contribution to avoid rare big winners dominating.
    ev_adj = max(-2.0, min(4.0, ev)) * 2.0
    # Penalize tail loss (if p10 is negative).
    tail_pen = max(0.0, min(6.0, -p10)) * 4.0
    # Penalize drawdown magnitude.
    dd_pen = max(0.0, min(8.0, -dd)) * 1.5

    s = p + ev_adj - tail_pen - dd_pen
    return max(0.0, min(100.0, s))


def _quant2d_why_from_evidence(evidence: dict[str, Any]) -> list[str]:
    """
    Short, deterministic 'why' bullets derived from numeric evidence. LLM may override later.
    """
    breakdown0 = evidence.get("breakdown")
    bd = cast(dict[str, Any], breakdown0) if isinstance(breakdown0, dict) else {}
    parts: list[tuple[str, float]] = []
    for k in ("trend", "breakout", "flow", "volume", "chips", "sectorHot"):
        parts.append((k, _finite_float(bd.get(k), 0.0)))
    parts.sort(key=lambda x: x[1], reverse=True)
    top = [x for x in parts[:3] if x[1] > 0]
    out: list[str] = []
    for k, v in top:
        out.append(f"{k}: {v:.2f}")
    risk = _norm_str(evidence.get("riskMode") or "")
    if risk:
        out.append(f"riskMode: {risk}")
    return out[:4]


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

        # Soft filters aligned with your A/B/C/D idea; keep best-effort.
        # IMPORTANT: never return an empty candidate list if we have theme names, because the UI
        # needs a "best theme" even in rotation/multi-line markets or when data sources are flaky.
        limitup_ok = int(it.get("limitupCount") or 0) >= 3
        strength_ok = float(it.get("todayStrength") or 0.0) >= 2.0
        vol_ok = float(it.get("volSurge") or 0.0) >= 1.2
        if not (limitup_ok or (strength_ok and vol_ok)):
            continue

        scored.append({**it, "step1Score": round(s * 100.0, 2)})

    # Fallback: if nothing passed filters, keep top candidates by score (best-effort).
    if not scored and items:
        debug["fallback"] = "no_item_passed_filters"
        tmp: list[dict[str, Any]] = []
        for i, it in enumerate(items):
            s = 0.0
            s += 0.35 * float(lu_n[i] if i < len(lu_n) else 0.0)
            s += 0.30 * float(today_n[i] if i < len(today_n) else 0.0)
            s += 0.20 * float(vol_n[i] if i < len(vol_n) else 0.0)
            s += 0.15 * float(ret3_n[i] if i < len(ret3_n) else 0.0)
            tmp.append({**it, "step1Score": round(s * 100.0, 2)})
        tmp.sort(key=lambda x: float(x.get("step1Score") or 0.0), reverse=True)
        scored = tmp[:12]

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
    selected_clear = False
    selected_reason = ""
    if themes_topk:
        top1 = themes_topk[0]
        top2 = themes_topk[1] if len(themes_topk) > 1 else None
        s1 = float(top1.get("compositeScore") or 0.0)
        s2 = float(top2.get("compositeScore") or 0.0) if top2 else 0.0
        # "Clear mainline" threshold (used to constrain Leaders candidate universe).
        # If not clear, still return Top1 as a "best theme" so the UI always has a conclusion,
        # but mark it as not clear to avoid over-fitting / false restriction.
        if s1 >= 70.0 and ((s1 - s2) >= 5.0):
            selected_clear = True
            selected_reason = "clear_mainline(score>=70 && gap>=5)"
            selected = dict(top1)
        else:
            selected_clear = False
            selected_reason = "weak_mainline(rotation_or_multi_line)"
            selected = dict(top1)
            # Help UI/users interpret: this is a best-effort Top1, not a confirmed mainline.
            selected.setdefault("decaySignals", [])
            if isinstance(selected.get("decaySignals"), list):
                selected["decaySignals"] = list(selected.get("decaySignals") or []) + ["weak_mainline"]

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
        "debug": {
            "step1": dbg1,
            "step2": dbg2,
            "aiError": ai_error,
            "selectedClear": bool(selected_clear),
            "selectedReason": selected_reason,
        },
    }


def _ai_quant_rank_explain(*, payload: dict[str, Any]) -> dict[str, Any]:
    url = f"{_ai_service_base_url()}/quant/rank/explain"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _do() -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
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


def _get_by_dot_path(obj: dict[str, Any], path: str) -> Any:
    cur: Any = obj
    for part in (path or "").split("."):
        p = part.strip()
        if not p:
            return None
        if not isinstance(cur, dict):
            return None
        if p not in cur:
            return None
        cur = cur.get(p)
    return cur


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
            r"^(##\s*(?:(?:0|1|2|3|4|5)\s*(?:结果摘要|资金板块|候选Top3|持仓计划|执行要点|条件单总表)|(?:1|2|3|4|5|6)\s*(?:总览|机会Top3|机会|资金板块|持仓计划|执行要点|条件单总表|总结)))\s+([^\n#].*)$",
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
        "accountId": aid,
        "broker": str(acct["broker"]),
        "updatedAt": now_iso(),
        "overview": {},
        "positions": [],
    }
    # Strategy context: keep it lean (overview + positions only).
    if isinstance(state_row, dict):
        state_row = {
            "accountId": _norm_str(state_row.get("accountId") or aid),
            "broker": _norm_str(state_row.get("broker") or acct["broker"]),
            "updatedAt": _norm_str(state_row.get("updatedAt") or "") or now_iso(),
            "overview": state_row.get("overview") if isinstance(state_row.get("overview"), dict) else {},
            "positions": state_row.get("positions") if isinstance(state_row.get("positions"), list) else [],
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

    watchlist_ctx: dict[str, Any] = {}
    if req.includeWatchlist and isinstance(req.watchlist, dict):
        wl_items0 = req.watchlist.get("items")
        wl_items: list[dict[str, Any]] = []
        if isinstance(wl_items0, list):
            for it in wl_items0[:50]:
                if not isinstance(it, dict):
                    continue
                sym = _norm_str(it.get("symbol") or "")
                if not sym:
                    continue
                name = _norm_str(it.get("name") or "") or None
                # Enrich local watchlist items with real-time (cached) CN daily signals so the LLM
                # can reason with actionable fields (TrendOK/Score/StopLoss/Buy).
                enriched: dict[str, Any] = {"symbol": sym, "name": name}
                try:
                    bars = _load_cached_bars(sym, days=120)
                    bars_tuples: list[
                        tuple[str, str | None, str | None, str | None, str | None, str | None]
                    ] = [
                        (
                            _norm_str(b.get("date") or ""),
                            b.get("open") or None,
                            b.get("high") or None,
                            b.get("low") or None,
                            b.get("close") or None,
                            b.get("volume") or None,
                        )
                        for b in bars
                        if isinstance(b, dict)
                    ]
                    t = _market_stock_trendok_one(symbol=sym, name=name, bars=bars_tuples)
                    enriched.update(
                        {
                            "asOfDate": t.asOfDate,
                            "close": t.values.close,
                            "trendOk": t.trendOk,
                            "score": t.score,
                            "stopLossPrice": t.stopLossPrice,
                            "buyMode": t.buyMode,
                            "buyAction": t.buyAction,
                            "buyZoneLow": t.buyZoneLow,
                            "buyZoneHigh": t.buyZoneHigh,
                            "missingData": list(t.missingData or []),
                        }
                    )
                except Exception as e:
                    enriched["enrichError"] = str(e)
                wl_items.append(enriched)
        watchlist_ctx = {
            "version": int(req.watchlist.get("version") or 1),
            "generatedAt": _norm_str(req.watchlist.get("generatedAt") or "") or None,
            "count": len(wl_items),
            "items": wl_items,
        }

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
                    bars=bars if isinstance(bars, list) else None,
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

    # Mainline snapshot context: DB-first (no generation), latest snapshot for the day.
    mainline_ctx: dict[str, Any] = {}
    if req.includeMainline:
        try:
            cached = _get_cn_mainline_snapshot_latest(account_id=aid, trade_date=d, universe_version="v0")
            if isinstance(cached, dict) and isinstance(cached.get("output"), dict):
                out0 = cast(dict[str, Any], cached.get("output"))
                mainline_ctx = {"id": str(cached.get("id") or ""), "createdAt": str(cached.get("createdAt") or "")}
                mainline_ctx.update(out0)
            else:
                mainline_ctx = {}
        except Exception as e:
            mainline_ctx = {"error": str(e)}

    # Quant (next2d) snapshot: optional context injection for Strategy.
    quant2d_ctx: dict[str, Any] = {}
    if req.includeQuant2d:
        try:
            cached_q = _get_cn_rank_snapshot(account_id=aid, as_of_date=d, universe_version="v0")
            if not isinstance(cached_q, dict) or not isinstance(cached_q.get("output"), dict):
                quant2d_ctx = {"asOfDate": d, "status": "no_snapshot"}
            else:
                outq = cast(dict[str, Any], cached_q.get("output"))
                items0 = outq.get("items")
                items: list[Any] = items0 if isinstance(items0, list) else []
                top3: list[dict[str, Any]] = []
                for it in items[:3]:
                    if not isinstance(it, dict):
                        continue
                    top3.append(
                        {
                            "symbol": _norm_str(it.get("symbol") or ""),
                            "ticker": _norm_str(it.get("ticker") or ""),
                            "name": _norm_str(it.get("name") or ""),
                            "score": _finite_float(it.get("score"), 0.0),
                            "rawScore": _finite_float(it.get("rawScore"), 0.0),
                            "probProfit2d": it.get("probProfit2d"),
                            "ev2dPct": it.get("ev2dPct"),
                            "dd2dPct": it.get("dd2dPct"),
                            "confidence": _norm_str(it.get("confidence") or "") or None,
                            "buyPrice": it.get("buyPrice"),
                            "buyPriceSrc": _norm_str(it.get("buyPriceSrc") or "") or None,
                            "whyBullets": it.get("whyBullets") if isinstance(it.get("whyBullets"), list) else [],
                        }
                    )
                dbg0 = outq.get("debug")
                dbg: dict[str, Any] = dbg0 if isinstance(dbg0, dict) else {}
                quant2d_ctx = {
                    "id": str(cached_q.get("id") or ""),
                    "createdAt": str(cached_q.get("createdAt") or ""),
                    "asOfTs": str(outq.get("asOfTs") or "") or None,
                    "asOfDate": str(outq.get("asOfDate") or d),
                    "riskMode": str(outq.get("riskMode") or "") or None,
                    "objective": str(outq.get("objective") or "") or None,
                    "horizon": str(outq.get("horizon") or "") or None,
                    "top3": top3,
                    "debug": {
                        "calibrationN": int((dbg.get("calibrationN") or 0) if isinstance(dbg, dict) else 0),
                        "calibrationReady": bool(dbg.get("calibrationReady")) if isinstance(dbg, dict) else False,
                    },
                }
        except Exception as e:
            quant2d_ctx = {"asOfDate": d, "error": str(e)}

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
        "watchlist": {} if not req.includeWatchlist else watchlist_ctx,
        "tradingView": {} if not req.includeTradingView else {"latest": tv_latest},
        "industryFundFlow": {}
        if not req.includeIndustryFundFlow
        else {"dailyTopInflow": industry_flow_daily, "error": industry_flow_error},
        "marketSentiment": {} if not req.includeMarketSentiment else sentiment_ctx,
        "leaderStocks": {} if not req.includeLeaders else leader_ctx,
        "mainline": {} if not req.includeMainline else mainline_ctx,
        "quant2d": {} if not req.includeQuant2d else quant2d_ctx,
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
        "watchlist": {} if not req.includeWatchlist else watchlist_ctx,
        "tradingView": {} if not req.includeTradingView else {"latest": tv_latest},
        "industryFundFlow": {}
        if not req.includeIndustryFundFlow
        else {"dailyTopInflow": industry_flow_daily, "error": industry_flow_error},
        "marketSentiment": {} if not req.includeMarketSentiment else sentiment_ctx,
        "leaderStocks": {} if not req.includeLeaders else leader_ctx,
        "mainline": {} if not req.includeMainline else mainline_ctx,
        "quant2d": {} if not req.includeQuant2d else quant2d_ctx,
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


# --- Trade journal module (v0) ---
def _trade_journal_from_row(r: tuple[Any, ...]) -> TradeJournal:
    return TradeJournal(
        id=str(r[0]),
        title=str(r[1]),
        contentMd=str(r[2]),
        createdAt=str(r[3]),
        updatedAt=str(r[4]),
    )


@app.get("/journals", response_model=ListTradeJournalsResponse)
def list_trade_journals(limit: int = 20, offset: int = 0) -> ListTradeJournalsResponse:
    limit2 = max(1, min(int(limit), 200))
    offset2 = max(0, int(offset))
    with _connect() as conn:
        total = int(conn.execute("SELECT COUNT(*) FROM trade_journals").fetchone()[0])
        rows = conn.execute(
            """
            SELECT id, title, content_md, created_at, updated_at
            FROM trade_journals
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit2, offset2),
        ).fetchall()
    items = [_trade_journal_from_row(tuple(r)) for r in rows]
    return ListTradeJournalsResponse(total=total, items=items)


@app.get("/journals/{journal_id}", response_model=TradeJournal)
def get_trade_journal(journal_id: str) -> TradeJournal:
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    with _connect() as conn:
        r = conn.execute(
            """
            SELECT id, title, content_md, created_at, updated_at
            FROM trade_journals
            WHERE id = ?
            """,
            (jid,),
        ).fetchone()
    if r is None:
        raise HTTPException(status_code=404, detail="Journal not found")
    return _trade_journal_from_row(tuple(r))


@app.post("/journals", response_model=TradeJournal)
def create_trade_journal(req: TradeJournalCreateRequest) -> TradeJournal:
    now = now_iso()
    jid = str(uuid.uuid4())
    title = (req.title or "").strip() or "Trading Journal"
    content = req.contentMd or ""
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO trade_journals(id, title, content_md, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (jid, title, content, now, now),
        )
        conn.commit()
        r = conn.execute(
            """
            SELECT id, title, content_md, created_at, updated_at
            FROM trade_journals
            WHERE id = ?
            """,
            (jid,),
        ).fetchone()
    return _trade_journal_from_row(tuple(r))


@app.put("/journals/{journal_id}", response_model=TradeJournal)
def update_trade_journal(journal_id: str, req: TradeJournalUpdateRequest) -> TradeJournal:
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT id, title, content_md, created_at, updated_at
            FROM trade_journals
            WHERE id = ?
            """,
            (jid,),
        ).fetchone()
        if cur is None:
            raise HTTPException(status_code=404, detail="Journal not found")
        cur_t = str(cur[1])
        cur_c = str(cur[2])
        next_title = (req.title.strip() if isinstance(req.title, str) else cur_t) or cur_t
        next_content = req.contentMd if isinstance(req.contentMd, str) else cur_c
        now = now_iso()
        conn.execute(
            """
            UPDATE trade_journals
            SET title = ?, content_md = ?, updated_at = ?
            WHERE id = ?
            """,
            (next_title, next_content, now, jid),
        )
        conn.commit()
        r = conn.execute(
            """
            SELECT id, title, content_md, created_at, updated_at
            FROM trade_journals
            WHERE id = ?
            """,
            (jid,),
        ).fetchone()
    return _trade_journal_from_row(tuple(r))


@app.delete("/journals/{journal_id}")
def delete_trade_journal(journal_id: str) -> dict[str, Any]:
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    with _connect() as conn:
        cur = conn.execute("SELECT id FROM trade_journals WHERE id = ?", (jid,)).fetchone()
        if cur is None:
            raise HTTPException(status_code=404, detail="Journal not found")
        conn.execute("DELETE FROM trade_journals WHERE id = ?", (jid,))
        conn.commit()
    return {"ok": True}


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
                sym = str(r["symbol"])
                series = _bars_series_since_cached(sym, str(r["date"]), limit=60)
                trend_series = _bars_series_last_cached(sym, limit=20)
                now_close = (trend_series[-1].get("close") if trend_series else None) or (series[-1].get("close") if series else None)
                today_chg_pct: float | None = None
                src2 = trend_series[-2:] if len(trend_series) >= 2 else series[-2:] if len(series) >= 2 else []
                if len(src2) >= 2:
                    c_prev = _safe_float(src2[0].get("close"))
                    c_last = _safe_float(src2[1].get("close"))
                    if c_prev and c_last and c_prev > 0:
                        today_chg_pct = (c_last / c_prev - 1.0) * 100.0
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
                    todayChangePct=float(today_chg_pct) if today_chg_pct is not None else None,
                    trendSeries=trend_series,
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
    mainline_selected_clear = False
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
                dbg_ml = mainline_out.get("debug") if isinstance(mainline_out.get("debug"), dict) else {}
                mainline_selected_clear = bool(dbg_ml.get("selectedClear")) if isinstance(dbg_ml, dict) else False
        except Exception:
            mainline_out = None
            mainline_selected = None
            mainline_selected_clear = False

    pool: list[dict[str, str]] = []
    seen: set[str] = set()
    # Only restrict candidate universe when the mainline is marked as "clear".
    if mainline_selected and mainline_selected_clear and aid_mainline:
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

    # Merge Quant (next2d) top candidates into leader candidate universe (best-effort).
    # This reduces misses when a strong stock is not surfaced by enabled TV screeners.
    quant_score_map: dict[str, float] = {}
    quant_ev_map: dict[str, float] = {}
    quant_prob_map: dict[str, float] = {}
    try:
        aid_quant = aid_mainline
        if not aid_quant:
            accs2 = list_broker_accounts(broker="pingan")
            aid_quant = accs2[0].id if accs2 else ""
        if aid_quant:
            q_snap = _get_cn_rank_snapshot(account_id=aid_quant, as_of_date=d, universe_version="v0")
            out0 = q_snap.get("output") if isinstance(q_snap, dict) else None
            outq: dict[str, Any] = out0 if isinstance(out0, dict) else {}
            items0 = outq.get("items")
            q_items: list[Any] = items0 if isinstance(items0, list) else []
            # First, build maps for all items (even if already in pool).
            for it in q_items[:120]:
                if not isinstance(it, dict):
                    continue
                sym0 = _norm_str(it.get("symbol") or "")
                ticker0 = _norm_str(it.get("ticker") or "")
                if not sym0 and ticker0:
                    sym0 = f"CN:{ticker0}"
                if not sym0:
                    continue
                quant_score_map[sym0] = _finite_float(it.get("score"), 0.0)
                quant_ev_map[sym0] = _finite_float(it.get("ev2dPct"), 0.0)
                quant_prob_map[sym0] = _finite_float(it.get("probProfit2d"), 0.0)

            for it in q_items[:40]:
                if not isinstance(it, dict):
                    continue
                sym = _norm_str(it.get("symbol") or "")
                ticker = _norm_str(it.get("ticker") or "")
                if not sym and ticker:
                    sym = f"CN:{ticker}"
                if not sym or sym in seen:
                    continue
                if not ticker and ":" in sym:
                    ticker = sym.split(":")[1]
                pool.append(
                    {
                        "symbol": sym,
                        "market": _norm_str(it.get("market") or "CN") or "CN",
                        "currency": "CNY",
                        "ticker": ticker or sym.split(":")[-1],
                        "name": _norm_str(it.get("name") or "") or (ticker or sym.split(":")[-1]),
                    }
                )
                seen.add(sym)
                if len(pool) >= 160:
                    break
    except Exception:
        pass

    # Deterministic candidate ordering: avoid "randomness" from TV/member ordering.
    # Rank by a simple strength score (Quant2D score if present + spot strength proxies).
    spot_rows2: list[StockRow] = []
    try:
        spot_rows2 = fetch_cn_a_spot()
    except Exception:
        spot_rows2 = []
    spot_map2: dict[str, StockRow] = {s.ticker: s for s in spot_rows2 if s.market == "CN" and s.ticker}

    def _cand_strength(c: dict[str, Any]) -> float:
        sym = _norm_str(c.get("symbol") or "")
        ticker = _norm_str(c.get("ticker") or "") or (sym.split(":")[1] if ":" in sym else "")
        q = _finite_float(quant_score_map.get(sym), 0.0)
        q_ev = _finite_float(quant_ev_map.get(sym), 0.0)
        chg = 0.0
        volr = 0.0
        turn = 0.0
        srow = spot_map2.get(ticker)
        if srow is not None:
            chg = _parse_pct(srow.quote.get("change_pct") or "")
            volr = _parse_num(srow.quote.get("vol_ratio") or "")
            turn = _parse_num(srow.quote.get("turnover") or "")
        # Compose: Leader objective = upside (larger expected move), accept lower win-rate.
        # Use Quant EV (if present) as a weak upside hint, but prioritize spot momentum/attention.
        base = 0.0
        if q > 0:
            base += 0.35 * q
        if q_ev > 0:
            base += 4.0 * max(0.0, min(8.0, float(q_ev)))  # 0..32
        base += 3.0 * max(-5.0, min(10.0, float(chg)))  # -15..30
        base += 6.0 * max(0.0, min(5.0, float(volr)))  # 0..30
        # Turnover in CNY: use log scaling (0..~20)
        try:
            base += 10.0 * math.log10(1.0 + max(0.0, float(turn)) / 1e8)
        except Exception:
            base += 0.0
        return float(base)

    pool.sort(key=_cand_strength, reverse=True)

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
    top_n = max(1, min(int(req.maxCandidates), 20))
    for c in pool[:top_n]:
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
                "candidateStrength": round(_cand_strength(c), 2),
                "quant2dScore": round(_finite_float(quant_score_map.get(sym), 0.0), 2) if sym else 0.0,
                "quant2dEv2dPct": round(_finite_float(quant_ev_map.get(sym), 0.0), 3) if sym else 0.0,
                "quant2dProbProfit2d": round(_finite_float(quant_prob_map.get(sym), 0.0), 2) if sym else 0.0,
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
        "candidateUniverseTop": [
            {
                "symbol": x.get("symbol"),
                "ticker": x.get("ticker"),
                "name": x.get("name"),
                "candidateStrength": round(_cand_strength(x), 2),
                "quant2dScore": round(_finite_float(quant_score_map.get(_norm_str(x.get("symbol") or "")), 0.0), 2),
                "quant2dEv2dPct": round(_finite_float(quant_ev_map.get(_norm_str(x.get("symbol") or "")), 0.0), 3),
                "quant2dProbProfit2d": round(_finite_float(quant_prob_map.get(_norm_str(x.get("symbol") or "")), 0.0), 2),
            }
            for x in pool[: min(40, len(pool))]
            if isinstance(x, dict)
        ],
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
        sym = str(r["symbol"])
        series = _bars_series_since_cached(sym, str(r["date"]), limit=60)
        trend_series = _bars_series_last_cached(sym, limit=20)
        now_close = (trend_series[-1].get("close") if trend_series else None) or (series[-1].get("close") if series else None)
        today_chg_pct: float | None = None
        src2 = trend_series[-2:] if len(trend_series) >= 2 else series[-2:] if len(series) >= 2 else []
        if len(src2) >= 2:
            c_prev = _safe_float(src2[0].get("close"))
            c_last = _safe_float(src2[1].get("close"))
            if c_prev and c_last and c_prev > 0:
                today_chg_pct = (c_last / c_prev - 1.0) * 100.0
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
            todayChangePct=float(today_chg_pct) if today_chg_pct is not None else None,
            trendSeries=trend_series,
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
        sym = str(r["symbol"])
        series = _bars_series_since_cached(sym, str(r["date"]), limit=60)
        trend_series = _bars_series_last_cached(sym, limit=20)
        now_close = (trend_series[-1].get("close") if trend_series else None) or (series[-1].get("close") if series else None)
        today_chg_pct: float | None = None
        src2 = trend_series[-2:] if len(trend_series) >= 2 else series[-2:] if len(series) >= 2 else []
        if len(src2) >= 2:
            c_prev = _safe_float(src2[0].get("close"))
            c_last = _safe_float(src2[1].get("close"))
            if c_prev and c_last and c_prev > 0:
                today_chg_pct = (c_last / c_prev - 1.0) * 100.0
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
                todayChangePct=float(today_chg_pct) if today_chg_pct is not None else None,
                trendSeries=trend_series,
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
        # Treat non-200 or ok=false as a step failure (so the UI shows it clearly).
        status = int(getattr(resp, "status_code", 200) or 200)
        ok_flag = bool(j.get("ok")) if isinstance(j, dict) else False
        if status >= 400 or (isinstance(j, dict) and j.get("ok") is False) or (status == 200 and not ok_flag and j):
            err = (j.get("error") if isinstance(j, dict) else None) or (j.get("detail") if isinstance(j, dict) else None) or body
            raise RuntimeError(str(err or "Market sync failed."))
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
        # If the latest row is not for today, treat the step as failed (stale data).
        last_date = str(last.get("date") or "")
        if last_date and last_date != d:
            raise RuntimeError(f"Sentiment sync stale (requested={d}, latest={last_date}). Upstream blocked/captcha.")
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
        asOfTs=now_iso(),
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
            "SELECT id FROM broker_snapshots WHERE broker = ? AND account_id IS NOT DISTINCT FROM ? AND sha256 = ?",
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
            WHERE broker = ? AND account_id IS NOT DISTINCT FROM ?
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
        exists = conn.execute("SELECT 1 FROM system_prompts WHERE id = ? LIMIT 1", (preset_id,)).fetchone()
        if exists is None:
            return False
        conn.execute("DELETE FROM system_prompts WHERE id = ?", (preset_id,))
        conn.commit()
        return True


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
