from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
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
    fetch_cn_industry_fund_flow_hist,
    fetch_cn_industry_fund_flow_eod,
    fetch_cn_a_spot,
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
    return BrokerAccountStateResponse(
        accountId=str(row["accountId"]),
        broker=str(row["broker"]),
        updatedAt=str(row["updatedAt"]),
        overview=row.get("overview") if isinstance(row.get("overview"), dict) else {},
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
def market_stock_bars(symbol: str, days: int = 60) -> MarketBarsResponse:
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

    if len(cached) < days2:
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
def market_stock_chips(symbol: str, days: int = 60) -> MarketChipsResponse:
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
    if len(cached) >= min(days2, 30):
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
def market_stock_fund_flow(symbol: str, days: int = 60) -> MarketFundFlowResponse:
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
    if len(cached) >= min(days2, 30):
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
        asOfDate=as_of.strftime("%Y-%m-%d"),
        days=days,
        rowsUpserted=len(items),
        histRowsUpserted=hist_rows_upserted,
        histFailures=hist_failures,
        message=None if hist_failures == 0 else f"Hist backfill partial: {hist_failures} industries failed.",
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
        levels_obj = r.get("levels") if isinstance(r.get("levels"), dict) else {}
        levels = StrategyLevels(
            support=[_norm_str(x) for x in (levels_obj.get("support") or []) if _norm_str(x)],
            resistance=[_norm_str(x) for x in (levels_obj.get("resistance") or []) if _norm_str(x)],
            invalidations=[_norm_str(x) for x in (levels_obj.get("invalidations") or []) if _norm_str(x)],
        )
        orders_in: list[Any] = r.get("orders") if isinstance(r.get("orders"), list) else []
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
        # If a model puts '... ## Heading' on the same line, split it.
        parts[i] = re.sub(r"([^\n])\s+(#{2,6}\s)", r"\1\n\n\2", parts[i])
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

    # Latest TradingView snapshots (default 2 screeners).
    _seed_default_tv_screeners()
    snaps: list[TvScreenerSnapshotDetail] = []
    if req.includeTradingView:
        for sid in ("falcon", "blackhorse"):
            s = _latest_tv_snapshot_for_screener(sid)
            if s is not None:
                snaps.append(s)

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

    # Ensure market universe has these symbols so we can fetch bars/chips/fund-flow.
    for c in pool:
        _ensure_market_stock_basic(
            symbol=c["symbol"],
            market=c["market"],
            ticker=c["ticker"],
            name=c.get("name") or c["ticker"],
            currency=c["currency"],
        )

    # Fetch deep data for candidates (bounded). IMPORTANT: keep context compact.
    stock_context: list[dict[str, Any]] = []
    if req.includeStocks:
        # Provide deep context only for a small subset: holdings + first TV candidates.
        deep_syms: list[str] = []
        if req.includeAccountState:
            raw_positions2 = state_row.get("positions")
            pos_list2: list[Any] = raw_positions2 if isinstance(raw_positions2, list) else []
            for p in pos_list2:
                if not isinstance(p, dict):
                    continue
                ticker2 = _norm_str(p.get("ticker") or p.get("Ticker") or p.get("symbol") or p.get("Symbol") or "")
                if not ticker2:
                    continue
                market2 = "HK" if (len(ticker2) in (4, 5)) else "CN"
                sym2 = f"{market2}:{ticker2}"
                if sym2 not in deep_syms:
                    deep_syms.append(sym2)
                if len(deep_syms) >= 5:
                    break
        for c in pool[:5]:
            sym2 = c["symbol"]
            if sym2 not in deep_syms:
                deep_syms.append(sym2)
            if len(deep_syms) >= 8:
                break
        deep_set = set(deep_syms)

        for c in pool:
            sym = c["symbol"]
            include_deep = sym in deep_set
            bars = _load_cached_bars(sym, days=60) if include_deep else []
            bars_error: str | None = None
            if include_deep and not bars:
                try:
                    bars_resp = market_stock_bars(sym, days=60)
                    bars = bars_resp.bars
                except Exception as e:
                    bars = []
                    bars_error = str(e)
            feats = _bars_features(bars) if include_deep else {}

            chips = _load_cached_chips(sym, days=30) if include_deep else []
            fund_flow = _load_cached_fund_flow(sym, days=30) if include_deep else []
            if include_deep and not chips:
                try:
                    chips = market_stock_chips(sym, days=30).items
                except Exception:
                    chips = []
            if include_deep and not fund_flow:
                try:
                    fund_flow = market_stock_fund_flow(sym, days=30).items
                except Exception:
                    fund_flow = []

            # Compact tails only (avoid sending full arrays).
            bars_tail = bars[-6:] if bars else []
            chips_tail = chips[-3:] if chips else []
            ff_tail = fund_flow[-5:] if fund_flow else []

            stock_context.append(
                {
                    "symbol": sym,
                    "market": c["market"],
                    "ticker": c["ticker"],
                    "name": c.get("name") or "",
                    "currency": c["currency"],
                    "deep": include_deep,
                    "availability": {
                        "barsCached": True if bars else False,
                        "chipsCached": True if chips else False,
                        "fundFlowCached": True if fund_flow else False,
                        "barsError": bars_error,
                    },
                    "features": feats,
                    "barsTail": bars_tail,
                    "chipsTail": chips_tail,
                    "fundFlowTail": ff_tail,
                },
            )

    # TradingView context: last 5 days history (AM/PM) for each screener.
    tv_history: list[dict[str, Any]] = []
    if req.includeTradingView:
        for sid in ("falcon", "blackhorse"):
            srow = _get_tv_screener_row(sid)
            if srow is None:
                continue
            hist = tv_screener_history(sid, days=5)
            # Expand cells into compact snapshot briefs; keep time markers.
            expanded: list[dict[str, Any]] = []
            for day in hist.rows:
                for slot in ("am", "pm"):
                    cell = getattr(day, slot)
                    if cell is None:
                        continue
                    brief = _tv_snapshot_brief(cell.snapshotId, max_rows=20)
                    expanded.append(
                        {
                            "date": day.date,
                            "slot": slot,
                            "capturedAt": cell.capturedAt,
                            "rowCount": cell.rowCount,
                            "snapshot": brief,
                        },
                    )
            tv_history.append(
                {
                    "screenerId": sid,
                    "screenerName": str(srow.get("name") or sid),
                    "days": 5,
                    "rows": expanded,
                },
            )

    # CN industry fund flow context (Top10 + 10d series), DB-first with best-effort sync.
    industry_flow_top: list[dict[str, Any]] = []
    industry_flow_meta: dict[str, Any] = {"asOfDate": d, "days": 10, "topN": 10, "metric": "netInflowEod"}
    industry_flow_error: str | None = None
    if req.includeIndustryFundFlow:
        try:
            flow = market_cn_industry_fund_flow(days=10, topN=10, asOfDate=d)
            if not flow.top:
                try:
                    market_cn_industry_fund_flow_sync(
                        MarketCnIndustryFundFlowSyncRequest(date=d, days=10, topN=10, force=False)
                    )
                except Exception:
                    pass
                flow = market_cn_industry_fund_flow(days=10, topN=10, asOfDate=d)
            industry_flow_meta = {
                "asOfDate": flow.asOfDate,
                "days": flow.days,
                "topN": flow.topN,
                "metric": "netInflowEod",
            }
            for r in flow.top:
                industry_flow_top.append(
                    {
                        "industryCode": r.industryCode,
                        "industryName": r.industryName,
                        "netInflow": r.netInflow,
                        "sum10d": r.sum10d,
                        "series10d": [{"date": p.date, "netInflow": p.netInflow} for p in r.series10d],
                    }
                )
        except Exception as e:
            industry_flow_error = str(e)

    input_snapshot: dict[str, Any] = {
        "date": d,
        "account": {
            "accountId": aid,
            "broker": acct["broker"],
            "accountTitle": acct["title"],
            "accountMasked": acct.get("accountMasked") or "",
        },
        "accountPrompt": strategy_prompt,
        "accountState": {}
        if not req.includeAccountState
        else {
            "overview": state_row.get("overview") if isinstance(state_row.get("overview"), dict) else {},
            "positions": state_row.get("positions") if isinstance(state_row.get("positions"), list) else [],
            "conditionalOrders": state_row.get("conditionalOrders")
            if isinstance(state_row.get("conditionalOrders"), list)
            else [],
            "trades": state_row.get("trades") if isinstance(state_row.get("trades"), list) else [],
        },
        "tradingView": {}
        if not req.includeTradingView
        else {
            "latest": [
                {
                    "snapshotId": s.id,
                    "screenerId": s.screenerId,
                    "capturedAt": s.capturedAt,
                    "screenTitle": s.screenTitle,
                    "filters": s.filters,
                    "url": s.url,
                    "rowCount": s.rowCount,
                }
                for s in snaps
            ],
            "history": tv_history,
        },
        "industryFundFlow": {}
        if not req.includeIndustryFundFlow
        else {
            "meta": industry_flow_meta,
            "top": industry_flow_top,
            "error": industry_flow_error,
        },
        "stocks": [] if not req.includeStocks else stock_context,
    }

    # Call ai-service
    ai_payload = {
        "date": d,
        "accountId": aid,
        "accountTitle": acct["title"],
        "accountPrompt": strategy_prompt,
        "context": input_snapshot,
    }
    try:
        out = _ai_strategy_daily_markdown(payload=ai_payload)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"ai-service request failed: {e}") from e

    output = out if isinstance(out, dict) else {"error": "Invalid strategy output", "raw": out}
    # Normalize markdown for UI rendering (avoid headings on same line).
    if isinstance(output.get("markdown"), str):
        raw_md = output.get("markdown") or ""
        output.setdefault("markdownRaw", raw_md)
        output["markdown"] = _normalize_strategy_markdown(raw_md)
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
