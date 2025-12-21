from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    fetch_cn_a_spot,
    fetch_hk_daily_bars,
    fetch_hk_spot,
)
from tv.capture import capture_screener_over_cdp_sync


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
        if market == "CN":
            bars = fetch_cn_a_daily_bars(ticker, days=days2)
        elif market == "HK":
            bars = fetch_hk_daily_bars(ticker, days=days2)
        else:
            raise HTTPException(status_code=400, detail="Unsupported market")
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
    items2 = fetch_cn_a_chip_summary(ticker, days=days2)
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
    items2 = fetch_cn_a_fund_flow(ticker, days=days2)
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
    positions: list[dict[str, Any]] | None = None
    orders: list[dict[str, Any]] | None = None
    trades: list[dict[str, Any]] | None = None

    for img in req.images:
        # We intentionally do NOT write images to disk in the state-first design.
        extracted = _ai_extract_pingan_screenshot(image_data_url=img.dataUrl)
        if not isinstance(extracted, dict):
            continue
        kind = str(extracted.get("kind") or "unknown")
        data = extracted.get("data")
        data2 = data if isinstance(data, dict) else {}

        if kind == "account_overview":
            overview = data2
        elif kind == "positions":
            ps = data2.get("positions")
            if isinstance(ps, list):
                positions = [p if isinstance(p, dict) else {"raw": p} for p in ps]
        elif kind == "conditional_orders":
            os_ = data2.get("orders")
            if isinstance(os_, list):
                orders = [o if isinstance(o, dict) else {"raw": o} for o in os_]
        elif kind == "trades":
            ts = data2.get("trades")
            if isinstance(ts, list):
                trades = [t if isinstance(t, dict) else {"raw": t} for t in ts]

    _upsert_account_state(
        account_id=aid,
        broker="pingan",
        updated_at=captured_at,
        overview=overview,
        positions=positions,
        conditional_orders=orders,
        trades=trades,
    )
    return _account_state_response(aid)


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
