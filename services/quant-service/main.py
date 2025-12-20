from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


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


class TvChromeStartRequest(BaseModel):
    port: int = TV_CDP_PORT_DEFAULT
    userDataDir: str = TV_USER_DATA_DIR_DEFAULT
    profileDirectory: str = TV_PROFILE_DIR_DEFAULT
    chromeBin: str = TV_CHROME_BIN_DEFAULT


class TvChromeStatusResponse(BaseModel):
    running: bool
    pid: int | None
    host: str
    port: int
    cdpOk: bool
    cdpVersion: dict[str, str] | None
    userDataDir: str
    profileDirectory: str


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
    )


@app.post("/integrations/tradingview/chrome/start", response_model=TvChromeStatusResponse)
def tradingview_chrome_start(req: TvChromeStartRequest) -> TvChromeStatusResponse:
    # If a previous PID is stored but dead, clear it.
    pid = _get_tv_chrome_pid()
    if pid and not _pid_is_running(pid):
        _set_tv_chrome_pid(None)
        pid = None

    # Persist desired config.
    port = int(req.port)
    user_data_dir = _home_path(req.userDataDir)
    profile_dir = req.profileDirectory.strip() or TV_PROFILE_DIR_DEFAULT
    chrome_bin = req.chromeBin.strip() or TV_CHROME_BIN_DEFAULT
    _set_tv_cdp_port(port)
    _set_tv_user_data_dir(user_data_dir)
    _set_tv_profile_dir(profile_dir)
    _set_tv_chrome_bin(chrome_bin)

    if pid and _pid_is_running(pid):
        return tradingview_status()

    # Fail fast if port is already taken.
    if _tcp_is_listening(TV_CDP_HOST, port):
        raise HTTPException(status_code=409, detail=f"Port {port} is already in use.")

    if not Path(chrome_bin).exists():
        raise HTTPException(status_code=400, detail=f"Chrome binary not found: {chrome_bin}")

    Path(user_data_dir).mkdir(parents=True, exist_ok=True)

    # Chrome requires a non-default user-data-dir for remote debugging.
    # We always use a dedicated directory to avoid interfering with the user's daily profile.
    args = [
        chrome_bin,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={profile_dir}",
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
