"""TradingView dedicated Chrome management (start/stop/status).

Ported from quant-service, but persisted in Postgres via `tv_chrome_settings`.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fastapi import HTTPException

from data_sync_service.db import tv_chrome_settings as kv

TV_CDP_HOST = "127.0.0.1"
TV_CDP_PORT_DEFAULT = 9222
TV_USER_DATA_DIR_DEFAULT = "~/.karios/chrome-tv-cdp"
TV_PROFILE_DIR_DEFAULT = "Default"
TV_CHROME_BIN_DEFAULT = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


@dataclass(frozen=True)
class TvChromeStatus:
    running: bool
    pid: int | None
    host: str
    port: int
    cdpOk: bool
    cdpVersion: dict[str, str] | None
    userDataDir: str
    profileDirectory: str
    headless: bool


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def get_setting(key: str) -> str | None:
    return kv.get_value(key)


def set_setting(key: str, value: str) -> None:
    kv.set_value(key=key, value=value, updated_at=_now_iso())


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
        with urllib.request.urlopen(url, timeout=0.8) as resp:  # noqa: S310
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


def _get_tv_chrome_bin() -> str:
    return (get_setting("tv_chrome_bin") or TV_CHROME_BIN_DEFAULT).strip()


def _set_tv_chrome_bin(chrome_bin: str) -> None:
    set_setting("tv_chrome_bin", chrome_bin)


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


def status() -> TvChromeStatus:
    pid = _get_tv_chrome_pid()
    running = bool(pid and _pid_is_running(pid))
    port = _get_tv_cdp_port()
    user_data_dir = _get_tv_user_data_dir()
    profile_dir = _get_tv_profile_dir()
    headless = _get_tv_headless()
    cdp = _cdp_version(TV_CDP_HOST, port) if running else None
    cdp_ok = cdp is not None
    return TvChromeStatus(
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


def start(
    *,
    port: int = TV_CDP_PORT_DEFAULT,
    userDataDir: str = TV_USER_DATA_DIR_DEFAULT,
    profileDirectory: str = TV_PROFILE_DIR_DEFAULT,
    chromeBin: str = TV_CHROME_BIN_DEFAULT,
    headless: bool = False,
    bootstrapFromChromeUserDataDir: str | None = None,
    bootstrapFromProfileDirectory: str | None = None,
    forceBootstrap: bool = False,
) -> TvChromeStatus:
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
    port2 = int(port)
    user_data_dir = _home_path(userDataDir)
    profile_dir = (profileDirectory or "").strip() or TV_PROFILE_DIR_DEFAULT
    chrome_bin = (chromeBin or "").strip() or TV_CHROME_BIN_DEFAULT
    headless2 = bool(headless)
    _set_tv_cdp_port(port2)
    _set_tv_user_data_dir(user_data_dir)
    _set_tv_profile_dir(profile_dir)
    _set_tv_chrome_bin(chrome_bin)
    _set_tv_headless(headless2)

    # If already running but config differs, restart so the new config takes effect.
    if pid and _pid_is_running(pid):
        changed = (
            current_port != port2
            or current_user_data_dir != user_data_dir
            or current_profile_dir != profile_dir
            or current_chrome_bin != chrome_bin
            or current_headless != headless2
        )
        if changed or forceBootstrap:
            stop()
        else:
            return status()

    # Fail fast if port is already taken.
    if _tcp_is_listening(TV_CDP_HOST, port2):
        raise HTTPException(status_code=409, detail=f"Port {port2} is already in use.")

    if not Path(chrome_bin).exists():
        raise HTTPException(status_code=400, detail=f"Chrome binary not found: {chrome_bin}")

    Path(user_data_dir).mkdir(parents=True, exist_ok=True)

    # Optional: bootstrap from an existing Chrome profile into the dedicated user-data-dir.
    if bootstrapFromChromeUserDataDir and bootstrapFromProfileDirectory:
        set_setting("tv_bootstrap_src_user_data_dir", bootstrapFromChromeUserDataDir)
        set_setting("tv_bootstrap_src_profile_dir", bootstrapFromProfileDirectory)
        _copy_chrome_profile(
            src_user_data_dir=bootstrapFromChromeUserDataDir,
            src_profile_dir=bootstrapFromProfileDirectory,
            dst_user_data_dir=user_data_dir,
            dst_profile_dir=profile_dir,
            force=bool(forceBootstrap),
        )

    args = [
        chrome_bin,
        f"--remote-debugging-port={port2}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={profile_dir}",
        *(["--headless=new", "--disable-gpu", "--window-size=1280,820"] if headless2 else []),
        "--no-first-run",
        "--no-default-browser-check",
    ]

    try:
        proc = subprocess.Popen(  # noqa: S603
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
        if _cdp_version(TV_CDP_HOST, port2) is not None:
            break
        time.sleep(0.2)

    return status()


def stop() -> TvChromeStatus:
    pid = _get_tv_chrome_pid()
    port = _get_tv_cdp_port()
    if not pid:
        return status()

    if not _pid_is_running(pid):
        _set_tv_chrome_pid(None)
        return status()

    # Terminate the process group (Chrome spawns children).
    try:
        os.killpg(pid, signal.SIGTERM)
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            _set_tv_chrome_pid(None)
            return status()

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
    return status()

