"""Shared HTTP helper for East Money push2 APIs."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# Scripts (backfill / healthcheck) run outside FastAPI — load the repo root
# .env so EASTMONEY_PROXY / EASTMONEY_COOKIE are available even when
# get_settings() was never called. Mirrors industry_fund_flow.py.
from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[5] / ".env")

_PROXY = os.environ.get("EASTMONEY_PROXY", "").strip()
_COOKIE = os.environ.get("EASTMONEY_COOKIE", "").strip()

# Once the proxy exit is confirmed down (302/502/connect errors), skip it
# for subsequent requests in this process — retrying a dead proxy on every
# page of a paginated fetch costs seconds per page (option_iv fetches 8+
# pages, each trying 6 backends). Reset on success so a recovered proxy
# is picked back up. Never read without a lock held by the caller.
_PROXY_DEGRADED = False

# When BOTH the proxy and direct connection fail with network-level errors
# (RemoteDisconnected / empty body) for several consecutive calls, eastmoney
# is throttling/banning this IP (2026-08-09 and 2026-08-17 events). Once
# latched, em_get_json fails fast so dashboard sync steps don't burn 30-60s
# retrying; the next day's process (or a later success) clears it.
_EM_BLOCKED = False
_EM_BLOCKED_AT: float = 0.0
_EM_FAIL_STREAK = 0


def _em_headers(referer: str) -> dict[str, str]:
    parsed = urllib.parse.urlparse(referer)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "https://quote.eastmoney.com"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Origin": origin,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Connection": "keep-alive",
    }
    # 2026-08-09: eastmoney IP-ban (caused by the fund-flow backfill storm)
    # rejects the home line; a proxy exit node + fingerprint cookie restores
    # access (same recipe as industry_fund_flow._eastmoney_board_fund_flow_daykline).
    if _COOKIE:
        headers["Cookie"] = _COOKIE
    return headers


def _json_dict_from_text(text: str, *, source: str) -> dict[str, Any]:
    body = str(text or "").strip()
    if not body:
        raise RuntimeError(f"{source}_empty_body")
    try:
        j = json.loads(body)
    except json.JSONDecodeError as e:
        preview = body[:160].replace("\n", " ")
        raise RuntimeError(f"{source}_invalid_json:{e.msg}:{preview}") from e
    if not isinstance(j, dict):
        raise RuntimeError(f"{source}_non_object_json:{type(j).__name__}")
    return j


def _curl_get_json(
    url: str, *, params: dict[str, str], referer: str, timeout: float, use_proxy: bool = True
) -> dict[str, Any]:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    headers = _em_headers(referer)
    args = ["curl", "-sS", "--compressed", "-w", "\n%{http_code}"]
    if use_proxy and _PROXY:
        args += ["-x", _PROXY]
    for name, value in headers.items():
        args.extend(["-H", f"{name}: {value}"])
    args.append(full_url)
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=max(5, int(timeout)),
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "curl_failed")
    stdout = proc.stdout or ""
    body, sep, status = stdout.rpartition("\n")
    if not sep:
        body = stdout
        status = "000"
    if status and status.isdigit() and int(status) >= 400:
        preview = body[:160].replace("\n", " ")
        raise RuntimeError(f"curl_http_{status}:{preview}")
    return _json_dict_from_text(body, source="curl")


def _urllib_get_json(
    url: str, *, params: dict[str, str], referer: str, timeout: float, use_proxy: bool = True
) -> dict[str, Any]:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url, headers=_em_headers(referer))
    opener: urllib.request.OpenerDirector | None = None
    if use_proxy and _PROXY:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": _PROXY, "https": _PROXY})
        )
    with (opener.open(req, timeout=timeout) if opener else urllib.request.urlopen(req, timeout=timeout)) as resp:
        raw = resp.read()
        status = getattr(resp, "status", 200)
    if int(status) >= 400:
        preview = raw[:160].decode("utf-8", errors="replace").replace("\n", " ")
        raise RuntimeError(f"urllib_http_{status}:{preview}")
    return _json_dict_from_text(raw.decode("utf-8", errors="replace"), source="urllib")


def _em_get_json_no_proxy(url, *, params, referer, timeout):
    """Try all three backends with the proxy disabled (direct connection)."""
    errors: list[str] = []
    try:
        import requests  # type: ignore[import-not-found]

        resp = requests.get(
            url,
            params=params,
            headers=_em_headers(referer),
            timeout=timeout,
            proxies={"http": None, "https": None},
        )
        if resp.status_code >= 400:
            preview = resp.text[:160].replace("\n", " ")
            raise RuntimeError(f"http_{resp.status_code}:{preview}")
        j = resp.json()
        if not isinstance(j, dict):
            raise RuntimeError(f"non_object_json:{type(j).__name__}")
        return j
    except Exception as e:  # noqa: BLE001
        errors.append(f"requests:{e}")
    try:
        return _curl_get_json(url, params=params, referer=referer, timeout=timeout, use_proxy=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"curl:{e}")
    try:
        return _urllib_get_json(url, params=params, referer=referer, timeout=timeout, use_proxy=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"urllib:{e}")
    raise RuntimeError("; ".join(errors[-3:]))


def em_get_json(
    url: str,
    *,
    params: dict[str, str],
    referer: str,
    timeout: float = 25.0,
) -> dict[str, Any]:
    global _PROXY_DEGRADED, _EM_BLOCKED, _EM_BLOCKED_AT, _EM_FAIL_STREAK
    errors: list[str] = []
    if _EM_BLOCKED:
        # Eastmoney IP ban latched this process — fail fast instead of
        # burning the sync cycle on retries. Success path never reaches
        # here; _EM_BLOCKED is cleared only by a later successful call in
        # a fresh process (a new uvicorn start after the ban cools).
        raise RuntimeError("eastmoney_ip_ban_latched")
    use_proxy = bool(_PROXY) and not _PROXY_DEGRADED
    try:
        import requests  # type: ignore[import-not-found]

        proxies: dict[str, str | None] = {"http": None, "https": None}
        if use_proxy:
            proxies = {"http": _PROXY, "https": _PROXY}
        resp = requests.get(
            url,
            params=params,
            headers=_em_headers(referer),
            timeout=timeout,
            proxies=proxies,
        )
        if resp.status_code >= 400:
            preview = resp.text[:160].replace("\n", " ")
            raise RuntimeError(f"http_{resp.status_code}:{preview}")
        j = resp.json()
        if not isinstance(j, dict):
            raise RuntimeError(f"non_object_json:{type(j).__name__}")
        _PROXY_DEGRADED = False
        _EM_FAIL_STREAK = 0
        _EM_BLOCKED = False
        return j
    except Exception as e:  # noqa: BLE001
        errors.append(f"requests:{e}")

    if use_proxy:
        try:
            result = _curl_get_json(url, params=params, referer=referer, timeout=timeout)
            _PROXY_DEGRADED = False
            _EM_FAIL_STREAK = 0
            return result
        except Exception as e:  # noqa: BLE001
            errors.append(f"curl:{e}")

        try:
            result = _urllib_get_json(url, params=params, referer=referer, timeout=timeout)
            _PROXY_DEGRADED = False
            _EM_FAIL_STREAK = 0
            return result
        except Exception as e:  # noqa: BLE001
            errors.append(f"urllib:{e}")

        # All proxy attempts failed — mark degraded so later calls go direct.
        _PROXY_DEGRADED = True

    try:
        result = _em_get_json_no_proxy(url, params=params, referer=referer, timeout=timeout)
        _EM_FAIL_STREAK = 0
        return result
    except Exception as e:  # noqa: BLE001
        errors.append(f"direct:{e}")

    _EM_FAIL_STREAK += 1
    if _EM_FAIL_STREAK >= 3:
        _EM_BLOCKED = True
        _EM_BLOCKED_AT = time.time()

    raise RuntimeError("; ".join(errors[-3:]))
