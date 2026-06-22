"""Shared HTTP helper for East Money push2 APIs."""

from __future__ import annotations

import json
import subprocess
import urllib.parse
import urllib.request
from typing import Any


def _curl_get_json(url: str, *, params: dict[str, str], referer: str, timeout: float) -> dict[str, Any]:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    proc = subprocess.run(
        [
            "curl",
            "-s",
            "-H",
            "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "-H",
            f"Referer: {referer}",
            full_url,
        ],
        capture_output=True,
        text=True,
        timeout=max(5, int(timeout)),
        check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        raise RuntimeError(proc.stderr.strip() or "curl_failed")
    j = json.loads(proc.stdout)
    return j if isinstance(j, dict) else {}


def _urllib_get_json(url: str, *, params: dict[str, str], referer: str, timeout: float) -> dict[str, Any]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
        "Connection": "keep-alive",
    }
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    return j if isinstance(j, dict) else {}


def em_get_json(
    url: str,
    *,
    params: dict[str, str],
    referer: str,
    timeout: float = 25.0,
) -> dict[str, Any]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
        "Connection": "keep-alive",
    }
    errors: list[str] = []
    try:
        import requests  # type: ignore[import-not-found]

        resp = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            proxies={"http": None, "https": None},
        )
        resp.raise_for_status()
        j = resp.json()
        if isinstance(j, dict):
            return j
    except Exception as e:  # noqa: BLE001
        errors.append(f"requests:{e}")

    try:
        return _curl_get_json(url, params=params, referer=referer, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        errors.append(f"curl:{e}")

    try:
        return _urllib_get_json(url, params=params, referer=referer, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        errors.append(f"urllib:{e}")

    raise RuntimeError("; ".join(errors[-3:]))
