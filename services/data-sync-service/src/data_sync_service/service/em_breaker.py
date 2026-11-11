"""Path-scoped East Money egress circuit breaker (2026-11-06).

Background
----------
``em_push2_http`` used one process-wide latch: three consecutive failures on
*any* East Money endpoint fail-fast-banned *every* EM call for 15 minutes.
During the 2026-11-03+ outage the failing endpoints were the push2 list APIs
(``clist`` / ``push2his``), so healthy routes were collateral damage:

- ``push2.eastmoney.com/api/qt/stock/get`` (single quote) was banned although
  it still answered;
- ``reportapi.eastmoney.com/report/list`` (research reports) was banned
  although it still answered — this is why ``research_report_sync`` reported
  ``eastmoney_ip_ban_latched`` while the report API was healthy.

The breaker is now keyed by **(host, path)**. A list-endpoint outage cannot
ban a single-quote or report endpoint. Real ban detection is preserved: the
same scope still latches after ``DEFAULT_FAIL_THRESHOLD`` consecutive
failures and retries after the cooldown. ``em_probe`` keeps its own direct
probe path and does not consult this breaker.

The error message intentionally keeps the substring
``eastmoney_ip_ban_latched`` so :mod:`em_source_status` still classifies a
latched fetch as EM-unavailable (degraded, not a fake success).
"""

from __future__ import annotations

import threading
import time
from typing import Any
from urllib.parse import urlparse

DEFAULT_FAIL_THRESHOLD = 3
DEFAULT_COOLDOWN_SECONDS = 15 * 60
LATCH_ERROR_PREFIX = "eastmoney_ip_ban_latched"

_LOCK = threading.Lock()
# scope -> {"fail_streak": float, "latched_at": float}
_STATE: dict[str, dict[str, float]] = {}


def scope_for(url: str) -> str:
    """Breaker scope for a request URL: ``host + path`` (never the query)."""
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower()
    path = parsed.path or "/"
    return f"{host}{path}"


def check(scope: str, *, cooldown_s: float = DEFAULT_COOLDOWN_SECONDS) -> None:
    """Raise when ``scope`` is latched; clear an expired latch in place."""
    if not scope:
        return
    now = time.time()
    with _LOCK:
        state = _STATE.get(scope)
        if not state:
            return
        latched_at = float(state.get("latched_at") or 0.0)
        if latched_at <= 0:
            return
        if now - latched_at < cooldown_s:
            raise RuntimeError(f"{LATCH_ERROR_PREFIX}:{scope}")
        # Cooldown elapsed: allow the next attempt and start the streak over.
        state["latched_at"] = 0.0
        state["fail_streak"] = 0.0


def record_success(scope: str) -> None:
    """Clear the streak/latch for one scope after a successful response."""
    if not scope:
        return
    with _LOCK:
        state = _STATE.get(scope)
        if state is not None:
            state["fail_streak"] = 0.0
            state["latched_at"] = 0.0


def record_failure(
    scope: str,
    *,
    threshold: int = DEFAULT_FAIL_THRESHOLD,
    cooldown_s: float = DEFAULT_COOLDOWN_SECONDS,
) -> None:
    """Count one transport/upstream failure for ``scope``; latch at threshold.

    ``cooldown_s`` is accepted for symmetry with :func:`check`; the latch
    timestamp is what the cooldown is measured from, so the value itself is
    not stored.
    """
    if not scope:
        return
    with _LOCK:
        state = _STATE.setdefault(scope, {"fail_streak": 0.0, "latched_at": 0.0})
        state["fail_streak"] = float(state.get("fail_streak") or 0.0) + 1.0
        if int(state["fail_streak"]) >= max(1, int(threshold)):
            state["latched_at"] = time.time()


def snapshot(*, cooldown_s: float = DEFAULT_COOLDOWN_SECONDS) -> dict[str, Any]:
    """Read-only breaker snapshot (never raises; no I/O).

    ``ban_latched`` means at least one scope is latched. ``scopes`` carries
    per-host/path detail so the health banner can say *which* route is down
    while other EM routes stay healthy.
    """
    now = time.time()
    scopes: dict[str, dict[str, Any]] = {}
    latched: list[str] = []
    max_remaining = 0
    max_streak = 0
    try:
        with _LOCK:
            raw = {k: dict(v) for k, v in _STATE.items()}
        for scope, state in sorted(raw.items()):
            latched_at = float(state.get("latched_at") or 0.0)
            remaining = 0
            if latched_at > 0:
                remaining = max(0, int(cooldown_s - (now - latched_at)))
            is_latched = latched_at > 0 and remaining > 0
            streak = int(state.get("fail_streak") or 0)
            if is_latched:
                latched.append(scope)
                max_remaining = max(max_remaining, remaining)
            max_streak = max(max_streak, streak)
            scopes[scope] = {
                "latched": is_latched,
                "cooldownRemainingS": remaining,
                "failStreak": streak,
            }
    except Exception:  # noqa: BLE001 - observability must never raise
        return {
            "ban_latched": False,
            "cooldown_remaining_s": 0,
            "fail_streak": 0,
            "latched_scopes": [],
            "scopes": {},
        }
    return {
        "ban_latched": bool(latched),
        "cooldown_remaining_s": max_remaining,
        "fail_streak": max_streak,
        "latched_scopes": latched,
        "scopes": scopes,
    }


def reset() -> None:
    """Clear all breaker state (tests / explicit operator reset)."""
    with _LOCK:
        _STATE.clear()
