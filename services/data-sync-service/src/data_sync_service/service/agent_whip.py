"""Same-key rebuy whip detection (server source of truth).

A whip is: a parking leg was just stopped out (sticky ``SELL_TO_REPO`` /
``trailExit`` / ``pendingOpenSell``) **and** today's parking pick is still the
same ETF key. The UI used to remember this in ``localStorage`` only; the agent
contract and the overlay API need a server-side answer.

This module is display / advice only. It never changes ``sleeve.action``,
``TRAIL_PCT``, the parking cooldown (k=0) or any target weight.

The short memory lives in ``app_settings`` (same store as the overlays), so a
restart or a different browser session sees the same whip flag.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import app_settings
from data_sync_service.service.agent_overlays import find_same_key_overlay

logger = logging.getLogger(__name__)

WHIP_ANNOTATION = "刚止损，回买=鞭打，请确认"
WHIP_OVERLAY_COPY = "建议暂缓 · 需确认"
WHIP_MEMORY_KEY = "agent_trail_exit_memory_v1"
WHIP_MEMORY_DAYS = 10


def _canonical_key(*, key: str | None = None, symbol: str | None = None) -> str | None:
    raw = str(key or "").strip().upper()
    if raw:
        return raw
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    try:
        from data_sync_service.service.multi_asset_sleeve import multi_key_for_symbol

        return multi_key_for_symbol(sym)
    except Exception:  # noqa: BLE001 — display helper, never a gate
        return None


def _today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _floor_iso(day: str, days: int = WHIP_MEMORY_DAYS) -> str:
    try:
        d = date.fromisoformat(str(day)[:10])
    except ValueError:
        d = date.today()
    return (d - timedelta(days=days)).isoformat()


def _load_memory(day: str) -> dict[str, str]:
    try:
        raw = app_settings.get_setting(WHIP_MEMORY_KEY, {})
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.warning("agent whip memory read failed: %s", exc)
        return {}
    if not isinstance(raw, dict):
        return {}
    floor = _floor_iso(day)
    out: dict[str, str] = {}
    for key, value in raw.items():
        k = _canonical_key(key=str(key))
        v = str(value or "")[:10]
        if k and len(v) == 10 and v >= floor:
            out[k] = v
    return out


def _save_memory(memory: dict[str, str]) -> None:
    try:
        app_settings.set_setting(WHIP_MEMORY_KEY, memory)
    except Exception as exc:  # noqa: BLE001 — a failed memory write must not break a read
        logger.warning("agent whip memory write failed: %s", exc)


def _observed_exits(
    sleeve: dict[str, Any] | None,
    holdings: list[dict[str, Any]] | None,
    *,
    day: str,
) -> dict[str, dict[str, Any]]:
    """Keys the backend is currently signalling an exit for (sticky flags)."""
    out: dict[str, dict[str, Any]] = {}
    s = sleeve or {}
    action = str(s.get("action") or "").upper()
    pending = bool(s.get("pendingOpenSell"))
    trail = bool(s.get("trailExit"))
    if pending or trail or action == "SELL_TO_REPO":
        # The exit belongs to the HELD leg (`symbol`), not necessarily today's
        # pick: on a trail exit the pick can already have rotated to BOND10.
        key = _canonical_key(symbol=s.get("symbol")) or _canonical_key(
            key=(s.get("pick") or {}).get("key")
        )
        if key:
            kind = "trailExit" if trail else ("pendingOpenSell" if pending else "SELL_TO_REPO")
            out[key] = {
                "key": key,
                "date": str(s.get("signalDate") or day)[:10],
                "kind": kind,
                "symbol": s.get("symbol"),
            }
    for h in holdings or []:
        if not (h.get("trailExit") or h.get("pendingOpenSell")):
            continue
        key = _canonical_key(key=h.get("key"), symbol=h.get("symbol"))
        if not key:
            continue
        kind = "trailExit" if h.get("trailExit") else "pendingOpenSell"
        out[key] = {
            "key": key,
            "date": str(h.get("signalDate") or day)[:10],
            "kind": kind,
            "symbol": h.get("symbol"),
        }
    return out


def evaluate_whip(
    sleeve: dict[str, Any] | None,
    holdings: list[dict[str, Any]] | None,
    *,
    day: str | None = None,
    observe: bool = True,
) -> dict[str, Any]:
    """Return the server-side whip state for one sleeve snapshot.

    ``sameKeyRebuy`` is true when today's pick key was stopped out recently.
    The accepted ``hold_same_key_rebuy`` overlay only changes the *copy*
    (``建议暂缓 · 需确认``); it never rewrites the engine action.
    """
    as_of = str(day or _today_iso())[:10]
    s = sleeve or {}
    pick = s.get("pick") or {}
    pick_key = _canonical_key(key=pick.get("key"), symbol=pick.get("symbol"))
    observed = _observed_exits(s, holdings, day=as_of)
    memory = _load_memory(as_of)

    # A completed HOLD means the leg is held again; drop the stale memory so
    # the badge does not stick for the whole 10-day window after a rebuy.
    changed = False
    if str(s.get("action") or "").upper() == "HOLD":
        for h in holdings or []:
            try:
                pos = float(h.get("positionPct") or 0)
            except (TypeError, ValueError):
                pos = 0.0
            if pos <= 0:
                continue
            held_key = _canonical_key(key=h.get("key"), symbol=h.get("symbol"))
            if held_key and held_key in memory:
                memory.pop(held_key, None)
                changed = True

    merged = dict(memory)
    for key, info in observed.items():
        stamp = str(info.get("date") or as_of)[:10]
        if merged.get(key) != stamp:
            changed = True
        merged[key] = stamp
    if observe and changed:
        _save_memory(merged)

    same_key = bool(pick_key and pick_key in merged)
    overlay = find_same_key_overlay(pick_key, day=as_of) if pick_key else None
    overlay_status = (overlay or {}).get("effectiveStatus")
    copy = None
    if same_key:
        copy = WHIP_OVERLAY_COPY if overlay_status == "accepted" else WHIP_ANNOTATION
    last = observed.get(pick_key) if pick_key else None
    last_exit = None
    if same_key:
        last_exit = {
            "key": pick_key,
            "date": merged.get(pick_key or ""),
            "kind": (last or {}).get("kind") or "observed",
            "symbol": (last or {}).get("symbol"),
        }
    return {
        "sameKeyRebuy": same_key,
        "pickKey": pick_key,
        "annotation": WHIP_ANNOTATION if same_key else None,
        "copy": copy,
        "lastExit": last_exit,
        "overlay": overlay,
        "keys": sorted(merged),
        "memoryDays": WHIP_MEMORY_DAYS,
    }
