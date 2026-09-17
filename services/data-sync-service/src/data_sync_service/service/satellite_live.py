"""Live 14:30 satellite panel snapshot (OPT-222).

The watchlist satellite card used to show the last replay day only (which, on
a fresh trading day, is yesterday) — so "what would I buy at today's 14:30"
was never on screen. This module builds TODAY's panel from a market-wide
realtime quote snapshot and persists it as a small JSON report the API and
UI read.

Design:

- Quotes are captured first (time-sensitive: the job fires at 14:30 sharp),
  then the frozen habit context is loaded and the quotes are injected as
  today's synthetic daily row / 14:30 print / 15:00 mark.
- The panel itself is produced by ``satellite_signals_for_day`` — the exact
  same code as the paper layer and the replay — so the live list and the
  frozen definitions cannot drift (parity test in tests/test_satellite_live.py).
- Documented approximations: total_mv comes from the previous session's
  ``stock_dailybasic`` (realtime quotes carry no market cap); names without
  yesterday's mv are skipped, same as the replay would skip them.
- Failure policy: only a complete panel is persisted (``decisionAvailable``
  true + quote coverage above the floor). On failure the previous snapshot
  stays untouched and the job records a failure for the watchdog/health.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

LIVE_PANEL_NAME = "satellite_live_panel_latest.json"
# Below this share of the context universe with a usable quote the snapshot is
# treated as broken (provider outage) — never persisted.
MIN_QUOTE_COVERAGE = 0.5
# Compact ranked entries persisted for the card (the full panel can be ~40).
RANKED_KEEP = 15

CN_TZ = ZoneInfo("Asia/Shanghai")


def report_path() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / LIVE_PANEL_NAME


def load_live_panel() -> dict[str, Any] | None:
    """Latest persisted live panel (None when never generated / unreadable)."""
    p = report_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("live panel read failed: %s", exc)
        return None


def save_live_panel(panel: dict[str, Any]) -> Path:
    """Atomic write (tmp + replace) so readers never see a partial file."""
    p = report_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(panel, ensure_ascii=False), encoding="utf-8")
    tmp.replace(p)
    return p


def _universe_ts_codes() -> list[str]:
    """CN stock universe for the quote snapshot (S-gap shape, cheap query).

    Mirrors the frozen universe filters (no ST / no BJ / not delisted); names
    without enough daily history are dropped later by the feature builder, so
    quoting a superset is safe.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code FROM stock_basic "
                "WHERE delist_date IS NULL "
                "AND COALESCE(name, '') NOT LIKE '%%ST%%' "
                "AND ts_code ~ '^(6\\d{5}\\.SH|(0|3)\\d{5}\\.SZ)$' "
                "ORDER BY ts_code"
            )
            return [str(r[0]) for r in cur.fetchall()]


def _fetch_quotes() -> dict[str, dict[str, Any]]:
    """Market-wide realtime quotes {ts_code: quote} (batched, ~6s)."""
    from data_sync_service.service.realtime_quote import fetch_realtime_quotes_batched

    codes = _universe_ts_codes()
    items = fetch_realtime_quotes_batched(codes, batch_size=50, max_workers=6)
    out: dict[str, dict[str, Any]] = {}
    for it in items:
        ts = str(it.get("ts_code") or "")
        if ts:
            out[ts] = it
    return out


def _num(value: Any) -> float | None:
    try:
        if value in (None, "", "0"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _latest_mv(mv_map: dict[str, dict[str, float]], day: str) -> dict[str, float]:
    """Total-mv map of the latest session strictly before ``day``."""
    for d in sorted((x for x in mv_map if x < day), reverse=True):
        mp = mv_map.get(d) or {}
        if mp:
            return mp
    return {}


def _inject_day(ctx: dict[str, Any], day: str, quotes: dict[str, dict[str, Any]]) -> int:
    """Write today's synthetic row/prints into the context. Returns injected count.

    Quote fields: ``price/open/high/low/pre_close/amount`` are required.
    Optional overrides (used by tests and by any future intraday source):
    ``mv`` = today's total_mv (falls back to the previous session's),
    ``px1430``/``px1500`` = the day's 14:30 print / 15:00 raw mark (fall back
    to ``price``), ``hl1430`` = (high, low) over bars <= 14:30 (falls back to
    the quote's session high/low — the 14:30 job's capture is the proxy).

    Overwrites the day's row when it already exists (historical parity tests
    inject a day the context already carries).
    """
    per_ts: dict[str, list[dict[str, Any]]] = ctx["per_ts"]
    date_idx: dict[str, dict[str, int]] = ctx["date_idx"]
    cal: list[str] = ctx["cal"]
    mv_prev = _latest_mv(ctx.get("mv_map") or {}, day)
    today_mv: dict[str, float] = {}
    px_by_hhmm: dict[str, dict[str, dict[str, float]]] = ctx["px_by_hhmm"]
    px1430 = px_by_hhmm.setdefault("1430", {})
    raw1500 = px_by_hhmm.setdefault("1500", {})
    hl = ctx["px_hl_1430"]
    close_by_ts = ctx["close_by_ts"]

    injected = 0
    for ts, q in quotes.items():
        series = per_ts.get(ts)
        if not series:
            continue
        price = _num(q.get("price"))
        open_px = _num(q.get("open"))
        high = _num(q.get("high"))
        low = _num(q.get("low"))
        pre_close = _num(q.get("pre_close"))
        if not (price and open_px and high and low and pre_close):
            continue
        row = {
            "date": day,
            "open": open_px,
            "high": high,
            "low": low,
            "close": price,
            "pre_close": pre_close,
            "amount": _num(q.get("amount")),
        }
        if series and series[-1].get("date") == day:
            series[-1] = row
        else:
            series.append(row)
        date_idx.setdefault(ts, {})[day] = len(series) - 1
        mv = _num(q.get("mv")) or mv_prev.get(ts)
        if mv:
            today_mv[ts] = mv
        # A present-but-None px key means "this name had no print that slot"
        # (historical parity); an absent key means the live quote IS the print.
        px_fill = _num(q.get("px1430")) if "px1430" in q else price
        px_mark = _num(q.get("px1500")) if "px1500" in q else px_fill
        if px_fill:
            px1430.setdefault(ts, {})[day] = px_fill
            close_by_ts.setdefault(ts, {})[day] = px_fill
        if px_mark:
            raw1500.setdefault(ts, {})[day] = px_mark
        hl_override = q.get("hl1430")
        if isinstance(hl_override, (list, tuple)) and len(hl_override) == 2:
            hl.setdefault(ts, {})[day] = (float(hl_override[0]), float(hl_override[1]))
        elif "hl1430" not in q:
            hl.setdefault(ts, {})[day] = (high, low)
        injected += 1

    ctx["px_1430"] = px1430
    if day not in ctx["idx_by_day"]:
        cal.append(day)
        ctx["idx_by_day"][day] = len(cal) - 1
    ctx["mv_map"].setdefault(day, {}).update(today_mv)
    return injected


def build_live_panel(day: str | None = None, *, quotes: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Build today's 14:30 panel. ``quotes`` is the injection seam for tests.

    Runs the frozen habit replay on the pre-injection context first: today has
    no daily row yet, so the replay keeps the legs that are DUE today (with
    ``exitDue``) — the exit side of the action does not depend on the watchlist
    registry being maintained.
    """
    from data_sync_service.service.satellite_signals import satellite_signals_for_day
    from data_sync_service.service.state_bucket_track import (
        HABIT_CTX_TIMES,
        HABIT_RECIPE,
        load_sgap_context,
        replay_sgap_from_context,
    )
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    day = day or shanghai_today_iso()
    if quotes is None:
        quotes = _fetch_quotes()
    ctx = load_sgap_context(day, day, times=HABIT_CTX_TIMES)
    replay_start = (date.fromisoformat(day) - timedelta(days=200)).isoformat()
    try:
        sat = replay_sgap_from_context(ctx, start=replay_start, end=day, **HABIT_RECIPE)
        legs = list(sat.get("openPositions") or [])
    except Exception as exc:  # noqa: BLE001
        logger.warning("live panel: replay legs failed: %s", exc)
        legs = []
    exits = [x for x in legs if str(x.get("exitDue") or "") == day]
    held = [x for x in legs if str(x.get("exitDue") or "") != day]

    injected = _inject_day(ctx, day, quotes)
    panel = satellite_signals_for_day(ctx, day, today=day)
    coverage = injected / max(1, len(ctx.get("per_ts") or {}))

    ranked_all = panel.get("ranked") or []
    ranked = [
        {
            "ts": e.get("ts"),
            "ampRank": e.get("ampRank"),
            "inBucket": e.get("inBucket"),
            "gapPct": e.get("gapPct"),
            "amp1430Pct": e.get("amp1430Pct"),
            "px1430": e.get("px1430"),
            "skipReason": e.get("skipReason"),
            "fillable": e.get("fillable"),
            "wouldFill": e.get("wouldFill"),
        }
        for e in ranked_all[:RANKED_KEEP]
    ]
    return {
        "tradeDate": day,
        "generatedAt": datetime.now(tz=CN_TZ).isoformat(timespec="seconds"),
        "decisionAvailable": bool(panel.get("decisionAvailable")),
        "reason": panel.get("reason"),
        "gateOpen": panel.get("gateOpen"),
        "breadth1430": panel.get("breadth1430"),
        "gapCount": panel.get("gapCount"),
        "bucketSize": panel.get("bucketSize"),
        "poolSize": panel.get("poolSize"),
        "coverage": round(coverage, 4),
        "quoted": injected,
        "universe": len(ctx.get("per_ts") or {}),
        "wouldFill": [e["ts"] for e in ranked_all if e.get("wouldFill")],
        "ranked": ranked,
        "exits": [
            {"ts": str(x.get("ts") or ""), "entryDate": x.get("entryDate"), "exitDue": x.get("exitDue")}
            for x in exits
            if x.get("ts")
        ],
        "heldLegs": [
            {
                "ts": str(x.get("ts") or ""),
                "entryDate": x.get("entryDate"),
                "exitDue": x.get("exitDue"),
                "daysLeft": x.get("daysLeft"),
            }
            for x in held
            if x.get("ts")
        ],
        "basis": (
            "实时快照合成当日面板（gap>3% → amp_1430 升序 → 顶部 1/3 桶 → skip_t1/C1/"
            "可成交 → strict 池；14:30 广度 >0.5 开闸）；mv 用昨收（实时报价无市值）；"
            "与 paper/回测同源 satellite_signals_for_day"
        ),
    }


def persist_if_complete(panel: dict[str, Any]) -> tuple[bool, str]:
    """Persist only a complete panel. Returns (saved, reason)."""
    if not panel.get("decisionAvailable"):
        return False, f"panel unavailable: {panel.get('reason') or 'unknown'}"
    if float(panel.get("coverage") or 0.0) < MIN_QUOTE_COVERAGE:
        return False, f"quote coverage too low: {panel.get('coverage')}"
    save_live_panel(panel)
    gate = "open" if panel.get("gateOpen") else "closed"
    return True, f"gate={gate} wouldFill={len(panel.get('wouldFill') or [])}"
