"""机会双子星 (Opportunity Twin-Star) 每日操作信号 — 14:30 前提醒用。

core  = live multi_asset_sleeve (择强单轨 mom_compare + trail8 同源) 当前目标
sat   = S-gap 卫星: 最新收盘 (t-1) 的 R-wide 闸 + 低波33% S-gap 候选
        + 引擎回放持仓簿 (openPositions / exitsDue)
资金  = 机会口径: 无卫星仓且今日不开新仓 → 核心 100%; 否则核心 50% / 卫星 50%

Truth: docs/backtests/state-bucket-algo-2026-08-31.md §7
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from data_sync_service.service.state_bucket_track import (
    BODY,
    BUCKET_Q,
    R_WIDE_THRESHOLD,
    _day_features,
    _load_calendar,
    _load_mv,
    _load_rows,
    build_sgap_timeline,
    select_live_gap_picks,
)

LOOKBACK_DAYS = 90
BOOK_LOOKBACK_CAL_DAYS = 45
TOP_N = 5


def _sat_signal(today: date) -> dict[str, Any] | None:
    """Satellite signal from the latest completed close before `today`."""
    w_start = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()
    w_end = today.isoformat()
    try:
        cal = _load_calendar(w_start, w_end)
        per_ts = _load_rows(w_start, w_end)
        mv_map = _load_mv(w_start, w_end)
    except Exception:
        return None
    if not cal:
        return None
    # signal day: most recent close strictly before today if today is a trading
    # day (execute next open), else the last close (weekend -> Monday execution).
    # stock_dailybasic (mv) may lag daily bars -> fall back to the latest day
    # that actually has mv coverage, and report the lag.
    if today.isoformat() in cal:
        cal = [d for d in cal if d < today.isoformat()]
    if not cal:
        return None
    signal_day = cal[-1]
    lag_note = None
    mv_floor = max(1, int(len(per_ts) * 0.5))
    for d in reversed(cal):
        if len(mv_map.get(d, {})) >= mv_floor:
            if d != signal_day:
                lag_note = f"卫星数据滞后（mv 至 {d}，最新日线 {signal_day}）"
            signal_day = d
            break
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    day_all, breadth = _day_features(per_ts, mv_map, cal, signal_day, date_idx)
    gap_stocks = [(ts, d["amp"], d["gap"]) for ts, d in day_all.items() if d["is_gap"]]

    def _limit_locked(ts: str) -> bool:
        di = date_idx.get(ts, {}).get(signal_day, -1)
        if di < 0:
            return False
        r = per_ts.get(ts, [])[di]
        pc = r.get("pre_close")
        if not pc or pc <= 0:
            return False
        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
        return float(r["close"]) >= pc * (1 + lim - 0.004)

    locked = {ts for ts, _amp, _gap in gap_stocks if _limit_locked(ts)}
    picks = select_live_gap_picks(
        gap_stocks, locked, bucket_q=BUCKET_Q, top_n=TOP_N
    )

    def _pack(rows: list, *, blocked: bool = False) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for ts, amp, gap in rows:
            series = per_ts.get(ts)
            idx = date_idx.get(ts, {}).get(signal_day, -1)
            close = series[idx]["close"] if idx >= 0 and series else None
            out.append(
                {
                    "ts": ts,
                    "amp": round(float(amp) * 100, 2),
                    "gapPct": round(float(gap) * 100, 2),
                    "close": float(close) if close else None,
                    "limitLocked": blocked,
                }
            )
        return out

    return {
        "asOf": signal_day,
        "gateOpen": breadth > R_WIDE_THRESHOLD,
        "breadth": round(float(breadth), 3),
        "gapCount": len(gap_stocks),
        "candidates": _pack(picks["primary"]),
        "blocked": _pack(picks["blocked"], blocked=True),
        "alternates": _pack(picks["alternates"]),
        "note": lag_note,
    }


def _sat_book(today: date) -> dict[str, Any]:
    """Replay recent S-gap engine for open holdings + exits due (body=3).

    This is the live satellite position book: same rules as backtest
    (skip_t1_limit=True), ending at the latest completed session before today.
    """
    end = (today - timedelta(days=1)).isoformat()
    start = (today - timedelta(days=BOOK_LOOKBACK_CAL_DAYS)).isoformat()
    try:
        built = build_sgap_timeline(
            start=start, end=end, skip_t1_limit=True, pool_mode="strict"
        )
    except Exception:
        return {"asOf": None, "holdings": [], "exitsDue": [], "error": "book_unavailable"}
    holdings = list(built.get("openPositions") or [])
    # Also surface names that just hit body on the last replay day (already closed
    # in openPositions, but exit reminder still useful from last row satActive).
    exits_due = [h for h in holdings if int(h.get("daysLeft") or 0) <= 0]
    # holdings with daysLeft==0 should exit at today's/next close — keep in list
    # but also mirror into exitsDue for the reminder card.
    if not exits_due:
        # positions that exit on the next session after end (daysLeft==1 and today
        # is that session): daysLeft counts remaining body days including today.
        exits_due = [h for h in holdings if int(h.get("daysLeft") or 99) <= 1]
    return {
        "asOf": end,
        "holdings": holdings,
        "exitsDue": exits_due,
        "body": BODY,
    }


def _core_target_pct(*, gate_open: bool, candidates: list, holdings: list) -> int:
    """Opportunity capital split for live guidance.

    100% core when satellite is idle (no open book and not opening new today).
    50% core / 50% sat when holding or opening.
    """
    opening = bool(gate_open) and bool(candidates)
    if holdings or opening:
        return 50
    return 100


def build_twin_star_daily_action(today: date | None = None) -> dict[str, Any]:
    """机会双子星今日操作信号 (core pick + satellite gate/candidates/book)."""
    today = today or date.today()
    core: dict[str, Any] = {"pick": None, "label": None, "action": None, "message": None}
    try:
        from data_sync_service.service.portfolio_health import build_portfolio_health

        h = build_portfolio_health(trade_date=None, markets=("CN", "HK"))
        sleeve = (h or {}).get("multiAssetSleeve") or {}
        pick = sleeve.get("pick") or {}
        core = {
            "pick": (pick or {}).get("key") if isinstance(pick, dict) else None,
            "symbol": (pick or {}).get("symbol") if isinstance(pick, dict) else None,
            "label": sleeve.get("label"),
            "action": sleeve.get("action"),
            "message": sleeve.get("message"),
            "active": bool(sleeve.get("active")),
        }
    except Exception:
        pass
    sat = _sat_signal(today) or {
        "asOf": None,
        "gateOpen": None,
        "breadth": None,
        "gapCount": 0,
        "candidates": [],
    }
    try:
        from data_sync_service.service.twin_star_intraday import load_intraday_sat

        intraday = load_intraday_sat(today)
        if intraday is not None:
            sat = intraday
    except Exception:
        pass
    book = _sat_book(today)
    holdings = book.get("holdings") or []
    core_pct = _core_target_pct(
        gate_open=bool(sat.get("gateOpen")),
        candidates=list(sat.get("candidates") or []),
        holdings=holdings,
    )
    sat = {
        **sat,
        "book": book,
        "coreTargetPct": core_pct,
        "satTargetPct": 100 - core_pct,
    }
    return {"core": core, "sat": sat}


def build_twin_star_reminder_payload(today: date | None = None) -> dict[str, Any]:
    """Text payload for the 14:20 webhook + notifications hub."""
    action = build_twin_star_daily_action(today)
    core = action["core"]
    sat = action["sat"]
    core_pct = sat.get("coreTargetPct") or 100
    core_line = (
        f"核心{core_pct}%: {core.get('label') or core.get('pick') or 'REPO'} "
        f"({core.get('action') or 'HOLD'})"
        if core.get("pick") or core.get("action")
        else f"核心{core_pct}%: 信号不可用"
    )
    if sat.get("asOf") is None:
        sat_line = "卫星: 数据不可用"
    elif not sat["gateOpen"]:
        sat_line = f"卫星: R-wide 关闸 (breadth {sat['breadth']}) — 今日不开仓"
    else:
        cands = ", ".join(f"{c['ts']}(amp{c['amp']}%)" for c in sat["candidates"][:3]) or "无候选"
        sat_line = (
            f"卫星: R-wide 开闸 (breadth {sat['breadth']}) · {sat['gapCount']} 只缺口票 · "
            f"买 {cands}"
        )
        blocked = sat.get("blocked") or []
        alts = sat.get("alternates") or []
        if blocked:
            swap = alts[0]["ts"] if alts else "—"
            sat_line += f" · 涨停跳过 {blocked[0]['ts']} 换 {swap}"
    if sat.get("approx"):
        snap = sat.get("snapshotAt") or "盘中快照"
        sat_line += f" · {snap} 当日行情"
    book = sat.get("book") or {}
    holdings = book.get("holdings") or []
    exits = book.get("exitsDue") or []
    sell_line = ""
    if exits:
        sell_line = f"今日卖 {', '.join(e['ts'] for e in exits[:5])} · "
    if holdings:
        hold_bits = ", ".join(
            f"{h['ts']}(剩{h.get('daysLeft')}d)" for h in holdings[:3]
        )
        sat_line += f" · 持仓簿 {len(holdings)}只: {hold_bits}"
    if sat.get("note"):
        sat_line += f" · {sat['note']}"
    return {
        "title": "机会双子星 · 14:30 操作",
        "detail": f"{sell_line}{core_line} · {sat_line}",
        "core": core,
        "sat": sat,
    }


def now_cn() -> datetime:
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Shanghai"))
