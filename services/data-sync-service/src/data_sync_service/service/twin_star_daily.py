"""机会双子星 (Opportunity Twin-Star) 每日操作信号 — 14:30 前提醒用。

core  = live multi_asset_sleeve (择强单轨 mom_compare + trail8 同源) 当前目标
sat   = S-gap 卫星: 最新收盘 (t-1) 的 R-wide 闸 + 低波33% S-gap 候选
        + Watchlist 4 槽占用 (liveHoldings)；引擎 openPositions 只作对照
资金  = 机会口径: 无直播卫星仓且今日不开新仓 → 核心 100%; 否则核心 50% / 卫星 50%

Truth: docs/backtests/state-bucket-algo-2026-08-31.md §7
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from data_sync_service.service.state_bucket_track import (
    BODY,
    BUCKET_Q,
    MAX_POS,
    POSITION_PCT,
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
TOP_N = MAX_POS
SAT_SLOT_NAV_PCT = round(50 * POSITION_PCT, 2)  # clip4: 4 × 12.5% NAV when sleeve is 50/50
SAT_PROTECT_STOP_PCT = 0.05

# JSON contract: packages/shared TwinStarActionResponseSchema (OPT-134).
# clip4 literals: maxPos=4 slotOfSleeve=0.25 satSlotNavPct=12.5 body=3 protectStopPct=0.05
# sat.coreTargetPct ∈ {50, 100}; sat.satTargetPct ∈ {0, 50}


def clip4_contract() -> dict[str, Any]:
    """Frozen clip4 numbers the UI Zod-parses as literals (do not silently drift)."""
    return {
        "maxPos": MAX_POS,
        "slotOfSleeve": POSITION_PCT,
        "satSlotNavPct": SAT_SLOT_NAV_PCT,
        "body": BODY,
        "protectStopPct": SAT_PROTECT_STOP_PCT,
    }


def count_weekdays_inclusive(from_iso: str, to_iso: str) -> int:
    """Mon–Fri count, inclusive. Live proxy for CN sessions (holidays may be ±1)."""
    try:
        start = date.fromisoformat(from_iso)
        end = date.fromisoformat(to_iso)
    except (TypeError, ValueError):
        return 0
    if end < start:
        return 0
    n = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            n += 1
        cur += timedelta(days=1)
    return n


def sat_body_progress(
    entry_date: str | None, as_of: str | None, *, body: int = BODY
) -> dict[str, Any]:
    if not entry_date or not as_of:
        return {"heldDays": None, "daysLeft": None, "due": False, "missingEntry": True}
    held = count_weekdays_inclusive(str(entry_date), str(as_of))
    return {
        "heldDays": held,
        "daysLeft": max(0, body - held),
        "due": held >= body,
        "missingEntry": False,
    }


def ts_from_cn_symbol(symbol: str) -> str | None:
    s = str(symbol or "").upper().strip()
    if not s.startswith("CN:"):
        return None
    ticker = s[3:].split(".")[0]
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    suffix = "SH" if ticker.startswith("6") else "SZ"
    return f"{ticker}.{suffix}"


def cn_symbol_from_ts(ts: str) -> str:
    code = str(ts or "").split(".")[0]
    return f"CN:{code}"


def sat_name_ts(sat: dict[str, Any], book: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for key in ("candidates", "blocked", "alternates"):
        for row in sat.get(key) or []:
            ts = str(row.get("ts") or "")
            if ts:
                names.add(ts)
    for row in book.get("holdings") or []:
        ts = str(row.get("ts") or "")
        if ts:
            names.add(ts)
    return names


def live_sat_ts_codes(today: date | None = None) -> set[str]:
    """Candidate + paper-open ts codes. Does not replay the S-gap engine.

    Used by notifications to split STOCK-day satellite names from leftover
    S-3 basket names without a second health/engine pass.
    """
    names: set[str] = set()
    try:
        from data_sync_service.service.twin_star_intraday import (
            load_intraday_sat,
            session_date,
        )

        sat = load_intraday_sat(today or session_date()) or {}
        names |= sat_name_ts(sat, {"holdings": []})
    except Exception:
        pass
    try:
        from data_sync_service.db.paper_trading import SOURCE_TWIN_STAR, list_paper_trades

        for row in list_paper_trades(status="open", market="CN", limit=20):
            if str(row.get("source") or "") != SOURCE_TWIN_STAR:
                continue
            ts = ts_from_cn_symbol(str(row.get("symbol") or ""))
            if ts:
                names.add(ts)
    except Exception:
        pass
    return names


def live_sat_holdings(
    *,
    health: dict[str, Any] | None,
    pick_key: str | None,
    sat_ts: set[str],
) -> list[dict[str, Any]]:
    """Watchlist CN stocks that count as satellite occupancy (clip4 truth)."""
    out: list[dict[str, Any]] = []
    for h in (health or {}).get("holdings") or []:
        sym = str(h.get("symbol") or "")
        ts = ts_from_cn_symbol(sym)
        if not ts:
            continue
        try:
            pct = float(h.get("positionPct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        if pct <= 0:
            continue
        if pick_key == "STOCK" and ts not in sat_ts:
            continue
        out.append(
            {
                "ts": ts,
                "symbol": sym,
                "name": h.get("name"),
                "positionPct": pct,
                "entryDate": h.get("entryDate"),
                "costPrice": h.get("costPrice"),
                "lastClose": h.get("lastClose"),
                "heldDays": None,
                "daysLeft": None,
                "due": False,
            }
        )
    return out


def annotate_live_body(rows: list[dict[str, Any]], as_of: str) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for row in rows:
        body = sat_body_progress(row.get("entryDate"), as_of)
        annotated.append({**row, **body})
    return annotated


def fill_candidate_names(*groups: list[dict[str, Any]]) -> None:
    """Attach stock_basic.display names onto packed candidate dicts (in place)."""
    codes = [str(r.get("ts") or "") for g in groups for r in g if r.get("ts")]
    codes = [c for c in codes if c]
    if not codes:
        return
    try:
        from data_sync_service.db.stock_basic import fetch_names

        names = fetch_names(codes)
    except Exception:
        return
    if not names:
        return
    for g in groups:
        for r in g:
            if r.get("name"):
                continue
            n = names.get(str(r.get("ts") or ""))
            if n:
                r["name"] = n


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

    candidates = _pack(picks["primary"])
    blocked = _pack(picks["blocked"], blocked=True)
    alternates = _pack(picks["alternates"])
    fill_candidate_names(candidates, blocked, alternates)
    return {
        "asOf": signal_day,
        "gateOpen": breadth > R_WIDE_THRESHOLD,
        "breadth": round(float(breadth), 3),
        "gapCount": len(gap_stocks),
        "candidates": candidates,
        "blocked": blocked,
        "alternates": alternates,
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

    100% core when the Watchlist satellite book is idle and today is not opening.
    50% core / 50% sat when holding live sat names or opening new clip4 slots.
    Engine ``openPositions`` must not flip this split.
    """
    opening = bool(gate_open) and bool(candidates)
    if holdings or opening:
        return 50
    return 100


def build_twin_star_daily_action(today: date | None = None) -> dict[str, Any]:
    """机会双子星今日操作信号 (core pick + satellite gate/candidates/book)."""
    from data_sync_service.service.twin_star_intraday import session_date

    today = today or session_date()
    health: dict[str, Any] | None = None
    core: dict[str, Any] = {"pick": None, "label": None, "action": None, "message": None}
    try:
        from data_sync_service.service.portfolio_health import build_portfolio_health

        health = build_portfolio_health(trade_date=None, markets=("CN", "HK"))
        sleeve = (health or {}).get("multiAssetSleeve") or {}
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
    snap = {
        "snapshotMissing": False,
        "snapshotStale": False,
        "snapshotAgeSec": None,
        "snapshotReason": None,
    }
    try:
        from data_sync_service.service.twin_star_intraday import (
            intraday_snapshot_status,
            load_intraday_sat,
        )

        intraday = load_intraday_sat(today)
        if intraday is not None:
            sat = intraday
            fill_candidate_names(
                list(sat.get("candidates") or []),
                list(sat.get("blocked") or []),
                list(sat.get("alternates") or []),
            )
        # Snapshot failure is a live-tape concern; historical `today` in tests
        # must not inherit today's East Money hang.
        if today == session_date():
            status = intraday_snapshot_status()
            snap = {
                "snapshotMissing": bool(status.get("missing")),
                "snapshotStale": bool(status.get("stale")),
                "snapshotAgeSec": status.get("ageSeconds"),
                "snapshotReason": status.get("reason"),
            }
    except Exception:
        pass
    book = _sat_book(today)
    live = annotate_live_body(
        live_sat_holdings(
            health=health,
            pick_key=core.get("pick"),
            sat_ts=sat_name_ts(sat, book),
        ),
        today.isoformat(),
    )
    live_exits = [h for h in live if h.get("due")]
    book = {
        **book,
        "liveHoldings": live,
        "liveExitsDue": live_exits,
        "liveHeld": len(live),
        "liveFreeSlots": max(0, MAX_POS - len(live)),
        "engineHeld": len(book.get("holdings") or []),
    }
    holdings = live
    core_pct = _core_target_pct(
        gate_open=bool(sat.get("gateOpen")),
        candidates=list(sat.get("candidates") or []),
        holdings=holdings,
    )
    sat = {
        **sat,
        **snap,
        "book": book,
        "coreTargetPct": core_pct,
        "satTargetPct": 100 - core_pct,
    }
    return {"core": core, "sat": sat, "clip4": clip4_contract()}


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
    snap_failed = bool(sat.get("snapshotMissing") or sat.get("snapshotStale"))
    if sat.get("asOf") is None:
        sat_line = "卫星: 数据不可用"
    elif snap_failed:
        sat_line = (
            "卫星: 今日盘中快照失败（东财），名单不可用 — 不要用 T-1 名单下单"
        )
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
    live = book.get("liveHoldings") or []
    exits = book.get("liveExitsDue") or []
    sell_line = ""
    if exits:
        sell_line = f"今日卖 {', '.join(e['ts'] for e in exits[:5])} · "
    if live:
        hold_bits = ", ".join(
            f"{h['ts']}(剩{h.get('daysLeft')}d)" for h in live[:3]
        )
        sat_line += f" · 你卫星仓 {len(live)}/{MAX_POS}: {hold_bits}"
    engine_n = int(book.get("engineHeld") or len(book.get("holdings") or []) or 0)
    if engine_n:
        sat_line += f" · 引擎模拟 {engine_n} 只（对照）"
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
