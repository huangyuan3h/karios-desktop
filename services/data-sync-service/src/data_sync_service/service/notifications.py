"""Notification aggregation service (2026-08-12).

Aggregates "things to do / things to know" from existing products —
portfolio-health (stop/trail alerts + EXIT), reconciliation (missing
backtest holdings), sync_job_record (today's cron failures), rolling OOS
warning. Pure aggregation: no new data collection, one endpoint the UI
polls. Each item carries a watchlist-page anchor so the UI can scroll to
the relevant block.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SAT_BODY = 3

REPORTS_DIR = Path(__file__).resolve().parents[3] / "data" / "backtest_reports"

# Which jobs matter to the user's trading day (intraday + post-close chain).
TRADING_JOB_TYPES = {
    "close_sync",
    "stock_close_sync",
    "watchlist_automation",
    "paper_s3_intake_CN",
    "paper_s3_intake_HK",
    "paper_trading_update",
    "paper_chain_watchdog",
    "cn_industry_post_close_sync",
    "index_basic_sync",
    # Twin-star live tape + core-leg freshness (knife 5 / OPT-133)
    "twin_star_intraday",
    "sleeve_etf_daily_sync",
    "stock_daily_basic_sync",
}


def _note(
    *,
    nid: str,
    type: str,
    severity: str,
    title: str,
    detail: str,
    anchor: str,
    lane: str,
    book: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    return {
        "id": nid,
        "type": type,
        "severity": severity,
        "title": title,
        "detail": detail,
        "anchor": anchor,
        "lane": lane,
        "book": book,
        "createdAt": created_at or datetime.now(UTC).isoformat(),
    }


def _parse_iso_day(raw: Any) -> date | None:
    try:
        return date.fromisoformat(str(raw)[:10])
    except (TypeError, ValueError):
        return None


def _count_weekdays_inclusive(start: date, end: date) -> int:
    """SSE open sessions in [start, end] (body-day counter, holiday-aware)."""
    try:
        from data_sync_service.service.trade_calendar_utils import count_open_sessions

        return count_open_sessions(start.isoformat(), end.isoformat())
    except Exception:  # noqa: BLE001
        if end < start:
            return 0
        n = 0
        cur = start
        while cur <= end:
            if cur.weekday() < 5:
                n += 1
            cur += timedelta(days=1)
        return n


def _nth_weekday_inclusive(start: date, n: int) -> date | None:
    """Date of the n-th open session on/after start (1-indexed)."""
    try:
        from data_sync_service.service.trade_calendar_utils import nth_open_session

        iso = nth_open_session(start.isoformat(), n)
        return date.fromisoformat(iso) if iso else None
    except Exception:  # noqa: BLE001
        if n < 1:
            return None
        seen = 0
        cur = start
        for _ in range(40):
            if cur.weekday() < 5:
                seen += 1
                if seen >= n:
                    return cur
            cur += timedelta(days=1)
        return None


def _as_float(v: Any) -> float | None:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n == n else None  # NaN


def _holding_book(
    mode: str,
    pick: str | None,
    market: str,
    symbol: str | None = None,
    sat_ts: set[str] | None = None,
) -> str:
    """Which rulebook a holding is under — delegates to the single leg truth
    (``twin_star_daily.holding_book``, OPT-140). Kept as a private alias so
    call sites and tests don't churn."""
    from data_sync_service.service.twin_star_daily import holding_book

    return holding_book(mode, pick, market, symbol=symbol, sat_ts=sat_ts)


def _load_health_ctx() -> dict[str, Any]:
    from data_sync_service.service.portfolio_health import build_portfolio_health

    try:
        h = build_portfolio_health(trade_date=None, markets=("CN", "HK"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: portfolio health failed: %s", exc)
        return {"blocks": {}, "pick": None, "tradeDate": None, "satTs": set()}
    blocks: dict[str, dict[str, Any]] = {}
    for key, market in (("", "CN"), ("hkHealth", "HK")):
        block = h if key == "" else h.get("hkHealth") or {}
        if block:
            blocks[market] = block
    pick_raw = (h.get("multiAssetSleeve") or {}).get("pick") or {}
    pick = pick_raw.get("key") if isinstance(pick_raw, dict) else None
    sat_ts: set[str] = set()
    try:
        from data_sync_service.service.twin_star_daily import live_sat_ts_codes

        sat_ts = live_sat_ts_codes()
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: sat ts codes failed: %s", exc)
    return {"blocks": blocks, "pick": pick, "tradeDate": h.get("tradeDate"), "satTs": sat_ts}


def _anchor_blocks() -> dict[str, dict[str, Any]]:
    return _load_health_ctx()["blocks"]


def _sat_holding_alerts(
    *,
    market: str,
    hold: dict[str, Any],
    as_of: date | None,
) -> list[dict[str, Any]]:
    """Twin-star satellite: body=3 day-3 14:30 sell. No protect stop (habit)."""
    symbol = str(hold.get("symbol") or "")
    name = str(hold.get("name") or symbol)
    out: list[dict[str, Any]] = []
    entry = _parse_iso_day(hold.get("entryDate"))
    if entry and as_of:
        held = _count_weekdays_inclusive(entry, as_of)
        due = _nth_weekday_inclusive(entry, SAT_BODY)
        due_s = due.isoformat() if due else None
        if held >= SAT_BODY:
            out.append(_note(
                nid=f"sat-exit:{market}:{symbol}",
                type="sat_exit",
                severity="high",
                title=f"卫星到期卖 · {name}",
                detail=f"{symbol} body3 第 {held} 个交易日 · 到期 {due_s or '今日'} 14:30卖",
                anchor="holdings",
                lane="trade",
                book="sat",
            ))
        elif held == SAT_BODY - 1:
            out.append(_note(
                nid=f"sat-soon:{market}:{symbol}",
                type="sat_expire_soon",
                severity="medium",
                title=f"卫星明日14:30卖 · {name}",
                detail=f"{symbol} 已持 {held}/{SAT_BODY} · 到期 {due_s}",
                anchor="holdings",
                lane="trade",
                book="sat",
            ))
    elif not entry:
        out.append(_note(
            nid=f"sat-entry:{market}:{symbol}",
            type="sat_missing_entry",
            severity="medium",
            title=f"卫星缺入场日 · {name}",
            detail=f"{symbol} 补录入场日才能算 body3 到期",
            anchor="holdings",
            lane="trade",
            book="sat",
        ))
    return out


def _s3_holding_alerts(market: str, hold: dict[str, Any]) -> list[dict[str, Any]]:
    """S-3 basket: EXIT / nearStop (price) / line updates / 60d expire. Not satellite."""
    symbol = str(hold.get("symbol") or "")
    name = str(hold.get("name") or symbol)
    out: list[dict[str, Any]] = []
    if hold.get("action") == "EXIT":
        out.append(_note(
            nid=f"exit:{market}:{symbol}",
            type="exit",
            severity="high",
            title=f"建议退出 · {name}",
            detail=f"{symbol} 触发 {hold.get('reason') or '退出规则'}（{market}）",
            anchor="holdings",
            lane="trade",
            book="s3",
        ))
        return out
    ops = hold.get("lineOps") or {}
    expire_date = ops.get("expireDate") or hold.get("expireDate")
    if "trail_up" in ops:
        prev_v, cur_v = ops["trail_up"]
        out.append(_note(
            nid=f"lineup:{market}:{symbol}:trail",
            type="line_update",
            severity="medium",
            title=f"需调单 · 移动线上调 · {name}",
            detail=(
                f"{symbol} 移动线 {prev_v} → {cur_v}（峰值上移）——"
                "请上调券商固定价条件单，或改用移动止损单"
            ),
            anchor="holdings",
            lane="trade",
            book="s3",
        ))
    if "stop_up" in ops:
        prev_v, cur_v = ops["stop_up"]
        out.append(_note(
            nid=f"lineup:{market}:{symbol}:stop",
            type="line_update",
            severity="medium",
            title=f"需调单 · 止损线上调 · {name}",
            detail=f"{symbol} 止损线 {prev_v} → {cur_v}（金字塔加仓）——请上调券商止损条件单",
            anchor="holdings",
            lane="trade",
            book="s3",
        ))
    if "expire_soon" in ops:
        out.append(_note(
            nid=f"expire:{market}:{symbol}",
            type="expire_soon",
            severity="medium",
            title=f"临近到期 · {name}",
            detail=(
                f"{symbol} 持有期剩 {ops['expire_soon']} 天"
                + (f"（到期 {expire_date}）" if expire_date else "")
                + "——券商条件单不会自动到期卖出，请记日历"
            ),
            anchor="holdings",
            lane="trade",
            book="s3",
        ))
    if hold.get("nearStop"):
        label = str(hold.get("nearStopLabel") or "止损")
        dist = hold.get("nearStopDistancePct")
        last = hold.get("lastClose") or hold.get("evaluatedPrice")
        line = hold.get("stopLossLine") if "止损" in label else hold.get("trailingLine")
        dist_s = f"{float(dist):.2f}%" if dist is not None else "—"
        out.append(_note(
            nid=f"near:{market}:{symbol}:{label}",
            type="near_line",
            severity="high" if dist is not None and float(dist) <= 0.5 else "medium",
            title=f"接近{label} · {name}",
            detail=f"{symbol} 距{label} {dist_s}（现价 {last} / 线 {line}）",
            anchor="holdings",
            lane="trade",
            book="s3",
        ))
    return out


def _stop_trail_alerts(mode: str = "single_track", ctx: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Holdings that need a broker action today. Book follows strategy mode + pick."""
    ctx = ctx or {"blocks": _anchor_blocks(), "pick": None, "tradeDate": None}
    pick = ctx.get("pick")
    as_of = _parse_iso_day(ctx.get("tradeDate")) or date.today()
    sat_ts = ctx.get("satTs") if isinstance(ctx.get("satTs"), set) else set()
    out: list[dict[str, Any]] = []
    rotate_n = 0
    for market, block in (ctx.get("blocks") or {}).items():
        for hold in block.get("holdings") or []:
            symbol = str(hold.get("symbol") or "")
            if not symbol:
                continue
            book = _holding_book(
                mode,
                pick if isinstance(pick, str) else None,
                market,
                symbol,
                sat_ts,
            )
            if book == "idle":
                continue
            if mode == "single_track" and pick not in (None, "STOCK") and market == "CN":
                rotate_n += 1
                continue
            if book == "sat":
                out.extend(_sat_holding_alerts(market=market, hold=hold, as_of=as_of))
            else:
                out.extend(_s3_holding_alerts(market, hold))
    if rotate_n > 0 and pick:
        out.insert(0, _note(
            nid=f"rotate:{pick}",
            type="rotate_out",
            severity="high",
            title=f"核心 pick={pick} · {rotate_n} 只股票应轮出",
            detail="单轨硬切：股票仓应减到 ETF / 逆回购，不要按 S-3 金字塔加仓",
            anchor="holdings",
            lane="trade",
            book="core",
        ))
    return out


def _recon_alerts() -> list[dict[str, Any]]:
    """Backtest vs paper missing positions (anchor: recon)."""
    from data_sync_service.db.reconciliation import latest_recon

    try:
        rows = latest_recon(limit=2)
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: recon failed: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if r.get("missing", 0) <= 0:
            continue
        market = "港股" if r.get("market") == "HK" else "A股"
        out.append(_note(
            nid=f"recon:{r.get('reconDate')}:{r.get('market')}",
            type="recon_missing",
            severity="low",
            title=f"回测口径 · {market}缺 {r.get('missing')} 只持仓",
            detail=f"回测应持 {r.get('expected')} · 实持 {r.get('actual')}（对账 {r.get('reconDate')}）——仅单轨 STOCK 日要对齐纸面仓",
            anchor="recon",
            lane="research",
            book="research",
        ))
    return out


def _cron_failures() -> list[dict[str, Any]]:
    """Today's trading-chain cron failures (anchor: scheduler)."""
    from data_sync_service.db.sync_job_record import list_recent_failures

    try:
        rows = list_recent_failures(hours=24)
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: cron failures failed: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        job = str(r.get("job_type") or "")
        if job not in TRADING_JOB_TYPES:
            continue
        out.append(_note(
            nid=f"cron:{job}:{r.get('sync_at')}",
            type="cron_failed",
            severity="high",
            title=f"任务失败 · {job}",
            detail=f"{job} 失败：{str(r.get('error_message') or '')[:160] or '未知错误'}",
            anchor="scheduler",
            lane="system",
            book="system",
            created_at=str(r.get("sync_at") or datetime.now(UTC).isoformat()),
        ))
    return out[:5]


def _rolling_oos_warning() -> list[dict[str, Any]]:
    """Rolling OOS window flagged (anchor: backtest)."""
    p = REPORTS_DIR / "rolling_oos_latest.json"
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not data.get("warning"):
        return []
    warnings = data.get("warnings") or []
    detail = " · ".join(str(w) for w in warnings[:3])
    return [_note(
        nid=f"oos:{data.get('windowEnd')}",
        type="oos_warning",
        severity="low",
        title="滚动 OOS 预警（近 90 天窗）",
        detail=f"{data.get('windowStart')} ~ {data.get('windowEnd')}：{detail}",
        anchor="backtest",
        lane="research",
        book="research",
    )]


def _pyramid_trigger_alerts(mode: str = "single_track", ctx: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """S-3 pyramid-add: leftover basket names only. Never on satellite."""
    ctx = ctx or {"blocks": _anchor_blocks(), "pick": None}
    pick = ctx.get("pick")
    if pick not in (None, "STOCK"):
        return []
    sat_ts = ctx.get("satTs") if isinstance(ctx.get("satTs"), set) else set()
    out: list[dict[str, Any]] = []
    for market, block in (ctx.get("blocks") or {}).items():
        for hold in block.get("holdings") or []:
            symbol = str(hold.get("symbol") or "")
            name = str(hold.get("name") or symbol)
            if not symbol:
                continue
            book = _holding_book(
                mode,
                pick if isinstance(pick, str) else None,
                market,
                symbol,
                sat_ts,
            )
            if book != "s3":
                continue
            if hold.get("pyramidAdded"):
                continue
            trigger = _as_float(hold.get("pyramidTriggerLine"))
            last_close = _as_float(hold.get("lastClose"))
            if trigger is None or last_close is None or trigger <= 0 or last_close < trigger:
                continue
            out.append(_note(
                nid=f"pyramid:{market}:{symbol}",
                type="pyramid_trigger",
                severity="high",
                title=f"金字塔加仓 · {name}",
                detail=(
                    f"{symbol} 收盘 {last_close} ≥ 触发线 {trigger}（成本 +2.5%）→ 建议加半仓（0.5x，每票至多 1 次）"
                ),
                anchor="holdings",
                lane="trade",
                book="s3",
            ))
    return out[:5]


def _third_asset_notification() -> list[dict[str, Any]]:
    """择强单轨 actionable hint (same source as Watchlist multiAssetSleeve)."""
    from data_sync_service.service.portfolio_health import build_portfolio_health

    try:
        h = build_portfolio_health(trade_date=None, markets=("CN", "HK"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications pick-strong failed: %s", exc)
        return []
    sleeve = h.get("multiAssetSleeve") or {}
    if not sleeve.get("active") or sleeve.get("action") in ("NONE", "DONT_BUY", None):
        return []
    severity = {
        "BUY": "medium",
        "BUY_513100": "medium",
        "ROTATE": "medium",
        "SELL_TO_A_SHARE": "high",
        "SELL_TO_REPO": "high",
    }.get(str(sleeve.get("action")), "medium")
    pick = (sleeve.get("pick") or {}).get("key") if isinstance(sleeve.get("pick"), dict) else None
    return [_note(
        nid=f"pick-strong:{pick or ''}:{sleeve.get('action')}",
        type="pick_strong",
        severity=severity,
        title=f"择强 · {sleeve.get('label') or sleeve.get('action')}",
        detail=str(sleeve.get("message") or ""),
        anchor="watchlist",
        lane="trade",
        book="core",
    )]


def _twin_star_snapshot_alert(mode: str = "single_track") -> list[dict[str, Any]]:
    """lane=system when today's 12:30 East Money snapshot is missing/stale."""
    if mode != "twin_star":
        return []
    try:
        from data_sync_service.service.twin_star_intraday import (
            intraday_snapshot_status,
            now_cn,
        )

        now = now_cn()
        if now.weekday() >= 5:
            return []
        status = intraday_snapshot_status(now=now)
        if status.get("ok"):
            return []
        reason = status.get("reason") or "snapshot unavailable"
        return [_note(
            nid=f"twin-star-snap:{status.get('session')}",
            type="twin_star_snapshot",
            severity="high",
            title="双子星 · 今日盘中快照失败",
            detail=(
                "东财 12:30 全市场快照不可用，卫星名单今日不可交易。"
                f"不要用 T-1 名单下单（{reason}）。"
            ),
            anchor="watchlist",
            lane="system",
            book="sat",
            created_at=now.isoformat(),
        )]
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications twin-star snapshot failed: %s", exc)
        return []


def _twin_star_notification(mode: str = "single_track") -> list[dict[str, Any]]:
    """双子星 14:30 前提醒 — only when the live strategy is twin_star."""
    if mode != "twin_star":
        return []
    from data_sync_service.service.twin_star_daily import (
        build_twin_star_reminder_payload,
        now_cn,
    )

    try:
        now = now_cn()
        if now.weekday() >= 5:
            return []
        payload = build_twin_star_reminder_payload(date.today())
        detail = payload.get("detail") or ""
        if not detail:
            return []
        sat = payload.get("sat") or {}
        gate = sat.get("gateOpen")
        severity = "medium" if gate else "low"
        return [_note(
            nid=f"twin-star:{now.date().isoformat()}",
            type="twin_star",
            severity=severity,
            title=payload.get("title") or "双子星 · 14:30 前操作提醒",
            detail=detail,
            anchor="watchlist",
            lane="trade",
            book="sat",
            created_at=now.isoformat(),
        )]
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications twin-star failed: %s", exc)
        return []


def build_notifications(mode: str = "twin_star") -> list[dict[str, Any]]:
    """All actionable notifications, most severe first.

    ``mode`` is the live Settings strategy (``twin_star`` | ``single_track``).
    Default is twin-star (clip4). Twin-star CN holdings use S-gap body=3 close
    only (no protect stop); S-3 pyramid/trail and paper-vs-backtest recon stay
    on the single-track book.
    """
    live_mode = "twin_star" if mode == "twin_star" else "single_track"
    ctx = _load_health_ctx()
    items = (
        _stop_trail_alerts(live_mode, ctx)
        + _pyramid_trigger_alerts(live_mode, ctx)
        + _cron_failures()
        + (_recon_alerts() if live_mode == "single_track" else [])
        + _rolling_oos_warning()
        + _third_asset_notification()
        + _twin_star_notification(live_mode)
        + _twin_star_snapshot_alert(live_mode)
    )
    order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda x: order.get(str(x.get("severity")), 2))
    return items
