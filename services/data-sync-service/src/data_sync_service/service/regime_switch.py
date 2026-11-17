"""2-state regime target for the Watchlist page (read-only, no orders).

Rule (frozen 2026-11-15):
  - month-end 000300 close > MA200 -> ``starship_robust`` (H2-a25)
  - otherwise -> ``starport`` (w=0.2)

The rule is confirmed at the last **completed** month-end, not intraday. This
keeps the watchlist auto label aligned with the backtest/paper rule and avoids
whipsaw from a mid-month MA200 cross.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Any

from data_sync_service.db.index_daily import fetch_last_closes_upto
from data_sync_service.db.trade_calendar import last_trading_day

INDEX_CODE = "000300.SH"
EXCHANGE = "SSE"
MA_WINDOW = 200
RULE = "月末确认：000300 > MA200 → 稳健星舰 H2-a25；否则 → 星港 0.2"


def _month_end_trading_day(day: date) -> date:
    last_calendar_day = date(day.year, day.month, calendar.monthrange(day.year, day.month)[1])
    return last_trading_day(EXCHANGE, last_calendar_day)


def _previous_month(day: date) -> date:
    return day.replace(day=1) - timedelta(days=1)


def get_regime_target(as_of_date: str | None = None) -> dict[str, Any]:
    """Return the confirmed 2-state regime target for the watchlist.

    ``as_of_date`` is inclusive (YYYY-MM-DD). Missing/invalid dates fall back to
    today. The latest *completed* month-end is used; a mid-month as-of keeps the
    previous month's confirmation.
    """
    try:
        as_of = date.fromisoformat(as_of_date) if as_of_date else date.today()
    except ValueError:
        as_of = date.today()

    rows = fetch_last_closes_upto(INDEX_CODE, as_of.isoformat(), days=400)
    if len(rows) < MA_WINDOW:
        return {
            "ok": False,
            "asOf": as_of.isoformat(),
            "error": f"000300 history {len(rows)} bars < {MA_WINDOW}",
            "rule": RULE,
        }

    dates = [date.fromisoformat(str(d)) for d, _ in rows]
    closes = [float(c) for _, c in rows]
    latest = dates[-1]

    current_month_end = _month_end_trading_day(latest)
    if latest < current_month_end:
        # Current month not finished: use the previous month's last bar.
        prev = _previous_month(latest)
        candidates = [i for i, d in enumerate(dates) if (d.year, d.month) == (prev.year, prev.month)]
        if not candidates:
            return {
                "ok": False,
                "asOf": as_of.isoformat(),
                "error": "no previous month-end bar",
                "rule": RULE,
            }
        idx = candidates[-1]
    else:
        idx = len(dates) - 1

    if idx < MA_WINDOW - 1:
        return {
            "ok": False,
            "asOf": as_of.isoformat(),
            "error": f"not enough bars before confirmed month-end ({idx + 1})",
            "rule": RULE,
        }

    confirmed_at = dates[idx]
    close = closes[idx]
    ma200 = sum(closes[idx - MA_WINDOW + 1 : idx + 1]) / MA_WINDOW
    state = "bull" if close > ma200 else "bear"
    target = "starship_robust" if state == "bull" else "starport"
    target_label = "稳健星舰 H2-a25" if state == "bull" else "星港 0.2"
    sgap_exposure = "high" if state == "bull" else "low"

    # Next confirmation = this month's last trading day, or next month's if
    # today's month-end has already been confirmed.
    next_review = current_month_end
    if as_of >= current_month_end:
        next_review = _month_end_trading_day(as_of.replace(day=28) + timedelta(days=4))

    return {
        "ok": True,
        "asOf": as_of.isoformat(),
        "confirmedAt": confirmed_at.isoformat(),
        "close": round(close, 2),
        "ma200": round(ma200, 2),
        "distancePct": round((close / ma200 - 1.0) * 100.0, 2),
        "state": state,
        "stateLabel": "牛市 / risk-on" if state == "bull" else "熊市 / risk-off",
        "target": target,
        "targetLabel": target_label,
        "sgapExposure": sgap_exposure,
        "nextReview": next_review.isoformat(),
        "rule": RULE,
        "note": (
            "自动跟随为只读标签；手动策略选择在 Watchlist 页已关闭。"
            + (" S-gap 为进攻引擎，拥挤/衰减时卫星腿收益打折。" if state == "bull" else "")
        ),
    }
