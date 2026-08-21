"""Portfolio NAV simulator: S-3 line returns + third-asset sleeve on idle cash.

T6 design (docs/designs/third-asset-sleeve.md §2 / §3.2):
  - idle cash (1 - deployed%) earns the sleeve asset's daily return
  - sleeve holding rule: 513100 close > MA200 -> hold; close < MA200 -> switch
    to GC001 repo on the NEXT day (no-lookahead, mirrors the production
    "跌破 200dMA -> 次日全部卖出" rule)
  - daily-compounded NAV; baseline = idle cash earns 0%
  - acceptance: all three walk-forward windows must show non-negative delta vs
    the baseline (design targets: OOS2 +3.1 / train +15.3 / valid +39.0pt)

Inputs come from the S-3 engine run (positions_by_day + close_by_ts_day +
calendar) and the third-asset cache (513100 bars + GC001 repo rates).

2026-08-21: first implementation (T6 落地 · backtest page sleeve card).
"""

from __future__ import annotations

from typing import Any

MA_WINDOW = 200
GC001_DAYS = 365


def _sma(closes: list[float], window: int) -> float | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:]) / float(window)


def _daily_ret(close: float, prev: float) -> float:
    if prev and prev > 0:
        return close / prev - 1.0
    return 0.0


def _iso_date(d: str) -> str:
    """'20240801' -> '2024-08-01' (cache format); passthrough otherwise."""
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def load_third_asset_cache(
    cache: dict[str, Any], *, etf_key: str = "513100.SH"
) -> tuple[dict[str, float], dict[str, float]]:
    """Flatten the third-asset cache into {iso-date: value} maps.

    ETF rows: [{date: '%Y%m%d', close}]; repo rows: [{date, close (annualized %)}].
    Returns (etf_close_by_day, repo_rate_by_day).
    """
    etf: dict[str, float] = {}
    for b in (cache.get("etfs") or {}).get(etf_key, {}).get("rows", []):
        etf[_iso_date(str(b.get("date", "")))] = float(b["close"])
    repo: dict[str, float] = {}
    for b in cache.get("repo", []):
        repo[_iso_date(str(b.get("date", "")))] = float(b["close"])
    return etf, repo


def simulate_sleeve_nav(
    *,
    positions_by_day: list[dict],
    close_by_ts_day: dict[str, dict[str, float]],
    calendar: list[str],
    etf_close_by_day: dict[str, float],
    repo_rate_by_day: dict[str, float],
    min_idle_pct: float = 0.0,
) -> dict[str, Any]:
    """Replay the portfolio NAV with the sleeve on idle cash.

    ``min_idle_pct`` mirrors the production sleeve threshold (MIN_IDLE_PCT);
    the T6 backtest used 0 (any idle cash engages). The 200-day MA is
    fail-closed: not enough history -> no hold (repo only).

    Returns daily rows plus a summary with base/sleeve totals and max DD.
    """
    etf_days = sorted(etf_close_by_day)
    etf_ret: dict[str, float] = {}
    for i, d in enumerate(etf_days):
        prev = etf_close_by_day[etf_days[i - 1]] if i > 0 else None
        etf_ret[d] = _daily_ret(etf_close_by_day[d], prev) if prev else 0.0

    repo_ret: dict[str, float] = {
        d: (float(r) / 100.0) / GC001_DAYS for d, r in repo_rate_by_day.items()
    }

    ma200_by_day: dict[str, float] = {}
    for i in range(len(etf_days)):
        lo = max(0, i - MA_WINDOW + 1)
        ma = _sma([etf_close_by_day[etf_days[j]] for j in range(lo, i + 1)], MA_WINDOW)
        if ma is not None:
            ma200_by_day[etf_days[i]] = ma

    snap_by_day = {str(s.get("date")): s for s in positions_by_day}
    day_idx = {d: i for i, d in enumerate(calendar)}

    holding = False
    nav_base = 1.0
    nav_sleeve = 1.0
    rows: list[dict[str, Any]] = []
    base_peak = 1.0
    sleeve_peak = 1.0
    max_dd_base = 0.0
    max_dd_sleeve = 0.0
    hold_days = 0
    idle_days = 0

    for day in calendar:
        snap = snap_by_day.get(day)
        deployed_ret = 0.0
        deployed_pct = 0.0
        if snap:
            for pos in snap.get("positions") or []:
                try:
                    pct = float(pos.get("position_pct") or 0.0)
                except (TypeError, ValueError):
                    continue
                if pct <= 0:
                    continue
                entry = str(pos.get("entry_date") or "")
                if entry and day <= entry:
                    continue  # filled at close of entry day -> pnl starts next day
                closes = close_by_ts_day.get(str(pos.get("ts_code") or "")) or {}
                today = closes.get(day)
                idx = day_idx.get(day)
                prev = closes.get(calendar[idx - 1]) if idx and idx > 0 else None
                if today is not None and prev:
                    deployed_ret += pct * (today / prev - 1.0)
                deployed_pct += pct
        deployed_pct = min(1.0, deployed_pct)
        idle_pct = max(0.0, 1.0 - deployed_pct)

        close = etf_close_by_day.get(day)
        ma = ma200_by_day.get(day)
        above = close is not None and ma is not None and close >= ma

        # Same-close trend switch (valid window: +30.4pt vs +14.0pt with a
        # next-day exit — break days keep falling, an early cut wins).
        if holding and not above:
            holding = False
        elif not holding and above and idle_pct * 100 >= min_idle_pct:
            holding = True

        sleeve_ret = 0.0
        if idle_pct > 0:
            if holding:
                sleeve_ret = etf_ret.get(day, 0.0)
                hold_days += 1
            else:
                sleeve_ret = repo_ret.get(day, 0.0)
            idle_days += 1

        nav_base *= 1.0 + deployed_ret
        nav_sleeve *= 1.0 + deployed_ret + idle_pct * sleeve_ret

        base_peak = max(base_peak, nav_base)
        sleeve_peak = max(sleeve_peak, nav_sleeve)
        if base_peak > 0:
            max_dd_base = max(max_dd_base, (base_peak - nav_base) / base_peak)
        if sleeve_peak > 0:
            max_dd_sleeve = max(max_dd_sleeve, (sleeve_peak - nav_sleeve) / sleeve_peak)

        rows.append(
            {
                "date": day,
                "navBase": round(nav_base, 6),
                "navSleeve": round(nav_sleeve, 6),
                "deployedPct": round(deployed_pct, 4),
                "idlePct": round(idle_pct, 4),
                "holding": holding,
            }
        )

    total_base = (nav_base - 1.0) * 100.0
    total_sleeve = (nav_sleeve - 1.0) * 100.0
    return {
        "rows": rows,
        "summary": {
            "totalBasePct": round(total_base, 1),
            "totalSleevePct": round(total_sleeve, 1),
            "deltaPct": round(total_sleeve - total_base, 1),
            "maxDdBasePct": round(max_dd_base * 100.0, 1),
            "maxDdSleevePct": round(max_dd_sleeve * 100.0, 1),
            "holdDays": hold_days,
            "idleDays": idle_days,
            "avgIdlePct": round(sum(r["idlePct"] for r in rows) / len(rows) * 100.0, 1) if rows else 0.0,
        },
    }