"""Portfolio NAV simulator: S-3 line returns + third-asset sleeve on idle cash.

T6 design (docs/designs/third-asset-sleeve.md §2 / §3.2):
  - idle cash (1 - deployed%) earns the sleeve asset's daily return
  - sleeve holding: 513100 close > MA200 -> hold ETF; break MA200 OR trail -8%
    from peak -> GC001 repo (2026-08-28 trail8)
  - baseline = idle cash earns 0%

2026-08-29 audit fix (P0-1): when ``engine_nav_by_day`` is supplied (from
``BacktestRun.nav_curve``), the baseline NAV is the engine cash+MTM curve
byte-for-byte — NOT fixed cost-basis weight × daily stock returns (that
method systematically overstated rising books). Sleeve overlay adds
``idle_pct * sleeve_asset_ret`` on top of the engine daily return.
"""

from __future__ import annotations

from typing import Any

MA_WINDOW = 200
TRAILING_PCT = 8.0  # peak -8% hard cut to GC001
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


def engine_nav_by_day_from_run(calendar: list[str], nav_curve: list[float]) -> dict[str, float]:
    """Map trading calendar -> engine NAV.

    Uses one point per calendar day; if the engine appended a terminal
    post-liquidation point (len = len(calendar)+1), bind it to the last
    calendar day so ``totalBasePct`` matches ``total_net_pnl_pct``.
    """
    n = min(len(calendar), len(nav_curve))
    out = {calendar[i]: float(nav_curve[i]) for i in range(n)}
    if calendar and len(nav_curve) > len(calendar):
        out[calendar[-1]] = float(nav_curve[-1])
    return out


def simulate_sleeve_nav(
    *,
    positions_by_day: list[dict],
    close_by_ts_day: dict[str, dict[str, float]],
    calendar: list[str],
    etf_close_by_day: dict[str, float],
    repo_rate_by_day: dict[str, float],
    min_idle_pct: float = 0.0,
    engine_nav_by_day: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Replay portfolio NAV with the sleeve on idle cash (trail8 + MA200).

    Prefer ``engine_nav_by_day`` from ``BacktestRun.nav_curve`` so baseline
    matches S-3 ``total_net_pnl_pct``. Without it, falls back to entry-price
    MTM reconstruction when snapshots carry ``entry_price``; otherwise uses
    cost-basis daily returns (unit-test / legacy path only).
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
    use_engine = bool(engine_nav_by_day)

    holding = False
    etf_peak = 0.0
    nav_base = 1.0
    nav_sleeve = 1.0
    prev_engine = 1.0
    rows: list[dict[str, Any]] = []
    base_peak = 1.0
    sleeve_peak = 1.0
    max_dd_base = 0.0
    max_dd_sleeve = 0.0
    hold_days = 0
    idle_days = 0

    for day in calendar:
        snap = snap_by_day.get(day)
        deployed_pct = 0.0
        deployed_ret = 0.0
        if snap:
            for pos in snap.get("positions") or []:
                try:
                    pct = float(pos.get("position_pct") or 0.0)
                except (TypeError, ValueError):
                    continue
                if pct <= 0:
                    continue
                entry = str(pos.get("entry_date") or "")
                # Entry day: capital deployed but pnl starts next session.
                if entry and day <= entry:
                    deployed_pct += pct
                    continue
                deployed_pct += pct
                if use_engine:
                    continue
                closes = close_by_ts_day.get(str(pos.get("ts_code") or "")) or {}
                today = closes.get(day)
                idx = day_idx.get(day)
                ep = pos.get("entry_price")
                try:
                    entry_px = float(ep) if ep is not None else 0.0
                except (TypeError, ValueError):
                    entry_px = 0.0
                if entry_px > 0 and today is not None:
                    prev_close = closes.get(calendar[idx - 1]) if idx and idx > 0 else None
                    if prev_close is not None and prev_close > 0:
                        deployed_ret += pct * (today - prev_close) / entry_px
                    elif today > 0:
                        deployed_ret += pct * (today / entry_px - 1.0)
                else:
                    prev = closes.get(calendar[idx - 1]) if idx and idx > 0 else None
                    if today is not None and prev:
                        deployed_ret += pct * (today / prev - 1.0)

        deployed_pct = min(1.0, deployed_pct)
        idle_pct = max(0.0, 1.0 - deployed_pct)

        close = etf_close_by_day.get(day)
        ma = ma200_by_day.get(day)
        above = close is not None and ma is not None and close >= ma

        if holding:
            should_exit = False
            if not above:
                should_exit = True
            elif close is not None and etf_peak > 0 and close < etf_peak * (1 - TRAILING_PCT / 100):
                should_exit = True
            if should_exit:
                holding = False
                etf_peak = 0.0
            elif close is not None and close > etf_peak:
                etf_peak = close
        if not holding and above and idle_pct * 100 >= min_idle_pct:
            holding = True
            etf_peak = close if close is not None else 0.0

        sleeve_ret = 0.0
        if idle_pct > 0:
            if holding:
                sleeve_ret = etf_ret.get(day, 0.0)
                hold_days += 1
            else:
                sleeve_ret = repo_ret.get(day, 0.0)
            idle_days += 1

        if use_engine:
            eng = float((engine_nav_by_day or {}).get(day, prev_engine))
            r_eng = (eng / prev_engine - 1.0) if prev_engine > 0 else 0.0
            nav_base = eng
            nav_sleeve *= 1.0 + r_eng + idle_pct * sleeve_ret
            prev_engine = eng
        else:
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
            "engineNav": bool(use_engine),
        },
    }
