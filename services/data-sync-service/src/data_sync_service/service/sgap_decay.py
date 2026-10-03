"""S-gap decay series (Karios H2k display layer, read-only).

Computes the time-dimension S-gap edge-decay dataset for the backtest page
"S-gap 失效趋势" tab. Pure functions only: no DB, no network, no clock,
no broker/order calls.

Source: per-trade S-gap trades (h2f_trades.json: entry/exit/contrib/large_pct/
amt_w/circ/win, sorted by entryDate). contrib is the satellite NAV contribution
in points (0.25 * (trade_ret - 32.28bp) * 100); summing it gives the satellite
total, so all rolling means/sums below are in the same satellite-point unit.

Method (frozen, matches H2k_sgap_revival.md where stated):
- Rolling windows are trade-count windows over entry-sorted trades:
  N=40 (main) and N=60. mean = window contrib mean (per-trade), sum = window
  contrib sum, win_rate = fraction with contrib > 0, t = mean / (sd / sqrt(N))
  with sample sd (ddof=1); sd <= 0 or non-finite => t = 0.0.
- Percentile vs random: H2k Sec 3 replay approximation using the F long R1
  calibration (same-day random baseline exists in scratch as f_placebo_*.json,
  but per-window random draws are not stored, so the frozen normal
  approximation is reused): mu/trade +0.0178, sigma/trade 1.2175
  (f_placebo_long.json R1 mean 19.15 / p95 84.91 over 1078 fills =>
  sigma_total 39.98). Window sum z = (sum - N*mu) / (sigma*sqrt(N)),
  percentile = Phi(z) * 100. Live paper uses 500 same-day random draws for the
  exact percentile; thresholds X below are frozen so the +/-10pt replay error
  (H2k Sec 3) does not change the display rule.
- Revival-rule state (K2 middle rule, H2k Sec 4): N=40, X=75 (ON), OFF when the
  same N window percentile < 50 (fixed hysteresis, not a free param), t >= 0
  fixed buffer, two-step 0% -> 10% -> 20% (H2j A25 20% cap). Display replay:
  ON needs 2 consecutive windows with percentile >= 75 and t >= 0 (double
  confirmation against single-window spikes, fixed); step-ups need >= 20
  trades since the last change; step-downs are immediate one level per window
  below 50 (20 -> 10 -> 0, no jump). Paper-20 + monthly-execution delays from
  H2k Sec 4 are omitted in this replay display (paper ~4/20 unmet would pin
  the live line at 0%); the live prerequisite still holds satellite at 0%.
- Crowding: rolling 40-trade medians of amt_w (turnover, 万元) and circ
  (circ market cap, 万元) over non-null values.
- Large-order edge: within each rolling 40-trade window, split trades with
  non-null large_pct at the window median; edge = high-group mean contrib
  minus low-group mean contrib. Null when fewer than 4 valid trades or when
  one side is empty. Same-day ex post attribution only (not tradable).
- Equity: cumulative sum of contrib in entry order (trade-space, not
  calendar-compounded); drawdown = cum - running peak (<= 0).
- Monthly: group by entry YYYY-MM: count + contrib sum.
- Window shading (frozen H2k cut): OOS2 2024-08-01~2025-08-07 /
  train 2025-08-08~2026-02-27 / valid 2026-03-02~2026-08-07 /
  holdout 2026-08-10~2026-09-30.
"""

from __future__ import annotations

import math
from typing import Any

# Frozen H2k cut for background shading (date strings, inclusive).
WINDOWS: dict[str, dict[str, str]] = {
    "OOS2": {"start": "2024-08-01", "end": "2025-08-07"},
    "train": {"start": "2025-08-08", "end": "2026-02-27"},
    "valid": {"start": "2026-03-02", "end": "2026-08-07"},
    "holdout": {"start": "2026-08-10", "end": "2026-09-30"},
}

# Frozen F long R1 calibration (H2k Sec 3).
PLACEBO_MU_PER_TRADE: float = 0.0178
PLACEBO_SIGMA_PER_TRADE: float = 1.2175

# Frozen K2 revival params (H2k Sec 3-4).
REVIVAL_N: int = 40
REVIVAL_X: float = 75.0
REVIVAL_OFF: float = 50.0
REVIVAL_STEP_GAP: int = 20

SOURCE_LABEL = "h2f_trades.json (1130 S-gap trades, 2021-08-09..2026-09-23)"


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def percentile_of_sum(window_sum: float, n: int) -> float:
    """Normal-approx percentile of a window sum vs the F long R1 baseline."""
    if n <= 0:
        return 50.0
    mu = n * PLACEBO_MU_PER_TRADE
    sigma = PLACEBO_SIGMA_PER_TRADE * math.sqrt(n)
    if sigma <= 0:
        return 50.0
    z = (window_sum - mu) / sigma
    # Clamp z so erf stays finite; 8 sigma already rounds to 0/100.
    z = max(-8.0, min(8.0, z))
    return _phi(z) * 100.0


def _mean(xs: list[float]) -> float | None:
    if not xs:
        return None
    return sum(xs) / len(xs)


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _t_stat(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    if not math.isfinite(var) or var <= 0:
        return 0.0
    sd = math.sqrt(var)
    return m / (sd / math.sqrt(n))


def _norm_trade(raw: dict[str, Any]) -> dict[str, Any] | None:
    entry = raw.get("entry") or raw.get("entryDate")
    if not entry:
        return None
    try:
        contrib = float(raw["contrib"] if "contrib" in raw else raw["contribPct"])
    except (KeyError, TypeError, ValueError):
        return None
    if not math.isfinite(contrib):
        return None

    def _opt(key: str) -> float | None:
        v = raw.get(key)
        if v is None:
            return None
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return f if math.isfinite(f) else None

    return {
        "entry": str(entry)[:10],
        "contrib": contrib,
        "large_pct": _opt("large_pct"),
        "amt_w": _opt("amt_w"),
        "circ": _opt("circ"),
    }


def compute_sgap_decay(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the full sgap-decay payload from raw per-trade rows."""
    normed = [_norm_trade(t) for t in trades]
    rows = sorted(
        (t for t in normed if t is not None),
        # Stable by entry date only: same-date trades keep file (blotter)
        # order so rolling/equity windows match the H2k replay.
        key=lambda t: t["entry"],
    )
    n = len(rows)
    contribs = [r["contrib"] for r in rows]
    entries = [r["entry"] for r in rows]

    rolling: list[dict[str, Any]] = []
    equity: list[dict[str, Any]] = []
    cum = 0.0
    peak = float("-inf")

    # Per-window revival inputs first (percentile + t for N=40).
    win40_stats: list[dict[str, Any] | None] = [None] * n
    for i in range(n):
        if i + 1 >= 40:
            w = contribs[i - 39 : i + 1]
            s = sum(w)
            win40_stats[i] = {
                "mean": s / 40,
                "sum": s,
                "win_rate": sum(1 for x in w if x > 0) / 40,
                "t": _t_stat(w),
                "percentile": percentile_of_sum(s, 40),
            }

    # K2 two-step state machine over the N=40 windows (display replay).
    revival_state: list[int | None] = [None] * n
    pos = 0
    last_change = -(10**9)
    prev_ok = False
    for i in range(n):
        st = win40_stats[i]
        if st is None:
            revival_state[i] = None
            prev_ok = False
            continue
        ok = st["percentile"] >= REVIVAL_X and st["t"] >= 0
        if st["percentile"] < REVIVAL_OFF:
            # Step down one level immediately (no jump 20 -> 0).
            if pos == 20:
                pos = 10
                last_change = i
            elif pos == 10:
                pos = 0
                last_change = i
            prev_ok = False
        elif ok and prev_ok:
            if pos == 0 and (i - last_change) >= REVIVAL_STEP_GAP:
                pos = 10
                last_change = i
            elif pos == 10 and (i - last_change) >= REVIVAL_STEP_GAP:
                pos = 20
                last_change = i
        prev_ok = ok
        revival_state[i] = pos

    for i in range(n):
        # N=40 window.
        r40: dict[str, Any] | None = None
        if i + 1 >= 40:
            w = contribs[i - 39 : i + 1]
            s = sum(w)
            r40 = {
                "mean": s / 40,
                "sum": s,
                "win_rate": sum(1 for x in w if x > 0) / 40,
                "t": _t_stat(w),
                "percentile": percentile_of_sum(s, 40),
            }
        # N=60 window.
        r60: dict[str, Any] | None = None
        if i + 1 >= 60:
            w60 = contribs[i - 59 : i + 1]
            s60 = sum(w60)
            r60 = {
                "mean": s60 / 60,
                "sum": s60,
                "win_rate": sum(1 for x in w60 if x > 0) / 60,
                "t": _t_stat(w60),
                "percentile": percentile_of_sum(s60, 60),
            }
        # Crowding medians (rolling 40).
        crowd_amt: float | None = None
        crowd_circ: float | None = None
        if i + 1 >= 40:
            amts = [rows[j]["amt_w"] for j in range(i - 39, i + 1) if rows[j]["amt_w"] is not None]
            circs = [rows[j]["circ"] for j in range(i - 39, i + 1) if rows[j]["circ"] is not None]
            crowd_amt = _median(amts)
            crowd_circ = _median(circs)
        # Large-order edge (rolling 40, window-median split).
        edge: float | None = None
        if i + 1 >= 40:
            pairs = [
                (rows[j]["large_pct"], contribs[j])
                for j in range(i - 39, i + 1)
                if rows[j]["large_pct"] is not None
            ]
            if len(pairs) >= 4:
                med = _median([p[0] for p in pairs])
                assert med is not None
                hi = [c for lp, c in pairs if lp >= med]
                lo = [c for lp, c in pairs if lp < med]
                if hi and lo:
                    edge = sum(hi) / len(hi) - sum(lo) / len(lo)
        rolling.append(
            {
                "i": i,
                "date": entries[i],
                "r40": r40,
                "r60": r60,
                "crowd_amt_w": crowd_amt,
                "crowd_circ": crowd_circ,
                "large_edge": edge,
                "revival_pos": revival_state[i],
            }
        )
        # Equity (trade-space).
        cum += contribs[i]
        peak = max(peak, cum)
        equity.append(
            {
                "i": i,
                "date": entries[i],
                "cum": cum,
                "drawdown": cum - peak,
            }
        )

    # Monthly signal counts.
    monthly_map: dict[str, dict[str, Any]] = {}
    for r in rows:
        m = r["entry"][:7]
        d = monthly_map.setdefault(m, {"month": m, "count": 0, "sum": 0.0})
        d["count"] += 1
        d["sum"] += r["contrib"]
    monthly = [monthly_map[k] for k in sorted(monthly_map)]

    # Summary (computed, frontend renders verbatim).
    last40 = next((rolling[i]["r40"] for i in range(n - 1, -1, -1) if rolling[i]["r40"]), None)
    last_state = next(
        (
            rolling[i]["revival_pos"]
            for i in range(n - 1, -1, -1)
            if rolling[i]["revival_pos"] is not None
        ),
        0,
    )
    max_dd = min((e["drawdown"] for e in equity), default=0.0)
    summary = {
        "n_trades": n,
        "date_start": entries[0] if entries else None,
        "date_end": entries[-1] if entries else None,
        "cum_total": cum,
        "max_drawdown": max_dd,
        "latest_r40_mean": (last40 or {}).get("mean"),
        "latest_r40_win_rate": (last40 or {}).get("win_rate"),
        "latest_r40_percentile": (last40 or {}).get("percentile"),
        "latest_r40_t": (last40 or {}).get("t"),
        "revival_pos": last_state,
    }

    return {
        "meta": {
            "source": SOURCE_LABEL,
            "n_trades": n,
            "windows": WINDOWS,
            "placebo": {
                "mu_per_trade": PLACEBO_MU_PER_TRADE,
                "sigma_per_trade": PLACEBO_SIGMA_PER_TRADE,
                "note": "F long R1 normal approx; live uses 500 same-day draws",
            },
            "revival": {
                "rule": "K2",
                "N": REVIVAL_N,
                "X": REVIVAL_X,
                "off_below": REVIVAL_OFF,
                "steps": [0, 10, 20],
                "note": "replay display; paper-20 + monthly delays omitted, live stays 0%",
            },
            "cost": "contrib net of 32.28bp/trade satellite cost",
        },
        "summary": summary,
        "rolling": rolling,
        "monthly": monthly,
        "equity": equity,
    }
