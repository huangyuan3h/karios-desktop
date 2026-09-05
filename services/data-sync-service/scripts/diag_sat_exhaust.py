#!/usr/bin/env python3
"""E-veto diagnostic battery: are exhausted gap candidates worse?

Selection windows OOS2+train; valid NOT touched (pre-reg §3.5).
Same eligible set as S4 (gate + skip_t1 + C1 3% upside cap).
Forward 3-day net (1430 -> day-3 1430, minus COSTS_ROUNDTRIP).

Exhausted = strong_scoop_exhaustion at T-1 (fully known at 14:30 D):
  ret60 > 0.40 & vol_ratio > 1.2, plus scoop shape gates copied verbatim
  from service/factor_signals_service.py (MA20>MA60, depth 5-18%,
  bottom within 15d, close >= bottom*1.03 and >= MA20*0.99, t>=60).
Detection point t = D-1 close (entry day D's prior close), zero lookahead.

Rule: BOTH windows exhausted-mean < clean-mean AND mechanism holds,
else REJECT without walk-forward. Read-only vs Postgres. Saves nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    COSTS_ROUNDTRIP,
    R_WIDE_THRESHOLD,
    _cached_day_features,
    _intraday_px,
    _same_1430_skip_reason,
    load_sgap_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}


def _load_vol_map(start: str, end: str) -> dict[tuple[str, str], float]:
    """(ts_code, date) -> vol. state_bucket context carries amount only;
    the exhaustion detector needs vol exactly as factor_signals_service."""
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[tuple[str, str], float] = {}
    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT ts_code, trade_date, vol FROM daily "
        "WHERE trade_date >= %s AND trade_date <= %s",
        (start, end),
    )
    for ts, d, v in cur.fetchall():
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out[(str(ts), ds)] = float(v) if v else 0.0
    conn.close()
    return out


def _is_exhausted(
    series: list[dict],
    di_d: int,
    volmap: dict[tuple[str, str], float],
    ts: str,
) -> bool:
    """T-1 exhaustion flag for entry day index di_d. False on any missing data."""
    t = di_d - 1
    if t < 60 or t >= len(series):
        return False
    try:
        closes = np.array([float(r.get("close") or 0) for r in series], dtype=float)
        highs = np.array([float(r.get("high") or 0) for r in series], dtype=float)
        lows = np.array([float(r.get("low") or 0) for r in series], dtype=float)
        dates = [r.get("date") for r in series]
        vols = np.array([volmap.get((ts, dd), 0.0) for dd in dates], dtype=float)
    except Exception:
        return False
    if np.any(closes[t - 60:t + 1] <= 0):
        return False
    # moving averages at t (simple, matches service logic directionally)
    if t < 89:
        return False
    ma20 = float(np.mean(closes[t - 19:t + 1]))
    ma60 = float(np.mean(closes[t - 59:t + 1]))
    ma60_m30 = float(np.mean(closes[t - 89:t - 29]))
    if not (ma20 > ma60 and closes[t - 30] > ma60_m30):
        return False
    ph = float(np.max(highs[t - 40:t - 20])) if t - 40 >= 0 else 0.0
    if ph <= 0:
        return False
    window = lows[t - 20:t + 1]
    if np.any(window <= 0):
        return False
    bottom = float(np.min(window))
    bi_rel = int(np.argmin(window))
    bi = t - 20 + bi_rel
    depth = (ph - bottom) / ph
    if not (0.05 <= depth <= 0.18):
        return False
    if not (closes[t] >= bottom * 1.03 and closes[t] >= ma20 * 0.99):
        return False
    if bi < t - 15:
        return False
    scoop_vol = float(np.mean(vols[t - 20:t + 1]))
    if scoop_vol <= 0 or vols[t] <= 0:
        return False
    vr = float(vols[t] / scoop_vol)
    ret60 = float(closes[t] / closes[t - 60] - 1)
    return bool(ret60 > 0.40 and vr > 1.2)


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    print(f"cal days: {len(cal)} names: {len(per_ts)}", flush=True)
    print("loading vol map ...", flush=True)
    volmap = _load_vol_map("2024-04-01", "2026-02-01")
    print(f"vol entries: {len(volmap)}", flush=True)

    acc: dict[str, dict[str, list[float]]] = {
        w: {"exhausted": [], "clean": []} for w in WINDOWS
    }
    n_cand = {w: 0 for w in WINDOWS}
    n_exh = {w: 0 for w in WINDOWS}
    for w, (s, e) in WINDOWS.items():
        for day in cal:
            if day <= s or day > e:
                continue
            ei = idx_by_day.get(day, -1)
            if ei < 0 or ei + 2 >= len(cal):
                continue
            exit_day = cal[ei + 2]
            feat_all, breadth = _cached_day_features(ctx, day)
            if breadth <= R_WIDE_THRESHOLD:
                continue
            for ts, d in feat_all.items():
                if not d.get("is_gap"):
                    continue
                di = date_idx.get(ts, {}).get(day, -1)
                series = per_ts.get(ts)
                if di < 0 or not series:
                    continue
                bar = series[di]
                px = _intraday_px(ctx, ts, day, "1430")
                reason = _same_1430_skip_reason(
                    ts=ts, px=px, open_px=bar.get("open"), pre_close=bar.get("pre_close"),
                    skip_t1_limit=True, max_open_to_1430_pct=0.03, near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                if not bar.get("open") or not bar.get("pre_close"):
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                n_cand[w] += 1
                if _is_exhausted(series, di, volmap, ts):
                    acc[w]["exhausted"].append(fwd)
                    n_exh[w] += 1
                else:
                    acc[w]["clean"].append(fwd)

    print("\n## E-veto diagnostic (eligible gap fills split by T-1 exhaustion)")
    print("| window | exhausted mean/hit/n | clean mean/hit/n | diff (exh-clean) | exh share |")
    print("|------|------------------------|------------------|------------------|-----------|")
    ok_both = True
    for w in WINDOWS:
        exh = acc[w]["exhausted"]
        cln = acc[w]["clean"]
        me = float(np.mean(exh)) * 100 if exh else 0.0
        mc = float(np.mean(cln)) * 100 if cln else 0.0
        he = float(np.mean([1.0 if x > 0 else 0.0 for x in exh])) * 100 if exh else 0.0
        hc = float(np.mean([1.0 if x > 0 else 0.0 for x in cln])) * 100 if cln else 0.0
        diff = me - mc
        share = (len(exh) / max(1, len(exh) + len(cln))) * 100
        print(f"| {w} | {me:+.2f}%/{he:.0f}%/{len(exh)} | {mc:+.2f}%/{hc:.0f}%/{len(cln)} | {diff:+.2f}pp | {share:.1f}% |")
        if not (exh and cln and me < mc):
            ok_both = False
    print(f"\neligible fills: OOS2 {n_cand['OOS2']} (exh {n_exh['OOS2']}), train {n_cand['train']} (exh {n_exh['train']})")
    if ok_both:
        print("DIAG PASS: exhausted worse in BOTH windows -> proceed to walk-forward.")
    else:
        print("DIAG REJECT: no consistent gradient -> stop, no walk-forward.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
