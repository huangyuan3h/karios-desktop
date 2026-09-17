#!/usr/bin/env python3
"""Offense grid for starship v2 (H-SAT-OFF prereg 2026-09-16).

Post-processing ONLY on the frozen satellite book (4x25% clips, C1, gate_1430):
no replay/fill/slot/gate changes. Two arms:

  Arm S: choppy-regime satellite down-scale k in {1.0, 0.75, 0.5}
         (freed capital parks in the sleeve; up stays 1.0 = no leverage
         available; down untouched = R-wide gate already blocks).
         w'_t = 1 - k_t * (1 - w_true_t), blended with _compose.
  Arm P: park idle in sleeve iff breadth_1430 > 0.5 (frozen R-wide level,
         no new threshold), else REPO. Switch friction charged honestly
         (bps * w_true * |dflag|), plus a 15bps sensitivity arm.

Diagnostic gates (readonly, run first; a VOID arm is skipped, not run):
  D2: satellite choppy-regime return sum < -2pt  (else S would cut winners)
  D3: sleeve low-breadth return sum < REPO equiv (else P parks nothing)

Selection on OOS2+train dev mean (satellite convention; valid verifies).
Offense verdict (prereg v1): long dTot vs A2_true >= +25 AND every window
absolute total >= -15% AND long MDD >= -40% AND valid dTot >= -8 AND
15bps long delta > 0.

Usage:
  PYTHONPATH=src:scripts python3 scripts/eval_sat_offense_grid.py --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_sat_offense_grid.py --windows OOS2,train
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_sat_idle_parking import (  # noqa: E402
    TRANSFER_BPS,
    _compose,
    _repo_nav,
    _sat_book,
    _sleeve_nav,
    _stats,
    _true_cash_share,
)
from eval_twin_star_parking import _fmt, _load_etf_closes  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    _breadth_at_1430,
    load_sgap_context,
)

WINS = ("OOS2", "train", "valid", "long")
DESCRIPTIVE = ("holdout",)
R_WIDE = 0.5  # frozen gate level, borrowed (no new threshold)
K_GRID = (1.0, 0.75, 0.5)


def _index_regime() -> dict[str, str]:
    """000001.SH ret20 rule (diag_sat_regime口径): up>+3%, down<-3%, else choppy."""
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, close FROM index_daily "
                "WHERE ts_code='000001.SH' AND trade_date >= '2021-06-01' "
                "ORDER BY trade_date"
            )
            rows = [(str(d)[:10], float(c)) for d, c in cur.fetchall() if c and c > 0]
    reg: dict[str, str] = {}
    closes = [c for _, c in rows]
    days = [d for d, _ in rows]
    for i in range(20, len(rows)):
        r = closes[i] / closes[i - 20] - 1.0
        reg[days[i]] = "up" if r > 0.03 else ("down" if r < -0.03 else "choppy")
    return reg


def _sum_by_mask(rets: list[float], mask: list[bool]) -> tuple[float, int]:
    sel = [r for r, m in zip(rets, mask, strict=True) if m]
    return round(sum(sel) * 100, 1), len(sel)


def _nav_rets(nav: list[float]) -> list[float]:
    return [0.0] + [(nav[i] / nav[i - 1] - 1.0) if nav[i - 1] else 0.0 for i in range(1, len(nav))]


def _max_dd_episode(nav: list[float], dates: list[str]) -> dict:
    peak, peak_i, worst = nav[0], 0, {"depth": 0.0}
    for i, v in enumerate(nav):
        if v > peak:
            peak, peak_i = v, i
        dd = v / peak - 1.0
        if dd * 100 < worst["depth"]:
            worst = {"depth": round(dd * 100, 1), "peak": dates[peak_i], "trough": dates[i]}
    return worst


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    px = _load_etf_closes()
    regime = _index_regime()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav = sat["dates"], sat["nav"]
        sleeve_nav = _sleeve_nav(px, dates)
        repo_nav = _repo_nav(dates)
        n = min(len(dates), len(sat_nav), len(sleeve_nav))
        dates, sat_nav = dates[:n], sat_nav[:n]
        sleeve_nav, repo_nav = sleeve_nav[:n], repo_nav[:n]

        sat_r = _nav_rets(sat_nav)
        slv_r = _nav_rets(sleeve_nav)
        reg = [regime.get(d, "unknown") for d in dates]
        ctx = load_sgap_context(s, e)
        br = [_breadth_at_1430(ctx, d) for d in dates]
        n_none = sum(1 for b in br if b is None)
        hi = [b is None or b > R_WIDE for b in br]  # None -> status quo (park)

        # ---- D1: sleeve episode (record only) ----
        ep = _max_dd_episode(sleeve_nav, dates)
        print(f"  D1 sleeve maxDD episode: {ep} (breadth-Nones: {n_none})", flush=True)

        # ---- D2: satellite x regime ----
        d2 = {k: _sum_by_mask(sat_r, [r == k for r in reg]) for k in ("up", "choppy", "down", "unknown")}
        print(f"  D2 sat x regime (sum%, n): {d2}", flush=True)
        s_gate = d2["choppy"][0] < -2.0

        # ---- D3: sleeve x breadth ----
        d3hi = _sum_by_mask(slv_r, hi)
        d3lo = _sum_by_mask(slv_r, [not h for h in hi])
        repo_equiv = round(0.7 / 100 * n / 252 * 100, 2)
        print(f"  D3 sleeve|breadth>0.5: {d3hi}  |<=0.5: {d3lo}  repo-equiv: {repo_equiv}pt", flush=True)
        p_gate = d3lo[0] < repo_equiv
        print(f"  gates: Arm-S {'OPEN' if s_gate else 'VOID'} | Arm-P {'OPEN' if p_gate else 'VOID'}", flush=True)

        # ---- baseline arm ----
        cash_true = _true_cash_share({**sat, "dates": dates, "nav": sat_nav})
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]
        arms: dict[str, list[float]] = {
            "A2_true": _compose(sat_nav, w_true, sleeve_nav, TRANSFER_BPS[0]),
        }
        # ---- Arm S ----
        if s_gate:
            for k in K_GRID:
                if k == 1.0:
                    continue
                w_p = [1.0 - (k if reg[i] == "choppy" else 1.0) * (1.0 - w_true[i]) for i in range(n)]
                w_p[0] = w_true[0]
                arms[f"S{int(k * 100)}"] = _compose(sat_nav, w_p, sleeve_nav, TRANSFER_BPS[0])
        # ---- Arm P ----
        if p_gate:
            for bps, suffix in ((TRANSFER_BPS[0], ""), (TRANSFER_BPS[1], "_15bps")):
                flag = [1 if h else 0 for h in hi]
                x_nav = [1.0]
                for t in range(1, n):
                    rs = sleeve_nav[t] / sleeve_nav[t - 1] - 1.0 if sleeve_nav[t - 1] else 0.0
                    rr = repo_nav[t] / repo_nav[t - 1] - 1.0 if repo_nav[t - 1] else 0.0
                    switch = bps / 1e4 * abs(flag[t] - flag[t - 1]) * w_true[t]
                    x_nav.append(x_nav[-1] * (1.0 + flag[t] * rs + (1 - flag[t]) * rr - switch))
                arms[f"P{suffix}"] = _compose(sat_nav, w_true, x_nav, bps)

        rec: dict[str, dict] = {"_diag": {
            "sleeve_episode": ep, "sat_x_regime": d2,
            "sleeve_hi": d3hi, "sleeve_lo": d3lo, "repo_equiv": repo_equiv,
            "s_gate": s_gate, "p_gate": p_gate, "breadth_nones": n_none,
        }}
        base = _stats(arms["A2_true"])
        for aid, nav in arms.items():
            m = _stats(nav)
            rec[aid] = {**m, "delta_total": round(m["total_pct"] - base["total_pct"], 1),
                        "delta_mdd": round(m["max_dd"] - base["max_dd"], 1),
                        "delta_sharpe": round(m["sharpe"] - base["sharpe"], 2)}
            print(f"  {aid:<10} {_fmt(m)}  dTot {rec[aid]['delta_total']:+.1f} "
                  f"dSr {rec[aid]['delta_sharpe']:+.2f} dDd {rec[aid]['delta_mdd']:+.1f}", flush=True)
        rec["_base"] = base
        results[wname] = rec

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = {"generatedAt": datetime.now(UTC).isoformat(), "windows": results,
               "prereg": "docs/designs/sat-offense-grid-prereg-2026-09-16.md"}
        (REPORT_DIR / "sat_offense_grid_2026-09-16.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=str))
        print("report -> sat_offense_grid_2026-09-16.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
