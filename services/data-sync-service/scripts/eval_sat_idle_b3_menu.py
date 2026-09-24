#!/usr/bin/env python3
"""H-SAT-IDLE-B3-MENU: broaden the B3 risk-budget menu with the commodity leg.

Prereg docs/designs/sat-idle-b3-menu-prereg-2026-09-21.md (frozen 2026-09-21).

Sat  = habit S-gap standalone (frozen, same as H-SAT-IDLE-B3).
Park = B3 risk-budget with a CAUSAL DYNAMIC UNIVERSE: an asset joins the menu
       only after >= 60 sessions of history (OIL 513350 lists 2023-11-28), so
       the long window is not corrupted by a missing series.
Arms:
  A4_true  B3 5-asset (incumbent candidate)                      [reference]
  A7_true  B3 6-asset: {300,500,GOLD,NASDAQ,BOND10,OIL}          [GATED]
  A8_true  B3 6 + 恒生科技 513180                                (price list)
  A9_true  B3 6 + 有色 512400                                    (price list)
port_ret_t = sat_ret_t + w_t * park_ret_t - 5bps/1e4 * |w_t - w_{t-1}|

Verdict (frozen, return premise):
  K1 A7_true 3w dTot vs A4_true all >= 0
  K2 A7_true long dTot vs A4_true >= +50pt
  K3 A7_true long MDD vs A4_true >= -3pt
  K4 A7_true long Sharpe > A4_true
  PASS = K1 & K2 & K3 & K4

Read-only; Live untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3_menu.py --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3_menu.py --windows long
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_sat_idle_parking import (  # noqa: E402
    DESCRIPTIVE,
    TRANSFER_BPS,
    WINS,
    _compose,
    _fmt,
    _repo_nav,
    _sat_book,
    _sleeve_nav,
    _stats,
    _true_cash_share,
    _year_deltas,
)
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service import homeport  # noqa: E402
from data_sync_service.service.harbor import merge_recent_db_closes  # noqa: E402

B3_5 = homeport.RISK_UNIVERSE
OIL = "513350.SH"
HK_TECH = "513180.SH"
NONFERROUS = "512400.SH"
B3_6 = (*B3_5, OIL)
UNIV_8 = (*B3_6, HK_TECH)
UNIV_9 = (*B3_6, NONFERROUS)
GATED_ARM = "A7_true"
REF_ARM = "A4_true"
LOOKBACK = homeport.VOL_LOOKBACK


def _load_closes(ts_list) -> dict[str, dict[str, float]]:
    wanted = set(ts_list)
    out: dict[str, dict[str, float]] = {}
    with (ROOT / "data" / "etf" / "etf_daily.csv").open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            ts = str(row.get("ts_code") or "")
            if ts not in wanted:
                continue
            d = str(row.get("trade_date") or "")
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            try:
                c = float(row.get("close_adj") or 0)
            except (TypeError, ValueError):
                continue
            if c > 0:
                out.setdefault(ts, {})[d] = c
    return merge_recent_db_closes(out, wanted)


def _weights_at(series: dict, universe, i: int) -> dict[str, float]:
    """Causal inverse-vol weights over assets eligible at ``i`` (empty = cash)."""
    elig: list[str] = []
    for ts in universe:
        s = series[ts]
        if i >= len(s) or s[i] is None:
            continue
        if i < LOOKBACK:
            elig.append(ts)
            continue
        rets = [s[j] / s[j - 1] - 1.0 for j in range(max(1, i - LOOKBACK), i) if s[j] and s[j - 1]]
        if len(rets) >= 2:
            elig.append(ts)
    if not elig:
        return {}
    if i < LOOKBACK:
        return {ts: 1.0 / len(elig) for ts in elig}
    vol = {ts: (homeport._vol_at(series[ts], i, LOOKBACK) or 1e-9) for ts in elig}
    return homeport.inverse_vol_weights(vol)


def _risk_budget_nav_dynamic(closes: dict, cal: list[str], universe) -> list[float]:
    series = {ts: homeport._series_on_cal(closes.get(ts) or {}, cal) for ts in universe}
    cur = _weights_at(series, universe, 0)
    nav = [1.0]
    for i in range(1, len(cal)):
        r = 0.0
        for ts, w in cur.items():
            a, b = series[ts][i - 1], series[ts][i]
            if a and b:
                r += w * (b / a - 1.0)
        nav.append(nav[-1] * (1.0 + r))
        new = cur
        if cal[i][:7] != cal[i - 1][:7] or i == LOOKBACK:
            new = _weights_at(series, universe, i)
        if new != cur:
            turn = sum(abs(new.get(ts, 0.0) - cur.get(ts, 0.0)) for ts in universe) / 2.0
            nav[-1] *= 1.0 - homeport.COST * turn
            cur = new
    return nav


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    from eval_twin_star_parking import _load_etf_closes

    px = _load_etf_closes()
    all_closes = _load_closes(set((*UNIV_8, *UNIV_9)))
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
        n = len(dates)

        sleeve_nav = _sleeve_nav(px, dates)
        b3_5 = homeport.risk_budget_nav(all_closes, dates)
        b3_6 = _risk_budget_nav_dynamic(all_closes, dates, B3_6)
        b3_8 = _risk_budget_nav_dynamic(all_closes, dates, UNIV_8)
        b3_9 = _risk_budget_nav_dynamic(all_closes, dates, UNIV_9)
        repo_nav = _repo_nav(dates)
        n = min(n, len(sleeve_nav), len(b3_5), len(b3_6), len(b3_8), len(b3_9), len(repo_nav))
        dates, sat_nav, pos = dates[:n], sat_nav[:n], pos[:n]

        cash_true = _true_cash_share(sat)
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]

        a0 = _stats(sat_nav)
        arms = {
            "A4_true": _compose(sat_nav, w_true, b3_5[:n], TRANSFER_BPS[0]),
            "A7_true": _compose(sat_nav, w_true, b3_6[:n], TRANSFER_BPS[0]),
            "A7_true_15bps": _compose(sat_nav, w_true, b3_6[:n], TRANSFER_BPS[1]),
            "A8_true": _compose(sat_nav, w_true, b3_8[:n], TRANSFER_BPS[0]),
            "A9_true": _compose(sat_nav, w_true, b3_9[:n], TRANSFER_BPS[0]),
        }
        row: dict[str, dict] = {
            "satellite": {**_stats(sat_nav), "delta_total": 0.0},
            "_x_standalone": {
                "sleeve": _stats(sleeve_nav[:n]),
                "b3_5": _stats(b3_5[:n]),
                "b3_6": _stats(b3_6[:n]),
                "b3_8": _stats(b3_8[:n]),
                "b3_9": _stats(b3_9[:n]),
            },
        }
        for aid, nav in arms.items():
            m = _stats(nav)
            row[aid] = {
                **m,
                "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                "year_deltas": _year_deltas(dates, nav, sat_nav) if wname == "long" else None,
            }
        a7, a4 = row[GATED_ARM], row[REF_ARM]
        row["_vs_A4"] = {
            "delta_total": round(a7["total_pct"] - a4["total_pct"], 1),
            "delta_mdd": round(a7["max_dd"] - a4["max_dd"], 1),
            "delta_sharpe": round(a7["sharpe"] - a4["sharpe"], 2),
        }
        results[wname] = row

        print(f"  satellite  {_fmt(a0)}", flush=True)
        xs = row["_x_standalone"]
        print(
            f"  X alone    sleeve {_fmt(xs['sleeve'])} | B3(5) {_fmt(xs['b3_5'])} | "
            f"B3(6+OIL) {_fmt(xs['b3_6'])} | B3(8) {_fmt(xs['b3_8'])} | B3(9) {_fmt(xs['b3_9'])}",
            flush=True,
        )
        for aid, rec in row.items():
            if aid.startswith("_") or aid == "satellite":
                continue
            yd = rec.get("year_deltas")
            extra = f"  years {yd}" if yd else ""
            print(
                f"  {aid:<14} {_fmt(rec)}  Δtot {rec['delta_total']:+7.1f}{extra}",
                flush=True,
            )
        v = row["_vs_A4"]
        print(
            f"  A7_true vs A4_true: Δtot {v['delta_total']:+.1f} Δmdd {v['delta_mdd']:+.1f} "
            f"Δsr {v['delta_sharpe']:+.2f}",
            flush=True,
        )

    verdict: dict[str, object] = {}
    if all(w in results for w in ("OOS2", "train", "valid", "long")):
        d3 = [results[w]["_vs_A4"]["delta_total"] for w in ("OOS2", "train", "valid")]
        dl = results["long"]["_vs_A4"]
        k1 = min(d3) >= 0
        k2 = dl["delta_total"] >= 50.0
        k3 = dl["delta_mdd"] >= -3.0
        k4 = dl["delta_sharpe"] > 0
        verdict = {
            "arm": GATED_ARM,
            "premise": "return (vs A4_true)",
            "deltas_3w": d3,
            "k1_no_window_dilution": k1,
            "long_delta": dl["delta_total"],
            "k2_long_ge_50": k2,
            "long_mdd_delta": dl["delta_mdd"],
            "k3_mdd_ge_-3": k3,
            "long_sharpe_delta": dl["delta_sharpe"],
            "k4_sharpe_better": k4,
            "pass": bool(k1 and k2 and k3 and k4),
            "note": (
                "menu expansion: add commodity OIL to the B3 risk-budget menu; "
                "PASS = satellite parking candidate only (NOT Harbor+satellite)"
            ),
        }
        print("\n## Verdict (frozen prereg)\n")
        print(f"  K1 (3w Δtot vs A4 all ≥ 0): {k1} {d3}")
        print(f"  K2 (long Δtot vs A4 ≥ +50): {k2} ({dl['delta_total']})")
        print(f"  K3 (long ΔMDD vs A4 ≥ −3): {k3} ({dl['delta_mdd']})")
        print(f"  K4 (long ΔSharpe > 0): {k4} ({dl['delta_sharpe']})")
        print(f"  → {'PASS' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_idle_b3_menu_2026-09-21.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "sat-idle-b3-menu-2026-09-21",
                    "protocol": (
                        "B3 risk-budget with causal dynamic universe (asset joins after "
                        ">=60 sessions); A7_true = {300,500,GOLD,NASDAQ,BOND10,OIL}; "
                        "A4_true = B3 5-asset reference; A8/A9 = +HK_TECH / +NONFERROUS; "
                        "satellite frozen; transfer 5bps/side; prereg "
                        "docs/designs/sat-idle-b3-menu-prereg-2026-09-21.md"
                    ),
                    "windows": results,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(),
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        print(f"\nreport: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
