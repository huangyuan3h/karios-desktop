#!/usr/bin/env python3
"""H-SAT-IDLE-B3: satellite idle cash parked in the B3 risk-budget sleeve.

Prereg docs/designs/sat-idle-b3-prereg-2026-09-21.md (frozen 2026-09-21).

Sat   = habit S-gap standalone (amp_1430 + C1 3% + same_1430 + gate_1430 + 30bps
        RT, 100% notional, 4x25% slots) -- frozen, untouched (same as H-SAT-IDLE).
Idle  = w_t causal from yesterday's close (true cash share for the gated arm).
X arms:
  A2_true  incumbent argmax parking sleeve (mom60+MA200 argmax + causal trail8)  [reference]
  A4       B3 risk-budget sleeve (homeport.risk_budget_nav, 5 assets inverse-vol monthly)
  A4_true  B3 + true cash share                                        [GATED]
  A5_true  partial: 50% idle -> B3, 50% -> REPO (price list)
  A6_true  partial: 50% idle -> argmax sleeve, 50% -> REPO (price list)
port_ret_t = sat_ret_t + w_t * x_ret_t - transfer_bps/1e4 * |w_t - w_{t-1}|

Verdict (frozen, drawdown premise):
  K1  A4_true 3w dTot vs standalone all >= 0
  K2  A4_true long dTot vs standalone >= +100pt
  K3  A4_true long MDD at least 10pt shallower than A2_true
  K4  A4_true long Sharpe > A2_true long Sharpe
  PASS = K1 & K2 & K3 & K4

Read-only; Live untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3.py --save-report
  PYTHONPATH=src:scripts python3 scripts/eval_sat_idle_b3.py --windows long
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
    DESCRIPTIVE,
    TRANSFER_BPS,
    WINS,
    _compose,
    _corr,
    _fmt,
    _idle_prev,
    _repo_nav,
    _rets,
    _sat_book,
    _sleeve_nav,
    _stats,
    _true_cash_share,
    _year_deltas,
)
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.homeport import load_risk_closes, risk_budget_nav  # noqa: E402

GATED_ARM = "A4_true"
REF_ARM = "A2_true"


def _blend_nav(a: list[float], b: list[float], p: float) -> list[float]:
    """Daily-rebalanced static blend NAV: p * a + (1-p) * b (no extra cost)."""
    n = min(len(a), len(b))
    out = [1.0]
    for t in range(1, n):
        ra = a[t] / a[t - 1] - 1.0 if a[t - 1] else 0.0
        rb = b[t] / b[t - 1] - 1.0 if b[t - 1] else 0.0
        out.append(out[-1] * (1.0 + p * ra + (1.0 - p) * rb))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument(
        "--premise",
        choices=("returns", "drawdown"),
        default="returns",
        help="returns = H-SAT-IDLE-B3 frozen K1-K4; drawdown = H-SAT-IDLE-B3-R re-adjudication",
    )
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    from eval_twin_star_parking import _load_etf_closes

    px = _load_etf_closes()
    risk_closes = load_risk_closes()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
        n = len(dates)

        sleeve_nav = _sleeve_nav(px, dates)
        repo_nav = _repo_nav(dates)
        b3_nav = risk_budget_nav(risk_closes, dates)
        n = min(n, len(sleeve_nav), len(repo_nav), len(b3_nav))
        dates, sat_nav, pos = dates[:n], sat_nav[:n], pos[:n]
        sleeve_nav, repo_nav, b3_nav = sleeve_nav[:n], repo_nav[:n], b3_nav[:n]

        w_causal = _idle_prev(pos, lag=True)
        cash_true = _true_cash_share(sat)
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]

        a0 = _stats(sat_nav)
        arms: dict[str, list[float]] = {}
        arms["A2_true"] = _compose(sat_nav, w_true, sleeve_nav, TRANSFER_BPS[0])
        arms["A4"] = _compose(sat_nav, w_causal, b3_nav, TRANSFER_BPS[0])
        arms["A4_true"] = _compose(sat_nav, w_true, b3_nav, TRANSFER_BPS[0])
        arms["A4_true_15bps"] = _compose(sat_nav, w_true, b3_nav, TRANSFER_BPS[1])
        arms["A5_true"] = _compose(
            sat_nav, w_true, _blend_nav(b3_nav, repo_nav, 0.5), TRANSFER_BPS[0]
        )
        arms["A6_true"] = _compose(
            sat_nav, w_true, _blend_nav(sleeve_nav, repo_nav, 0.5), TRANSFER_BPS[0]
        )

        row: dict[str, dict] = {
            "satellite": {**_stats(sat_nav), "delta_total": 0.0, "delta_sharpe": 0.0, "delta_mdd": 0.0},
            "_x_standalone": {
                "sleeve": _stats(sleeve_nav),
                "b3": _stats(b3_nav),
                "repo": _stats(repo_nav),
            },
        }
        sat_r = _rets(sat_nav)
        b3_r = _rets(b3_nav)
        idle_mask = [w_causal[i] > 0 for i in range(n)]
        for aid, nav in arms.items():
            m = _stats(nav)
            row[aid] = {
                **m,
                "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                "delta_sharpe": round(m["sharpe"] - a0["sharpe"], 2),
                "delta_mdd": round(m["max_dd"] - a0["max_dd"], 1),
                "year_deltas": _year_deltas(dates, nav, sat_nav) if wname == "long" else None,
            }
        a4, a2 = row[GATED_ARM], row[REF_ARM]
        row["_vs_incumbent"] = {
            "delta_mdd_vs_A2": round(a4["max_dd"] - a2["max_dd"], 1),
            "delta_total_vs_A2": round(a4["total_pct"] - a2["total_pct"], 1),
            "delta_sharpe_vs_A2": round(a4["sharpe"] - a2["sharpe"], 2),
        }
        row["_diag"] = {
            "parked_pct_days": round(100 * float(sum(w_causal[1:]) / max(1, n - 1)), 1),
            "w_true_mean": round(float(sum(w_true[1:]) / max(1, n - 1)), 3),
            "corr_b3_sat_all": _corr(b3_r, sat_r),
            "corr_b3_sat_idle": _corr(b3_r, sat_r, idle_mask),
        }
        results[wname] = row

        print(f"  satellite       {_fmt(a0)}", flush=True)
        print(
            f"  X standalone    sleeve {_fmt(row['_x_standalone']['sleeve'])} | "
            f"B3 {_fmt(row['_x_standalone']['b3'])} | repo {_fmt(row['_x_standalone']['repo'])}",
            flush=True,
        )
        for aid, rec in row.items():
            if aid.startswith("_") or aid == "satellite":
                continue
            yd = rec.get("year_deltas")
            extra = f"  years {yd}" if yd else ""
            print(
                f"  {aid:<14} {_fmt(rec)}  Δtot {rec['delta_total']:+7.1f} "
                f"Δsr {rec['delta_sharpe']:+.2f} Δdd {rec['delta_mdd']:+.1f}{extra}",
                flush=True,
            )
        v = row["_vs_incumbent"]
        print(
            f"  A4_true vs A2_true: Δtot {v['delta_total_vs_A2']:+.1f} "
            f"Δmdd {v['delta_mdd_vs_A2']:+.1f} Δsr {v['delta_sharpe_vs_A2']:+.2f} | "
            f"corr B3/sat all {row['_diag']['corr_b3_sat_all']} idle {row['_diag']['corr_b3_sat_idle']}",
            flush=True,
        )

    verdict: dict[str, object] = {}
    if all(w in results for w in ("OOS2", "train", "valid", "long")):
        d3 = [results[w][GATED_ARM]["delta_total"] for w in ("OOS2", "train", "valid")]
        d_long = results["long"][GATED_ARM]["delta_total"]
        d_mdd = results["long"]["_vs_incumbent"]["delta_mdd_vs_A2"]
        sr_a4 = results["long"][GATED_ARM]["sharpe"]
        sr_a2 = results["long"][REF_ARM]["sharpe"]
        sr_sat = results["long"]["satellite"]["sharpe"]
        if args.premise == "drawdown":
            k1 = min(d3) >= -5.0
            k2 = d_long >= 100.0
            k3 = d_mdd >= 15.0
            k4 = sr_a4 >= sr_a2 + 0.50 and sr_a4 >= sr_sat
            verdict = {
                "arm": GATED_ARM,
                "premise": "drawdown",
                "deltas_3w": d3,
                "k1_no_window_dilution_ge_-5": k1,
                "long_delta": d_long,
                "k2_long_ge_100": k2,
                "long_mdd_vs_A2": d_mdd,
                "k3_mdd_15pt_shallower": k3,
                "sharpe_A4": sr_a4,
                "sharpe_A2": sr_a2,
                "sharpe_sat": sr_sat,
                "k4_sharpe_ge_A2_plus_0.5": k4,
                "pass": bool(k1 and k2 and k3 and k4),
                "note": (
                    "drawdown-premise re-adjudication (H-SAT-IDLE-B3-R); does NOT "
                    "override the returns-premise REJECT; PASS = satellite-anchored "
                    "product candidate only (NOT Harbor+satellite, H-SAT-W stays REJECT)"
                ),
            }
            print("\n## Verdict (frozen prereg · drawdown premise)\n")
            print(f"  K1 (3w Δtot all ≥ −5): {k1} {d3}")
            print(f"  K2 (long Δtot ≥ +100): {k2} ({d_long})")
            print(f"  K3 (long MDD ≥ 15pt shallower than A2): {k3} ({d_mdd})")
            print(f"  K4 (long Sharpe ≥ A2+0.5 and ≥ standalone): {k4} ({sr_a4} vs {sr_a2}/{sr_sat})")
        else:
            k1 = min(d3) >= 0
            k2 = d_long >= 100.0
            k3 = d_mdd >= 10.0
            k4 = sr_a4 > sr_a2
            verdict = {
                "arm": GATED_ARM,
                "premise": "returns",
                "deltas_3w": d3,
                "k1_no_window_dilution": k1,
                "long_delta": d_long,
                "k2_long_ge_100": k2,
                "long_mdd_vs_A2": d_mdd,
                "k3_mdd_10pt_shallower": k3,
                "sharpe_A4": sr_a4,
                "sharpe_A2": sr_a2,
                "k4_sharpe_better": k4,
                "pass": bool(k1 and k2 and k3 and k4),
                "note": (
                    "returns premise; A4_true = satellite idle -> B3 risk-budget; "
                    "PASS = satellite-anchored capital structure candidate only "
                    "(NOT Harbor+satellite, H-SAT-W stays REJECT)"
                ),
            }
            print("\n## Verdict (frozen prereg · returns premise)\n")
            print(f"  K1 (3w Δtot all ≥ 0): {k1} {d3}")
            print(f"  K2 (long Δtot ≥ +100): {k2} ({d_long})")
            print(f"  K3 (long MDD ≥ 10pt shallower than A2): {k3} ({d_mdd})")
            print(f"  K4 (long Sharpe > A2): {k4} ({sr_a4} vs {sr_a2})")
        print(f"  → {'PASS' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        fname = (
            "sat_idle_b3_drawdown_2026-09-21.json"
            if args.premise == "drawdown"
            else "sat_idle_b3_2026-09-21.json"
        )
        tag = "sat-idle-b3-drawdown-2026-09-21" if args.premise == "drawdown" else "sat-idle-b3-2026-09-21"
        prereg = (
            "docs/designs/sat-idle-b3-drawdown-prereg-2026-09-21.md"
            if args.premise == "drawdown"
            else "docs/designs/sat-idle-b3-prereg-2026-09-21.md"
        )
        path = REPORT_DIR / fname
        path.write_text(
            json.dumps(
                {
                    "tag": tag,
                    "protocol": (
                        "frozen habit sat + causal idle parking; A4_true = B3 risk-budget "
                        "(homeport.risk_budget_nav, 5 assets inverse-vol monthly, causal, 5bp); "
                        "A2_true = incumbent argmax sleeve (reference); A5/A6 = partial 50% "
                        "parking; transfer 5bps/side (15bps sensitivity); "
                        f"prereg {prereg}"
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
