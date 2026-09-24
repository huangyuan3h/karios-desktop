#!/usr/bin/env python3
"""H-SAT-PARK-FRONT: Starship parking-asset frontier.

``--sleeve-mode h2`` is the current canonical H2-a25 report. Use
``--sleeve-mode legacy-canonical`` only to reproduce the historical 0pt arm.
Prereg docs/designs/sat-parking-frontier-prereg-2026-09-21.md (frozen 2026-09-21).

Park asset = static blend a*sleeve + b*B3 + c*REPO (a+b+c=1, daily-constant
weights, no rebalance cost). Satellite frozen; w_t = cashShare(T-1) causal;
transfer 5bps/side on |dw|.
port_ret_t = sat_ret_t + w_t * park_ret_t - 5bps/1e4 * |w_t - w_{t-1}|

Read-only; Live untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_parking_frontier.py --sleeve-mode h2 --save-report
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

from data_sync_service.service.harbor import HYST_BAND, load_etf_closes  # noqa: E402
from data_sync_service.service.homeport import load_risk_closes, risk_budget_nav  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    A25_B3_WEIGHT,
    A25_PARKING_MODE,
    A25_SLEEVE_WEIGHT,
    A25_TAG,
    A25_TRANSFER_BPS,
)

# (label, a=sleeve, b=B3, c=REPO)
GRID: tuple[tuple[str, float, float, float], ...] = (
    ("pure_B3", 0.0, 1.0, 0.0),
    ("a10", 0.1, 0.9, 0.0),
    ("h2_a25", A25_SLEEVE_WEIGHT, A25_B3_WEIGHT, 0.0),
    ("A6_half", 0.5, 0.5, 0.0),
    ("a75", 0.75, 0.25, 0.0),
    ("a90", 0.9, 0.1, 0.0),
    ("pure_sleeve", 1.0, 0.0, 0.0),
    ("b3_repo_5050", 0.0, 0.5, 0.5),
    ("sleeve25_b3_50_repo25", 0.25, 0.5, 0.25),
    ("sleeve50_b3_25_repo25", 0.5, 0.25, 0.25),
)


def _blend3(sleeve: list[float], b3: list[float], repo: list[float], a: float, b: float, c: float) -> list[float]:
    n = min(len(sleeve), len(b3), len(repo))
    out = [1.0]
    for t in range(1, n):
        rs = sleeve[t] / sleeve[t - 1] - 1.0 if sleeve[t - 1] else 0.0
        rb = b3[t] / b3[t - 1] - 1.0 if b3[t - 1] else 0.0
        rr = repo[t] / repo[t - 1] - 1.0 if repo[t - 1] else 0.0
        out.append(out[-1] * (1.0 + a * rs + b * rb + c * rr))
    return out


def _grid_for_mode(mode: str) -> tuple[tuple[str, float, float, float], ...]:
    if mode == "h2":
        return GRID
    return tuple(
        ("a25_0pt", a, b, c) if label == "h2_a25" else (label, a, b, c)
        for label, a, b, c in GRID
    )


def _a25_verdict(results: dict[str, dict], label: str) -> dict[str, object]:
    required = ("OOS2", "train", "valid", "long")
    if not all(window in results for window in required):
        return {
            "status": "INCOMPLETE",
            "pass": False,
            "arm": label,
            "baseline": "pure_B3",
            "missing_windows": [window for window in required if window not in results],
        }

    rows = {window: results[window] for window in required}
    deltas = [
        round(rows[window][label]["total_pct"] - rows[window]["pure_B3"]["total_pct"], 1)
        for window in ("OOS2", "train", "valid")
    ]
    long_delta = round(
        rows["long"][label]["total_pct"] - rows["long"]["pure_B3"]["total_pct"], 1
    )
    long_mdd_delta = round(
        rows["long"][label]["max_dd"] - rows["long"]["pure_B3"]["max_dd"], 1
    )
    long_sharpe_delta = round(
        rows["long"][label]["sharpe"] - rows["long"]["pure_B3"]["sharpe"], 2
    )
    checks = {
        "K1_three_window_total_ge_0": all(delta >= 0 for delta in deltas),
        "K2_long_total_ge_50": long_delta >= 50.0,
        "K3_long_mdd_ge_minus_1": long_mdd_delta >= -1.0,
        "K4_long_sharpe_ge_minus_0_30": long_sharpe_delta >= -0.30,
        "K5_long_mdd_ge_minus_10": rows["long"][label]["max_dd"] >= -10.0,
    }
    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "REJECT",
        "pass": passed,
        "arm": label,
        "baseline": "pure_B3",
        "deltas_3w": deltas,
        "long_delta_total": long_delta,
        "long_delta_mdd": long_mdd_delta,
        "long_delta_sharpe": long_sharpe_delta,
        "checks": checks,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument(
        "--sleeve-mode",
        choices=("h2", "legacy-canonical"),
        default="h2",
    )
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]
    grid = _grid_for_mode(args.sleeve_mode)
    canonical_label = A25_PARKING_MODE if args.sleeve_mode == "h2" else "a25_0pt"
    hyst_band = HYST_BAND if args.sleeve_mode == "h2" else 0.0

    from eval_twin_star_parking import _load_etf_closes

    px = load_etf_closes() if args.sleeve_mode == "h2" else _load_etf_closes()
    risk_closes = load_risk_closes()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
        sleeve = _sleeve_nav(px, dates, hyst_band=hyst_band)
        b3 = risk_budget_nav(risk_closes, dates)
        repo = _repo_nav(dates)
        n = min(len(dates), len(sleeve), len(b3), len(repo))
        dates, sat_nav, pos = dates[:n], sat_nav[:n], pos[:n]
        sleeve, b3, repo = sleeve[:n], b3[:n], repo[:n]

        cash_true = _true_cash_share(sat, from_rows=True)
        w_true = [0.0] * n
        for i in range(1, n):
            w_true[i] = cash_true[i - 1]

        a0 = _stats(sat_nav)
        row: dict[str, dict] = {"satellite": {**_stats(sat_nav), "delta_total": 0.0}}
        for label, a, b, c in grid:
            park = _blend3(sleeve, b3, repo, a, b, c)
            nav = _compose(sat_nav, w_true, park, A25_TRANSFER_BPS)
            m = _stats(nav)
            row[label] = {
                **m,
                "a_sleeve": a,
                "b_b3": b,
                "c_repo": c,
                "delta_total": round(m["total_pct"] - a0["total_pct"], 1),
                "calmar": round(m["cagr"] / abs(m["max_dd"]), 2) if m["max_dd"] else None,
                "year_deltas": _year_deltas(dates, nav, sat_nav) if wname == "long" else None,
            }
        results[wname] = row
        print(f"  satellite  {_fmt(a0)}", flush=True)
        for label, *_ in grid:
            r = row[label]
            print(
                f"  {label:<24} {_fmt(r)}  calmar {r['calmar']}  Δtot {r['delta_total']:+7.1f}",
                flush=True,
            )

    verdict = _a25_verdict(results, canonical_label) if args.sleeve_mode == "h2" else {}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        is_canonical = args.sleeve_mode == "h2"
        path = REPORT_DIR / (
            "sat_h2_a25_2026-09-24.json"
            if is_canonical
            else "sat_parking_frontier_0pt_2026-09-24.json"
        )
        path.write_text(
            json.dumps(
                {
                    "schemaVersion": 2,
                    "tag": A25_TAG if is_canonical else "sat-parking-0pt-20260924",
                    "role": "canonical" if is_canonical else "historical",
                    "canonical": is_canonical,
                    "variant": {
                        "sleeveMode": args.sleeve_mode,
                        "hystBand": hyst_band,
                        "sleeveWeight": A25_SLEEVE_WEIGHT,
                        "b3Weight": A25_B3_WEIGHT,
                        "transferBpsPerSide": A25_TRANSFER_BPS,
                    },
                    "protocol": (
                        "park asset = a*sleeve + b*B3 + c*REPO static blend; satellite frozen; "
                        "w_t=cashShare(T-1); transfer 5bps/side; "
                        + (
                            "H2 sleeve = hyst_band=0.02; product data loader"
                            if is_canonical
                            else "legacy 0pt sleeve; frozen research data loader"
                        )
                    ),
                    "grid": [{"label": g[0], "a": g[1], "b": g[2], "c": g[3]} for g in grid],
                    "windows": results,
                    "verdict": verdict,
                    "supersedes": "sat_parking_frontier_2026-09-21.json" if is_canonical else None,
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
