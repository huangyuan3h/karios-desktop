#!/usr/bin/env python3
"""H-SAT-SPLIT: capital-split price list — cap the satellite's capital share (2026-09-15).

`port = c * satellite + (1-c) * other`, monthly rebalanced back to c (house caliber,
`_blend_monthly`, 5bps/side on the traded fraction). Grid c in {0, .20, 1/3, .50,
2/3, .80, 1}; others = Harbor (S-3+parking) / Homeport M50 / B3 risk-budget /
ETF parking sleeve / REPO. Windows OOS2/train/valid/long (+holdout descriptive).

Price list only (no K gates) — see docs/designs/sat-capital-split-prereg-2026-09-15.md.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_capital_split.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_harbor_riskbudget import (  # noqa: E402
    COST,
    _blend_monthly,
    _harbor_nav,
    _load_panel,
    _rp_nav,
)
from eval_twin_star_parking import _fmt, _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

WINS = ("OOS2", "train", "valid", "long")
DESCRIPTIVE = ("holdout",)
GRID = (0.0, 0.20, 1 / 3, 0.50, 2 / 3, 0.80, 1.0)
OTHERS = ("港湾", "母港M50", "B3", "套筒", "REPO")
OTHER_KEYS = {
    "港湾": "harbor",
    "母港M50": "m50",
    "B3": "b3",
    "套筒": "sleeve",
    "REPO": "repo",
}
REPO_ANNUAL = 0.007


def _align_day_nav(day_nav: dict[str, float], dates: list[str]) -> list[float]:
    cal = sorted(day_nav)
    out: list[float] = []
    last, j = 1.0, 0
    for d in dates:
        while j < len(cal) and cal[j] <= d:
            last = day_nav[cal[j]]
            j += 1
        out.append(last)
    return out


def _sleeve_on_cal(px: dict[str, dict[str, float]], cal: list[str]) -> dict[str, float]:
    recs = parking_replay(px, cal, idle_by_day=None)
    day_nav = {cal[0]: 1.0} if cal else {}
    nav = 1.0
    for rec in recs:
        nav *= 1.0 + float(rec["parking_ret"]) - COST * int(rec["sides"])
        day_nav[str(rec["date"])] = nav
    return day_nav


def _repo_on(dates: list[str]) -> list[float]:
    daily = REPO_ANNUAL / 252.0
    out = [1.0]
    for _ in range(1, len(dates)):
        out.append(out[-1] * (1.0 + daily))
    return out


def _corr(a: list[float], b: list[float]) -> float | None:
    n = min(len(a), len(b))
    ra = [a[i] / a[i - 1] - 1 for i in range(1, n) if a[i - 1]]
    rb = [b[i] / b[i - 1] - 1 for i in range(1, n) if b[i - 1]]
    if len(ra) < 10:
        return None
    x, y = np.array(ra), np.array(rb)
    if x.std() == 0 or y.std() == 0:
        return None
    return round(float(np.corrcoef(x, y)[0, 1]), 2)


def _year_deltas(dates: list[str], a: list[float], b: list[float]) -> dict[str, float]:
    idx_by_year: dict[str, list[int]] = {}
    for i, d in enumerate(dates):
        idx_by_year.setdefault(d[:4], []).append(i)
    out: dict[str, float] = {}
    for y, idx in idx_by_year.items():
        if len(idx) < 2:
            continue
        i0, i1 = idx[0], idx[-1]
        base = max(0, i0 - 1)
        ra = (a[i1] / a[base] - 1.0) * 100 if a[base] else 0.0
        rb = (b[i1] / b[base] - 1.0) * 100 if b[base] else 0.0
        out[y] = round(ra - rb, 1)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default=",".join((*WINS, *DESCRIPTIVE)))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    px = _load_panel()
    results: dict[str, dict] = {}
    for wname in wins:
        s, e = WINDOWS[wname]
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        cal, harbor = _harbor_nav(px, s, e)
        rp = _rp_nav(px, cal, COST)
        m50 = _blend_monthly(harbor, rp, 0.5, cal, COST)
        sleeve_day = _sleeve_on_cal(px, cal)
        ctx = load_sgap_context(s, e)
        sat = replay_sgap_from_context(
            ctx,
            start=s,
            end=e,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
            body=3,
            fill_mode=FILL_SAME_1430,
            fill_hhmm="1430",
            exit_hhmm="1430",
            max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
            gate_1430=True,
        )
        rows = sat.get("rows") or []
        dates = [str(r["date"]) for r in rows]
        sat_nav = [float(r.get("satNav") or 1.0) for r in rows]
        if sat_nav and sat_nav[0] > 0:
            base = sat_nav[0]
            sat_nav = [v / base for v in sat_nav]
        day_maps = {
            "harbor": dict(zip(cal, harbor, strict=True)),
            "m50": dict(zip(cal, m50, strict=True)),
            "b3": dict(zip(cal, rp, strict=True)),
            "sleeve": sleeve_day,
        }
        others = {k: _align_day_nav(m, dates) for k, m in day_maps.items()}
        n = min(len(dates), len(sat_nav), *(len(v) for v in others.values()))
        dates, sat_nav = dates[:n], sat_nav[:n]
        others = {k: v[:n] for k, v in others.items()}
        others["repo"] = _repo_on(dates)[:n]

        row: dict[str, dict] = {}
        for label, key in OTHER_KEYS.items():
            other = others[key]
            cells: dict[str, dict] = {}
            for c in GRID:
                nav = _blend_monthly(sat_nav, other, c, dates, COST)
                m = _stats(nav)
                cells[str(round(c, 4))] = {**m, "c": round(c, 4)}
            row[label] = cells
            ref = cells["0.0"]
            for cc in cells.values():
                cc["delta_total"] = round(cc["total_pct"] - ref["total_pct"], 1)
                cc["delta_sharpe"] = round(cc["sharpe"] - ref["sharpe"], 2)
                cc["delta_mdd"] = round(cc["max_dd"] - ref["max_dd"], 1)
        row["_refs"] = {
            "satellite": _stats(sat_nav),
            "corr": {label: _corr(sat_nav, others[OTHER_KEYS[label]]) for label in OTHERS},
        }
        if wname == "long":
            for label in ("港湾", "母港M50"):
                mid = _blend_monthly(sat_nav, others[OTHER_KEYS[label]], 0.5, dates, COST)
                row["_refs"].setdefault("year_deltas_c50", {})[label] = _year_deltas(
                    dates, mid, sat_nav
                )
        results[wname] = row

        print(f"  satellite 100% {_fmt(_stats(sat_nav))}", flush=True)
        print(
            "  corr(sat, other): "
            + " ".join(f"{label} {row['_refs']['corr'][label]}" for label in OTHERS),
            flush=True,
        )
        for label in OTHERS:
            for c in GRID:
                cell = row[label][str(round(c, 4))]
                print(
                    f"  {label:<8} c={c:<5} {_fmt(cell)}  Δtot {cell['delta_total']:+7.1f} "
                    f"Δsr {cell['delta_sharpe']:+.2f} Δdd {cell['delta_mdd']:+.1f}",
                    flush=True,
                )
        print("", flush=True)
        del ctx, rows, sat

    if all(w in results for w in WINS):
        print("## Price list (long window): total% / CAGR / MDD / Sharpe — c = satellite share")
        print(
            "| other | " + " | ".join(f"c={round(c, 2)}" for c in GRID) + " |"
        )
        print("|" + "---|" * (1 + len(GRID)))
        for label in OTHERS:
            cells = []
            for c in GRID:
                cell = results["long"][label][str(round(c, 4))]
                cells.append(f"{cell['total_pct']:.0f} / {cell['max_dd']:.0f} / {cell['sharpe']:.2f}")
            print(f"| {label} | " + " | ".join(cells) + " |")
        sat_m = results["long"]["_refs"]["satellite"]
        print(
            f"\nsatellite 100%: {_fmt(sat_m)} | 3w totals "
            + " ".join(
                f"{w} {results[w]['_refs']['satellite']['total_pct']:.1f}"
                for w in ("OOS2", "train", "valid")
            )
        )
        for label in OTHERS:
            best_ret = max(GRID[1:], key=lambda c: results["long"][label][str(round(c, 4))]["total_pct"])
            best_sr = max(GRID[1:], key=lambda c: results["long"][label][str(round(c, 4))]["sharpe"])
            print(
                f"  {label:<8} max long-total c={round(best_ret, 2)} "
                f"({results['long'][label][str(round(best_ret, 4))]['total_pct']:.0f}%) | "
                f"max long-Sharpe c={round(best_sr, 2)} "
                f"({results['long'][label][str(round(best_sr, 4))]['sharpe']:.2f})"
            )
        yd = results["long"]["_refs"].get("year_deltas_c50")
        if yd:
            print(f"  c=0.50 year deltas vs satellite: {yd}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_capital_split_2026-09-15.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "sat-capital-split-2026-09-15",
                    "protocol": (
                        "sat = frozen habit S-gap (amp_1430 + C1 3% + same_1430 + gate_1430 "
                        "+ 30bps RT, 4x25%); others = Harbor / M50 / B3 / sleeve / REPO; "
                        "static split c*sat + (1-c)*other, monthly rebalance (_blend_monthly, "
                        "5bps/side); grid c in {0,.2,1/3,.5,2/3,.8,1};price list only; prereg "
                        "docs/designs/sat-capital-split-prereg-2026-09-15.md"
                    ),
                    "windows": results,
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
