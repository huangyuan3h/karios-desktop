#!/usr/bin/env python3
"""Parking re-entry cooldown after a trail exit (H-PARK-C · 2026-09-13).

C0 = incumbent Harbor P1 (no cooldown). Ck = same-key trail-exit cooldown of k
sessions; the parking sleeve rotates to the next eligible candidate or REPO.
Risk-targeted candidate from B11 §3.3.
See docs/designs/parking-cooldown-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_parking_cooldown.py --save-report
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

from eval_etf_parking_baseline import _load_etf_closes, _metrics  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402

GRID = (0, 1, 2, 3, 5)
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}
COST = 0.0005
B11_P1 = {"OOS2": 62.0, "train": 45.3, "valid": 50.3, "long": 219.9}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--grid", default=",".join(str(k) for k in GRID))
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()
    grid = tuple(int(x) for x in args.grid.split(","))

    px = _load_etf_closes()
    etf_days = {d for mp in px.values() for d in mp}
    print(f"etf panel: {len(px)} series / {len(etf_days)} sessions (through {max(etf_days)})")

    res: dict[str, dict[str, dict]] = {}
    for w, (s, e) in WINS.items():
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        cal = [d for d in data.calendar if d in etf_days]
        eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
        snap_by = {str(x.get("date")): x for x in run.positions_by_day}

        idle_by_day: dict[str, float] = {}
        for idx in range(1, len(cal)):
            prev = cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))

        records = {
            k: parking_replay(px, cal, idle_by_day=idle_by_day, cooldown_days=k) for k in grid
        }
        navs = {k: [1.0] for k in grid}
        trades = {k: 0 for k in grid}
        cooling = {k: 0 for k in grid}
        for recs in zip(*(records[k] for k in grid), strict=True):
            day, prev = str(recs[0]["date"]), str(recs[0]["prev"])
            idle = idle_by_day.get(prev, 0.0)
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
            for k, rec in zip(grid, recs, strict=True):
                sides = int(rec["sides"])
                if sides:
                    trades[k] += 1
                if rec["cooldown_active"]:
                    cooling[k] += 1
                navs[k].append(
                    navs[k][-1] * (1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * sides))
                )

        base_total = 100 * (navs[0][-1] - 1)
        res[w] = {
            str(k): {
                "total": round(100 * (navs[k][-1] - 1), 1),
                "delta": round(100 * (navs[k][-1] - 1) - base_total, 1),
                "trades": trades[k],
                "cooldown_days_active": cooling[k],
                **_metrics(navs[k]),
            }
            for k in grid
        }
        row = "  ".join(
            f"k{k}:{res[w][str(k)]['total']:+.1f}({res[w][str(k)]['delta']:+.1f})" for k in grid
        )
        print(f"  {w:<6} {row}")
        print(
            f"  {'':<6} k0={res[w]['0']['total']:+.1f} (B11 P1 {B11_P1[w]:+.1f})"
            f"  mdd/sr {res[w]['0']['mdd']}/{res[w]['0']['sharpe']}"
        )

    verdicts: dict[str, dict] = {}
    for k in grid:
        if k == 0:
            continue
        d = {w: res[w][str(k)]["delta"] for w in WINS}
        dv = res["valid"][str(k)]["mdd"] - res["valid"]["0"]["mdd"]
        ds = res["valid"][str(k)]["sharpe"] - res["valid"]["0"]["sharpe"]
        verdicts[str(k)] = {
            "deltas": d,
            "k1": all(d[w] >= -0.5 for w in ("OOS2", "train", "valid")),
            "k2": sum(d[w] for w in ("OOS2", "train", "valid")) > 0,
            "k3": d["long"] >= 0,
            "k4": (dv >= 3.0 or ds >= 0.10)
            and (res["long"][str(k)]["mdd"] >= res["long"]["0"]["mdd"] - 1.0),
            "valid_mdd_delta": round(dv, 1),
            "valid_sharpe_delta": round(ds, 2),
        }
    for k in grid:
        if k == 0:
            continue
        v = verdicts[str(k)]
        neighbors = [n for n in (k - 1, k + 1) if n in grid and n != 0]
        v["k5"] = any(
            verdicts.get(str(n), {}).get("k1") and verdicts.get(str(n), {}).get("k2")
            for n in neighbors
        )
        v["pass"] = all(v[x] for x in ("k1", "k2", "k3", "k4", "k5"))
    adopted = min((k for k in grid if k and verdicts[str(k)]["pass"]), default=None)

    print("\n## verdict (H-PARK-C · frozen K1-K5)")
    for k in grid:
        if k == 0:
            continue
        v = verdicts[str(k)]
        flags = " ".join(f"{n}{'ok' if v[n] else 'X'}" for n in ("k1", "k2", "k3", "k4", "k5"))
        deltas = {w: round(x, 1) for w, x in v["deltas"].items()}
        print(
            f"  k{k}: {flags}  d(valid mdd/sr)={v['valid_mdd_delta']:+.1f}/{v['valid_sharpe_delta']:+.2f}"
            f"  deltas={deltas}"
        )
    print(
        f"  adopted: {'k' + str(adopted) if adopted is not None else 'none -> incumbent k0 stays'}"
    )

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "parking_cooldown_2026-09-13.json").write_text(
            json.dumps(
                {
                    "tag": "parking-cooldown-2026-09-13",
                    "prereg": "docs/designs/parking-cooldown-prereg-2026-09-13.md",
                    "grid": list(grid),
                    "results": res,
                    "verdicts": verdicts,
                    "adopted": adopted,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
