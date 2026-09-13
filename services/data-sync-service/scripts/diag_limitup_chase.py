#!/usr/bin/env python3
"""T1 v0 (read-only): does chasing a limit-up board have positive expectancy?

Signal = day i closes limit-up (sealed) for a non-ST / non-BJ A-share.
Chase execution (honest next-day entry): buy i+1 open, earliest T+1 sell is
i+2 open, so mark to i+2 close (1 full extra day). Cost = CN round trip 30bp.

Reports, by window and by board height (consecutive limit-ups ending i) and
board type (10cm main vs 20cm 双创):
  - open premium  = open(i+1)/close(i) - 1        (what you pay to chase)
  - next-day move = close(i+1)/open(i+1) - 1      (T+0 impossible; reference)
  - chase net     = close(i+2)/open(i+1) - 1 - c  (T+1-compliant, the real test)
  - unfillable %  = i+1 opens one-word limit (can't get in)
Diagnostic only; no engine change, no Live. A pre-registered replay only if
chase net is robustly positive across windows.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_limitup_chase.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    _load_calendar,
    _load_rows,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
FULL_START = "2024-06-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
COST = 0.003


def _lim(ts: str) -> float:
    return 0.20 if str(ts).startswith(("3", "68")) else 0.10


def _sealed_up(bar: dict, ts: str) -> bool:
    pc = bar.get("pre_close")
    c, h = bar.get("close"), bar.get("high")
    if not pc or not c or not h or pc <= 0:
        return False
    return c >= pc * (1 + _lim(ts) - 0.004) and c >= h - 1e-6


def _one_word(bar: dict, ts: str) -> bool:
    pc = bar.get("pre_close")
    o, c, h, low = bar.get("open"), bar.get("close"), bar.get("high"), bar.get("low")
    if not pc or not o or c is None or h is None or low is None or pc <= 0:
        return False
    return o == h == low == c and c >= pc * (1 + _lim(ts) - 0.004)


def _agg(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0, "prem": None, "d1": None, "chase": None, "hit": None, "unfill": None}
    prem = np.array([r["prem"] for r in rows], dtype=float)
    d1 = np.array([r["d1"] for r in rows], dtype=float)
    chase = np.array([r["chase"] for r in rows], dtype=float)
    return {
        "n": len(rows),
        "prem": float(np.mean(prem)),
        "d1": float(np.mean(d1)),
        "chase": float(np.mean(chase)),
        "hit": float(np.mean(chase > 0) * 100),
        "unfill": float(np.mean([r["unfill"] for r in rows]) * 100),
    }


def _p(v: float | None) -> str:
    return "—" if v is None else f"{v * 100:+.2f}%"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Limit-up chase v0 diagnostic (read-only)\n", flush=True)
    print(f"loading daily rows {FULL_START}~{FULL_END} ...", flush=True)
    per_ts = _load_rows(FULL_START, FULL_END)
    cal = _load_calendar(FULL_START, FULL_END)
    idx_by_day = {d: i for i, d in enumerate(cal)}
    print(f"  {len(per_ts)} symbols, {len(cal)} sessions", flush=True)

    # Collect signal events: day i sealed limit-up, with i+1/i+2 available.
    events: list[dict] = []
    for ts, series in per_ts.items():
        bar_by_date = {r["date"]: r for r in series}
        run = 0
        for bar in series:
            if _sealed_up(bar, ts):
                run += 1
            else:
                run = 0
            if run == 0:
                continue
            day = bar["date"]
            ci = idx_by_day.get(day, -1)
            if ci < 0 or ci + 2 >= len(cal):
                continue
            b1 = bar_by_date.get(cal[ci + 1])
            b2 = bar_by_date.get(cal[ci + 2])
            if not b1 or not b2:
                continue
            o1, c1 = b1.get("open"), b1.get("close")
            c2 = b2.get("close")
            if not o1 or not c1 or not c2 or bar.get("close") is None:
                continue
            events.append({
                "date": day,
                "ts": ts,
                "board": ("20cm" if str(ts).startswith(("3", "68")) else "10cm"),
                "height": min(run, 3),
                "prem": o1 / bar["close"] - 1.0,
                "d1": c1 / o1 - 1.0,
                "chase": c2 / o1 - 1.0 - COST,
                "unfill": _one_word(b1, ts),
            })
    print(f"  raw events (all history): {len(events)}", flush=True)

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        wev = [ev for ev in events if s <= ev["date"] <= e]
        row: dict[str, dict] = {"ALL": _agg(wev)}
        for b in ("10cm", "20cm"):
            row[b] = _agg([ev for ev in wev if ev["board"] == b])
        for h in (1, 2, 3):
            row[f"height{h}"] = _agg([ev for ev in wev if ev["height"] == h])
        results[wname] = row
        print(f"\n=== {wname} ({s}~{e}) events={len(wev)} ===", flush=True)
        print(f"{'bucket':<10}{'n':>7}{'openPrem':>11}{'T+0 move':>11}{'chaseNet':>11}{'hit':>7}{'unfill':>8}")
        for k in ("ALL", "10cm", "20cm", "height1", "height2", "height3"):
            a = row[k]
            hit = "—" if a["hit"] is None else f"{a['hit']:.0f}%"
            unfill = "—" if a["unfill"] is None else f"{a['unfill']:.0f}%"
            print(f"{k:<10}{a['n']:>7}{_p(a['prem']):>11}{_p(a['d1']):>11}{_p(a['chase']):>11}{hit:>7}{unfill:>8}")

    print("\n## Chase net (close i+2 / open i+1 − 1 − 30bp)\n")
    print(f"{'bucket':<10}" + "".join(f"{w:>12}" for w in WINDOWS) + "  verdict")
    for k in ("ALL", "10cm", "20cm", "height1", "height2", "height3"):
        cells, vals = [], []
        for w in WINDOWS:
            a = results[w][k]
            cells.append("—" if a["chase"] is None else f"{a['chase'] * 100:+.2f}%")
            if a["chase"] is not None:
                vals.append(a["chase"])
        if len(vals) < 3:
            verdict = "underpowered"
        elif all(v > 0 for v in vals):
            verdict = "positive all 3"
        elif all(v < 0 for v in vals):
            verdict = "NEGATIVE all 3"
        else:
            verdict = "mixed"
        print(f"{k:<10}" + "".join(f"{c:>12}" for c in cells) + f"  {verdict}")

    payload = {
        "tag": "limitup-chase-2026-09-12",
        "protocol": (
            "read-only; signal = day i sealed limit-up (non-ST/BJ A-share, 10/20cm); "
            "chase entry i+1 open, T+1 exit at i+2 close, cost 30bp; report open premium, "
            "T+0 move, chase net, unfillable (one-word open), by board height."
        ),
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "limitup_chase_2026-09-12.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
