#!/usr/bin/env python3
"""G1: market-regime vs satellite fills — description only, no strategy change.

For each base-habit fill (C1 3% same_1430, body=3, day-3 1430 exit), label the
ENTRY-day market regime from 000001.SH (known at entry, no lookahead):
  trend: 20d index return >+3% up / <-3% down / else choppy
  vol:   20d realized vol above/below full-sample median
Report per regime x window: n fills, mean fill net pnl, hit rate; plus gate
(open-day) frequency per regime. Selection+valid windows AND holdout shown
separately (holdout descriptive only). Read-only. Prints tables, saves nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

SEGMENTS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "holdout": ("2026-08-10", "2026-09-03"),
}


def _load_index_closes() -> dict[str, float]:
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[str, float] = {}
    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_date, close FROM index_daily "
        "WHERE ts_code = '000001.SH' AND trade_date >= '2024-06-01' ORDER BY trade_date"
    )
    for d, c in cur.fetchall():
        if c and c > 0:
            out[str(d)[:10]] = float(c)
    conn.close()
    return out


def main() -> int:
    idx = _load_index_closes()
    days = sorted(idx)
    px = np.array([idx[d] for d in days])
    ret20 = px[20:] / px[:-20] - 1
    vol20 = np.array([
        float(np.std(np.log(px[i - 18: i + 1] / px[i - 19: i]))) * (252 ** 0.5)
        for i in range(19, len(px))
    ])
    vol_med = float(np.median(vol20))
    reg: dict[str, tuple[str, str]] = {}
    for i, d in enumerate(days[20:]):
        r = ret20[i]
        trend = "up" if r > 0.03 else ("down" if r < -0.03 else "choppy")
        reg[d] = (trend, "high" if vol20[i] > vol_med else "low")
    print(f"index days={len(days)} vol_median={vol_med:.3f}")

    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-09-03")
    acc: dict[str, dict[str, list[float]]] = {}
    gate_days: dict[str, dict[str, int]] = {}
    all_days: dict[str, dict[str, int]] = {}
    for seg, (s, e) in SEGMENTS.items():
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430, fill_hhmm="1430",
            exit_hhmm="1430", max_open_to_1430_pct=0.03,
        )
        for b in sat["blotter"]:
            if b.get("kind") != "fill" or b.get("pnlPct") is None:
                continue
            key = f"{reg.get(str(b.get('entryDate')), ('?', '?'))[0]}"
            key = f"{key}/{reg.get(str(b.get('entryDate')), ('?', '?'))[1]}"
            acc.setdefault(seg, {}).setdefault(key, []).append(float(b["pnlPct"]))
        for r in sat["rows"]:
            rk = reg.get(str(r["date"]))
            if not rk:
                continue
            key = f"{rk[0]}/{rk[1]}"
            all_days.setdefault(seg, {})[key] = all_days.setdefault(seg, {}).get(key, 0) + 1
            if r.get("gateOpen"):
                gate_days.setdefault(seg, {})[key] = gate_days.setdefault(seg, {}).get(key, 0) + 1

    order = ["up/low", "up/high", "choppy/low", "choppy/high", "down/low", "down/high"]
    for seg in SEGMENTS:
        print(f"\n## {seg} fills by entry regime (mean pnl / hit / n)")
        tot_n = sum(len(v) for v in acc.get(seg, {}).values())
        for k in order:
            v = acc.get(seg, {}).get(k, [])
            m = float(np.mean(v)) if v else 0.0
            hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
            g = gate_days.get(seg, {}).get(k, 0)
            d = all_days.get(seg, {}).get(k, 0)
            print(f"  {k:<12} {m:+6.2f}% / {hit:4.0f}% / n={len(v):4d}   gate {g}/{d}")
        print(f"  total fills n={tot_n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
