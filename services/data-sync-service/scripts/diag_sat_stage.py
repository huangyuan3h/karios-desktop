"""Satellite stage labeling (descriptive, pre-registered 2026-09-11).

Question: are satellite losses concentrated in a lifecycle stage
(blast-off -> distribution -> ebb), which linear IC can never see?

Method: frozen replay t1430_b3 (same_1430 fill, body=3, defaults) over
OOS2/train/valid. Every filled leg gets trailing-only stage labels at
entry; report n / avg pnl / hit% per bucket per window + pooled.

Stage vars (all PiT-safe, close data <= entry day):
  dd60    : 1 - close/high60            (<2% | 2-8% | >8%)
  rally20 : 20d return                  (<0 | 0-15% | >15%)
  blast   : days since trailing-20d ret >= +25%  (<=10 | 11-40 | >40/never)
  wein    : P vs MA20/60 (S2 advance | S3 distribution | S4 decline | S1 base)
  runup5  : 5d return                   (<3% | 3-10% | >10%)

No Live line at this step. Output decides only whether a stage-gate
hypothesis (H-SAT-STAGE) is worth pre-registering.

Usage:
    PYTHONPATH=src:scripts python3 scripts/diag_sat_stage.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-28"),
}


from data_sync_service.service.state_bucket_track import stage_labels as _labels  # noqa: E402


def main() -> int:
    import pandas as pd

    from data_sync_service.service.fin_panel import load_price_map, load_trade_calendar
    from data_sync_service.service.state_bucket_track import (
        FILL_SAME_1430,
        load_sgap_context,
        replay_sgap_from_context,
    )

    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    hist = {}
    for ts, days in px.items():
        hist[ts] = [days[d] for d in cal if d in days]

    legs = []
    for w, (s, e) in WINDOWS.items():
        ctx = load_sgap_context(s, e)
        res = replay_sgap_from_context(ctx, start=s, end=e,
                                       fill_mode=FILL_SAME_1430, fill_hhmm="1430",
                                       body=3)
        fills = [b for b in res["blotter"]
                 if b.get("kind") == "fill" and b.get("pnlPct") is not None]
        print(f"{w}: fills={len(fills)} sat={res['summary']['satPct']}%")
        for b in fills:
            legs.append({"w": w, "ts": b["ts"], "entry": b["entryDate"],
                         "pnl": b["pnlPct"]})
    print(f"total legs={len(legs)}")
    rows = []
    for leg in legs:
        ei = idx_of.get(leg["entry"], -1)
        if ei < 61:
            continue
        h = hist.get(leg["ts"])
        if not h:
            continue
        # map calendar idx -> position in this ts history (missing days safe)
        closes, ci = [], None
        seq = [d for d in cal[: ei + 1]]
        vals = [px[leg["ts"]].get(d) for d in seq]
        vals = [v for v in vals if v]
        if len(vals) < 61:
            continue
        lab = _labels(vals)
        if lab:
            rows.append({**leg, **lab})
    df = pd.DataFrame(rows)
    print(f"labeled={len(df)}")
    for var in ["dd60", "rally20", "blast", "wein", "runup5"]:
        print(f"\n== {var} ==")
        piv = df.groupby([var, "w"])["pnl"].agg(["count", "mean"]).round(2)
        print(piv.to_string())
        pool = df.groupby(var)["pnl"].agg(["count", "mean",
                                           lambda s: round((s > 0).mean() * 100, 1)])
        pool.columns = ["n", "avg", "hit%"]
        print("-- pooled --")
        print(pool.round(2).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
