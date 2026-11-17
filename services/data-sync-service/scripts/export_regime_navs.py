#!/usr/bin/env python3
"""Export exact daily NAVs for the regime-switch candidates (read-only).

Strategies:
  - homeport_m30  -> M30 (70% harbor / 30% B3)
  - homeport      -> M50 (50/50, starport base)
  - harbor        -> Harbor (Live base)
  - starship      -> satellite standalone (for starport blend)
  - starship_robust -> H2-a25 (satellite + 25% H2 + 75% B3)
  - starport      -> blended from homeport + starship (w=0.2)

Outputs (default data/backtest_reports/regime_navs):
  <strategy>.json      raw timeline payload (rows)
  daily_navs.csv       date,h2a25,starport,m30,harbor,m50,satellite,b3
  meta.json            start/end/strategy tags

B3 is loaded from the audit daily panel if available; otherwise left blank.
No orders, no Live writes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DEFAULT = ROOT / "data" / "backtest_reports" / "regime_navs"
STRATEGIES = ("homeport_m30", "homeport", "harbor", "starship", "starship_robust")


def _load_cached(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2021-08-02")
    ap.add_argument("--end", default="2026-08-07")
    ap.add_argument("--out", default=str(OUT_DEFAULT))
    ap.add_argument("--resume", action="store_true", help="reuse existing <strategy>.json")
    ap.add_argument("--strategies", default=",".join(STRATEGIES))
    args = ap.parse_args()

    from data_sync_service.api import backtest_routes as br
    from data_sync_service.service.starport import blend_starport_timeline

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    wanted = [s.strip() for s in args.strategies.split(",") if s.strip()]
    payloads: dict[str, dict] = {}

    for strat in wanted:
        path = out / f"{strat}.json"
        cached = _load_cached(path) if args.resume else None
        if cached is not None and cached.get("start") == args.start and cached.get("end") == args.end:
            print(f"{strat}: cached rows={len(cached.get('rows') or [])}", flush=True)
            payloads[strat] = cached
            continue
        print(f"{strat}: building {args.start}~{args.end} ...", flush=True)
        result, _ = br._get_or_build_timeline(args.start, args.end, strategy=strat)
        rows = result.get("rows") or []
        rec = {
            "start": args.start,
            "end": args.end,
            "strategy": strat,
            "mode": result.get("mode"),
            "tag": result.get("tag"),
            "summary": result.get("summary"),
            "rows": rows,
        }
        path.write_text(json.dumps(rec, ensure_ascii=False))
        payloads[strat] = rec
        print(f"{strat}: rows={len(rows)}", flush=True)

    # starport = homeport (M50) x satellite overlay w=0.2
    if "homeport" in payloads and "starship" in payloads:
        print("starport: blending homeport + starship ...", flush=True)
        sp = blend_starport_timeline(payloads["homeport"], payloads["starship"], sat_weight=0.2)
        rec = {
            "start": args.start,
            "end": args.end,
            "strategy": "starport",
            "mode": sp.get("mode"),
            "summary": sp.get("summary"),
            "rows": sp.get("rows") or [],
        }
        (out / "starport.json").write_text(json.dumps(rec, ensure_ascii=False))
        payloads["starport"] = rec
        print(f"starport: rows={len(rec['rows'])}", flush=True)

    def series(name: str, value_key: str = "navSingle") -> pd.Series:
        rows = payloads.get(name, {}).get("rows") or []
        return pd.Series(
            {str(r.get("date")): float(r.get(value_key) or 1.0) for r in rows if r.get("date")}
        ).sort_index()

    df = pd.DataFrame(
        {
            "h2a25": series("starship_robust"),
            "starport": series("starport"),
            "m30": series("homeport_m30"),
            "harbor": series("harbor"),
            "m50": series("homeport"),
            "satellite": series("starship", "satNav"),
        }
    )
    df.index.name = "date"

    # Optional B3 from the audit compare panel (exact through its end date).
    b3_candidates = [
        Path.home() / "Projects/wealth-ideas/scratch/compare/compare_nav.npz",
        Path.home() / "Projects/wealth-ideas/karios-audit-2026-10/scratch/compare_nav.npz",
    ]
    for p in b3_candidates:
        if p.exists():
            z = pd.read_pickle(p) if p.suffix == ".pkl" else None
            if z is None:
                import numpy as np

                z = np.load(p, allow_pickle=True)
            if "cal" in z and "b3" in z:
                cal = pd.to_datetime(z["cal"]).strftime("%Y-%m-%d")
                df["b3"] = pd.Series(z["b3"], index=cal).reindex(df.index)
                print(f"b3: loaded {p}", flush=True)
                break

    # Forward-fill only for comparison; leave missing at the tail.
    df = df.sort_index()
    df.to_csv(out / "daily_navs.csv", float_format="%.6f")
    (out / "meta.json").write_text(
        json.dumps(
            {
                "start": args.start,
                "end": args.end,
                "strategies": wanted + ["starport"],
                "rows": int(len(df)),
                "tail": str(df.index[-1]) if len(df) else None,
                "note": "starport = homeport(M50) x satellite(w=0.2); b3 from compare_nav if present",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"wrote {out/'daily_navs.csv'} rows={len(df)} tail={df.index[-1] if len(df) else None}")


if __name__ == "__main__":
    main()
