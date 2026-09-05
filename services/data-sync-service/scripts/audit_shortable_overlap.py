#!/usr/bin/env python3
"""Stage 2: shortable overlap audit — do strict scoop-short signals live in
borrowable names?

Shortable(D) = cn_margin_detail has a row for ts_code with trade_date in
(S-10d, S] where S = signal date (as-of safe: only past rows; 10d tolerance
covers sparse sync days). Reports coverage overall / by year / by board,
plus rqmcl>0 share as a DESCRIPTIVE column (gate uses row-exists only).

Gate (design §3.2): coverage >= 50% else close the building.
Read-only vs Postgres. Saves nothing.
"""
from __future__ import annotations

import sys
from datetime import date as _date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from repro_scoop_short import _load_bars, detect  # noqa: E402


def _load_margin() -> dict[str, list[str]]:
    import psycopg

    from data_sync_service.config import get_settings

    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT ts_code, trade_date, rqmcl FROM cn_margin_detail WHERE trade_date >= '2023-01-01'"
    )
    per_ts: dict[str, list] = {}
    for ts, d, rqmcl in cur.fetchall():
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts.setdefault(str(ts), []).append((ds, rqmcl))
    conn.close()
    for v in per_ts.values():
        v.sort()
    return per_ts


def _shortable(rows: list, sig_date: str) -> tuple[bool, bool]:
    """(row_exists_in_window, rqmcl_positive_on_latest)."""
    s = _date.fromisoformat(sig_date)
    best = None
    for ds, rqmcl in rows:
        dd = _date.fromisoformat(ds)
        if dd <= s and (s - dd).days <= 10:
            best = (ds, rqmcl)
    if best is None:
        return (False, False)
    try:
        return (True, float(best[1] or 0) > 0)
    except (TypeError, ValueError):
        return (True, False)


def main() -> int:
    print("loading bars ...", flush=True)
    per_ts = _load_bars()
    print("loading margin rows ...", flush=True)
    margin = _load_margin()
    print(f"margin names: {len(margin)}", flush=True)
    tot = short = short_rqmcl = 0
    by_year: dict[str, list[int]] = {}
    by_board: dict[str, list[int]] = {"main": [0, 0], "20cm": [0, 0]}
    for ts, s in per_ts.items():
        for sig in detect(s):
            if not (sig["ret60"] > 0.40 and sig["vr"] > 1.2):
                continue
            if sig["date"] < "2023-01-01":
                continue
            rows = margin.get(ts)
            ok, rq = _shortable(rows, sig["date"]) if rows else (False, False)
            tot += 1
            short += 1 if ok else 0
            short_rqmcl += 1 if rq else 0
            y = by_year.setdefault(sig["date"][:4], [0, 0])
            y[1] += 1
            y[0] += 1 if ok else 0
            b = by_board["20cm" if ts.split(".")[0].startswith(("3", "68")) else "main"]
            b[1] += 1
            b[0] += 1 if ok else 0
    cov = short / max(1, tot)
    print(f"\nstrict signals (margin era): {tot}")
    print(f"shortable coverage: {short}/{tot} = {cov:.1%}")
    print(f"rqmcl>0 share (descriptive): {short_rqmcl}/{tot} = {short_rqmcl/max(1,tot):.1%}")
    print("by year:", {y: f"{v[0]}/{v[1]}={v[0]/max(1,v[1]):.0%}" for y, v in sorted(by_year.items())})
    print("by board:", {b: f"{v[0]}/{v[1]}={v[0]/max(1,v[1]):.0%}" for b, v in by_board.items()})
    print()
    print("STAGE-2 VERDICT:", "PASS (actionable)" if cov >= 0.50 else "FAIL (<50%) — close the building")
    return 0 if cov >= 0.50 else 1


if __name__ == "__main__":
    raise SystemExit(main())
