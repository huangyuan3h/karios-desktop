#!/usr/bin/env python3
"""Parking-asset blend frontier: bond ballast + offense ETFs, inverse-vol monthly.

Search for a parking asset with return > B3 and drawdown < the H2 sleeve (-29%).
Each candidate = monthly inverse-vol risk budget over {511260 BOND, offense ETF(s)}.
Causal (vol through i-1, weight applied to i), 5bp/side on turnover.

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_parking_blend_frontier.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_twin_star_parking import _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.homeport import (  # noqa: E402
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    risk_budget_nav,
    load_risk_closes,
)

STRESS = ("2022-01-01", "2023-12-31")
ALL = {**WINDOWS, "stress": STRESS}
BOND = "511260.SH"
OFFENSE = {
    "黄金": "518880.SH",
    "纳指": "513100.SH",
    "半导体": "512480.SH",
    "创业板": "159915.SZ",
    "可转债": "511380.SH",
}
# candidate blends: name -> list of offense legs (bond auto-added), equal-risk-budget
CANDIDATES = {
    "BOND only": [],
    "BOND+黄金": ["黄金"],
    "BOND+纳指": ["纳指"],
    "BOND+可转债": ["可转债"],
    "BOND+黄金+纳指": ["黄金", "纳指"],
    "BOND+黄金+纳指+可转债": ["黄金", "纳指", "可转债"],
    "BOND+纳指+可转债": ["纳指", "可转债"],
}


def load_raw(ts: str) -> dict[str, float]:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT trade_date, close, adj_factor FROM daily WHERE ts_code=%s ORDER BY trade_date", (ts,))
        rows = cur.fetchall()
    # ETFs: adj_factor may be NULL (raw). 518880 has adj; use close*adj if present else close.
    out = {}
    for d, c, a in rows:
        if c is None:
            continue
        out[d.isoformat()] = float(c) * (float(a) if a is not None else 1.0)
    return out


def blend_nav(series_map, cal, *, lookback=60, cost=0.0005) -> list[float]:
    ts_list = list(series_map)
    ser = {ts: _series_on_cal(series_map[ts], cal) for ts in ts_list}
    w_by_i = {}
    for i in range(len(cal)):
        if i < lookback:
            w_by_i[i] = {ts: 1.0 / len(ts_list) for ts in ts_list}
        elif cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
        else:
            vol = {ts: (_vol_at(ser[ts], i, lookback) or 1e-9) for ts in ts_list}
            w_by_i[i] = inverse_vol_weights(vol)
    nav = [1.0]
    cur = w_by_i[0]
    for i in range(1, len(cal)):
        r = sum(
            cur.get(ts, 0.0) * (ser[ts][i] / ser[ts][i - 1] - 1.0)
            for ts in ts_list
            if ser[ts][i - 1] and ser[ts][i]
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur:
            turn = sum(abs(w_by_i[i][ts] - cur.get(ts, 0.0)) for ts in ts_list) / 2.0
            nav[-1] *= 1.0 - cost * turn
            cur = w_by_i[i]
    return nav


def main() -> int:
    bond = load_raw(BOND)
    off = {k: load_raw(ts) for k, ts in OFFENSE.items()}
    b3_px = load_risk_closes()
    print("Parking-asset blend frontier (bond + offense, inverse-vol monthly)\n", flush=True)
    print(f"{'候选':<26}{'OOS2':>17}{'train':>17}{'valid':>17}{'long':>17}{'stress':>17}")
    rows = []
    for name, legs in CANDIDATES.items():
        sm = {BOND: bond}
        for lg in legs:
            sm[lg] = off[lg]
        cells = []
        for w in ("OOS2", "train", "valid", "long", "stress"):
            s, e = ALL[w]
            cal = sorted({d for d in bond if s <= d <= e})
            nav = blend_nav(sm, cal)
            nav = nav[: len([d for d in cal])]
            m = _stats(nav)
            cells.append(f"{m['total_pct']:+6.1f}/{m['max_dd']:+5.1f}")
        s, e = ALL["long"]
        cal = sorted({d for d in bond if s <= d <= e})
        lt = _stats(blend_nav(sm, cal))["total_pct"]
        rows.append((lt, name, cells))
    for lt, name, cells in sorted(rows, reverse=True):
        print(f"{name:<26}" + "".join(f"{c:>17}" for c in cells), flush=True)
    # B3 + H2 sleeve references
    s, e = ALL["long"]
    cal = sorted({d for d in bond if s <= d <= e})
    print(f"\n{'[ref] B3 as-is':<26}" + f"{_stats(risk_budget_nav(b3_px, sorted({d for mp in b3_px.values() for d in mp if s<=d<=e})))}")
    print("\n列 = 总收益%/maxDD%。目标：收益 > B3(+46) 且 maxDD 优于 H2 套筒(-29%)。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
