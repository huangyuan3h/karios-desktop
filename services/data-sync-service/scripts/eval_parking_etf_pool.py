#!/usr/bin/env python3
"""Parking-asset pool screen: more ETFs for the idle-cash parking sleeve.

Goal (returns-first): find ETFs whose momentum-parking sleeve beats B3 on return
while keeping drawdown acceptable. Universe = liquid ETFs with >=800 daily rows
(2023-01+ cohort), physical ETFs only (exclude 货币 1590xx / 债 511xxx variants
that are just cash-ish ballast unless explicitly labeled).

For each ETF, a single-asset causal parking rule:
  park in the ETF when close(t-1) >= MA200(t-1), else REPO; trail8 from prev close.
Also the same rule as a cross-sectional top-N sleeve over the pool.

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_parking_etf_pool.py
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
from data_sync_service.service.harbor import LOOKBACK, MA_WINDOW  # noqa: E402

STRESS = ("2022-01-01", "2023-12-31")
ALL = {**WINDOWS, "stress": STRESS}
# liquid ETFs, physical (exclude 货币/短融 1590/159003 etc., and pure-bond 511)
POOL = {
    "510300.SH": "沪深300", "510500.SH": "中证500", "510510.SH": "中证500低波",
    "159915.SZ": "创业板", "518880.SH": "黄金", "513100.SH": "纳指100",
    "513110.SH": "纳指100b", "513180.SH": "恒生科技", "513350.SH": "油气QDII",
    "512480.SH": "半导体", "515880.SH": "通信", "511260.SH": "10年国债",
    "511380.SH": "可转债", "518800.SH": "黄金b",
}


def load_etf(ts: str) -> dict[str, float]:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close, adj_factor FROM daily WHERE ts_code=%s ORDER BY trade_date",
            (ts,),
        )
        rows = cur.fetchall()
    # ETF rows are raw (adj_factor NULL); use raw close consistently.
    return {r[0].isoformat(): float(r[1]) for r in rows if r[1]}



def _ma(series: list[float], i: int, n: int) -> float | None:
    if i < n - 1:
        return None
    return sum(series[i - n + 1:i + 1]) / n


def park_nav(closes: dict[str, float], cal: list[str], *, trail=8.0) -> list[float]:
    """Causal MA200+trail on the *full* series, evaluated on ``cal``.

    MA200 uses history strictly before the decision day (not window-relative), so
    windows with <200 own bars still get a valid signal from prior data.
    """
    all_days = sorted(closes)
    idx = {d: i for i, d in enumerate(all_days)}
    vals = [closes[d] for d in all_days]
    ma = [None] * len(all_days)
    s = 0.0
    for i, v in enumerate(vals):
        s += v
        if i >= MA_WINDOW:
            s -= vals[i - MA_WINDOW]
        if i >= MA_WINDOW - 1:
            ma[i] = s / MA_WINDOW
    nav = [1.0]
    holding = False
    peak = 0.0
    for d in cal:
        i = idx[d]
        if i == 0:
            nav.append(nav[-1]); continue
        prev, prev_ma, day = vals[i - 1], ma[i - 1], vals[i]
        above = prev_ma is not None and prev >= prev_ma
        if holding:
            peak = max(peak, prev)
            if not above or (peak > 0 and prev < peak * (1 - trail / 100)):
                holding = False; peak = 0.0
        if not holding and above:
            holding = True; peak = prev
        nav.append(nav[-1] * (day / prev if holding else 1.0))
    return nav


def main() -> int:
    px = {ts: load_etf(ts) for ts in POOL}
    print("single-ETF causal parking (MA200 + trail8), B3-lite comparison\n", flush=True)
    header = f"{'ETF':<14}{'OOS2':>16}{'train':>16}{'valid':>16}{'long':>16}"
    print(header)
    rows = []
    for ts, name in POOL.items():
        closes = px.get(ts) or {}
        cells = []
        for w in ("OOS2", "train", "valid", "long"):
            s, e = ALL[w]
            cal = sorted(d for d in closes if s <= d <= e)
            nav = park_nav(closes, cal)
            m = _stats(nav)
            cells.append(f"{m['total_pct']:+6.1f}/{m['max_dd']:+5.1f}")
        # sort key = long total
        s, e = ALL["long"]
        cal = sorted(d for d in closes if s <= d <= e)
        lt = _stats(park_nav(closes, cal))["total_pct"]
        rows.append((lt, ts, name, cells))
        print(f"{name:<14}" + "".join(f"{c:>16}" for c in cells), flush=True)
    print("\n列 = 总收益%/maxDD%（long=2021-08+，覆盖不到则从上市起）。货币/短融未纳入。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
