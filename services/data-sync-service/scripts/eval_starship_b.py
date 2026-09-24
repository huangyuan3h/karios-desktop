#!/usr/bin/env python3
"""稳健 B 星舰 = 卫星 + cashShare(T-1) x (parking = BOND+黄金+纳指 inverse-vol).

vs 现行 H2-a25 = 卫星 + cashShare(T-1) x (25% H2 套筒 + 75% B3).

Park leg B: monthly inverse-vol over {511260 国债, 518880 黄金, 513100 纳指}
(1/3 each until warmup), 5bp/side. Same satellite book and cash share as a25.

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_starship_b.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import (  # noqa: E402
    _compose,
    _repo_nav,
    _sat_book,
    _sleeve_nav,
    _true_cash_share,
)
from eval_twin_star_parking import _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.harbor import HYST_BAND, load_etf_closes  # noqa: E402
from data_sync_service.service.homeport import (  # noqa: E402
    _series_on_cal,
    _vol_at,
    inverse_vol_weights,
    load_risk_closes,
    risk_budget_nav,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    A25_B3_WEIGHT,
    A25_SLEEVE_WEIGHT,
)

STRESS = ("2022-01-01", "2023-12-31")
ALL = {**WINDOWS, "stress": STRESS}
PARK_B = ("511260.SH", "518880.SH", "513100.SH")


def load_raw(ts: str) -> dict[str, float]:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close, adj_factor FROM daily WHERE ts_code=%s ORDER BY trade_date",
            (ts,),
        )
        rows = cur.fetchall()
    return {
        d.isoformat(): float(c) * (float(a) if a is not None else 1.0)
        for d, c, a in rows
        if c is not None
    }


def blend_b(series_map, cal, *, lookback=60, cost=0.0005) -> list[float]:
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
    px = load_etf_closes()
    risk = load_risk_closes()
    park_sm = {ts: load_raw(ts) for ts in PARK_B}
    a, b = A25_SLEEVE_WEIGHT, A25_B3_WEIGHT
    print("星舰 B (park = BOND+黄金+纳指 inv-vol) vs H2-a25（现行）\n", flush=True)
    print(f"{'窗口':<9}{'H2-a25':>26}{'星舰B':>26}{'纯B3':>26}{'纯B星舰停放腿':>26}")
    for wname in ("OOS2", "train", "valid", "long", "stress", "holdout"):
        s, e = ALL[wname]
        print(f"  computing {wname} ...", flush=True)
        sat = _sat_book(s, e)
        dates, sat_nav = sat["dates"], sat["nav"]
        b3 = risk_budget_nav(risk, dates)
        repo = _repo_nav(dates)
        sleeve = _sleeve_nav(px, dates, hyst_band=HYST_BAND)
        calB = sorted({d for d in park_sm[PARK_B[0]] if d in set(dates)})
        parkB = blend_b(park_sm, calB)
        # align parkB to dates
        byd = dict(zip(calB, parkB, strict=True))
        last, navB = 1.0, []
        for d in dates:
            last = byd.get(d, last)
            navB.append(last)
        n = min(len(dates), len(b3), len(navB), len(sleeve), len(repo))
        dates, sat_nav, b3, navB, sleeve, repo = (
            dates[:n], sat_nav[:n], b3[:n], navB[:n], sleeve[:n], repo[:n],
        )
        cash = _true_cash_share(sat, from_rows=True)
        w = [0.0] * n
        for i in range(1, n):
            w[i] = cash[i - 1]
        parkA = [1.0]
        for t in range(1, n):
            rs = sleeve[t] / sleeve[t - 1] - 1 if sleeve[t - 1] else 0.0
            rb = b3[t] / b3[t - 1] - 1 if b3[t - 1] else 0.0
            parkA.append(parkA[-1] * (1.0 + a * rs + b * rb))
        navA = _compose(sat_nav, w, parkA, 5.0)
        navStarB = _compose(sat_nav, w, navB, 5.0)
        navPure = _compose(sat_nav, w, b3, 5.0)
        def cell(nav):
            m = _stats(nav)
            return f"{m['total_pct']:+7.1f}/{m['max_dd']:+6.1f}/{m['sharpe']:4.2f}"

        print(f"{wname:<9}{cell(navA):>26}{cell(navStarB):>26}{cell(navPure):>26}{cell(navB):>26}", flush=True)
    print("\n列 = 总收益%/maxDD%/Sharpe。星舰B = 卫星 + cashShare(T-1) x (BOND+黄金+纳指 逆波动率)。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
