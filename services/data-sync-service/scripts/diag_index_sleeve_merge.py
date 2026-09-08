"""Sleeve-merge replay: inject 000688-MA daily returns into twin core slice.

Construction (frozen §6): core'[t] = (1-w)*core[t] + w*idxMA[t], w in {0.1, 0.2};
idxMA uses §2 rule (bull at close -> position next session, 5bps/side on
switches), daily returns approximated close-to-close x prior-day position
(declared non-Live-grade fill). Sat leg + opp blend untouched.
Compares fused tot/maxDD/sr per window vs frozen realistic twin.
Read-only, saves nothing.
"""
from __future__ import annotations

import datetime as _dt
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from compare_twin_realistic import HABIT, REAL, _merge  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.pick_strong_track import (  # noqa: E402
    build_mom_compare_timeline,
    build_twin_star_timeline,
    fetch_etf_closes,
)
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
CODE = "000688.SH"
WEIGHTS = (0.1, 0.2)
COST_SIDE = 0.0005


def load_index(code: str) -> list[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close FROM index_daily "
            "WHERE ts_code=%s ORDER BY trade_date",
            (code,),
        )
        rows = [{"date": str(r[0]), "close": float(r[1])} for r in cur.fetchall()]
    closes = [r["close"] for r in rows]
    for i, r in enumerate(rows):
        ma20 = sum(closes[i - 19:i + 1]) / 20 if i >= 19 else None
        ma60 = sum(closes[i - 59:i + 1]) / 60 if i >= 59 else None
        r["bull"] = bool(ma20 is not None and ma60 is not None
                         and r["close"] > ma20 and ma20 > ma60)
    # daily strategy returns: pos decided at close t-1 applies to t-1->t move
    bydate = {r["date"]: i for i, r in enumerate(rows)}
    rets: dict[str, float] = {}
    prev_pos, prev_close = 0, None
    for r in rows:
        i = bydate[r["date"]]
        pos = (1 if rows[i - 1]["bull"] else 0) if i >= 1 else 0
        if prev_close is not None:
            raw = r["close"] / prev_close - 1
            cost = COST_SIDE if pos != prev_pos else 0.0
            rets[r["date"]] = pos * raw - cost
        prev_pos, prev_close = pos, r["close"]
    return rows, rets


def build_window(start: str, end: str, ctx, etf_close):
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    cn_data = BacktestData(cfg)
    cn_run = simulate(cfg, cn_data)
    cfg_hk = BacktestConfig(start_date=start, end_date=end,
                            **{**HK_S3_CONFIG, **REAL})
    hk_data = BacktestData(cfg_hk)
    hk_run = simulate(cfg_hk, hk_data)
    merged, closes, cal = _merge(cn_run, cn_data, hk_run, hk_data)
    sat = replay_sgap_from_context(ctx, start=start, end=end, **HABIT)
    core = build_mom_compare_timeline(
        calendar=cal, positions_by_day=merged,
        close_by_ts_day=closes, etf_close=etf_close)
    twin = build_twin_star_timeline(
        core_rows=core["rows"], core_summary=core["summary"],
        sat_rows=sat["rows"])
    return core, sat, twin


def sr_of(navs: list[float]) -> float:
    r = [navs[i + 1] / navs[i] - 1 for i in range(len(navs) - 1)]
    if len(r) < 3:
        return float("nan")
    mu = sum(r) / len(r)
    var = sum((x - mu) ** 2 for x in r) / len(r)
    return mu / math.sqrt(var) * math.sqrt(252) if var > 0 else float("nan")


def main() -> int:
    _rows, idx_rets = load_index(CODE)
    print(f"[universe] {CODE}: {len(_rows)} rows", flush=True)
    print("loading sgap context + etf closes ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    etf_close = fetch_etf_closes()
    print("window | leg | fused% | maxDD | sr | (base: realistic twin)", flush=True)
    for w, (s, e) in WINDOWS.items():
        print(f"--- {w} ...", flush=True)
        core, sat, twin = build_window(s, e, ctx, etf_close)
        sm = twin["summary"]
        base_navs = [float(r["navSingle"]) for r in twin["rows"]]
        print(f"{w:6s} | base | {sm['fusedPct']:+7.1f} | "
              f"{sm['maxDdFusedPct']:5.1f} | {sr_of(base_navs):+.2f}")
        core_navs = [float(r["navSingle"]) for r in core["rows"]]
        core_dates = [str(r["date"])[:10] for r in core["rows"]]
        for wt in WEIGHTS:
            mod_rows = []
            nav = 1.0
            for i, r in enumerate(core["rows"]):
                rc = core_navs[i] / core_navs[i - 1] - 1 if i else 0.0
                ri = idx_rets.get(core_dates[i], 0.0)
                nav *= 1.0 + (1 - wt) * rc + wt * ri
                rr = dict(r)
                rr["navSingle"] = round(nav, 6)
                mod_rows.append(rr)
            twin2 = build_twin_star_timeline(
                core_rows=mod_rows, core_summary=core["summary"],
                sat_rows=sat["rows"])
            sm2 = twin2["summary"]
            navs2 = [float(r["navSingle"]) for r in twin2["rows"]]
            peak, mdd = 1.0, 0.0
            for v in navs2:
                peak = max(peak, v)
                mdd = min(mdd, v / peak - 1)
            print(f"{w:6s} | w={wt:.1f} | "
                  f"{(navs2[-1]-1)*100:+7.1f} | {mdd*100:5.1f} | {sr_of(navs2):+.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
