"""Combination diagnostics: index-MA x realistic twin (pre-registered, read-only).

D1 gate: twin next-session returns grouped by index bull/bear state.
D2 sleeve asset: corr(index-MA daily returns, twin daily returns) + standalone.
D3 idle buy-hold: buy-hold maxDD per window vs twin maxDD.
Diagnosis pool OOS2+train; valid confirm-only. Saves nothing.
"""
from __future__ import annotations

import datetime as _dt
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
UNIVERSE = ("000300.SH", "000688.SH")


def load_index(code: str) -> list[dict]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, open, close FROM index_daily "
            "WHERE ts_code=%s ORDER BY trade_date",
            (code,),
        )
        rows = [{"date": r[0], "open": float(r[1]), "close": float(r[2])}
                for r in cur.fetchall()]
    closes = [r["close"] for r in rows]
    for i, r in enumerate(rows):
        ma20 = sum(closes[i - 19:i + 1]) / 20 if i >= 19 else None
        ma60 = sum(closes[i - 59:i + 1]) / 60 if i >= 59 else None
        r["bull"] = bool(ma20 is not None and ma60 is not None
                         and r["close"] > ma20 and ma20 > ma60)
    return rows


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else float("nan")


def pearson(a: list[float], b: list[float]) -> float:
    n = len(a)
    if n < 3:
        return float("nan")
    ma, mb = mean(a), mean(b)
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va == 0 or vb == 0:
        return float("nan")
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (va ** 0.5 * vb ** 0.5)


def twin_daily_nav(start: str, end: str, ctx, etf_close) -> list[dict]:
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
        sat_rows=sat["rows"], opportunity=True)
    nav_rows = twin["rows"] if isinstance(twin, dict) else twin
    out = []
    for r in nav_rows:
        out.append({"date": str(r["date"])[:10], "nav": float(r["navSingle"])})
    return out


def main() -> int:
    idx = {c: load_index(c) for c in UNIVERSE}
    for c in UNIVERSE:
        print(f"[universe] {c}: {len(idx[c])} rows")
    print("loading sgap context + etf closes ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    etf_close = fetch_etf_closes()

    navs: dict[str, list[dict]] = {}
    for w, (s, e) in WINDOWS.items():
        print(f"--- building realistic twin {w} ...", flush=True)
        navs[w] = twin_daily_nav(s, e, ctx, etf_close)
    rets: dict[str, list[float]] = {}
    for w, rows in navs.items():
        rets[w] = [rows[i + 1]["nav"] / rows[i]["nav"] - 1
                   for i in range(len(rows) - 1)]

    bull_by_code_date: dict[str, dict[str, bool]] = {}
    for c in UNIVERSE:
        bull_by_code_date[c] = {str(r["date"]): r["bull"] for r in idx[c]}

    print("\n== D1 gate: twin next-session ret by index state (pp/session) ==")
    for c in UNIVERSE:
        bd = bull_by_code_date[c]
        for grp, ws in (("OOS2+train", ("OOS2", "train")), ("valid", ("valid",))):
            bull_r, bear_r = [], []
            for w in ws:
                rows = navs[w]
                for i in range(len(rows) - 1):
                    st = bd.get(rows[i]["date"])
                    if st is None:
                        continue
                    (bull_r if st else bear_r).append(rets[w][i] * 100)
            tot = len(bull_r) + len(bear_r)
            print(f"{c} {grp}: bull n={len(bull_r)} mu={mean(bull_r):+.4f} | "
                  f"bear n={len(bear_r)} mu={mean(bear_r):+.4f} | "
                  f"gap={mean(bull_r)-mean(bear_r):+.4f} | "
                  f"bear_cov={len(bear_r)/tot:.2f}" if tot else f"{c} {grp}: empty")

    print("\n== D2 sleeve: corr(index-MA daily, twin daily) ==")
    for c in UNIVERSE:
        rows = idx[c]
        sig = {str(r["date"]): (1 if r["bull"] else 0) for r in rows}
        for grp, ws in (("OOS2+train", ("OOS2", "train")), ("valid", ("valid",))):
            ma_r, tw_r = [], []
            for w in ws:
                days = navs[w]
                for i in range(len(days) - 1):
                    p = sig.get(days[i]["date"])
                    if p is None:
                        continue
                    cl = {str(r["date"]): r["close"] for r in rows}
                    d0, d1 = days[i]["date"], days[i + 1]["date"]
                    if d0 in cl and d1 in cl and cl[d0] != 0:
                        ma_r.append(p * (cl[d1] / cl[d0] - 1) * 100)
                        tw_r.append(rets[w][i] * 100)
            print(f"{c} {grp}: corr={pearson(ma_r, tw_r):+.3f} n={len(ma_r)}")

    print("\n== D3 idle buy-hold maxDD per window (pct) ==")
    for w, (s, e) in WINDOWS.items():
        rows = navs[w]
        peak, mdd = 1.0, 0.0
        for r in rows[1:]:
            peak = max(peak, r["nav"] / rows[0]["nav"])
            mdd = min(mdd, (r["nav"] / rows[0]["nav"]) / peak - 1)
        line = f"{w} twinDD={mdd*100:.1f} |"
        for c in UNIVERSE:
            seg = [r for r in idx[c] if s <= str(r["date"]) <= e]
            pk, bd = seg[0]["close"], 0.0
            for r in seg[1:]:
                pk = max(pk, r["close"])
                bd = min(bd, r["close"] / pk - 1)
            line += f" {c}BHdd={bd*100:.1f} |"
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
