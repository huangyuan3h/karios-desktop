"""Index MA120: §7 pool rotation with SSE300/STAR50 on MA120 (§8, frozen).

Only the two index keys use MA120; the four ETFs stay on MA200.
Flag-off (no override) must reproduce §7 +idx numbers bit-for-bit:
OOS2 +80.9 / train +63.1 / valid +108.2, or the run stops.
Read-only, saves nothing.
"""
from __future__ import annotations

MA120 = {"SSE300": 120, "STAR50": 120}
SEVEN = {"OOS2": (80.9, 16.8), "train": (63.1, 12.6), "valid": (108.2, 15.3)}
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from compare_twin_realistic import HABIT, REAL, _merge  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service import pick_strong_track as pst  # noqa: E402
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
INDEX_KEYS = {"SSE300": "000300.SH", "STAR50": "000688.SH"}
BASE_MULTI_TS = dict(pst.MULTI_TS)


def load_index_closes(code: str) -> dict[str, float]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close FROM index_daily "
            "WHERE ts_code=%s ORDER BY trade_date",
            (code,),
        )
        return {str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}


def sr_of(navs: list[float]) -> float:
    r = [navs[i + 1] / navs[i] - 1 for i in range(len(navs) - 1)]
    if len(r) < 3:
        return float("nan")
    mu = sum(r) / len(r)
    var = sum((x - mu) ** 2 for x in r) / len(r)
    return mu / math.sqrt(var) * math.sqrt(252) if var > 0 else float("nan")


def run_window(start: str, end: str, ctx, etf_close, ma_override=None):
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
        close_by_ts_day=closes, etf_close=etf_close,
        ma_window_by_key=ma_override)
    twin = build_twin_star_timeline(
        core_rows=core["rows"], core_summary=core["summary"],
        sat_rows=sat["rows"])
    return core, twin


def _line(tag, w, sm, navs, extra=""):
    return (f"{w:6s} | {tag:5s} | {sm['fusedPct']:+7.1f} | "
            f"{sm['maxDdFusedPct']:5.1f} | {sr_of(navs):+.2f} | {extra}")


def main() -> int:
    idx_close = {k: load_index_closes(ts) for k, ts in INDEX_KEYS.items()}
    for k, mp in idx_close.items():
        ds = sorted(mp.keys())
        print(f"[universe] {k}: {len(mp)} closes {ds[0]}..{ds[-1]}")
    print("loading sgap context + etf closes ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    base_etf = fetch_etf_closes()
    pool_etf = dict(base_etf)
    pool_etf.update(idx_close)

    print("window | leg | fused% | maxDD | sr | idxPickDays | trailExits", flush=True)
    for w, (s, e) in WINDOWS.items():
        print(f"--- {w} ...", flush=True)
        pst.MULTI_TS = {**BASE_MULTI_TS, "SSE300": "000300.SH", "STAR50": "000688.SH"}
        core7, twin7 = run_window(s, e, ctx, pool_etf)
        sm7 = twin7["summary"]
        navs7 = [float(r["navSingle"]) for r in twin7["rows"]]
        seven_line = (round(sm7["fusedPct"], 1), round(sm7["maxDdFusedPct"], 1))
        assert seven_line == SEVEN[w], f"§7 flag-off mismatch: {seven_line}"
        picks7 = [r["pick"] for r in core7["rows"]]
        idx7 = sum(1 for p in picks7 if p in INDEX_KEYS)
        print(_line("+idx", w, sm7, navs7, f"{idx7}/{len(picks7)} | {core7.get('trailExits')}"))
        core8, twin8 = run_window(s, e, ctx, pool_etf, ma_override=MA120)
        sm8 = twin8["summary"]
        navs8 = [float(r["navSingle"]) for r in twin8["rows"]]
        picks8 = [r["pick"] for r in core8["rows"]]
        idx8 = sum(1 for p in picks8 if p in INDEX_KEYS)
        print(_line("ma120", w, sm8, navs8, f"{idx8}/{len(picks8)} | {core8.get('trailExits')}"))
    pst.MULTI_TS = dict(BASE_MULTI_TS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
