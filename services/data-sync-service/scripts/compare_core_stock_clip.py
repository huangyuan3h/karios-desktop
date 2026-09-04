#!/usr/bin/env python3
"""Concentrate the pick=STOCK S-3 basket vs frozen 10×10%, blended with clip4 sat.

Live freeze: CN+HK S-3 are 10 × 10% of the STOCK sleeve (≈5% NAV each when
pick=STOCK). Watchlist already shows the top 5 of that 10. User cannot operate
10 names; this grid asks whether fewer names × larger clip beats frozen twin.

Fair variants keep ~100% of the STOCK sleeve invested. Cutting names without
raising clip is the sat-clip trap (n10_c10 REJECT). Concentrated variants turn
pyramid off so 0.25 × 1.5x does not blow the live 15% SIZE_CAP.

Satellite is frozen clip4 for every row (4 × 25%, strict skip_t1). Verdict is
twin NAV vs frozen 10×10% core + clip4 sat. >5pt worse on any OOS2/train/valid
window → reject. Do not change live until PASS+.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_core_stock_clip.py
  PYTHONPATH=src:scripts python3 scripts/compare_core_stock_clip.py --save-report
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData  # noqa: E402
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402
from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0

# CN and HK use the same n × clip so STOCK days are not 4 CN + 10 HK.
VARIANTS: tuple[dict[str, object], ...] = (
    {
        "id": "base",
        "max_positions": 10,
        "position_pct": 0.10,
        "pyramid_max_adds": 1,
        "label": "10×10% S-3 · frozen pyramid",
    },
    {
        "id": "n5_c20",
        "max_positions": 5,
        "position_pct": 0.20,
        "pyramid_max_adds": 0,
        "label": "5×20% S-3 · no pyramid",
    },
    {
        "id": "n4_c25",
        "max_positions": 4,
        "position_pct": 0.25,
        "pyramid_max_adds": 0,
        "label": "4×25% S-3 · no pyramid",
    },
    {
        "id": "n3_c33",
        "max_positions": 3,
        "position_pct": 0.33,
        "pyramid_max_adds": 0,
        "label": "3×33% S-3 · no pyramid",
    },
    {
        "id": "n5_c10",
        "max_positions": 5,
        "position_pct": 0.10,
        "pyramid_max_adds": 0,
        "label": "5×10% underinvest control",
    },
)


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets) / std * (252**0.5))
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, active


def _core_nav(
    dates: list[str],
    start: str,
    end: str,
    etf_close,
    *,
    s3_over: dict,
    data_cn: BacktestData | None,
    data_hk: BacktestData | None,
) -> tuple[list[float], dict, BacktestData, BacktestData]:
    cache = warm_window(
        start,
        end,
        etf_close,
        s3_cn=s3_over,
        s3_hk=s3_over,
        data_cn=data_cn,
        data_hk=data_hk,
    )
    r = build_nav_from_cache(
        cache,
        lookback=60,
        ma_window=200,
        min_hold=1,
        cost=0.0,
        score="mom",
        top2=False,
        trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    out: list[float] = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        out.append(last)
    meta = {
        "core": _stats(out),
        "stockDayPct": r["stockDayPct"],
        "avgStockNames": r["avgStockNames"],
        "maxStockNames": r["maxStockNames"],
        "fusedPct": r["fusedPct"],
    }
    return out, meta, cache["data_cn"], cache["data_hk"]


def _s3_over(var: dict[str, object]) -> dict:
    return {
        "max_positions": int(var["max_positions"]),
        "position_pct": float(var["position_pct"]),
        "pyramid_max_adds": int(var["pyramid_max_adds"]),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("S-3 STOCK-basket clip vs frozen 10×10% core + clip4 sat (opp_50, strict)\n")
    print(f"loading sat context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  sat loaded.", flush=True)
    etf_close = fetch_etf_closes()

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        sat = replay_sgap_from_context(
            ctx,
            start=s,
            end=e,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
        )
        dates, sat_nav, active = _sat_series(sat)
        sat_m = _stats(sat_nav)
        print(f"  sat clip4 {_fmt(sat_m)}", flush=True)

        data_cn: BacktestData | None = None
        data_hk: BacktestData | None = None
        row: dict[str, dict] = {}
        for var in VARIANTS:
            if data_cn is None:
                # Warm bars once with frozen dates; clip fields do not change the load.
                probe = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
                probe_hk = BacktestConfig(start_date=s, end_date=e, **HK_S3_CONFIG)
                print("  loading S-3 bars ...", flush=True)
                data_cn = BacktestData(probe)
                data_hk = BacktestData(probe_hk)
                print("  S-3 bars loaded.", flush=True)
            core, meta, data_cn, data_hk = _core_nav(
                dates,
                s,
                e,
                etf_close,
                s3_over=_s3_over(var),
                data_cn=data_cn,
                data_hk=data_hk,
            )
            n = min(len(core), len(sat_nav))
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            core_m = meta["core"]
            delta_core = round(twin_m["total_pct"] - core_m["total_pct"], 1)
            row[str(var["id"])] = {
                "label": var["label"],
                "max_positions": var["max_positions"],
                "position_pct": var["position_pct"],
                "pyramid_max_adds": var["pyramid_max_adds"],
                "core": core_m,
                "twin": twin_m,
                "delta_core_pt": delta_core,
                "stockDayPct": meta["stockDayPct"],
                "avgStockNames": meta["avgStockNames"],
                "maxStockNames": meta["maxStockNames"],
            }
            print(
                f"  {var['id']:<8} twin {_fmt(twin_m)}  core {_fmt(core_m)}  "
                f"STOCK {meta['stockDayPct']:.0f}% × {meta['avgStockNames']:.1f} names "
                f"(max {meta['maxStockNames']})",
                flush=True,
            )
        base_twin = row["base"]["twin"]["total_pct"]
        for rec in row.values():
            rec["delta_base_pt"] = round(rec["twin"]["total_pct"] - base_twin, 1)
        results[wname] = {"sat": sat_m, "variants": row}

    print("\n## Twin NAV (total/Sharpe/maxDD) vs frozen 10×10% core + clip4 sat\n")
    hdr = "| 窗口 | " + " | ".join(v["id"] for v in VARIANTS) + " |"
    sep = "|" + "|".join(["------"] * (1 + len(VARIANTS))) + "|"
    print(hdr)
    print(sep)
    for wname in WINDOWS:
        cells = []
        for v in VARIANTS:
            rec = results[wname]["variants"][str(v["id"])]
            extra = f" ({rec['delta_base_pt']:+.1f})" if v["id"] != "base" else ""
            cells.append(f"{_fmt(rec['twin'])}{extra}")
        print(f"| {wname} | " + " | ".join(cells) + " |")

    print("\n## Walk-forward vs frozen twin (OOS2/train/valid, reject if any Δ < -5pt)\n")
    verdicts: dict[str, str] = {}
    for v in VARIANTS:
        vid = str(v["id"])
        if vid == "base":
            verdicts[vid] = "baseline"
            continue
        deltas = [results[w]["variants"][vid]["delta_base_pt"] for w in WF_WINDOWS]
        ok = all(d >= -REJECT_PT for d in deltas)
        tag = "PASS" if ok else "REJECT"
        if ok and all(d > 0 for d in deltas):
            tag = "PASS+"
        verdicts[vid] = tag
        print(
            f"- {vid} ({v['label']}): "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, deltas, strict=True))
            + f" → {tag}"
        )

    payload = {
        "tag": "core-stock-clip-2026-09-03",
        "protocol": (
            "window-local empty book; frozen clip4 sat (4×25% strict skip_t1); "
            "core = pick-strong trail8 over CN+HK S-3 with n×clip overlay; opp_50"
        ),
        "variants": VARIANTS,
        "windows": results,
        "verdicts": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "core_stock_clip_2026-09-03.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
