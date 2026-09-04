#!/usr/bin/env python3
"""S4: habit satellite CHURN filter — full-day amp vs no-lookahead keys.

Base is the current habit Live (C1 3% upside cap, same_1430 fill, body=3,
day-3 1430 exit, rank amp, bucket top-1/3, R-wide 0.5). Variant adds ONLY
the CHURN filter (skip when T-1 amount > 4x trailing average): exclude
names distributed yesterday gapping today. Loss-exclusion direction (S4).
Diagnostic: T-1 turn >4x OOS2 -6.71%/9%hit (n2561), train -0.32% (n245).
Single variant to limit selection bias. Selection on OOS2+train;
valid only validates.

Score twin tot+Sharpe+maxDD vs core (beats_core) and vs base (±5pt REJECT).
Frozen engine untouched. Do not rewrite Live.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_sat_rank_hhmm.py --save-report
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

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

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
BASE_ID = "base"

VARIANTS: tuple[dict[str, object], ...] = (
    {"id": "base", "label": "C1 3% (Live, no churn filter)", "churn": None},
    {"id": "churn4x", "label": "C1 3% + skip T-1 turn>4x", "churn": 4.0},
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


def _habit_tag(tot: list[float], sr: list[float], dd: list[float]) -> str:
    flags: list[str] = []
    if not all(t > 0 for t in tot):
        flags.append("loses_core_tot")
    if not all(s >= 0 for s in sr):
        flags.append("worse_sharpe")
    if not all(d <= 0 for d in dd):
        flags.append("worse_dd")
    return "beats_core" if not flags else "+".join(flags)


def _habit_line(tot: list[float], sr: list[float], dd: list[float]) -> str:
    return (
        "tot "
        + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, tot, strict=True))
        + "  sr "
        + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, sr, strict=True))
        + "  dd "
        + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, dd, strict=True))
    )


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[int], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    slots = [int(r.get("satPositions") or 0) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, slots, active


def _pick_strong_nav(dates: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
    r = build_nav_from_cache(
        cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
        score="mom", top2=False, trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    out: list[float] = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        out.append(last)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("S4 habit CHURN filter: none vs T-1 turn>4x skip\n")
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)

    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        core: list[float] | None = None
        core_m: dict[str, float] | None = None
        row: dict[str, dict] = {}
        for var in VARIANTS:
            churn = var["churn"]
            assert churn is None or isinstance(churn, float)
            sat = replay_sgap_from_context(
                ctx,
                start=s,
                end=e,
                skip_t1_limit=True,
                pool_mode="strict",
                max_pos=4,
                position_pct=0.25,
                fill_mode=FILL_SAME_1430,
                fill_hhmm="1430",
                exit_hhmm="1430",
                max_open_to_1430_pct=0.03,
                max_t1_turnover_mult=churn,
            )
            dates, sat_nav, slots, active = _sat_series(sat)
            if core is None:
                print("  warming frozen pick-strong core ...", flush=True)
                core = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(core)
                print(f"  core     {_fmt(core_m)}", flush=True)
            n = min(len(core), len(sat_nav))
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            sat_m = _stats(sat_nav[:n])
            summary = sat.get("summary") or {}
            delta_core = round(twin_m["total_pct"] - core_m["total_pct"], 1)
            delta_core_sr = round(twin_m["sharpe"] - core_m["sharpe"], 2)
            delta_core_dd = round(twin_m["max_dd"] - core_m["max_dd"], 1)
            row[str(var["id"])] = {
                "label": var["label"],
                "churn": churn,
                "sat": sat_m,
                "twin": twin_m,
                "delta_core_pt": delta_core,
                "delta_core_sharpe": delta_core_sr,
                "delta_core_dd": delta_core_dd,
                "fillCount": summary.get("fillCount"),
                "skipC1Count": summary.get("skipC1Count"),
                "skipChurnCount": summary.get("skipChurnCount"),
            }
            print(
                f"  {var['id']:<12} twin {_fmt(twin_m)}  "
                f"Δcore tot {delta_core:+.1f} sr {delta_core_sr:+.2f} dd {delta_core_dd:+.1f}  "
                f"sat {_fmt(sat_m)}  fills {summary.get('fillCount')}",
                flush=True,
            )
        assert core_m is not None
        base = row[BASE_ID]["twin"]
        for rec in row.values():
            rec["delta_base_pt"] = round(rec["twin"]["total_pct"] - base["total_pct"], 1)
            rec["delta_base_sharpe"] = round(rec["twin"]["sharpe"] - base["sharpe"], 2)
            rec["delta_base_dd"] = round(rec["twin"]["max_dd"] - base["max_dd"], 1)
        results[wname] = {"core": core_m, "variants": row}

    print("\n## Walk-forward vs core (tot / Sharpe / maxDD)\n")
    verdicts: dict[str, str] = {}
    for var in VARIANTS:
        vid = str(var["id"])
        vs_core_tot = [results[w]["variants"][vid]["delta_core_pt"] for w in WF_WINDOWS]
        vs_core_sr = [results[w]["variants"][vid]["delta_core_sharpe"] for w in WF_WINDOWS]
        vs_core_dd = [results[w]["variants"][vid]["delta_core_dd"] for w in WF_WINDOWS]
        harvest = _habit_tag(vs_core_tot, vs_core_sr, vs_core_dd)
        if vid == BASE_ID:
            verdicts[vid] = f"baseline/{harvest}"
            print(f"- {vid}: vs core " + _habit_line(vs_core_tot, vs_core_sr, vs_core_dd) + f" → {harvest}")
            continue
        d_tot = [results[w]["variants"][vid]["delta_base_pt"] for w in WF_WINDOWS]
        d_sr = [results[w]["variants"][vid]["delta_base_sharpe"] for w in WF_WINDOWS]
        d_dd = [results[w]["variants"][vid]["delta_base_dd"] for w in WF_WINDOWS]
        tot_ok = all(d >= -REJECT_PT for d in d_tot)
        risk_flags: list[str] = []
        if any(s < 0 for s in d_sr):
            risk_flags.append("worse_sharpe")
        if any(d > 0 for d in d_dd):
            risk_flags.append("worse_dd")
        if not tot_ok:
            tag = "REJECT/total"
        elif risk_flags:
            tag = "PASS/" + "+".join(risk_flags)
        elif all(d > 0 for d in d_tot):
            tag = "PASS+"
        else:
            tag = "PASS"
        verdicts[vid] = f"{tag}/{harvest}"
        print(
            f"- {vid} ({var['label']}):\n"
            f"    vs {BASE_ID} tot "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot, strict=True))
            + "  sr "
            + ", ".join(f"{w} {d:+.2f}" for w, d in zip(WF_WINDOWS, d_sr, strict=True))
            + "  dd "
            + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_dd, strict=True))
            + f" → {tag}\n"
            f"    vs core "
            + _habit_line(vs_core_tot, vs_core_sr, vs_core_dd)
            + f" → {harvest}"
        )

    payload = {
        "tag": "sat-churn-2026-09-04",
        "protocol": (
            "window-local empty book; clip4 C1 3% same_1430 fill; body=3; "
            "day-3 1430 exit; frozen pick-strong trail8; opp_50. "
            "Variant adds ONLY CHURN filter (skip T-1 amount > 4x average). "
            "Select on OOS2+train; valid only validates. "
            "Score tot+Sharpe+maxDD vs core and vs base. Do not rewrite Live."
        ),
        "variants": VARIANTS,
        "windows": results,
        "verdicts": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_churn_2026-09-04.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
