#!/usr/bin/env python3
"""Reproduce R11 static core/satellite blend baseline vs pick-strong single track.

Baseline = pick_strong trail8 (core) blended 50/50 with satellite daily returns.
Default satellite variants (from scout_state_pk_combo / state_pk_combo_latest.json):

  slice2_opt — S-limit(2,10,3) + S-gap(3,15,3) at 50/50  (~169% past_year, sr~3.87)
  G_opt      — S-gap(3,15,3) alone as satellite           (~171.7% past_year, sr~3.98)

This is NOT opportunity twin-star (dynamic satActive). Historical scout fill model
(no skip_t1_limit). Use as research baseline; executable re-run is a follow-up.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/reproduce_static_blend_baseline.py
  PYTHONPATH=src:scripts python3 scripts/reproduce_static_blend_baseline.py --save-baseline

Writes data/backtest_reports/static_blend_baseline_YYYY-MM-DD.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402
from scout_state_bucket_pickstrong import (  # noqa: E402
    _load_calendar,
    _load_daily,
    _load_list_dates,
    _load_mv_map,
    simulate_state_bucket,
    stats,
)
from scout_state_pk_combo import SATELLITES, combo_nav, daily_ret  # noqa: E402

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
BASELINE_VARIANTS = ("slice2_opt", "G_opt")
CORE_WEIGHT = 0.5  # 50/50 static blend

# Official research baseline (recommended: G_opt @ 50/50 — past_year sr~3.98, dd~6.4)
BASELINE_ID = "PS-G50"
BASELINE_NAME_ZH = "G50 稳态半仓基线"
BASELINE_NAME_EN = "Pick-Strong G50 Static Half Baseline"
BASELINE_VARIANT = "G_opt"  # S-gap bq3/15槽/body3 satellite · static 50/50 with trail8 core
BASELINE_TAG = "pick-strong-g50-baseline-frozen-2026-09-01"

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
FROZEN_PK = REPORT_DIR / "state_pk_combo_latest.json"


def _pick_strong_nav(cal: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
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
    pk: list[float] = []
    last = 1.0
    for d in cal:
        v = pk_map.get(d)
        if v is not None:
            last = v
        pk.append(last)
    return pk


def _satellite_nav(
    sname: str,
    cal: list[str],
    per_ts,
    mv_map,
    list_dates,
    date_idx,
) -> list[float]:
    sts, ws, params = SATELLITES[sname]
    state_navs = {}
    for st in sts:
        bq, mp, body = params[st]
        nav, _ = simulate_state_bucket(
            cal,
            per_ts,
            mv_map,
            list_dates,
            date_idx,
            state_filter={st},
            bucket_q=bq,
            max_pos=mp,
            hold_map={st: body},
        )
        state_navs[st] = nav
    return combo_nav(cal, [state_navs[st] for st in sts], ws)


def _blend_nav(pk: list[float], sat: list[float], core_w: float) -> list[float]:
    n = min(len(pk), len(sat))
    pk2, sat2 = pk[:n], sat[:n]
    pr, sr = daily_ret(pk2), daily_ret(sat2)
    nav = [1.0]
    for i in range(1, n):
        nav.append(nav[-1] * (1 + core_w * pr[i - 1] + (1 - core_w) * sr[i - 1]))
    return nav


def _load_frozen() -> dict | None:
    if not FROZEN_PK.exists():
        return None
    return json.loads(FROZEN_PK.read_text())


def _md_table(results: dict[str, dict]) -> str:
    lines = [
        "| 窗口 | 单轨择强 | blend 变体 | blend total/dd/sr | Δtotal | Δsr |",
        "|------|---------:|------------|------------------:|-------:|----:|",
    ]
    for w in WINDOWS:
        core = results[w]["pick_strong_single"]
        for v in BASELINE_VARIANTS:
            b = results[w]["blends"][v]
            dt = round(b["total_pct"] - core["total_pct"], 1)
            ds = round((b["sharpe"] or 0) - (core["sharpe"] or 0), 2)
            lines.append(
                f"| {w} | {core['total_pct']:+.1f}/{core['max_dd']:.1f}/{core['sharpe']} | "
                f"{v} 50/50 | {b['total_pct']:+.1f}/{b['max_dd']:.1f}/{b['sharpe']} | "
                f"{dt:+.1f}pt | {ds:+.2f} |"
            )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--save-baseline",
        action="store_true",
        help="Also write static_blend_baseline_frozen.json for future diffs",
    )
    ap.add_argument("--variants", default=",".join(BASELINE_VARIANTS))
    ap.add_argument("--core-weight", type=float, default=CORE_WEIGHT)
    args = ap.parse_args()

    variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    cw = args.core_weight
    sw = 1.0 - cw

    etf_close = fetch_etf_closes()
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    frozen = _load_frozen()

    results: dict[str, dict] = {}
    print(f"Static blend baseline: core {cw:.0%} / sat {sw:.0%} · scout fill (no skip_t1_limit)\n")

    for wname, (s, e) in WINDOWS.items():
        cal = _load_calendar(s, e)
        if len(cal) < 5:
            continue
        pk = _pick_strong_nav(cal, s, e, etf_close)
        core_m = stats(cal, pk)
        blends: dict[str, dict] = {}
        sats: dict[str, dict] = {}
        print(f"=== {wname} ({s}~{e}) ===")
        print(
            f"  单轨择强: total {core_m['total_pct']:+.1f}%  dd {core_m['max_dd']:.1f}  "
            f"sr {core_m['sharpe']:.2f}",
            flush=True,
        )
        for vname in variants:
            if vname not in SATELLITES:
                print(f"ERROR: unknown variant {vname}", file=sys.stderr)
                return 2
            sat = _satellite_nav(vname, cal, per_ts, mv_map, list_dates, date_idx)
            n = min(len(pk), len(sat))
            pr, sr = daily_ret(pk[:n]), daily_ret(sat[:n])
            corr = float(np.corrcoef(pr, sr)[0, 1]) if n > 2 else 0.0
            blended = _blend_nav(pk, sat, cw)
            bm = stats(cal[:n], blended)
            sm = stats(cal[:n], sat[:n])
            blends[vname] = {**bm, "corr": round(corr, 3), "delta_vs_core_pt": round(bm["total_pct"] - core_m["total_pct"], 2)}
            sats[vname] = sm
            diff_frozen = ""
            if frozen and wname in frozen:
                fb = frozen[wname]["sats"].get(vname, {}).get("blends", {}).get(str(cw))
                if fb:
                    d = round(bm["total_pct"] - float(fb["total_pct"]), 2)
                    diff_frozen = f"  (Δ vs frozen {d:+.2f}pt)"
            print(
                f"  {vname} sat: total {sm['total_pct']:+.1f}% sr {sm['sharpe']:.2f} corr {corr:.2f}",
                flush=True,
            )
            print(
                f"  {vname} {cw:.0%}/{sw:.0%}: total {bm['total_pct']:+.1f}%  dd {bm['max_dd']:.1f}  "
                f"sr {bm['sharpe']:.2f}  vs core {blends[vname]['delta_vs_core_pt']:+.1f}pt"
                f"{diff_frozen}",
                flush=True,
            )
        results[wname] = {
            "start": s,
            "end": e,
            "pick_strong_single": core_m,
            "satellites": sats,
            "blends": blends,
        }

    table = _md_table(results)
    print("\n" + table)

    # Recommend default baseline variant (best mean sharpe among variants)
    mean_sr = {
        v: sum(results[w]["blends"][v]["sharpe"] or 0 for w in results) / len(results)
        for v in variants
    }
    best_v = max(mean_sr, key=mean_sr.get)
    g50 = results.get("past_year", {}).get("blends", {}).get(BASELINE_VARIANT, {})
    verdict = (
        f"{BASELINE_ID} ({BASELINE_NAME_ZH}) reproduced. "
        f"Primary variant {BASELINE_VARIANT} @ {cw:.0%}/{sw:.0%}; "
        f"past_year total {g50.get('total_pct', '—')}% sr {g50.get('sharpe', '—')}. "
        "Historical fill (no skip_t1_limit). Compare vs pick_strong_single in table."
    )
    print(f"\n{verdict}")

    today = date.today().isoformat()
    out = REPORT_DIR / f"static_blend_baseline_{today}.json"
    primary_window_metrics = {
        w: {
            "pick_strong_single": results[w]["pick_strong_single"],
            "baseline_blend": results[w]["blends"].get(BASELINE_VARIANT),
        }
        for w in results
    }
    payload = {
        "tag": BASELINE_TAG,
        "baseline_id": BASELINE_ID,
        "baseline_name_zh": BASELINE_NAME_ZH,
        "baseline_name_en": BASELINE_NAME_EN,
        "baseline_variant": BASELINE_VARIANT,
        "generated_at": datetime.now(UTC).isoformat(),
        "scheme": f"{BASELINE_ID}: R11 static core/sat {cw:.0%}/{sw:.0%} (NOT opportunity twin-star)",
        "core": "pick_strong_grid.build_nav_from_cache trail_pct=8.0",
        "satellite_engine": "scout_state_bucket_pickstrong.simulate_state_bucket",
        "variants": {v: {"states": SATELLITES[v][0], "params": SATELLITES[v][2]} for v in variants},
        "core_weight": cw,
        "fill_model": "historical (no skip_t1_limit)",
        "windows": {k: {"start": v[0], "end": v[1]} for k, v in WINDOWS.items()},
        "per_window": results,
        "primary": primary_window_metrics,
        "recommended_baseline_variant": BASELINE_VARIANT,
        "all_variants_mean_sharpe": {v: round(mean_sr[v], 2) for v in variants},
        "markdown_table": table,
        "verdict": verdict,
        "refs": {
            "frozen_pk_combo": str(FROZEN_PK),
            "design": "docs/designs/state-bucket-slice-stock-leg.md",
            "r11_doc": "docs/backtests/state-bucket-algo-2026-08-31.md R11",
            "not_same_as": "机会双子星 (Opportunity Twin-Star) — dynamic satActive blend",
        },
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out}")

    if args.save_baseline:
        frozen_path = REPORT_DIR / "pick_strong_g50_baseline_frozen.json"
        frozen_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        # Legacy alias for scripts that already point at static_blend_baseline_frozen.json
        legacy = REPORT_DIR / "static_blend_baseline_frozen.json"
        legacy.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print(f"baseline frozen -> {frozen_path}")
        print(f"legacy alias   -> {legacy}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
