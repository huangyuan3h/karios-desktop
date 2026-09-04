#!/usr/bin/env python3
"""PS-G50 executable fill models vs pick-strong single track.

Compares three satellite fill assumptions blended 50/50 with pick_strong trail8:

  historical          — scout simulate_state_bucket (frozen PS-G50 research baseline)
  executable_strict   — skip_t1_limit, top bucket only (涨停跳过 → 槽位可能空)
  executable_fallback — skip_t1_limit + walk full amp-ranked pool (买下一个可成交标的)

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_ps_g50_executable.py
  PYTHONPATH=src:scripts python3 scripts/compare_ps_g50_executable.py --save-report
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

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    BUCKET_Q,
    BODY,
    MAX_POS,
    build_sgap_timeline,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402
from scout_state_bucket_pickstrong import (  # noqa: E402
    _load_calendar,
    _load_daily,
    _load_list_dates,
    _load_mv_map,
    simulate_state_bucket,
    stats,
)
from scout_state_pk_combo import daily_ret  # noqa: E402

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
CORE_WEIGHT = 0.5
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}

FILL_MODELS = {
    "historical": {
        "label_zh": "历史 scout（无涨停过滤）",
        "skip_t1_limit": False,
        "limit_fallback": False,
        "engine": "scout",
    },
    "executable_strict": {
        "label_zh": "可执行·仅顶桶（涨停跳过）",
        "skip_t1_limit": True,
        "limit_fallback": False,
        "engine": "service",
    },
    "executable_fallback": {
        "label_zh": "可执行·顺位替补（涨停买下一个）",
        "skip_t1_limit": True,
        "limit_fallback": True,
        "engine": "service",
    },
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
FROZEN = REPORT_DIR / "pick_strong_g50_baseline_frozen.json"


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


def _historical_sgap_nav(
    cal: list[str],
    per_ts,
    mv_map,
    list_dates,
    date_idx,
) -> list[float]:
    nav, _ = simulate_state_bucket(
        cal,
        per_ts,
        mv_map,
        list_dates,
        date_idx,
        state_filter={"S-gap"},
        bucket_q=BUCKET_Q,
        max_pos=MAX_POS,
        hold_map={"S-gap": BODY},
    )
    return nav


def _service_sgap_nav(start: str, end: str, *, skip_t1_limit: bool, limit_fallback: bool) -> list[float]:
    sat = build_sgap_timeline(
        start=start,
        end=end,
        bucket_q=BUCKET_Q,
        max_pos=MAX_POS,
        body=BODY,
        skip_t1_limit=skip_t1_limit,
        limit_fallback=limit_fallback,
    )
    return [float(r["satNav"]) for r in sat["rows"]]


def _blend_nav(pk: list[float], sat: list[float], core_w: float) -> list[float]:
    n = min(len(pk), len(sat))
    pk2, sat2 = pk[:n], sat[:n]
    pr, sr = daily_ret(pk2), daily_ret(sat2)
    nav = [1.0]
    for i in range(1, n):
        nav.append(nav[-1] * (1 + core_w * pr[i - 1] + (1 - core_w) * sr[i - 1]))
    return nav


def _align_nav(cal: list[str], nav: list[float], start: str) -> list[float]:
    """Service sgap rows start at ``start``; pad to full calendar length."""
    if len(nav) >= len(cal):
        return nav[: len(cal)]
    pad = len(cal) - len(nav)
    if not nav:
        return [1.0] * len(cal)
    return [1.0] * pad + nav


def _md_table(results: dict[str, dict], models: list[str]) -> str:
    hdr = "| 窗口 | 单轨择强 | " + " | ".join(models) + " |"
    sep = "|------|--------:|" + "|".join(["--------:"] * len(models)) + "|"
    lines = [hdr, sep]
    for w in WINDOWS:
        core = results[w]["pick_strong_single"]
        cells = [f"{core['total_pct']:+.1f}/{core['sharpe']}/{core['max_dd']:.1f}"]
        for m in models:
            b = results[w]["blends"][m]
            cells.append(f"{b['total_pct']:+.1f}/{b['sharpe']}/{b['max_dd']:.1f}")
        lines.append(f"| {w} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _delta_table(results: dict[str, dict], models: list[str]) -> str:
    lines = ["| 窗口 | " + " | ".join(f"Δ {m} vs core" for m in models) + " |", "|------|" + "|".join(["------:"] * len(models)) + "|"]
    for w in WINDOWS:
        core_t = results[w]["pick_strong_single"]["total_pct"]
        cells = []
        for m in models:
            dt = results[w]["blends"][m]["total_pct"] - core_t
            ds = (results[w]["blends"][m]["sharpe"] or 0) - (results[w]["pick_strong_single"]["sharpe"] or 0)
            cells.append(f"{dt:+.1f}pt / sr{ds:+.2f}")
        lines.append(f"| {w} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--models", default=",".join(FILL_MODELS))
    args = ap.parse_args()
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    for m in models:
        if m not in FILL_MODELS:
            print(f"unknown model {m}", file=sys.stderr)
            return 2

    etf_close = fetch_etf_closes()
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}

    results: dict[str, dict] = {}
    print(f"PS-G50 ({CORE_WEIGHT:.0%}/{1-CORE_WEIGHT:.0%}) vs pick-strong single · fill model comparison\n")

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

        for mname in models:
            cfg = FILL_MODELS[mname]
            if cfg["engine"] == "scout":
                sat = _historical_sgap_nav(cal, per_ts, mv_map, list_dates, date_idx)
            else:
                sat_raw = _service_sgap_nav(
                    s,
                    e,
                    skip_t1_limit=cfg["skip_t1_limit"],
                    limit_fallback=cfg["limit_fallback"],
                )
                sat = _align_nav(cal, sat_raw, s)
            n = min(len(pk), len(sat))
            pr, sr = daily_ret(pk[:n]), daily_ret(sat[:n])
            corr = float(np.corrcoef(pr, sr)[0, 1]) if n > 2 else 0.0
            blended = _blend_nav(pk, sat, CORE_WEIGHT)
            bm = stats(cal[:n], blended)
            sm = stats(cal[:n], sat[:n])
            blends[mname] = {
                **bm,
                "corr": round(corr, 3),
                "delta_vs_core_pt": round(bm["total_pct"] - core_m["total_pct"], 2),
                "delta_sr_vs_core": round((bm["sharpe"] or 0) - (core_m["sharpe"] or 0), 2),
            }
            sats[mname] = sm
            print(
                f"  {mname}: sat {sm['total_pct']:+.1f}%  blend {bm['total_pct']:+.1f}%  "
                f"dd {bm['max_dd']:.1f}  sr {bm['sharpe']:.2f}  vs core {blends[mname]['delta_vs_core_pt']:+.1f}pt",
                flush=True,
            )

        results[wname] = {
            "start": s,
            "end": e,
            "pick_strong_single": core_m,
            "satellites": sats,
            "blends": blends,
        }

    table = _md_table(results, models)
    delta = _delta_table(results, models)
    print("\n" + table)
    print("\n" + delta)

    # Verdict: count windows where each model beats core on total and sharpe
    wins_total = {m: 0 for m in models}
    wins_sr = {m: 0 for m in models}
    for w in results:
        core_t = results[w]["pick_strong_single"]["total_pct"]
        core_s = results[w]["pick_strong_single"]["sharpe"] or 0
        for m in models:
            if results[w]["blends"][m]["total_pct"] > core_t:
                wins_total[m] += 1
            if (results[w]["blends"][m]["sharpe"] or 0) > core_s:
                wins_sr[m] += 1

    py = results.get("past_year", {})
    hist = py.get("blends", {}).get("historical", {})
    fb = py.get("blends", {}).get("executable_fallback", {})
    strict = py.get("blends", {}).get("executable_strict", {})
    verdict = (
        f"past_year: historical {hist.get('total_pct')}% sr{hist.get('sharpe')} · "
        f"strict {strict.get('total_pct')}% sr{strict.get('sharpe')} · "
        f"fallback {fb.get('total_pct')}% sr{fb.get('sharpe')}. "
        f"Win counts (total/sr vs core): "
        + ", ".join(f"{m} {wins_total[m]}/{wins_sr[m]}" for m in models)
    )
    print(f"\n{verdict}")

    if args.save_report:
        today = date.today().isoformat()
        payload = {
            "tag": f"ps-g50-executable-{today}",
            "baseline_id": "PS-G50",
            "generated_at": datetime.now(UTC).isoformat(),
            "core_weight": CORE_WEIGHT,
            "fill_models": FILL_MODELS,
            "windows": {k: {"start": v[0], "end": v[1]} for k, v in WINDOWS.items()},
            "per_window": results,
            "markdown_table": table,
            "delta_table": delta,
            "win_counts_total_vs_core": wins_total,
            "win_counts_sharpe_vs_core": wins_sr,
            "verdict": verdict,
            "refs": {
                "frozen_historical": str(FROZEN),
                "design": "docs/designs/state-bucket-slice-stock-leg.md §9",
            },
        }
        out = REPORT_DIR / f"ps_g50_executable_{today}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        print(f"\nreport -> {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
