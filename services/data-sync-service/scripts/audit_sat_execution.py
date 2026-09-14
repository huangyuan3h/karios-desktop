#!/usr/bin/env python3
"""星舰（卫星 standalone）执行审计 — 只读，2026-09-14。

回答三个问题：
  1. 成本敏感性：30/45/60/90bps 往返（Live 30bps）→ NAV/Sharpe/MDD。
  2. 入场可交易性：每笔 fill 的 raw 基期 14:30 溢价（距涨停）、一字板（high==low）、
     与当日 [low, high] 的一致性。
  3. 容量：成交额分布 + 不同规模下的下单占比（4×25% 槽位）。
并打印冻结配方清单（不后视/口径检查参考 OPT-182/183）。

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/audit_sat_execution.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from eval_twin_star_parking import _stats
from run_walk_forward import WINDOWS

from data_sync_service.service import state_bucket_track as sbt

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
COSTS = (0.0030, 0.0045, 0.0060, 0.0090)
CAPITALS = (1_000_000, 5_000_000, 20_000_000)
SLOT_PCT = 0.25


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    start, end = WINDOWS["long"]
    print(f"loading ctx {start}..{end} …", flush=True)
    ctx = sbt.load_sgap_context(start, end)

    entries: list[tuple[str, str]] = []
    base = sbt.replay_sgap_from_context(ctx, start=start, end=end, debug_fills=entries, **sbt.HABIT_RECIPE)
    nav = [float(r.get("satNav") or 1.0) for r in base.get("rows") or []]
    base_stats = _stats(nav)
    print(f"fills: {len(entries)} | baseline {base_stats}", flush=True)

    # 1) cost sensitivity
    cost_rows: list[dict] = []
    original_cost = sbt.COSTS_ROUNDTRIP
    try:
        for cost in COSTS:
            sbt.COSTS_ROUNDTRIP = cost
            run = sbt.replay_sgap_from_context(ctx, start=start, end=end, **sbt.HABIT_RECIPE)
            rows = run.get("rows") or []
            st = _stats([float(r.get("satNav") or 1.0) for r in rows])
            cost_rows.append({"bps": round(cost * 10000), **st})
            print(f"  cost {round(cost*10000):>3}bps  {st}", flush=True)
    finally:
        sbt.COSTS_ROUNDTRIP = original_cost

    # 2) entry tradability screen (raw basis)
    per_ts = ctx["per_ts"]
    close_by_ts = ctx["close_by_ts"]
    px1430 = ctx.get("px_1430") or {}
    px1500 = (ctx.get("px_by_hhmm") or {}).get("1500") or {}
    hl1430 = ctx.get("px_hl_1430") or {}
    rows_by_ts_day = {ts: {r["date"]: r for r in series} for ts, series in per_ts.items()}

    premiums: list[float] = []
    near_limit: list[dict] = []
    sealed: list[dict] = []
    flat_proxy: list[dict] = []
    sampled_counts: list[int] = []
    range_bad: list[dict] = []
    sample_times = ("1000", "1330", "1400", "1430")
    for day, ts in entries:
        bar = rows_by_ts_day.get(ts, {}).get(day)
        px = (px1430.get(ts) or {}).get(day)
        if not bar or not px or not bar.get("pre_close"):
            continue
        samples = [
            (ctx["px_by_hhmm"].get(t) or {}).get(ts, {}).get(day) for t in sample_times
        ]
        nonnull = [v for v in samples if v]
        sampled_counts.append(len(nonnull))
        raw1500 = (px1500.get(ts) or {}).get(day)
        qfq_close = (close_by_ts.get(ts) or {}).get(day)
        factor = (raw1500 / qfq_close) if raw1500 and qfq_close else None
        pre_raw = float(bar["pre_close"]) * factor if factor else None
        if not pre_raw or pre_raw <= 0:
            continue
        premium = float(px) / pre_raw - 1.0
        premiums.append(premium)
        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
        rec = {"date": day, "ts": ts, "premiumPct": round(premium * 100, 2)}
        if premium >= lim - 0.02:
            near_limit.append(rec)
        if premium >= lim - 0.005:
            sealed.append(rec)
        hl = (hl1430.get(ts) or {}).get(day)
        if hl and float(hl[0]) == float(hl[1]):
            flat_proxy.append(rec)
        if factor:
            lo, hi = float(bar["low"]) * factor, float(bar["high"]) * factor
            if not (lo - 1e-6 <= float(px) <= hi + 1e-6):
                range_bad.append({**rec, "low": lo, "high": hi, "px": px})

    premiums.sort()
    sampled_counts.sort()
    screen = {
        "fillsChecked": len(premiums),
        "premiumPct": {
            "median": round(_pct(premiums, 0.5) * 100, 2),
            "p90": round(_pct(premiums, 0.9) * 100, 2),
            "max": round((premiums[-1] if premiums else 0) * 100, 2),
        },
        "nearLimit2pct": len(near_limit),
        "sealed0p5pct": len(sealed),
        "flatProxyAmp0": len(flat_proxy),
        "sampledBarsLe1430": {
            "median": sampled_counts[len(sampled_counts) // 2] if sampled_counts else 0,
            "zero": sum(1 for c in sampled_counts if c == 0),
        },
        "rangeBad": len(range_bad),
        "nearLimitExamples": near_limit[:5],
        "sealedExamples": sealed[:5],
        "flatProxyExamples": flat_proxy[:5],
    }
    print(f"entry screen: {screen}", flush=True)

    # 3) capacity (amount is in 千元)
    amounts_yuan: list[float] = []
    for day, ts in entries:
        bar = rows_by_ts_day.get(ts, {}).get(day)
        if bar and bar.get("amount"):
            amounts_yuan.append(float(bar["amount"]) * 1000.0)
    amounts_yuan.sort()
    capacity = {
        "entriesWithAmount": len(amounts_yuan),
        "advYi": {
            "p10": round(_pct(amounts_yuan, 0.1) / 1e8, 3),
            "median": round(_pct(amounts_yuan, 0.5) / 1e8, 3),
            "p90": round(_pct(amounts_yuan, 0.9) / 1e8, 3),
        },
        "participationSlotPct": {
            f"{cap // 10000}万": {
                "median": round(100 * SLOT_PCT * cap / _pct(amounts_yuan, 0.5), 3)
                if amounts_yuan
                else None,
                "p90adv": round(100 * SLOT_PCT * cap / _pct(amounts_yuan, 0.9), 3)
                if amounts_yuan
                else None,
            }
            for cap in CAPITALS
        },
    }
    print(f"capacity: {capacity}", flush=True)

    # 4) caliber checklist for the record
    recipe = {k: (v if not callable(v) else str(v)) for k, v in sbt.HABIT_RECIPE.items()}
    out = {
        "tag": "sat-execution-audit-2026-09-14",
        "window": {"start": start, "end": end},
        "fills": len(entries),
        "baseline30bps": base_stats,
        "costSensitivity": cost_rows,
        "entryScreen": screen,
        "capacity": capacity,
        "recipe": recipe,
        "roundTripCostSource": "paper_cost_model.round_trip_cost_pct('CN')",
        "priorAudits": [
            "OPT-182/183: amp_1430 rank / same_1430 fills / gate_1430 / raw basis / calendar",
            "fills 1036/1036 within daily [low, high] (2026-09-14 audit)",
        ],
        "asOf": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "sat_execution_audit_2026-09-14.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
