#!/usr/bin/env python3
"""择强单轨加固网格 — mom_compare absolute NAV, three-window + past_year.

Questions prior sleeve-grid claims (60/200, hold5, etc.) under the *product*
strategy: equal-weight asset pool, 100% hard switch.

S-3 CN/HK engines are run once per window and cached; only pick params vary.

Usage:
  PYTHONPATH=src:scripts python3 scripts/pick_strong_grid.py --batch A
  PYTHONPATH=src:scripts python3 scripts/pick_strong_grid.py --batch all
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from run_walk_forward import HK_S3_CONFIG, S3_CONFIG, WINDOWS  # noqa: E402

import psycopg  # noqa: E402

PAST_YEAR = ("2025-08-28", "2026-08-28")
ALL_WINDOWS = {**WINDOWS, "past_year": PAST_YEAR}
MULTI_TS = {
    "GOLD": "518880.SH",
    "OIL": "513350.SH",
    "NASDAQ": "513110.SH",
    "BOND10": "511260.SH",
}
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

BATCH_A = [
    {"id": "A0", "lookback": 60, "ma": 200},
    {"id": "A1", "lookback": 40, "ma": 200},
    {"id": "A2", "lookback": 90, "ma": 200},
    {"id": "A3", "lookback": 20, "ma": 200},
    {"id": "A4", "lookback": 60, "ma": 120},
    {"id": "A5", "lookback": 60, "ma": 250},
    {"id": "A6", "lookback": 40, "ma": 120},
    {"id": "A7", "lookback": 90, "ma": 120},
]
BATCH_B = [
    {"id": "B0", "min_hold": 1},
    {"id": "B1", "min_hold": 3},
    {"id": "B2", "min_hold": 5},
    {"id": "B3", "min_hold": 10},
]
BATCH_C = [
    {"id": "C0", "cost": 0.0},
    {"id": "C1", "cost": 0.0005},
    {"id": "C2", "cost": 0.001},
]
BATCH_D = [
    {"id": "D0", "score": "mom"},
    {"id": "D1", "score": "risk_adj"},
    {"id": "D2", "score": "mom", "top2": True},
]
# Q8: ETF-leg peak −trail% → REPO (live already has trail8; fused NAV not yet).
BATCH_E = [
    {"id": "E0", "trail_pct": 0.0},
    {"id": "E1", "trail_pct": 8.0},
]
# STOCK pool entry gates (baseline = A0 + trail8). Single variable each.
BATCH_S = [
    {"id": "S0", "stock_min_n": 1, "stock_mom_gt0": False, "stock_ma_frac": 0.0, "stock_cn_only": False},
    {"id": "S1", "stock_min_n": 2, "stock_mom_gt0": False, "stock_ma_frac": 0.0, "stock_cn_only": False},
    {"id": "S2", "stock_min_n": 1, "stock_mom_gt0": True, "stock_ma_frac": 0.0, "stock_cn_only": False},
    {"id": "S3", "stock_min_n": 1, "stock_mom_gt0": False, "stock_ma_frac": 0.5, "stock_cn_only": False},
    {"id": "S4", "stock_min_n": 1, "stock_mom_gt0": False, "stock_ma_frac": 0.0, "stock_cn_only": True},
]


def fetch_etf_closes() -> dict[str, dict[str, float]]:
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    out: dict[str, dict[str, float]] = {}
    for key, ts in MULTI_TS.items():
        cur.execute(
            "select trade_date, close from daily where ts_code=%s order by trade_date",
            (ts,),
        )
        out[key] = {str(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}
    conn.close()
    return out


def _vol20(mp: dict[str, float], days_k: list[str], pi: int) -> float:
    if pi < 21:
        return 1e-6
    rets = []
    for j in range(pi - 19, pi + 1):
        a, b = mp[days_k[j - 1]], mp[days_k[j]]
        if a and a != 0:
            rets.append(b / a - 1.0)
    if len(rets) < 5:
        return 1e-6
    mean = sum(rets) / len(rets)
    var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
    return max(math.sqrt(var), 1e-6)


def warm_window(start: str, end: str, etf_close: dict) -> dict:
    """Run S-3 once; return reusable engine + ETF return maps."""
    cfg_cn = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    cfg_hk = BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG)
    data_cn = BacktestData(cfg_cn)
    run_cn = simulate(cfg_cn, data_cn)
    data_hk = BacktestData(cfg_hk)
    run_hk = simulate(cfg_hk, data_hk)
    calendar = sorted(set(data_cn.calendar) | set(data_hk.calendar))
    close_by_ts = {**data_cn.close_by_ts_day, **data_hk.close_by_ts_day}
    etf_ret: dict[str, dict[str, float]] = {}
    for k, mp in etf_close.items():
        days = sorted(mp.keys())
        ret: dict[str, float] = {}
        for i in range(1, len(days)):
            d, prev = days[i], days[i - 1]
            if mp[prev] != 0:
                ret[d] = mp[d] / mp[prev] - 1.0
        etf_ret[k] = ret
    return {
        "calendar": calendar,
        "close_by_ts": close_by_ts,
        "etf_close": etf_close,
        "etf_ret": etf_ret,
        "snap_cn": {str(s.get("date")): s for s in run_cn.positions_by_day},
        "snap_hk": {str(s.get("date")): s for s in run_hk.positions_by_day},
        "ts_days": {ts: sorted(mp.keys()) for ts, mp in close_by_ts.items()},
    }


def build_nav_from_cache(
    cache: dict,
    *,
    lookback: int = 60,
    ma_window: int = 200,
    min_hold: int = 1,
    cost: float = 0.0,
    score: str = "mom",
    top2: bool = False,
    trail_pct: float = 0.0,
    stock_min_n: int = 1,
    stock_mom_gt0: bool = False,
    stock_ma_frac: float = 0.0,
    stock_cn_only: bool = False,
) -> dict:
    """Pick loop only — matches fused_timeline_walk mom_compare (+ hold/cost/score/trail)."""
    calendar = cache["calendar"]
    close_by_ts = cache["close_by_ts"]
    etf_close = cache["etf_close"]
    etf_ret = cache["etf_ret"]
    snap_cn = cache["snap_cn"]
    snap_hk = cache["snap_hk"]
    ts_days = cache["ts_days"]

    def mom_at(ts: str, prev_day: str) -> float | None:
        mp = close_by_ts.get(ts)
        days = ts_days.get(ts)
        if not mp or not days:
            return None
        try:
            pi = days.index(prev_day)
        except ValueError:
            return None
        if pi < lookback:
            return None
        prev_c, ago = mp.get(prev_day), mp.get(days[pi - lookback])
        if not prev_c or not ago:
            return None
        return prev_c / ago - 1.0

    nav = 1.0
    peak = 1.0
    max_dd = 0.0
    hold_pick: str | None = None
    hold_days = 0
    switches = 0
    etf_peak = 0.0
    trail_exits = 0

    for idx, day in enumerate(calendar):
        if idx == 0:
            continue
        prev = calendar[idx - 1]
        stock_poses = []
        for snap in (snap_cn.get(prev), snap_hk.get(prev)):
            if not snap:
                continue
            for pos in snap.get("positions") or []:
                entry = str(pos.get("entry_date") or "")
                if entry and day <= entry:
                    continue
                ts = str(pos.get("ts_code") or "")
                if stock_cn_only and (ts.endswith(".HK") or str(pos.get("symbol") or "").upper().startswith("HK:")):
                    continue
                stock_poses.append(pos)

        stock_rets: list[float] = []
        stock_moms: list[float] = []
        above_ma_n = 0
        for pos in stock_poses:
            ts = str(pos.get("ts_code") or "")
            closes = close_by_ts.get(ts) or {}
            today_c, prev_c = closes.get(day), closes.get(prev)
            if today_c and prev_c and prev_c != 0:
                stock_rets.append(today_c / prev_c - 1.0)
            m = mom_at(ts, prev)
            if m is not None:
                stock_moms.append(m)
            if stock_ma_frac > 0 and prev_c is not None:
                days_ts = ts_days.get(ts) or []
                try:
                    pi = days_ts.index(prev)
                except ValueError:
                    pi = -1
                if pi >= ma_window - 1:
                    ma_vals = [closes.get(days_ts[j]) for j in range(pi - ma_window + 1, pi + 1)]
                    if all(v is not None and v > 0 for v in ma_vals):
                        ma = sum(float(v) for v in ma_vals) / ma_window  # type: ignore[arg-type]
                        if prev_c >= ma:
                            above_ma_n += 1
        stock_ret = sum(stock_rets) / len(stock_rets) if stock_rets else 0.0
        stock_mom = sum(stock_moms) / len(stock_moms) if stock_moms else -1e9

        candidates: dict[str, float] = {}
        stock_ok = len(stock_poses) >= stock_min_n
        if stock_ok and stock_mom_gt0 and not (stock_moms and stock_mom > 0):
            stock_ok = False
        if stock_ok and stock_ma_frac > 0:
            frac = above_ma_n / len(stock_poses) if stock_poses else 0.0
            if frac < stock_ma_frac:
                stock_ok = False
        if stock_ok and stock_poses:
            if score == "risk_adj" and stock_moms:
                mean = stock_mom if stock_mom > -1e8 else 0.0
                vol = max(
                    (sum((m - mean) ** 2 for m in stock_moms) / max(1, len(stock_moms))) ** 0.5,
                    1e-4,
                )
                candidates["STOCK"] = mean / vol
            else:
                candidates["STOCK"] = stock_mom

        for k, ts in MULTI_TS.items():
            mp = etf_close.get(k) or {}
            if prev not in mp:
                continue
            days_k = sorted(mp.keys())
            try:
                pi = days_k.index(prev)
            except ValueError:
                continue
            if pi < max(lookback, ma_window) - 1:
                continue
            ma = sum(mp[days_k[j]] for j in range(pi - ma_window + 1, pi + 1)) / ma_window
            if mp[prev] < ma:
                continue
            ago = mp[days_k[pi - lookback]]
            mom = mp[prev] / ago - 1.0 if ago else -1e9
            if score == "risk_adj":
                candidates[k] = mom / _vol20(mp, days_k, pi)
            else:
                candidates[k] = mom

        new_pick = "REPO"
        if candidates:
            if top2 and len(candidates) >= 2:
                ranked = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:2]
                new_pick = "TOP2:" + "+".join(r[0] for r in ranked)
            else:
                new_pick = max(candidates, key=lambda kk: candidates[kk])

        switched = False
        if hold_pick is None:
            hold_pick = new_pick
            hold_days = 1
            etf_peak = 0.0
        elif new_pick != hold_pick and hold_days >= min_hold:
            hold_pick = new_pick
            hold_days = 1
            switches += 1
            switched = True
            etf_peak = 0.0
        else:
            hold_days += 1

        # ETF-leg trail: peak since entry −trail% → REPO (same-day exit, earn repo).
        if (
            trail_pct > 0
            and hold_pick
            and hold_pick not in ("STOCK", "REPO")
            and not hold_pick.startswith("TOP2:")
        ):
            mp = etf_close.get(hold_pick) or {}
            close = mp.get(day)
            if close is not None:
                if etf_peak <= 0 or switched or hold_days == 1:
                    etf_peak = close
                elif close < etf_peak * (1.0 - trail_pct / 100.0):
                    hold_pick = "REPO"
                    switches += 1
                    switched = True
                    trail_exits += 1
                    etf_peak = 0.0
                    hold_days = 1
                else:
                    etf_peak = max(etf_peak, close)
        elif hold_pick in ("STOCK", "REPO") or (hold_pick and hold_pick.startswith("TOP2:")):
            etf_peak = 0.0

        if hold_pick == "REPO":
            fused_ret = 0.0
        elif hold_pick == "STOCK":
            fused_ret = stock_ret
        elif hold_pick.startswith("TOP2:"):
            keys = hold_pick[5:].split("+")
            parts = []
            for pk in keys:
                if pk == "STOCK":
                    parts.append(stock_ret)
                else:
                    parts.append(etf_ret.get(pk, {}).get(day, 0.0))
            fused_ret = sum(parts) / len(parts) if parts else 0.0
        else:
            fused_ret = etf_ret.get(hold_pick, {}).get(day, 0.0)

        nav *= 1.0 + fused_ret - (cost if switched else 0.0)
        peak = max(peak, nav)
        if peak > 0:
            max_dd = max(max_dd, (peak - nav) / peak)

    return {
        "fusedPct": round((nav - 1.0) * 100.0, 2),
        "maxDdFusedPct": round(max_dd * 100.0, 1),
        "switches": switches,
        "trailExits": trail_exits,
        "calendarDays": len(calendar),
    }


def _verdict(row: dict[str, dict], baseline: dict[str, dict]) -> str:
    fails = []
    for w in ("OOS2", "train", "valid"):
        if w not in row or w not in baseline:
            continue
        d = row[w]["fusedPct"] - baseline[w]["fusedPct"]
        if d < -5.0:
            fails.append(f"{w}{d:+.1f}")
    if fails:
        return "❌ " + ",".join(fails)
    ups = []
    for w in ("OOS2", "train", "valid"):
        if w not in row or w not in baseline:
            continue
        d = row[w]["fusedPct"] - baseline[w]["fusedPct"]
        if d >= 2.0:
            ups.append(f"{w}{d:+.1f}")
    if ups:
        return "✅ " + ",".join(ups)
    return "≈ 持平"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch", default="A", help="A|B|C|D|E|S|all")
    ap.add_argument("--windows", default="OOS2,train,valid,past_year")
    ap.add_argument("--base-lb", type=int, default=60)
    ap.add_argument("--base-ma", type=int, default=200)
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    windows = [w.strip() for w in args.windows.split(",") if w.strip()]

    print("Fetching ETF closes …", flush=True)
    etf_close = fetch_etf_closes()

    caches: dict[str, dict] = {}
    for w in windows:
        start, end = ALL_WINDOWS[w]
        print(f"Warming S-3 cache {w} ({start}→{end}) …", flush=True)
        caches[w] = warm_window(start, end, etf_close)

    batches = args.batch.lower().split(",")
    if "all" in batches:
        batches = ["a", "b", "c", "d", "e", "s"]

    # STOCK-pool batch uses trail8 absorbed baseline; E compares trail on/off vs trail0.
    base_trail = 8.0 if ("s" in batches and "e" not in batches) else 0.0

    def _stock_defaults() -> dict:
        return {
            "stock_min_n": 1,
            "stock_mom_gt0": False,
            "stock_ma_frac": 0.0,
            "stock_cn_only": False,
        }

    combos: list[dict] = []
    if "a" in batches:
        for c in BATCH_A:
            combos.append(
                {
                    **_stock_defaults(),
                    **c,
                    "lookback": c["lookback"],
                    "ma": c["ma"],
                    "min_hold": 1,
                    "cost": 0.0,
                    "score": "mom",
                    "top2": False,
                    "trail_pct": 0.0,
                }
            )
    if "b" in batches:
        for c in BATCH_B:
            combos.append(
                {
                    **_stock_defaults(),
                    **c,
                    "lookback": args.base_lb,
                    "ma": args.base_ma,
                    "min_hold": c["min_hold"],
                    "cost": 0.0,
                    "score": "mom",
                    "top2": False,
                    "trail_pct": 0.0,
                }
            )
    if "c" in batches:
        for c in BATCH_C:
            combos.append(
                {
                    **_stock_defaults(),
                    **c,
                    "lookback": args.base_lb,
                    "ma": args.base_ma,
                    "min_hold": 1,
                    "cost": c["cost"],
                    "score": "mom",
                    "top2": False,
                    "trail_pct": 0.0,
                }
            )
    if "d" in batches:
        for c in BATCH_D:
            combos.append(
                {
                    **_stock_defaults(),
                    **c,
                    "lookback": args.base_lb,
                    "ma": args.base_ma,
                    "min_hold": 1,
                    "cost": 0.0,
                    "score": c.get("score", "mom"),
                    "top2": bool(c.get("top2")),
                    "trail_pct": 0.0,
                }
            )
    if "e" in batches:
        for c in BATCH_E:
            combos.append(
                {
                    **_stock_defaults(),
                    **c,
                    "lookback": args.base_lb,
                    "ma": args.base_ma,
                    "min_hold": 1,
                    "cost": 0.0,
                    "score": "mom",
                    "top2": False,
                    "trail_pct": c["trail_pct"],
                }
            )
    if "s" in batches:
        for c in BATCH_S:
            combos.append(
                {
                    **c,
                    "lookback": args.base_lb,
                    "ma": args.base_ma,
                    "min_hold": 1,
                    "cost": 0.0,
                    "score": "mom",
                    "top2": False,
                    "trail_pct": 8.0,
                }
            )

    seen: set[str] = set()
    uniq = []
    for c in combos:
        if c["id"] in seen:
            continue
        seen.add(c["id"])
        uniq.append(c)

    def eval_params(**params) -> dict[str, dict]:
        return {w: build_nav_from_cache(caches[w], **params) for w in windows}

    print(f"Evaluating baseline (trail={base_trail:g}) …", flush=True)
    baseline = eval_params(
        lookback=args.base_lb,
        ma_window=args.base_ma,
        min_hold=1,
        cost=0.0,
        score="mom",
        top2=False,
        trail_pct=base_trail,
        stock_min_n=1,
        stock_mom_gt0=False,
        stock_ma_frac=0.0,
        stock_cn_only=False,
    )
    results: dict = {
        "baseline": {
            "params": {
                "lookback": args.base_lb,
                "ma": args.base_ma,
                "min_hold": 1,
                "cost": 0.0,
                "trail_pct": base_trail,
            },
            "windows": baseline,
        }
    }

    hdr = (
        "| ID | trail | stockGate | "
        + " | ".join(windows)
        + " | 判定 |"
    )
    print(hdr, flush=True)
    print("|" + "|".join(["---"] * (4 + len(windows))) + "|", flush=True)
    a0_cells = " | ".join(
        f"{baseline[w]['fusedPct']:.1f}/{baseline[w]['maxDdFusedPct']:.1f}" for w in windows
    )
    print(
        f"| base | {base_trail:g} | anyPos | {a0_cells} | 基线 |",
        flush=True,
    )

    skip_ids = {"A0", "E0", "S0"}
    for c in uniq:
        if c["id"] in skip_ids:
            if c["id"] in ("E0", "S0"):
                results[c["id"]] = {"params": c, "windows": baseline}
                gate = _stock_gate_label(c)
                print(
                    f"| {c['id']} | {float(c.get('trail_pct') or 0):g} | {gate} | {a0_cells} | =base |",
                    flush=True,
                )
            continue
        params = {
            "lookback": c["lookback"],
            "ma_window": c["ma"],
            "min_hold": c["min_hold"],
            "cost": c["cost"],
            "score": c["score"],
            "top2": c["top2"],
            "trail_pct": float(c.get("trail_pct") or 0.0),
            "stock_min_n": int(c.get("stock_min_n") or 1),
            "stock_mom_gt0": bool(c.get("stock_mom_gt0")),
            "stock_ma_frac": float(c.get("stock_ma_frac") or 0.0),
            "stock_cn_only": bool(c.get("stock_cn_only")),
        }
        row = eval_params(**params)
        results[c["id"]] = {"params": c, "windows": row}
        cells = " | ".join(
            f"{row[w]['fusedPct']:.1f}/{row[w]['maxDdFusedPct']:.1f}" for w in windows
        )
        v = _verdict(row, baseline)
        gate = _stock_gate_label(c)
        extra = ""
        if params["trail_pct"] > 0:
            te = ",".join(str(row[w].get("trailExits", 0)) for w in windows)
            extra = f" exits[{te}]"
        print(
            f"| {c['id']} | {params['trail_pct']:g} | {gate} | {cells} | {v}{extra} |",
            flush=True,
        )

    report = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "strategy": "择强单轨",
        "baseline": f"mom_compare lb60 ma200 hold1 trail{base_trail:g}",
        "windows": {w: ALL_WINDOWS[w] for w in windows},
        "results": results,
    }
    out = (
        Path(args.json)
        if args.json
        else REPORT_DIR / f"pick_strong_grid_{datetime.now(UTC).strftime('%Y%m%d')}.json"
    )
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out}", flush=True)
    return 0


def _stock_gate_label(c: dict) -> str:
    if c.get("stock_cn_only"):
        return "CN-only"
    if float(c.get("stock_ma_frac") or 0) > 0:
        return f"MA≥{c['stock_ma_frac']:.0%}"
    if c.get("stock_mom_gt0"):
        return "mom>0"
    if int(c.get("stock_min_n") or 1) > 1:
        return f"n≥{c['stock_min_n']}"
    return "anyPos"


if __name__ == "__main__":
    raise SystemExit(main())
