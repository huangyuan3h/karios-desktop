#!/usr/bin/env python3
"""Stage 3: hedge-twin v0.2 vs habit-twin vs core — triple windows + 2023 + aligned.

v0.2 spec (designs/hedge-twin-short-2026-09-05.md §2+§6) + v0.2.1 impl freeze:
- signals: strict scoop-exhaustion (ret60>0.40 & vr>1.2), shortable-restricted
  universe (last-known margin roster <= decision day; as-of safe; coincides
  with the 10d rule on the daily-sync era so stage-2/2b transfer).
- entry: signal-day next stock session open (pending queue, expire S+4 cal
  days); max 10 concurrent (skip when full, strict); dup names skipped.
- sizing: 3% NAV per name fixed (the only reading consistent with 30% total
  AND 5% cap AND 10 names); idle budget rides core (opportunity-style).
- exits: target=bottom*0.99 / stop=ph*1.02 (same-bar -> stop-first,
  conservative) / 20 stock sessions -> close; first check on entry session.
- costs: 0.6% roundtrip booked at exit + borrow 8%/252 per session held
  (entry..exit inclusive). Dividends unmodeled (noted limitation).
- portfolio daily: R = core_ret*(1-D) + sum(position session rets),
  D = 0.03 * N_open. Window-local books (start empty).

Habit twin = frozen habit satellite (C1 3% same_1430 body=3 exit1430 strict
clip4) opp_50 blended with the same frozen core. Universe/survivor/qfq
conventions identical on both sides; verdict on DELTA.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/replay_hedge_twin.py --save-report
"""
from __future__ import annotations

import argparse
import bisect
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
from pick_strong_grid import (  # noqa: E402
    build_nav_from_cache,
    fetch_etf_closes,
    warm_window,
)
from repro_scoop_short import _load_bars, detect  # noqa: E402
from audit_shortable_overlap import _load_margin  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "Y2023": ("2023-01-01", "2023-12-31"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2023-01-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REJECT_PT = 5.0
BORROW_APY = 0.08
PER_NAME = 0.03
MAX_SHORTS = 10
COST_RT = 0.006
LIMIT_PCT = 0.095  # bar range >= 9.5% treated as limit-day (fill optimism flag)


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


def _shortable_asof(margin: dict[str, list], ts: str, day: str) -> bool:
    rows = margin.get(ts)
    if not rows:
        return False
    for ds, _ in reversed(rows):
        if ds <= day:
            return True
    return False


def _rqmcl_asof(margin: dict[str, list], ts: str, day: str) -> float:
    rows = margin.get(ts)
    if not rows:
        return 0.0
    for ds, rqmcl in reversed(rows):
        if ds <= day:
            try:
                return float(rqmcl or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--windows", default="",
                    help="comma list to restrict windows (default: all)")
    ap.add_argument("--strict-borrow", action="store_true",
                    help="entries require rqmcl>0 as-of entry day (sensitivity, not the gate)")
    ap.add_argument("--report-name", default="hedge_twin_2026-09-05.json")
    args = ap.parse_args()
    only = {w.strip() for w in args.windows.split(",") if w.strip()}
    wins = {k: v for k, v in WINDOWS.items() if not only or k in only}
    if only and "valid" not in wins:
        pass

    print("loading bars + signals (strict only) ...", flush=True)
    per_ts = _load_bars()
    sig_by_date: dict[str, list[dict]] = {}
    n_strict = 0
    for ts, s in per_ts.items():
        for sig in detect(s):
            if sig["ret60"] > 0.40 and sig["vr"] > 1.2:
                sig_by_date.setdefault(sig["date"], []).append(
                    {"ts": ts, "t": sig["t"], "target": sig["target"], "stop": sig["stop"]})
                n_strict += 1
    print(f"  strict signals: {n_strict}", flush=True)
    print("loading margin roster ...", flush=True)
    margin = _load_margin()
    print("loading mv map ...", flush=True)
    import psycopg
    from data_sync_service.config import get_settings
    _mconn = psycopg.connect(get_settings().database_url)
    _mcur = _mconn.cursor()
    _mcur.execute(
        "SELECT ts_code, trade_date, total_mv FROM stock_dailybasic "
        "WHERE trade_date >= '2023-01-01' AND trade_date <= '2026-08-28' AND total_mv IS NOT NULL")
    mv_map: dict[tuple[str, str], float] = {}
    for _ts, _d, _mv in _mcur.fetchall():
        _ds = _d.strftime("%Y-%m-%d") if hasattr(_d, "strftime") else str(_d)
        mv_map[(str(_ts), _ds)] = float(_mv) / 10000.0
    _mconn.close()
    print(f"  mv entries: {len(mv_map)}", flush=True)
    print("loading sgap context ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    cal: list[str] = ctx["cal"]
    print("fetching etf closes ...", flush=True)
    etf_close = fetch_etf_closes()

    results: dict[str, dict] = {}
    for wname, (s, e) in wins.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        dates = [d for d in cal if s <= d <= e]
        # frozen core
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(
            cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
            score="mom", top2=False, trail_pct=8.0)
        core_map: dict[str, float] = r["nav"]
        core_nav, last_c = [], 1.0
        for d in dates:
            if d in core_map:
                last_c = core_map[d]
            core_nav.append(last_c)
        core_nav = [v / core_nav[0] for v in core_nav]
        core_ret = [0.0] + [core_nav[i] / core_nav[i - 1] - 1 for i in range(1, len(core_nav))]
        core_m = _stats(core_nav)
        print(f"  core     {_fmt(core_m)}", flush=True)
        # habit twin (frozen)
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03)
        sat_map = {row["date"]: (float(row["satNav"]), bool(row.get("satActive"))) for row in sat["rows"]}
        sat_nav, sat_act, last_s, last_a = [], [], 1.0, False
        for d in dates:
            if d in sat_map:
                last_s, last_a = sat_map[d]
            sat_nav.append(last_s)
            sat_act.append(last_a)
        sat_nav = [v / sat_nav[0] for v in sat_nav]
        habit_nav = blend_nav_opportunity(core_nav, sat_nav, sat_act, sat_weight=0.5)
        habit_m = _stats(habit_nav)
        print(f"  habit    {_fmt(habit_m)}  fills {sat['summary'].get('fillCount')}", flush=True)
        # hedge twin: short book
        open_pos: dict[str, dict] = {}
        pending: list[dict] = []
        hedge_nav = [1.0]
        idx_cache: dict[str, dict[str, int]] = {}
        def _idxmap(key: str) -> dict[str, int]:
            hit = idx_cache.get(key)
            if hit is None:
                s0 = per_ts[key]
                hit = {dd: i for i, dd in enumerate(s0["d"])}
                idx_cache[key] = hit
            return hit
        diag = {"signals": 0, "shorted": 0, "skipMargin": 0, "skipFull": 0,
                "skipDup": 0, "skipNoOpen": 0, "skipExpire": 0, "skipNoBorrow": 0,
                "exitTarget": 0, "exitStop": 0, "exitDue": 0,
                "limitEntry": 0, "limitExit": 0,
                "borrowPaid": 0.0, "holds": [], "pnls": [], "mvs": [],
                "cumSleeve": 0.0, "cumCorePart": 0.0, "sumD": 0.0, "nD": 0}
        cal_idx = {d: i for i, d in enumerate(cal)}
        for di, d in enumerate(dates):
            for sig in sig_by_date.get(dates[di - 1] if di > 0 else "", []):
                pending.append({**sig, "S": dates[di - 1] if di > 0 else d, "exp": d})
                diag["signals"] += 1
            # exits first (free slots same day)
            # exits first (free slots same day). Pure daily-incremental
            # accounting: each session books only its own move; full-trade
            # net is diagnostics-only (never added to NAV).
            dep_ret = 0.0
            for key in list(open_pos):
                p = open_pos[key]
                s = per_ts[key]
                idxmap = _idxmap(key)
                ti = idxmap.get(d, -1)
                if ti < 0:
                    continue  # suspended: hold, no mtm (borrow dust ignored)
                cur_c = float(s["c"][ti]) if s["c"][ti] > 0 else None
                if ti <= p["entry_idx"]:
                    if cur_c and p["entry"] > 0:  # entry session: open->close
                        dep_ret += PER_NAME * (-(cur_c / p["entry"] - 1) - BORROW_APY / 252)
                    continue  # exits start next bar
                prev_c = float(s["c"][ti - 1]) if ti > 0 and s["c"][ti - 1] > 0 else None
                px = None
                reason = None
                exit_j = -1
                # v0.2.2 limit-fill-correct: a below-market buy level fills iff
                # the day's low trades at/below it (gap-through fills at open);
                # a true stop (>= entry) fills iff high trades at/above it
                # (gap-over fills at open). Untouched levels = hold (no phantom).
                stop_is_take = p["stop"] < p["entry"]
                for j in range(max(p["entry_idx"] + 1, p["scan_from"]), ti + 1):
                    oj = float(s["o"][j])
                    hj = float(s["h"][j])
                    lj = float(s["l"][j])
                    if lj <= p["target"]:
                        px, reason, exit_j = (oj if oj < p["target"] else p["target"]), "target", j
                        break
                    if stop_is_take:
                        if lj <= p["stop"]:
                            px, reason, exit_j = (oj if oj < p["stop"] else p["stop"]), "stop", j
                            break
                    else:
                        if hj >= p["stop"]:
                            px, reason, exit_j = (oj if oj > p["stop"] else p["stop"]), "stop", j
                            break
                if px is None and ti >= p["entry_idx"] + 20:
                    px, reason, exit_j = cur_c, "due", ti
                if px is not None and px > 0 and reason is not None:
                    hold = ti - p["entry_idx"] + 1
                    if prev_c and prev_c > 0:
                        dep_ret += PER_NAME * (-(px / prev_c - 1) - BORROW_APY / 252 - COST_RT)
                    full_net = (p["entry"] - px) / p["entry"] - COST_RT - BORROW_APY * hold / 252
                    diag["borrowPaid"] += PER_NAME * BORROW_APY * hold / 252
                    diag["holds"].append(hold)
                    diag["pnls"].append(full_net * 100)
                    _rng = (float(s["h"][exit_j]) - float(s["l"][exit_j])) / px if px > 0 else 0.0
                    if _rng >= LIMIT_PCT:
                        diag["limitExit"] += 1
                    diag[{"target": "exitTarget", "stop": "exitStop", "due": "exitDue"}[reason]] += 1
                    del open_pos[key]
                else:
                    if prev_c and cur_c:
                        dep_ret += PER_NAME * (-(cur_c / prev_c - 1) - BORROW_APY / 252)
                    p["scan_from"] = ti + 1
            # entries (oldest first)
            still_pending = []
            for sig in sorted(pending, key=lambda x: x["S"]):
                if sig["exp"] < d and (cal_idx.get(d, 0) - cal_idx.get(sig["exp"], 0)) > 4:
                    diag["skipExpire"] += 1
                    continue
                key = sig["ts"]
                s = per_ts[key]
                idxmap = _idxmap(key)
                ti = idxmap.get(d, -1)
                if ti < 0 or not s["o"][ti] or float(s["o"][ti]) <= 0:
                    still_pending.append(sig)
                    continue
                if key in open_pos:
                    diag["skipDup"] += 1
                    continue
                if len(open_pos) >= MAX_SHORTS:
                    diag["skipFull"] += 1
                    still_pending.append(sig)
                    continue
                if not _shortable_asof(margin, key, d):
                    diag["skipMargin"] += 1
                    continue
                if args.strict_borrow and _rqmcl_asof(margin, key, d) <= 0:
                    diag["skipNoBorrow"] += 1
                    continue
                entry = float(s["o"][ti])
                open_pos[key] = {"entry": entry, "target": sig["target"], "stop": sig["stop"],
                                 "entry_idx": ti, "scan_from": ti + 1 if ti + 1 <= len(s["d"]) - 1 else ti}
                # entry-day session move open->close + borrow; exit check starts next bar
                cur_c = float(s["c"][ti]) if s["c"][ti] > 0 else entry
                dep_ret += PER_NAME * (-(cur_c / entry - 1) - BORROW_APY / 252)
                if cur_c > 0 and (float(s["h"][ti]) - float(s["l"][ti])) / cur_c >= LIMIT_PCT:
                    diag["limitEntry"] += 1
                _mv = mv_map.get((key, d))
                if _mv:
                    diag["mvs"].append(_mv)
                diag["shorted"] += 1
            pending = [p for p in still_pending
                       if (cal_idx.get(d, 0) - cal_idx.get(p["exp"], 0)) <= 4]
            D = PER_NAME * len(open_pos)
            diag["cumSleeve"] += dep_ret
            diag["cumCorePart"] += core_ret[di] * (1.0 - D)
            diag["sumD"] += D
            diag["nD"] += 1
            hedge_nav.append(hedge_nav[-1] * (1.0 + core_ret[di] * (1.0 - D) + dep_ret))
        hedge_nav = hedge_nav[1:]
        base = hedge_nav[0]
        hedge_nav = [v / base for v in hedge_nav]
        hedge_m = _stats(hedge_nav)
        pnls = diag["pnls"]
        holds = diag["holds"]
        pprof = float(np.mean([1.0 if x > 0 else 0.0 for x in pnls])) * 100 if pnls else 0.0
        mean_hold = float(np.mean(holds)) if holds else 0.0
        mean_net = float(np.mean(pnls)) if pnls else 0.0
        simple_sum = sum(0.03 * x / 100 for x in pnls)
        print(f"  hedge    {_fmt(hedge_m)}  shorts {diag['shorted']} "
              f"P(profit)/fill {pprof:.0f}% skipM {diag['skipMargin']} skipF {diag['skipFull']}", flush=True)
        print(f"    reconcile: avgHold {mean_hold:.1f}d meanNet/fill {mean_net:+.2f}% "
              f"simpleSum {simple_sum:+.2f} exits T/S/D {diag['exitTarget']}/{diag['exitStop']}/{diag['exitDue']}",
              flush=True)
        _mv_med = float(np.median(diag["mvs"])) if diag["mvs"] else 0.0
        _avgD = diag["sumD"] / max(1, diag["nD"])
        print(f"    honesty: avgDeployed {_avgD:.3f} cumSleeve {diag['cumSleeve']:+.2f} "
              f"cumCorePart {diag['cumCorePart']:+.2f} limitEntry {diag['limitEntry']} "
              f"limitExit {diag['limitExit']} mvMedian {_mv_med:.1f}亿 nMv {len(diag['mvs'])}",
              flush=True)
        diag["mvMedianYi"] = round(_mv_med, 1)
        diag["avgDeployed"] = round(_avgD, 4)
        results[wname] = {"core": core_m, "habit": habit_m, "hedge": hedge_m,
        "short_diag": {k: (round(v, 4) if isinstance(v, float) else v)
                       for k, v in diag.items()
                       if k not in ("holds", "pnls", "mvs", "cumSleeve", "cumCorePart", "sumD", "nD")},
                          "fill_P_profit": round(pprof, 1),
                          "d_hedge_habit_tot": round(hedge_m["total_pct"] - habit_m["total_pct"], 1),
                          "d_hedge_habit_sr": round(hedge_m["sharpe"] - habit_m["sharpe"], 2),
                          "d_hedge_habit_dd": round(hedge_m["max_dd"] - habit_m["max_dd"], 1),
                          "d_hedge_core_tot": round(hedge_m["total_pct"] - core_m["total_pct"], 1),
                          "d_hedge_core_sr": round(hedge_m["sharpe"] - core_m["sharpe"], 2),
                          "d_hedge_core_dd": round(hedge_m["max_dd"] - core_m["max_dd"], 1)}

    print("\n## Verdicts (hedge vs habit-twin primary; vs core secondary)\n")
    verdicts: dict[str, str] = {}
    if not all(w in results for w in WF_WINDOWS):
        print("(restricted windows: verdicts skipped)")
    else:
        for key in ("hedge",):
            d_tot = [results[w][f"d_{key}_habit_tot"] for w in WF_WINDOWS]
            d_sr = [results[w][f"d_{key}_habit_sr"] for w in WF_WINDOWS]
            d_dd = [results[w][f"d_{key}_habit_dd"] for w in WF_WINDOWS]
            c_tot = [results[w][f"d_{key}_core_tot"] for w in WF_WINDOWS]
            c_sr = [results[w][f"d_{key}_core_sr"] for w in WF_WINDOWS]
            c_dd = [results[w][f"d_{key}_core_dd"] for w in WF_WINDOWS]
            flags = []
            if any(t < -REJECT_PT for t in d_tot):
                flags.append("REJECT/total")
            if any(s < -0.3 for s in d_sr):
                flags.append("worse_sharpe")
            if any(x > 0 for x in d_dd):
                flags.append("worse_dd")
            if not flags:
                flags.append("PASS+" if all(t > 0 for t in d_tot) else "PASS")
            core_flags = []
            if not all(t > 0 for t in c_tot):
                core_flags.append("loses_core_tot")
            if not all(s >= 0 for s in c_sr):
                core_flags.append("worse_sharpe")
            if not all(x <= 0 for x in c_dd):
                core_flags.append("worse_dd")
            core_tag = "beats_core" if not core_flags else "+".join(core_flags)
            valid_ok = results["valid"][f"d_{key}_core_tot"] > 1.3
            verdicts[key] = f"{'+'.join(flags)}/{core_tag}/valid_buf_ok={valid_ok}"
            print(f"- {key} vs habit: " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, d_tot))
                  + f" -> {'+'.join(flags)}")
            print(f"  {key} vs core:  " + ", ".join(f"{w} {d:+.1f}" for w, d in zip(WF_WINDOWS, c_tot))
                  + f" -> {core_tag}; validΔcore {results['valid'][f'd_{key}_core_tot']:+.1f} (>1.3: {valid_ok})")
    payload = {
        "tag": "hedge-twin-v02-2026-09-05" + ("-strictborrow" if args.strict_borrow else ""),
        "variant": "strict_borrow" if args.strict_borrow else "primary_roster",
        "protocol": ("window-local books; frozen pick-strong trail8 core; frozen habit sat "
                     "(C1 same_1430 body3 exit1430 strict clip4) opp_50; short sleeve v0.2 "
                     "(shortable-restricted strict scoop, next-open, tgt/stop/20d, 0.6% + "
                     "8% borrow, 3%/name max10, idle->core). Survivor/qfq conventions "
                     "identical both sides; verdict on DELTA."),
        "windows": results,
        "verdicts": verdicts,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        if set(wins) != set(WINDOWS):
            print("(restricted run: report NOT overwritten)")
            return 0
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / args.report_name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
