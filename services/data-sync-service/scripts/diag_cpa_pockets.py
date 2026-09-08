#!/usr/bin/env python3
"""CPA pockets diagnostic: single-signal realized pnl sliced by dimensions.

Read-only vs Postgres. No Live impact. Discovery on OOS2+train, valid is
confirmation (no tuning on valid).

For each CPA Pop/Crossback signal (same _signal_at as replay_cpa_cn v1),
walk forward with CPA-native exits (exhaustion 10% / wedge_drop / 60d max),
entry next open (skip limit-up), exit next open (limit-down delay 5d),
cost 0.3% round-trip. Each signal independent (no slot constraints) -> pure
signal quality, free of portfolio ordering artefacts.

Slices: tightness quintile, 10d range bucket, board (main vs 300/301/688),
amount quintile, market breadth regime (close>MA20 share across universe that day:
low<0.4 / mid / high>0.6), exit reason, hold-day bucket.

Usage:
  PYTHONPATH=src python3 scripts/diag_cpa_pockets.py --windows OOS2,train,valid
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from replay_cpa_cn import (  # noqa: E402
    COSTS_ROUNDTRIP,
    MAX_HOLD_DAYS,
    WINDOWS,
    WARMUP_DAYS,
    _at_limit_down,
    _at_limit_up,
    _build_features,
    _exit_reason_at,
    _load_daily,
    _signal_at,
)

CAL_ALL: list[str] = []


def _board(ts: str) -> str:
    code = str(ts).split(".")[0]
    if code.startswith(("300", "301", "688")):
        return "growth20"
    return "main10"


def _breadth_by_day(feats: dict) -> dict[str, float]:
    """Per date: share of symbols with close > MA20 (simple, 20d mean)."""
    # build per-date lists via per-symbol closes; O(ts*days) but one pass
    per_date_close: dict[str, list[float]] = defaultdict(list)
    per_date_ma20: dict[str, list[float]] = defaultdict(list)
    for ts, f in feats.items():
        rows = f["rows"]
        closes = [r["close"] or 0 for r in rows]
        acc = 0.0
        win: list[float] = []
        for i, r in enumerate(rows):
            c = closes[i]
            if c and c > 0:
                win.append(c)
                acc += c
                if len(win) > 20:
                    acc -= win.pop(0)
            d = r["date"]
            if len(win) >= 20 and c and c > 0:
                per_date_close[d].append(c)
                per_date_ma20[d].append(acc / len(win))
    out: dict[str, float] = {}
    for d, cs in per_date_close.items():
        ms = per_date_ma20[d]
        n = len(cs)
        out[d] = sum(1 for c, m in zip(cs, ms) if c > m) / n if n else 0.5
    return out


def realize_signal(feats: dict, ts: str, si: int, entry_day: str) -> dict | None:
    """Walk one signal forward. Returns dict(pnl_net, exit_reason, hold_days) or None if no entry."""
    f = feats.get(ts)
    if f is None:
        return None
    rows = f["rows"]
    j = f["d2i"].get(entry_day)
    if j is None:
        return None
    row = rows[j]
    op = row["open"]
    if op is None or op <= 0:
        return None
    prev = row["pre_close"] if row["pre_close"] else (rows[j - 1]["close"] if j > 0 else None)
    if _at_limit_up(ts, prev, op):
        return None
    entry_px = op
    # walk holding days H = entry_day .. entry+60
    # find entry index j, then evaluate exits at each H close starting H=entry_day
    hold = 0
    for k in range(j, min(j + MAX_HOLD_DAYS + 6, len(rows))):
        d = rows[k]["date"]
        r = _exit_reason_at(f, k)
        hold += 1
        if r is not None or hold >= MAX_HOLD_DAYS:
            reason = r or "max_hold"
            # exit next trading day open after k
            for kk in range(k + 1, min(k + 7, len(rows))):
                erow = rows[kk]
                eop = erow["open"]
                if eop is None or eop <= 0:
                    continue
                eprev = erow["pre_close"] if erow["pre_close"] else rows[kk - 1]["close"]
                if _at_limit_down(ts, eprev, erow["close"]):
                    continue
                net = eop * (1 - COSTS_ROUNDTRIP) / entry_px - 1
                return {"net": net, "reason": reason, "hold": hold,
                        "entry_day": entry_day, "exit_day": erow["date"]}
            # never got exit print: force at last close
            last = rows[min(k + 6, len(rows) - 1)]
            if last["close"]:
                net = last["close"] * (1 - COSTS_ROUNDTRIP) / entry_px - 1
                return {"net": net, "reason": reason, "hold": hold,
                        "entry_day": entry_day, "exit_day": last["date"], "forced": True}
            return None
    return None


def diag_window(wname: str, wstart: str, wend: str) -> dict:
    warm = (date.fromisoformat(wstart) - timedelta(days=WARMUP_DAYS)).isoformat()
    per_ts, cal_all = _load_daily(warm, wend, a_only=True)  # A-only since 2026-09-08 fix
    feats = _build_features(per_ts)
    cal = [d for d in cal_all if wstart <= d <= wend]
    cal_set = set(cal)
    d2next = {cal_all[i]: cal_all[i + 1] for i in range(len(cal_all) - 1)}
    breadth = _breadth_by_day(feats)
    recs: list[dict] = []
    for ts, f in feats.items():
        rows = f["rows"]
        for i, r in enumerate(rows):
            d = r["date"]
            if d not in cal_set or d == cal[-1]:
                continue
            sig = _signal_at(f, i)
            if not sig:
                continue
            kind, tight = sig
            ed = d2next.get(d)
            if ed is None or ed not in f["d2i"]:
                continue
            out = realize_signal(feats, ts, i, ed)
            if out is None:
                continue
            # features at signal day
            hh = ll = None
            for j in range(max(0, i - 10), i + 1):
                hj, lj = rows[j]["high"], rows[j]["low"]
                if hj is None or lj is None or hj <= 0 or lj <= 0:
                    hh = None
                    break
                hh = hj if hh is None else max(hh, hj)
                ll = lj if ll is None else min(ll, lj)
            rng = (hh - ll) / rows[i]["close"] if hh and ll and rows[i]["close"] else None
            aa = f["avg_amt"][i]
            b = breadth.get(d, 0.5)
            recs.append({"net": out["net"], "reason": out["reason"], "hold": out["hold"],
                         "tight": tight, "range": rng, "amt": aa, "board": _board(ts),
                         "breadth": b, "kind": kind})
    def agg(rs: list[dict]) -> dict:
        n = len(rs)
        if not n:
            return {"n": 0}
        nets = [x["net"] for x in rs]
        return {"n": n, "mean_pp": round(sum(nets) / n * 100, 2),
                "win_pct": round(sum(1 for v in nets if v > 0) / n * 100, 1)}
    res: dict = {"window": wname, "n": len(recs), "all": agg(recs)}
    # tight quintiles (within window)
    ts_sorted = sorted([x["tight"] for x in recs])
    if ts_sorted:
        q = [ts_sorted[min(int(len(ts_sorted) * p / 5), len(ts_sorted) - 1)] for p in range(6)]
        for qi in range(5):
            lo, hi = q[qi], q[qi + 1] if qi < 4 else 1e9
            bucket = [x for x in recs if (x["tight"] >= lo and (x["tight"] < hi if qi < 4 else True))]
            res[f"tightQ{qi + 1}"] = {"lo": round(lo, 4), **agg(bucket)}
    # board
    for b in ("main10", "growth20"):
        res[f"board_{b}"] = agg([x for x in recs if x["board"] == b])
    # breadth regime
    res["breadth_low"] = agg([x for x in recs if x["breadth"] < 0.4])
    res["breadth_mid"] = agg([x for x in recs if 0.4 <= x["breadth"] <= 0.6])
    res["breadth_high"] = agg([x for x in recs if x["breadth"] > 0.6])
    # exit reason
    for r in ("wedge_drop", "exhaustion", "max_hold"):
        res[f"exit_{r}"] = agg([x for x in recs if x["reason"] == r])
    # hold buckets
    res["hold_1_3"] = agg([x for x in recs if x["hold"] <= 3])
    res["hold_4_10"] = agg([x for x in recs if 4 <= x["hold"] <= 10])
    res["hold_11p"] = agg([x for x in recs if x["hold"] >= 11])
    # amount halves
    amts = sorted([x["amt"] for x in recs if x["amt"] is not None])
    if amts:
        med = amts[len(amts) // 2]
        res["amt_low"] = agg([x for x in recs if (x["amt"] or 0) < med])
        res["amt_high"] = agg([x for x in recs if (x["amt"] or 0) >= med])
        res["amt_median_yi"] = med
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    args = ap.parse_args()
    for w in [x.strip() for x in args.windows.split(",") if x.strip() in WINDOWS]:
        s, e = WINDOWS[w]
        print(f"[{w}] {s}..{e} diagnosing...", flush=True)
        r = diag_window(w, s, e)
        print(f"[{w}] n={r['n']} all={r['all']}", flush=True)
        for k in ("tightQ1", "tightQ2", "tightQ3", "tightQ4", "tightQ5",
                  "board_main10", "board_growth20",
                  "breadth_low", "breadth_mid", "breadth_high",
                  "exit_wedge_drop", "exit_exhaustion", "exit_max_hold",
                  "hold_1_3", "hold_4_10", "hold_11p", "amt_low", "amt_high"):
            if k in r:
                print(f"  {k}: {r[k]}", flush=True)


if __name__ == "__main__":
    main()
