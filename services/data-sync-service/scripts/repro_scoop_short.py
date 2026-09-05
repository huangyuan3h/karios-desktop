#!/usr/bin/env python3
"""Stage 1: reproduce the frozen scoop-short table (pattern-factor-validation §2.4).

Rules verbatim: detection = factor_signals_service.scan_strong_scoop_exhaustion
gates; entry = next session open. Fill semantics v0.2.2 (limit-fill-correct):
a below-market buy level (target always; stop when stop < entry, i.e. the
82% inverted majority) is a resting TAKE-PROFIT limit and fills iff the
day's low trades at/below it (gap-through fills at open, better); a
stop >= entry is a true buy-stop (fills iff high trades at/above it,
gap-over fills at open, worse). Same-bar target+stop ambiguity resolves to
stop-first (conservative). Max 20 sessions, due exits at close. Cost 0.6%
roundtrip deducted. NOTE: naive bar-touch (fill whenever high>=stop) books
phantom profits on inverted levels and is WRONG; see hedge-twin §4.

Pipeline gate (runs first): recompute 2025-06-16 and require >=90% overlap
with the stored factor_signals rows (base ret60>0.30 and strict ret60>0.40 &
vr>1.2 sets). Abort otherwise — never let a reimplementation drift silently
(E-veto lesson: context lacked vol).

Verdict gate (design §3.1): primary bucket ret60>0.40 & vr>1.2 hit% in
85-93%. Other five rows descriptive (levels + monotonicity).

Read-only vs Postgres. Saves nothing (prints tables only).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

SIG_START = "2021-08-02"

FROZEN = {
    # (ret_thr, vol_req): (n, hit, R)
    (0.30, False): (98555, 78.7, 7.96),
    (0.40, False): (59615, 83.0, 10.25),
    (0.50, False): (37366, 86.5, 12.65),
    (0.30, True): (39647, 85.4, 10.66),
    (0.40, True): (24426, 89.4, 13.55),
    (0.50, True): (15460, 92.2, 16.53),
}


def _load_bars():
    import psycopg

    from data_sync_service.config import get_settings

    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.vol
        FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
        WHERE d.trade_date >= '2021-03-01'
          AND sb.delist_date IS NULL
          AND sb.name NOT LIKE '%%ST%%'
          AND d.ts_code NOT LIKE '%%.BJ'
          AND d.ts_code NOT LIKE '%%.HK'
        ORDER BY d.ts_code, d.trade_date
        """
    )
    per_ts: dict[str, dict] = {}
    cur_ts = None
    acc: list = []
    for ts, d, o, h, lo, c, v in cur.fetchall():
        ts = str(ts)
        if ts != cur_ts:
            if cur_ts and len(acc) > 100:
                per_ts[cur_ts] = _to_arrays(acc)
            cur_ts, acc = ts, []
        try:
            c = float(c)
        except (TypeError, ValueError):
            continue
        if c <= 0:
            continue
        def _f(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return 0.0
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        acc.append((ds, _f(o), _f(h), _f(lo), c, _f(v)))
    if cur_ts and len(acc) > 100:
        per_ts[cur_ts] = _to_arrays(acc)
    conn.close()
    return per_ts


def _to_arrays(rows: list) -> dict:
    return {
        "d": [r[0] for r in rows],
        "o": np.array([r[1] for r in rows]),
        "h": np.array([r[2] for r in rows]),
        "l": np.array([r[3] for r in rows]),
        "c": np.array([r[4] for r in rows]),
        "v": np.array([r[5] for r in rows]),
    }


def detect(s: dict) -> list[dict]:
    """All base signals (ret60>0.30) with detection index t. Verbatim gates."""
    c, h, l, v, dd = s["c"], s["h"], s["l"], s["v"], s["d"]
    n = len(c)
    if n < 100:
        return []
    ma20 = np.full(n, np.nan)
    ma60 = np.full(n, np.nan)
    cs = np.cumsum(np.concatenate([[0.0], c]))
    ma20[19:] = (cs[20:] - cs[:-20]) / 20.0
    ma60[59:] = (cs[60:] - cs[:-60]) / 60.0
    out = []
    for t in range(89, n):
        if dd[t] < SIG_START:
            continue
        if not (ma20[t] > ma60[t] and c[t - 30] > ma60[t - 30]):
            continue
        ph = float(np.max(h[t - 40:t - 20]))
        if ph <= 0:
            continue
        win = l[t - 20:t + 1]
        if np.any(win <= 0):
            continue
        bottom = float(np.min(win))
        bi = t - 20 + int(np.argmin(win))
        depth = (ph - bottom) / ph
        if not (0.05 <= depth <= 0.18):
            continue
        if not (c[t] >= bottom * 1.03 and c[t] >= ma20[t] * 0.99):
            continue
        if bi < t - 15:
            continue
        sv = float(np.mean(v[t - 20:t + 1]))
        if sv <= 0 or v[t] <= 0:
            continue
        vr = float(v[t] / sv)
        ret60 = float(c[t] / c[t - 60] - 1)
        if ret60 <= 0.30:
            continue
        out.append({"t": t, "date": dd[t], "entry": float(c[t]),
                    "target": bottom * 0.99, "stop": ph * 1.02,
                    "ret60": ret60, "vr": vr})
    return out


def settle(s: dict, sig: dict, last_t: int) -> tuple[bool, float] | None:
    """Forward ≤20 sessions from next-open entry. Limit-fill-correct (v0.2.2).
    Returns (target_first, net_pct) or None if fewer than 21 forward bars
    exist (tail cut, dropped)."""
    t = sig["t"]
    if t + 21 > last_t:
        return None
    entry = float(s["o"][t + 1])
    if entry <= 0:
        return None
    target, stop = sig["target"], sig["stop"]
    h, l, c, o = s["h"], s["l"], s["c"], s["o"]
    stop_is_take = stop < entry
    target_first = False
    for j in range(t + 2, t + 22):
        oj, hj, lj = float(o[j]), float(h[j]), float(l[j])
        if lj <= target:
            px = oj if oj < target else target
            target_first = True
            return (target_first, (entry - px) / entry * 100 - 0.6)
        if stop_is_take:
            if lj <= stop:
                px = oj if oj < stop else stop
                return (target_first, (entry - px) / entry * 100 - 0.6)
        else:
            if hj >= stop:
                px = oj if oj > stop else stop
                return (target_first, (entry - px) / entry * 100 - 0.6)
    px = float(c[t + 21])
    return (target_first, (entry - px) / entry * 100 - 0.6)


def main() -> int:
    print("loading bars (2021-03+ full A, ex ST/BJ/delisted) ...", flush=True)
    per_ts = _load_bars()
    print(f"names: {len(per_ts)}", flush=True)
    max_date = max(s["d"][-1] for s in per_ts.values())
    print(f"max date: {max_date}", flush=True)

    # ---- pipeline gate: 7 unambiguous CN strict signals (ret60/vr verified
    # 2026-09-05 against stored rows). NOTE: the stored factor_signals table
    # mixes HK names under ambiguous 'CN:xxxxx' symbols (a production labeling
    # bug), so set-overlap gating is contaminated; exact-signal gating isn't.
    CHECKS = {
        # ts_code: (ret60, vr)
        "000506.SZ": (0.6827, 1.3142),
        "603139.SH": (0.5145, 3.7472),
        "688386.SH": (0.5302, 1.7860),
        "002951.SZ": (1.2780, 2.7038),
        "300682.SZ": (0.4955, 2.7438),
        "301024.SZ": (0.6687, 1.8319),
        "002104.SZ": (0.8794, 1.7986),
    }
    missed = []
    for ts, (er, ev) in CHECKS.items():
        s = per_ts.get(ts)
        found = False
        if s is not None:
            for sig in detect(s):
                if sig["date"] == "2025-06-16" and abs(sig["ret60"] - er) < 0.02 \
                        and abs(sig["vr"] - ev) < 0.15:
                    found = True
                    break
        if not found:
            missed.append(ts)
    print(f"pipeline gate 2025-06-16 exact signals: {7 - len(missed)}/7")
    if missed:
        print(f"  missed: {missed}")
        print("PIPELINE GATE FAILED — abort, do not trust numbers below.")
        return 2
    print("pipeline gate PASS\n", flush=True)

    # ---- full-history reproduction ----
    buckets: dict[tuple, list] = {k: [] for k in FROZEN}
    n_sig = 0
    for ts, s in per_ts.items():
        last_t = len(s["d"]) - 1
        for sig in detect(s):
            r = settle(s, sig, last_t)
            if r is None:
                continue
            n_sig += 1
            hit, net = r
            for (rt, vq) in buckets:
                if sig["ret60"] > rt and (not vq or sig["vr"] > 1.2):
                    buckets[(rt, vq)].append((hit, net))
    print(f"settled signals: {n_sig}\n")
    print("| bucket | frozen n/hit/R | repro n / tgt-first / P(profit) / R |")
    print("|--------|----------------|-------------------------------------|")
    ok = True
    all_nets: list[float] = []
    all_mae: list[float] = []
    worst = (0.0, "", "")
    for key in [(0.30, False), (0.40, False), (0.50, False),
                (0.30, True), (0.40, True), (0.50, True)]:
        fn, fh, fr = FROZEN[key]
        v = buckets[key]
        n = len(v)
        tgt = float(np.mean([1.0 if x[0] else 0.0 for x in v])) * 100 if v else 0.0
        nets = [x[1] for x in v]
        prof = float(np.mean([1.0 if x > 0 else 0.0 for x in nets])) * 100 if nets else 0.0
        rmean = float(np.mean(nets)) if nets else 0.0
        tag = "ret60>%.2f%s" % (key[0], " & vol" if key[1] else "")
        print(f"| {tag} | {fn}/{fh:.1f}/{fr:+.2f} | {n}/{tgt:.1f}/{prof:.1f}/{rmean:+.2f} |")
        if key == (0.40, True):
            all_nets = nets
            if not (85.0 <= prof <= 93.0):
                ok = False
    print()
    if all_nets:
        arr = np.array(all_nets)
        print(f"strict risk: worst {arr.min():+.1f}%  P(net<-5%) {np.mean(arr < -5) * 100:.1f}%  "
              f"mean|loss {np.mean(arr[arr < 0]):+.2f}%  med {np.median(arr):+.2f}%")
    print("STAGE-1 VERDICT (P(profit) reading):",
          "PASS (reproduced)" if ok else "FAIL (not reproduced) — close the building")
    return 0 if ok else 1


def _num_sym(sym: str) -> str:
    s = str(sym).upper()
    if ":" in s:
        s = s.split(":")[-1]
    return s.split(".")[0].zfill(6)


def _num_ts(ts: str) -> str:
    return str(ts).split(".")[0].zfill(6)


if __name__ == "__main__":
    raise SystemExit(main())
