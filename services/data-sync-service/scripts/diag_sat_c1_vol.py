"""Diagnostic (read-only): does the fixed C1 3% cutoff misfire by volatility stratum?

Motivation (user): the frozen habit S-gap filter C1 skips a name when
``14:30 print / today's open - 1 > 3%`` ("pulse already spent"). A fixed 3%
means different things for a calm name vs a high-volatility one, so C1 may be
(误伤) killing calm names that still had room, or (错买) letting volatile names
through. This script quantifies that by stratifying the strict bucket by a
trailing volatility measure.

Scope = DIAGNOSTIC ONLY. It reconstructs the frozen bucket and prints
descriptive tables; it does NOT scan C1 thresholds, does not write any table,
and does not change Live. Any rule change would need a fresh preregistration
(see AGENTS.md "Strategy / parameter changes" + first-principles §一.9
vol-non-factor / §三死因 #4 single-window overfit).

Reconstruction (mirrors ``replay_sgap_from_context`` with HABIT_RECIPE):
  gate      : R-wide 14:30 breadth > 0.5 (``_breadth_at_1430``, OPT-224 fallback)
  universe  : S-gap ``open/pre_close-1 > 3%`` (decision day, qfq daily)
  rank      : amp_1430 ascending (max high - min low over bars <=14:30 / 14:30 print)
  bucket    : top 1/BUCKET_Q of the ranked gap names
  skip_t1   : T-1 limit lock (excluded, not a C1 decision)
  C1 input  : run = 14:30 raw print / raw open - 1   (qfq daily scaled by
              raw_1500/qfq_close, the same basis fix the engine uses)

Per bucket name we also compute:
  vol  = trailing 20-session mean daily amplitude (high-low)/close (prior days)
  fwd  = 14:30 T -> 14:30 T+2 net COSTS_ROUNDTRIP (body=3: entry day = hold day 1)

Outputs per window: per-vol-tercile split of C1-kept vs C1-skipped forward
returns, a run-bin table, and a cross-window consistency summary. A line is
printed comparing the reconstructed C1 skip count against the engine blotter
so the reader can trust the reconstruction.

Usage:
  PYTHONPATH=src python3 scripts/diag_sat_c1_vol.py
  PYTHONPATH=src python3 scripts/diag_sat_c1_vol.py --windows OOS2,train,valid,long
"""
from __future__ import annotations

import argparse

import numpy as np

from data_sync_service.service.state_bucket_track import (
    BUCKET_Q,
    COSTS_ROUNDTRIP,
    HABIT_CTX_TIMES,
    MIN_GAP_PCT,
    R_WIDE_THRESHOLD,
    _breadth_at_1430,
    _cached_day_features,
    _limit_locked_px,
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}
VOL_LOOKBACK = 20
RUN_BINS = [(0.0, 0.01), (0.01, 0.02), (0.02, 0.03), (0.03, 0.04), (0.04, 0.05), (0.05, 9.9)]
STRATUM_NAMES = ("calm", "mid", "wild")


def _trailing_vol(series, di: int, lookback: int = VOL_LOOKBACK) -> float | None:
    """Mean daily amplitude (high-low)/close over the `lookback` sessions *before*
    the decision day (strictly causal: decision day's H/L is unknown at 14:30)."""
    if di < lookback:
        return None
    vals: list[float] = []
    for r in series[di - lookback : di]:
        c, h, lo = r.get("close"), r.get("high"), r.get("low")
        if c and h and lo and c > 0:
            vals.append((float(h) - float(lo)) / float(c))
    if len(vals) < 15:
        return None
    return sum(vals) / len(vals)


def _fwd_net(ctx, ts: str, day: str, cost: float = COSTS_ROUNDTRIP) -> float | None:
    """14:30 T -> 14:30 T+2 net (body=3). None when either print is missing."""
    cal = ctx["cal"]
    i = ctx["idx_by_day"].get(day, -1)
    if i < 0 or i + 2 >= len(cal):
        return None
    entry = (ctx.get("px_1430") or {}).get(ts, {}).get(day)
    exitp = (ctx.get("px_1430") or {}).get(ts, {}).get(cal[i + 2])
    if not entry or not exitp or entry <= 0:
        return None
    return float(exitp) / float(entry) - 1.0 - cost


def _bucket_records(ctx) -> tuple[list[dict], dict]:
    """Reconstruct the strict bucket per decision day and return per-name records.

    Each record: day, ts, amp_rank, run (C1 input), vol, fwd (net), c1_skip.
    Also returns counters for coverage reporting.
    """
    cal = ctx["cal"]
    date_idx = ctx["date_idx"]
    per_ts = ctx["per_ts"]
    px_1430 = ctx.get("px_1430") or {}
    hl_1430 = ctx.get("px_hl_1430") or {}
    raw_1500 = (ctx.get("px_by_hhmm") or {}).get("1500") or {}
    close_by_ts = ctx.get("close_by_ts") or {}

    counters = {
        "days_gate_open": 0,
        "bucket_names": 0,
        "no_print": 0,
        "no_basis": 0,
        "skip_t1": 0,
        "c1_skip": 0,
        "used": 0,
    }
    out: list[dict] = []

    start = ctx.get("_window_start")
    end = ctx.get("_window_end")
    for day in cal:
        if day <= start or day > end:
            continue
        feat_all, breadth = _cached_day_features(ctx, day)
        b1430 = _breadth_at_1430(ctx, day)
        if b1430 is not None:
            breadth = b1430
        if not (breadth > R_WIDE_THRESHOLD):
            continue
        counters["days_gate_open"] += 1

        gap_stocks = [
            ts
            for ts, d in feat_all.items()
            if d.get("gap") is not None and d["gap"] == d["gap"] and d["gap"] > MIN_GAP_PCT
        ]

        def _amp_key(ts: str, _day: str = day) -> tuple:
            px = (px_1430.get(ts) or {}).get(_day)
            hl = (hl_1430.get(ts) or {}).get(_day)
            if not px or px <= 0 or not hl:
                return (1, float("inf"))
            return (0, float(hl[0] - hl[1]) / float(px))

        ranked = sorted(gap_stocks, key=_amp_key)
        if not ranked:
            continue
        qn = max(1, len(ranked) // BUCKET_Q)
        bucket = ranked[:qn]
        counters["bucket_names"] += len(bucket)

        for rank_pos, ts in enumerate(bucket):
            px = (px_1430.get(ts) or {}).get(day)
            if not px or px <= 0:
                counters["no_print"] += 1
                continue
            di = date_idx.get(ts, {}).get(day, -1)
            if di < 0:
                continue
            bar = per_ts[ts][di]
            raw_v = (raw_1500.get(ts) or {}).get(day)
            q_close = close_by_ts.get(ts, {}).get(day)
            if not raw_v or not q_close or float(q_close) <= 0:
                counters["no_basis"] += 1
                continue
            k = float(raw_v) / float(q_close)
            raw_open = bar.get("open")
            raw_pre = bar.get("pre_close")
            if raw_open:
                raw_open = float(raw_open) * k
            if raw_pre:
                raw_pre = float(raw_pre) * k
            if _limit_locked_px(px, raw_pre, ts):
                counters["skip_t1"] += 1
                continue
            if not raw_open or raw_open <= 0:
                continue
            run = float(px) / raw_open - 1.0
            vol = _trailing_vol(per_ts[ts], di)
            fwd = _fwd_net(ctx, ts, day)
            if vol is None or fwd is None:
                continue
            c1_skip = run > 0.03
            if c1_skip:
                counters["c1_skip"] += 1
            counters["used"] += 1
            out.append(
                {
                    "day": day,
                    "ts": ts,
                    "amp_rank": rank_pos + 1,
                    "run": run,
                    "vol": vol,
                    "fwd": fwd,
                    "c1_skip": c1_skip,
                }
            )
    return out, counters


def _tercile_bounds(vols: np.ndarray) -> tuple[float, float]:
    return float(np.quantile(vols, 1 / 3)), float(np.quantile(vols, 2 / 3))


def _stratum(vol: float, lo: float, hi: float) -> int:
    if vol <= lo:
        return 0
    if vol <= hi:
        return 1
    return 2


def _mean(xs: list[float]) -> float:
    return float(np.mean(xs)) if xs else float("nan")


def _winrate(xs: list[float]) -> float:
    return 100.0 * sum(1 for x in xs if x > 0) / len(xs) if xs else float("nan")


def _fmt_pct(x: float) -> str:
    return "   n/a" if x != x else f"{x * 100:+6.2f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid,long")
    args = ap.parse_args()
    windows = [w.strip() for w in args.windows.split(",") if w.strip()]

    print(
        "C1 fixed 3% cutoff vs trailing-20d volatility strata (diagnostic, read-only)\n"
        f"cost={COSTS_ROUNDTRIP * 100:.2f}bp round-trip | fwd = 14:30 T -> T+2 net | "
        f"vol = mean (high-low)/close prior 20 sessions\n"
    )

    summary: dict[str, dict] = {}
    for w in windows:
        s, e = WINDOWS[w]
        print(f"=== {w}  ({s} ~ {e}) ===", flush=True)
        ctx = load_sgap_context(s, e, times=HABIT_CTX_TIMES)
        ctx["_window_start"], ctx["_window_end"] = s, e
        records, counters = _bucket_records(ctx)

        engine = replay_sgap_from_context(ctx, start=s, end=e, **_HABIT_KWARGS)
        eng_skip = int((engine.get("summary") or {}).get("skipC1Count") or 0)
        print(
            f"  gate-open days {counters['days_gate_open']} | bucket names {counters['bucket_names']} "
            f"| no-print {counters['no_print']} | no-basis {counters['no_basis']} | "
            f"skip_t1 {counters['skip_t1']} | C1-skip {counters['c1_skip']} (engine {eng_skip}) | "
            f"analyzed {counters['used']}"
        )
        if not records:
            print("  (no analyzable bucket names)\n", flush=True)
            del ctx
            continue

        vols = np.array([r["vol"] for r in records])
        lo, hi = _tercile_bounds(vols)
        for r in records:
            r["stratum"] = _stratum(r["vol"], lo, hi)
        print(f"  vol terciles (mean daily amp): calm <= {lo * 100:.2f}% < mid <= {hi * 100:.2f}% < wild")

        print(
            "  stratum |    n | C1-skip | skip fwd% | skip win% | kept n | kept fwd% | kept win% | "
            "Δ(skip-kept) fwd%"
        )
        for si, name in enumerate(STRATUM_NAMES):
            grp = [r for r in records if r["stratum"] == si]
            skipped = [r["fwd"] for r in grp if r["c1_skip"]]
            kept = [r["fwd"] for r in grp if not r["c1_skip"]]
            delta = _mean(skipped) - _mean(kept) if skipped and kept else float("nan")
            print(
                f"  {name:>7s} | {len(grp):4d} | {len(skipped):7d} | "
                f"{_fmt_pct(_mean(skipped))} | {_winrate(skipped):8.1f} | "
                f"{len(kept):6d} | {_fmt_pct(_mean(kept))} | {_winrate(kept):8.1f} | "
                f"{_fmt_pct(delta)}"
            )

        print("  run-bin -> mean fwd% by stratum (C1 line is between [2-3%] and [3-4%]):")
        header = "  run bin   |" + "".join(f" {n:>7s} |" for n in STRATUM_NAMES)
        print(header)
        for lo_b, hi_b in RUN_BINS:
            cells = []
            for si in range(3):
                xs = [
                    r["fwd"]
                    for r in records
                    if r["stratum"] == si and lo_b <= r["run"] < hi_b
                ]
                cells.append(f"{_fmt_pct(_mean(xs))}n{len(xs):<3d}")
            print(f"  {lo_b * 100:>4.0f}-{hi_b * 100:<4.0f}% |" + "".join(f" {c:>9s} |" for c in cells))

        summary[w] = {
            "n": len(records),
            "strata": [],
        }
        for si in range(3):
            grp = [r for r in records if r["stratum"] == si]
            skipped = [r["fwd"] for r in grp if r["c1_skip"]]
            kept = [r["fwd"] for r in grp if not r["c1_skip"]]
            summary[w]["strata"].append(
                {
                    "name": STRATUM_NAMES[si],
                    "n": len(grp),
                    "n_skip": len(skipped),
                    "skip_fwd": _mean(skipped),
                    "kept_fwd": _mean(kept),
                    "delta": _mean(skipped) - _mean(kept) if skipped and kept else float("nan"),
                }
            )
        print("", flush=True)
        del ctx

    print("## Cross-window Δ(skip-kept) by stratum (positive = C1 skipped better-than-kept names)")
    print("| window | " + " | ".join(f"{n} Δ" for n in STRATUM_NAMES) + " |")
    print("|---|" + "---|" * 3)
    for w in windows:
        if w not in summary:
            continue
        cells = []
        for st in summary[w]["strata"]:
            cells.append(f"{_fmt_pct(st['delta'])} (n{st['n_skip']})")
        print(f"| {w} | " + " | ".join(cells) + " |")

    print("\nReading: Δ>0 in a stratum means C1 removed names that, on average, would have")
    print("earned more than what it kept — i.e. a candidate '误伤'. A rule change still needs a")
    print("preregistration (first-principles §一.9 / §三 #4); this page only measures.")
    return 0


_HABIT_KWARGS = {
    "skip_t1_limit": True,
    "pool_mode": "strict",
    "max_pos": 4,
    "position_pct": 0.25,
    "body": 3,
    "fill_mode": "same_1430",
    "fill_hhmm": "1430",
    "exit_hhmm": "1430",
    "max_open_to_1430_pct": 0.03,
    "rank_key": "amp_1430",
    "gate_1430": True,
}


if __name__ == "__main__":
    raise SystemExit(main())
