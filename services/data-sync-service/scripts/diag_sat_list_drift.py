#!/usr/bin/env python3
"""S1-followup diagnostic: how much of the habit pick list depends on the close?

Backtest ranks S-gap names by FULL-DAY amplitude (high/low/close only known
at 15:00). Live at 14:30 can only rank by a 14:30-proxy amplitude built from
09:30->14:30 5-minute bars. habit-clock already showed fill-minute P&L is
flat (13:30~15:00 no better minute); this asks the selection question: for
each R-wide-open day, how different are the two top-4 pick lists, and do
their forward 3-day returns differ?

Windows: OOS2+train only (selection discipline, valid untouched).
Read-only vs Postgres. Prints a table, saves nothing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.config import get_settings  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    COSTS_ROUNDTRIP,
    R_WIDE_THRESHOLD,
    _cached_day_features,
    _intraday_px,
    _same_1430_skip_reason,
    load_sgap_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}
TOP_N = 4
BUCKET_Q = 3


def _proxy_amp_1430(
    day: str, names: set[str], px_1430: dict[str, float | None]
) -> dict[str, float | None]:
    """14:30-knowable amplitude per name: (max high - min low over bars with
    trade_time <= '1430') / 1430 print. None = unrankable at 14:30."""
    if not names:
        return {}
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT ts_code, high, low FROM bar_5min "
        "WHERE trade_date = %s AND trade_time <= '1430' "
        "AND ts_code = ANY(%s) AND high IS NOT NULL AND low IS NOT NULL",
        (day, sorted(names)),
    )
    hi: dict[str, float] = {}
    lo: dict[str, float] = {}
    for ts, h, low in cur.fetchall():
        t = str(ts)
        h, low = float(h), float(low)
        hi[t] = max(hi.get(t, h), h)
        lo[t] = min(lo.get(t, low), low)
    conn.close()
    out: dict[str, float | None] = {}
    for t in names:
        px = px_1430.get(t)
        if t not in hi or px is None or px <= 0:
            out[t] = None
        else:
            out[t] = (hi[t] - lo[t]) / px
    return out


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    days = sum_days = 0
    elig_total = 0
    blind_total = 0
    jacs: list[float] = []
    identical = 0
    top1_same = 0
    fwd_a: list[float] = []
    fwd_b: list[float] = []
    for s, e in WINDOWS.values():
        for day in cal:
            if day <= s or day > e:
                continue
            ei = idx_by_day.get(day, -1)
            if ei < 0 or ei + 2 >= len(cal):
                continue
            exit_day = cal[ei + 2]
            feat_all, breadth = _cached_day_features(ctx, day)
            if breadth <= R_WIDE_THRESHOLD:
                continue
            # Habit fill-side skips (frozen recipe: skip_t1 + C1 3%).
            elig: dict[str, float] = {}
            for ts, d in feat_all.items():
                if not d.get("is_gap"):
                    continue
                di = date_idx.get(ts, {}).get(day, -1)
                series = per_ts.get(ts)
                if di < 0 or not series:
                    continue
                bar = series[di]
                px = _intraday_px(ctx, ts, day, "1430")
                reason = _same_1430_skip_reason(
                    ts=ts,
                    px=px,
                    open_px=bar.get("open"),
                    pre_close=bar.get("pre_close"),
                    skip_t1_limit=True,
                    max_open_to_1430_pct=0.03,
                    near_limit_buffer_pct=None,
                )
                if reason:
                    continue
                if not px or px <= 0:
                    continue  # no fill at 14:30 in either list (symmetric)
                amp = d.get("amp")
                if amp is None:
                    continue
                elig[ts] = float(amp)
            if not elig:
                continue
            px_in_map = {t: _intraday_px(ctx, t, day, "1430") for t in elig}
            proxy = _proxy_amp_1430(day, set(elig), px_in_map)
            blind_total += sum(1 for t in elig if proxy.get(t) is None)
            rank_a = sorted(elig, key=lambda t: elig[t])
            rank_b = sorted(
                elig, key=lambda t: (proxy.get(t) is None, proxy.get(t) or 0.0)
            )
            qn = max(1, len(rank_a) // BUCKET_Q)
            pool_a = [t for t in rank_a[:qn]][:TOP_N]
            pool_b = [t for t in rank_b[:qn]][:TOP_N]
            set_a, set_b = set(pool_a), set(pool_b)
            union = set_a | set_b
            jac = len(set_a & set_b) / len(union) if union else 1.0
            jacs.append(jac)
            if set_a == set_b:
                identical += 1
            if pool_a and pool_b and pool_a[0] == pool_b[0]:
                top1_same += 1
            days += 1
            sum_days += 1
            elig_total += len(elig)
            for t in pool_a:
                px_exit = _intraday_px(ctx, t, exit_day, "1430")
                px_in = px_in_map.get(t)
                if px_exit and px_exit > 0 and px_in and px_in > 0:
                    fwd_a.append(px_exit / px_in - 1 - COSTS_ROUNDTRIP)
            for t in pool_b:
                px_exit = _intraday_px(ctx, t, exit_day, "1430")
                px_in = px_in_map.get(t)
                if px_exit and px_exit > 0 and px_in and px_in > 0:
                    fwd_b.append(px_exit / px_in - 1 - COSTS_ROUNDTRIP)
    print(f"\ndays={days} eligible_names={elig_total} proxy_blind={blind_total}")

    def _cell(v: list[float]) -> str:
        if not v:
            return "n/a"
        m = float(np.mean(v)) * 100
        hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100
        return f"{m:+.2f}%/{hit:.0f}%"

    mj = float(np.mean(jacs)) if jacs else 0.0
    print("\n| list | n_names | mean fwd3d / hit |")
    print("|------|---------|------------------|")
    print(f"| A full-day amp (backtest) | {len(fwd_a)} | {_cell(fwd_a)} |")
    print(f"| B 14:30-proxy amp (live-knowable) | {len(fwd_b)} | {_cell(fwd_b)} |")
    spread = (
        float(np.mean(fwd_a)) * 100 - float(np.mean(fwd_b)) * 100
        if fwd_a and fwd_b
        else 0.0
    )
    print(f"\nper-day Jaccard(A4,B4) mean {mj:.2f} | identical {identical}/{days} "
          f"| top1 same {top1_same}/{days} | fwd spread A-B {spread:+.2f}pp")
    print(f"(windows OOS2+train; valid untouched; {sum_days} R-wide-open days)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
