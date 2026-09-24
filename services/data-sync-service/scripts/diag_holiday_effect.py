#!/usr/bin/env python3
"""Diagnostic: CN market holiday effect ("节前买点差 / 节后涨"?).

Read-only event study. A "holiday" is a run of >=3 consecutive closed SSE
sessions (i.e. at least one weekday off; a normal weekend is only 2). For each
holiday we take d0 = last open session before it, d1 = first open session after.

Segments measured on each index:
  pre_N   : close(d0) / close(d0-N) - 1     (buy N sessions before holiday, hold into d0)
  lastday : close(d0) / close(d0-1) - 1     (the final pre-holiday session)
  gap     : close(d1) / close(d0) - 1       (the holiday jump)
  post_N  : close(d1+N) / close(d1) - 1     (buy on the first post-holiday session)

Each is compared against the unconditional same-horizon forward return
("baseline") on the same asset/period: abnormal = event - baseline. t-stat is
event-mean / (sd/sqrt(n)) with the usual iid caveat.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_holiday_effect.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from math import sqrt
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

ASSETS = {
    "000001.SH": "上证综指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "399006.SZ": "创业板指",
}
WINDOWS = (1, 2, 3, 5, 10)
PERIODS = {
    "2016-2026": ("2016-01-01", "2026-12-31"),
    "2020-2026": ("2020-01-01", "2026-12-31"),
    "full": ("2000-01-01", "2030-01-01"),
}
# The three "big" holidays the user cares about; everything else (元旦/清明/端午/中秋) -> other.
BIG = ("春节", "五一", "十一")


def _classify(a: date, b: date) -> str:
    last = b - timedelta(days=1)
    m, d = last.month, last.day
    if (m == 1 and d >= 20) or (m == 2 and d <= 25):
        return "春节"
    if (m == 4 and d >= 28) or (m == 5 and d <= 6):
        return "五一"
    if (m == 9 and d >= 30) or (m == 10 and d <= 8):
        return "十一"
    return "其他"


def _load_closes(ts_code: str) -> dict[date, float]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, close FROM index_daily WHERE ts_code=%s ORDER BY trade_date",
                (ts_code,),
            )
            return {r[0]: float(r[1]) for r in cur.fetchall()}


def _open_dates() -> list[date]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cal_date FROM trade_calendar WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date"
            )
            return [r[0] for r in cur.fetchall()]


def _holidays(opens: list[date], lo: date, hi: date) -> list[tuple[date, date, int]]:
    """(d0, d1, closed_run) for each closed run >= 3 days bracketed by opens in [lo,hi]."""
    out = []
    for a, b in zip(opens, opens[1:]):
        run = (b - a).days - 1
        if run >= 3 and lo <= a and b <= hi:
            out.append((a, b, run))
    return out


def _fwd(closes: dict[date, float], days: list[date], i: int, n: int) -> float | None:
    j = i + n
    if i < 0 or j >= len(days):
        return None
    a, b = closes.get(days[i]), closes.get(days[j])
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0


def _stats(vals: list[float], base: float | None) -> str:
    if not vals:
        return "     n=0"
    a = np.array(vals)
    abn = a.mean() - base if base is not None else float("nan")
    t = a.mean() / (a.std(ddof=1) / sqrt(len(a))) if len(a) > 1 and a.std(ddof=1) > 0 else float("nan")
    return (
        f"{100*a.mean():+6.2f}% abn{100*abn:+6.2f} t{t:+5.2f} "
        f"med{100*np.median(a):+6.2f} hit{100*np.mean(a>0):3.0f}% n{len(a):>3}"
    )


def _baseline(closes: dict[date, float], days: list[date], n: int, lo: date, hi: date) -> float | None:
    vals = [_fwd(closes, days, i, n) for i, d in enumerate(days) if lo <= d <= hi]
    vals = [v for v in vals if v is not None]
    return float(np.mean(vals)) if vals else None


def main() -> int:
    opens = _open_dates()
    all_closes = {ts: _load_closes(ts) for ts in ASSETS}

    for ts, label in ASSETS.items():
        closes = all_closes[ts]
        days = sorted(closes)
        lo_d, hi_d = days[0], days[-1]
        hol = _holidays(opens, lo_d, hi_d)
        idx = {d: i for i, d in enumerate(days)}
        print(f"\n{'='*88}\n{label} {ts}   sessions {lo_d}..{hi_d}   holidays={len(hol)}")
        for pname, (plo, phi) in PERIODS.items():
            p_lo, p_hi = date.fromisoformat(plo), date.fromisoformat(phi)
            for name in BIG:
                ev = [(a, b, r) for (a, b, r) in hol if plo <= a.isoformat() <= phi and _classify(a, b) == name]
                if len(ev) < 3:
                    continue
                print(f"  [{pname} | {name}] n_holidays={len(ev)}")
                for n in WINDOWS:
                    pre = [v for v in (_fwd(closes, days, idx[a] - n, n) for a, b, r in ev if idx[a] - n >= 0) if v is not None]
                    print(f"    pre_{n:<2d}  {_stats(pre, _baseline(closes, days, n, p_lo, p_hi))}")
                last = [v for v in (_fwd(closes, days, idx[a] - 1, 1) for a, b, r in ev if idx[a] - 1 >= 0) if v is not None]
                print(f"    lastday {_stats(last, _baseline(closes, days, 1, p_lo, p_hi))}")
                gap = [v for v in (_fwd(closes, days, idx[a], 1) for a, b, r in ev if idx[a] + 1 < len(days)) if v is not None]
                print(f"    gap     {_stats(gap, _baseline(closes, days, 1, p_lo, p_hi))}")
                for n in WINDOWS:
                    post = [v for v in (_fwd(closes, days, idx[b], n) for a, b, r in ev if idx[b] + n < len(days)) if v is not None]
                    print(f"    post_{n:<2d} {_stats(post, _baseline(closes, days, n, p_lo, p_hi))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
