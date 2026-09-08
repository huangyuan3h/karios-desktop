#!/usr/bin/env python3
"""Amihud price-impact diagnostic v1: quintile forward returns, OOS2/train only.

Pre-reg frozen (2026-09-08, read-only vs Postgres, valid NOT touched):
- Factor: Amihud (2002) ILLIQ[d] = mean over prior 20 trading days of
  |ret_t| / amount_yi_t, ret_t = close[t]/close[t-1]-1 (simple, qfq),
  amount_yi = amount(千元)/100000. Limit-pinned days (board-limit close,
  same _board_limit_pct as backtest_engine) excluded as censored; require
  >=15 valid days else unrankable.
- Universe: daily JOIN stock_basic, delist_date IS NULL, name NOT LIKE '%ST%',
  ts_code NOT LIKE '%.BJ' (same as state_bucket_track / replay_cpa_cn).
- Sort day S: cross-sectional quintiles of ILLIQ (Q1 = most liquid/lowest
  impact ... Q5 = most illiquid). Direction NOT pre-committed; quintile table
  resolves premium-vs-tradability empirically.
- Forward: entry next trading day open (skip limit-up / missing open),
  hold 20 trading days, exit 20th-day close; net = open->close - 0.3% round-trip
  (same COSTS as CPA v1). Signals needing forward data past window end dropped
  (no peeking into next window).
- Windows: OOS2 2024-08-01~2025-08-01 / train 2025-08-01~2026-02-01 (discovery).
  valid 2026-03-01~2026-08-07 untouched until direction frozen.
- Report per (window, quintile): n, mean net pp, win%.

Usage:
  PYTHONPATH=src:scripts python3 scripts/diag_amihud.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from replay_cpa_cn import (  # noqa: E402
    COSTS_ROUNDTRIP,
    MAX_HOLD_DAYS,
    WINDOWS,
    WARMUP_DAYS,
    _board_limit_pct,
    _build_features,
)

MARKETS_A = ("主板", "创业板", "科创板", "中小板")


def _load_daily_a(start: str, end: str):
    """A-share only universe (ex HK/ETF/BJ/ST/delisted)."""
    from collections import defaultdict

    from data_sync_service.db import get_connection

    per_ts: dict[str, list[dict]] = defaultdict(list)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.trade_date, d.ts_code, d.open, d.high, d.low, d.close,
                       d.pre_close, d.vol, d.amount
                FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
                WHERE d.trade_date >= %s::date AND d.trade_date <= %s::date
                  AND sb.delist_date IS NULL
                  AND sb.name NOT LIKE '%%ST%%'
                  AND d.ts_code NOT LIKE '%%.BJ'
                  AND sb.market IN ('主板','创业板','科创板','中小板')
                ORDER BY d.ts_code, d.trade_date
                """,
                (start, end),
            )
            rows = cur.fetchall()
    cal_set: set[str] = set()
    for d, ts, o, h, low, c, pc, v, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        cal_set.add(ds)
        per_ts[str(ts)].append({
            "date": ds,
            "open": float(o) if o is not None else None,
            "high": float(h) if h is not None else None,
            "low": float(low) if low is not None else None,
            "close": float(c) if c is not None else None,
            "pre_close": float(pc) if pc is not None else None,
            "vol": float(v) if v is not None else None,
            "amount": float(amt) if amt is not None else None,
        })
    return per_ts, sorted(cal_set)

COSTS_ROUNDTRIP = 0.003
LOOKBACK = 20
MIN_OBS = 15
FWD_HOLD = 20

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}


def _pinned(ts: str, prev: float | None, close: float | None) -> bool:
    if prev is None or close is None or prev <= 0 or close <= 0:
        return False
    lim = _board_limit_pct(ts)
    if lim is None:
        return False
    up = round(prev * (1.0 + lim), 2)
    dn = round(prev * (1.0 - lim), 2)
    return close >= up - 0.01 or close <= dn + 0.01


def main() -> None:
    w0 = min(s for s, _ in WINDOWS.values())
    w1 = max(e for _, e in WINDOWS.values())
    warm = (date.fromisoformat(w0) - timedelta(days=120)).isoformat()
    per_ts, cal_all = _load_daily_a(warm, w1)
    d2k = {d: k for k, d in enumerate(cal_all)}
    avgfeats = _build_features(per_ts)  # 60d avg amount (亿元) for tradable cut

    # per-symbol arrays
    data: dict[str, dict] = {}
    for ts, rows in per_ts.items():
        n = len(rows)
        closes = np.array([r["close"] or np.nan for r in rows], dtype=float)
        opens = np.array([r["open"] or np.nan for r in rows], dtype=float)
        amts = np.array([(r["amount"] / 100000.0) if r["amount"] else np.nan for r in rows], dtype=float)
        prev = np.concatenate([[np.nan], closes[:-1]])
        ret = np.abs(closes / prev - 1)
        # limit-pinned days -> censored
        mask = np.zeros(n, dtype=bool)
        for i in range(1, n):
            c, p = closes[i], closes[i - 1]
            if np.isfinite(c) and np.isfinite(p) and p > 0:
                if _pinned(ts, float(p), float(c)):
                    mask[i] = True
        impact = ret / amts
        impact[~np.isfinite(impact)] = np.nan
        impact[mask] = np.nan
        amts[~np.isfinite(amts)] = np.nan
        # rolling 20d mean + count via cumsum on filled arrays
        filled = np.where(np.isfinite(impact), impact, 0.0)
        cnt = np.where(np.isfinite(impact), 1.0, 0.0)
        cs = np.cumsum(np.concatenate([[0.0], filled]))
        cc = np.cumsum(np.concatenate([[0.0], cnt]))
        illiq = (cs[LOOKBACK:] - cs[:-LOOKBACK]) / np.maximum(cc[LOOKBACK:] - cc[:-LOOKBACK], 1)
        ok = (cc[LOOKBACK:] - cc[:-LOOKBACK]) >= MIN_OBS
        illiq[~ok] = np.nan
        # align: illiq value at row i uses days (i-19..i) -> series length n-19, pad front
        full = np.full(n, np.nan)
        full[LOOKBACK - 1:] = illiq[: n - LOOKBACK + 1] if len(illiq) >= n - LOOKBACK + 1 else illiq
        data[ts] = {"rows": rows, "illiq": full, "closes": closes, "opens": opens,
                    "amts": amts}

    for w, (s, e) in WINDOWS.items():
        cal = [d for d in cal_all if s <= d <= e]
        # drop last FWD_HOLD+2 trading days (need entry next-open + 20 closes)
        sig_cals = cal[:-FWD_HOLD - 2] if len(cal) > FWD_HOLD + 2 else []
        buckets: dict[int, list[float]] = defaultdict(list)
        buckets_trad: dict[int, list[float]] = defaultdict(list)  # avg60_amt >= 0.7亿
        liq_check: dict[int, list[float]] = defaultdict(list)  # quintile -> avg amts for orthogonality
        for d in sig_cals:
            k = d2k[d]
            # cross-section of ILLIQ at S
            vals: list[tuple[str, float]] = []
            for ts, f in data.items():
                j = None
                # find row index for date d: use date->idx via rows scan is slow;
                # instead precompute? n small; linear via dict built on the fly per symbol
                rows = f["rows"]
                # binary search by date (rows sorted)
                lo, hi = 0, len(rows) - 1
                jj = -1
                while lo <= hi:
                    mid = (lo + hi) // 2
                    rd = rows[mid]["date"]
                    if rd == d:
                        jj = mid
                        break
                    if rd < d:
                        lo = mid + 1
                    else:
                        hi = mid - 1
                if jj < 0:
                    continue
                v = f["illiq"][jj]
                if np.isfinite(v) and v > 0:
                    vals.append((ts, float(v)))
            if len(vals) < 100:
                continue
            vals.sort(key=lambda x: x[1])
            n = len(vals)
            # entry day = next trading day
            ed = cal_all[k + 1] if k + 1 < len(cal_all) else None
            if ed is None:
                continue
            for qi in range(5):
                lo_i, hi_i = int(n * qi / 5), int(n * (qi + 1) / 5) if qi < 4 else n
                for ts, _v in vals[lo_i:hi_i]:
                    f = data[ts]
                    rows = f["rows"]
                    # entry open
                    lo2, hi2 = 0, len(rows) - 1
                    ej = -1
                    while lo2 <= hi2:
                        mid = (lo2 + hi2) // 2
                        rd = rows[mid]["date"]
                        if rd == ed:
                            ej = mid
                            break
                        if rd < ed:
                            lo2 = mid + 1
                        else:
                            hi2 = mid - 1
                    if ej < 0:
                        continue
                    eop = f["opens"][ej]
                    if not np.isfinite(eop) or eop <= 0:
                        continue
                    pc = rows[ej - 1]["close"] if ej > 0 else np.nan
                    # limit-up skip
                    lim = _board_limit_pct(ts)
                    if lim and np.isfinite(pc) and pc > 0:
                        if eop >= round(float(pc) * (1 + lim), 2) - 0.01:
                            continue
                    # exit: 20th trading day close after entry (count rows with valid close)
                    cnt_c = 0
                    xp = np.nan
                    for kk in range(ej, len(rows)):
                        c = f["closes"][kk]
                        if np.isfinite(c) and c > 0:
                            cnt_c += 1
                            if cnt_c == FWD_HOLD:
                                xp = c
                                break
                    if not np.isfinite(xp):
                        continue
                    net = float(xp) * (1 - COSTS_ROUNDTRIP) / float(eop) - 1
                    buckets[qi].append(net)
                    # tradable cut: 60d avg amount at sort day >= 0.7亿
                    aa = None
                    af = avgfeats.get(ts)
                    if af is not None:
                        ajj = af["d2i"].get(d)
                        if ajj is not None:
                            aa = af["avg_amt"][ajj]
                    if aa is not None and aa >= 0.7:
                        buckets_trad[qi].append(net)
                    a = f["amts"][ej]
                    if np.isfinite(a):
                        liq_check[qi].append(float(a))
        print(f"[{w}] signals window {s}..{e} (first {len(sig_cals)} sortable days)", flush=True)
        print(f"  --- full A ---", flush=True)
        for qi in range(5):
            arr = np.array(buckets[qi])
            if len(arr):
                print(f"  Q{qi + 1} (Q1=most liquid): n={len(arr)} mean={arr.mean() * 100:+.2f}% "
                      f"win={np.mean(arr > 0) * 100:.1f}% med={np.median(arr) * 100:+.2f}%", flush=True)
            else:
                print(f"  Q{qi + 1}: n=0", flush=True)
        print(f"  --- tradable (avg60>=0.7亿) ---", flush=True)
        for qi in range(5):
            arr = np.array(buckets_trad[qi])
            if len(arr):
                print(f"  tQ{qi + 1}: n={len(arr)} mean={arr.mean() * 100:+.2f}% "
                      f"win={np.mean(arr > 0) * 100:.1f}% med={np.median(arr) * 100:+.2f}%", flush=True)
            else:
                print(f"  tQ{qi + 1}: n=0", flush=True)
        # orthogonality: mean amount per ILLIQ quintile
        for qi in range(5):
            a = liq_check[qi]
            if a:
                print(f"    Q{qi + 1} mean_amt_yi={float(np.mean(a)):.2f}", flush=True)


if __name__ == "__main__":
    main()
