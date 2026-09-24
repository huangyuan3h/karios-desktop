"""Broad event-family scan: raw forward net edge of information-shock events.

Frozen set of daily events (single definition each), CN A-share liquid universe,
buy close T -> sell close T+h (h=1/3/5/10), 30bp round trip. Purpose: a lower
bound on which event families carry *any* tradeable drift at the daily tier
(the S-gap engine's real edge lives in its intraday layer, not here).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import psycopg
from collections import defaultdict
from data_sync_service.config import get_settings

AMT_GATE = 70000.0
START = "2021-08-01"
COST = 0.0030
HOLDS = (1, 3, 5, 10)
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}


def rollmean(a, w):
    a = np.asarray(a, float)
    cs = np.cumsum(np.concatenate([[0.0], a]))
    out = np.full(len(a), np.nan)
    idx = np.arange(w - 1, len(a))
    out[idx] = (cs[idx + 1] - cs[idx - w + 1]) / w
    return out


def is_ashare(ts):
    code, _, suf = ts.partition(".")
    return suf in ("SH", "SZ") and code[:3] in (
        "600", "601", "603", "605", "688", "000", "001", "002", "003", "300", "301")


def main():
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor(name="ev")
    cur.itersize = 200000
    cur.execute(
        "SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, amount "
        "FROM daily WHERE trade_date >= %s ORDER BY ts_code, trade_date", (START,))

    rows = defaultdict(list)  # event -> list[(date, fwd1, fwd3, fwd5, fwd10)]
    buf, cur_sym = {}, None

    def flush(ts, g):
        if not is_ashare(ts):
            return
        g = g.sort_values("trade_date")
        n = len(g)
        if n < 120:
            return
        c = g["close"].astype(float).values
        o = g["open"].astype(float).values
        pc = g["pre_close"].astype(float).values
        pct = g["pct_chg"].astype(float).values
        v = g["vol"].astype(float).values
        amt = g["amount"].astype(float).values
        dates = g["trade_date"].values
        ma10 = rollmean(c, 10)
        vma20 = rollmean(v, 20)
        high20 = np.full(n, np.nan)
        for i in range(19, n):
            high20[i] = np.max(c[i - 19:i + 1])
        fwd = {}
        for h in HOLDS:
            f = np.full(n, np.nan)
            f[:-h] = c[h:] / c[:-h] - 1
            fwd[h] = f
        gap = o / pc - 1
        liq = (amt >= AMT_GATE) & (c > 0)

        def emit(name, mask):
            mask = mask & liq
            mask[:60] = False
            mask &= ~np.isnan(fwd[max(HOLDS)])
            for i in np.nonzero(mask)[0]:
                rows[name].append((dates[i],) + tuple(fwd[h][i] for h in HOLDS))

        emit("E1_gapup3", gap > 0.03)
        emit("E2_gapup5", gap > 0.05)
        emit("E3_gapup7", gap > 0.07)
        emit("E4_gapdn3", gap < -0.03)
        emit("E5_gapdn5", gap < -0.05)
        emit("E6_limitup", pct >= 9.8)
        emit("E7_limitdn", pct <= -9.8)
        emit("E8_breakout20v", (c >= high20 * 0.999) & (v >= 1.5 * vma20))
        emit("E9_pullback_ma10", (np.abs(c - ma10) / ma10 <= 0.02) & (c >= ma10)
             & (c > rollmean(c, 20)) & (rollmean(c, 20) > rollmean(c, 60)))

    def f(x):
        return float(x) if x is not None else np.nan

    for r in cur:
        ts = r[0]
        if ts != cur_sym:
            if buf:
                flush(cur_sym, pd.DataFrame(buf))
            cur_sym, buf = ts, defaultdict(list)
        buf["ts_code"].append(ts)
        buf["trade_date"].append(r[1])
        for k, idx in zip(("open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"),
                          range(2, 10)):
            buf[k].append(f(r[idx]))
    if buf:
        flush(cur_sym, pd.DataFrame(buf))
    conn.close()

    base = defaultdict(list)
    print("event family scan (net of 30bp, buy close T -> sell close T+h)\n")
    for name in sorted(rows):
        d = pd.DataFrame(rows[name], columns=["date"] + [f"f{h}" for h in HOLDS])
        d["date"] = pd.to_datetime(d["date"])
        print(f"== {name}  n={len(d)} ==")
        for w, (a, b) in WINDOWS.items():
            m = d[(d["date"] >= a) & (d["date"] < b)]
            if len(m) < 100:
                continue
            cells = []
            for h in HOLDS:
                net = m[f"f{h}"].mean() - COST
                cells.append(f"h{h}={net*100:+.3f}% win{(m[f'f{h}']>0).mean()*100:.0f}%")
            print(f"   {w:6s} n={len(m):6d} | " + " | ".join(cells))
        print()


if __name__ == "__main__":
    main()
