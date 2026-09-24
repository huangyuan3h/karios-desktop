"""Diagnostic: folk strategy "多头排列 + 有量能 + 回踩 MA10" with hold 1/2/3 days,
top-K selection by a single indicator strength.

Signal day T (close basis, qfq):
  bull   : close > MA5 > MA10 > MA20 > MA60
  vol    : vol[T] >= VOL_K * MA20(vol)[T]
  pull   : close within PULL_TOL of MA10 and close >= MA10
Universe: SH/SZ A-share, amount >= AMT_GATE (thousand CNY), >=120 bars.
Forward: buy close T, sell close T+h (h=1,2,3).
Top-K selection: per day take K names ranked by one feature (higher = stronger).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import psycopg
from collections import defaultdict
from data_sync_service.config import get_settings

AMT_GATE = 70000.0          # thousand CNY -> 0.7e8 CNY
VOL_K = 1.2
PULL_TOL = 0.02
START = "2021-08-01"
COST_RT = 0.0030            # 30bp round trip
HOLDS = (1, 2, 3)
KS = (4, 10)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}
FEATS = ("amount", "volr", "ret5", "ret20", "ret60", "dist10")


def rollmean(a: np.ndarray, w: int) -> np.ndarray:
    a = np.asarray(a, float)
    cs = np.cumsum(np.concatenate([[0.0], a]))
    out = np.full(len(a), np.nan)
    idx = np.arange(w - 1, len(a))
    out[idx] = (cs[idx + 1] - cs[idx - w + 1]) / w
    return out


def is_ashare_stock(ts: str) -> bool:
    code, _, suf = ts.partition(".")
    if suf not in ("SH", "SZ"):
        return False
    return code[:3] in ("600", "601", "603", "605", "688", "000", "001", "002", "003", "300", "301")


def main() -> None:
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor(name="daily_scan")
    cur.itersize = 200000
    cur.execute(
        """
        SELECT ts_code, trade_date, open, high, low, close, vol, amount
        FROM daily
        WHERE trade_date >= %s
        ORDER BY ts_code, trade_date
        """,
        (START,),
    )

    rows = []
    base_rows = []  # (date, fwd1) all liquid, for base rate
    n_sym = 0
    cur_sym = None
    buf = {}

    def flush(ts, g):
        nonlocal n_sym
        if not is_ashare_stock(ts):
            return
        g = g.sort_values("trade_date")
        n = len(g)
        if n < 120:
            return
        c = g["close"].astype(float).values
        o = g["open"].astype(float).values
        h = g["high"].astype(float).values
        lo = g["low"].astype(float).values
        v = g["vol"].astype(float).values
        amt = g["amount"].astype(float).values
        dates = g["trade_date"].values
        ma5 = rollmean(c, 5)
        ma10 = rollmean(c, 10)
        ma20 = rollmean(c, 20)
        ma60 = rollmean(c, 60)
        vma20 = rollmean(v, 20)
        n_sym += 1
        fwd = {}
        for hh in HOLDS:
            f = np.full(n, np.nan)
            f[:-hh] = c[hh:] / c[:-hh] - 1
            fwd[hh] = f
        liq = amt >= AMT_GATE
        valid = (~np.isnan(fwd[1])) & liq & (c > 0)
        valid[:60] = False
        for i in np.nonzero(valid)[0]:
            base_rows.append((dates[i], fwd[1][i]))
        bull = (c > ma5) & (ma5 > ma10) & (ma10 > ma20) & (ma20 > ma60)
        volok = v >= VOL_K * vma20
        pull = (np.abs(c - ma10) / ma10 <= PULL_TOL) & (c >= ma10)
        touch = (lo <= ma10) & (ma10 <= h) & (c >= ma10)
        for pname, pcond in (("near2", pull), ("touch", touch)):
            mask = bull & volok & pcond
            mask[:60] = False
            mask &= ~np.isnan(fwd[3])
            for i in np.nonzero(mask)[0]:
                rows.append((pname, dates[i], ts,
                             fwd[1][i], fwd[2][i], fwd[3][i],
                             amt[i], v[i] / vma20[i] if vma20[i] else np.nan,
                             c[i] / c[i - 5] - 1, c[i] / c[i - 20] - 1,
                             c[i] / c[i - 60] - 1, (c[i] - ma10[i]) / ma10[i]))
        return

    def f(x):
        return float(x) if x is not None else np.nan

    for r in cur:
        ts = r[0]
        if ts != cur_sym:
            if buf:
                flush(cur_sym, pd.DataFrame(buf))
            cur_sym = ts
            buf = defaultdict(list)
        buf["ts_code"].append(ts)
        buf["trade_date"].append(r[1])
        buf["open"].append(f(r[2]))
        buf["high"].append(f(r[3]))
        buf["low"].append(f(r[4]))
        buf["close"].append(f(r[5]))
        buf["vol"].append(f(r[6]))
        buf["amount"].append(f(r[7]))
    if buf:
        flush(cur_sym, pd.DataFrame(buf))
    conn.close()

    cols = ["pull", "date", "ts", "fwd1", "fwd2", "fwd3", "amount", "volr", "ret5", "ret20", "ret60", "dist10"]
    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["date"])
    base = pd.DataFrame(base_rows, columns=["date", "fwd1"])
    base["date"] = pd.to_datetime(base["date"])
    print(f"symbols={n_sym} base={len(base)} signals={len(df)}")

    def wmask(d, w):
        a, b = WINDOWS[w]
        return (d["date"] >= a) & (d["date"] < b)

    print("\n=== base 1d close-close (all liquid) ===")
    for w in WINDOWS:
        b = base.loc[wmask(base, w), "fwd1"]
        print(f"{w:6s} n={len(b):8d} mean={b.mean()*100:+.3f}%")

    for pull in ("near2", "touch"):
        sub = df[df["pull"] == pull]
        print(f"\n=== bull_full + {pull} pool (hold 1/2/3d, gross / net30bp) ===")
        for w in WINDOWS:
            d = sub[wmask(sub, w)]
            if len(d) < 30:
                continue
            days = d.groupby("date").size()
            parts = []
            for hh in HOLDS:
                m = d[f"fwd{hh}"].mean()
                parts.append(f"h{hh}: {m*100:+.3f}/{((m-COST_RT)*100):+.3f}%")
            print(f"{w:6s} n={len(d):6d} pool/day={days.mean():5.1f} | " + " | ".join(parts))

    print("\n=== top-K/day by feature: mean fwd_h net30bp (pool baseline in parens) ===")
    for pull in ("near2",):
        sub = df[df["pull"] == pull].copy()
        for w in WINDOWS:
            d = sub[wmask(sub, w)].copy()
            if len(d) < 100:
                continue
            pool = {hh: d[f"fwd{hh}"].mean() - COST_RT for hh in HOLDS}
            print(f"\n-- {pull} {w}  pool: " + " ".join(f"h{hh}={pool[hh]*100:+.3f}%" for hh in HOLDS) + f"  (n={len(d)})")
            for feat in FEATS:
                dd = d.dropna(subset=[feat])
                cells = []
                for K in KS:
                    top = dd.sort_values(feat, ascending=False).groupby("date").head(K)
                    cells.append("K%d " % K + " ".join(
                        f"h{hh}={top.groupby('date')[f'fwd{hh}'].mean().mean()*100:+.3f}%" for hh in HOLDS))
                print(f"   {feat:7s} | " + " | ".join(cells))

    # slot-aware portfolio replay (compare vs S-gap / satellite standalone)
    dates = np.sort(pd.to_datetime(base["date"]).values.astype("datetime64[ns]"))
    dates = np.unique(dates)
    sig = df[df["pull"] == "near2"].copy()
    print("\n=== slot-aware replay (4 slots x 25%, hold 3d, 30bp) vs S-gap standalone ===")
    print(f"{'window':7s} {'total':>9s} {'CAGR':>8s} {'MDD':>8s} {'Sharpe':>7s} {'fills':>6s} {'net/trade':>10s}")
    for w in WINDOWS:
        r = portfolio_sim(sig, dates, w, slots=4, rank_feat="volr")
        print(f"{w:7s} {r['total']*100:+8.1f}% {r['cagr']*100:+7.1f}% {r['mdd']*100:+7.1f}% "
              f"{r['sharpe']:7.2f} {r['fills']:6d} {r['fill_net']*100:+9.3f}%")
    print("\n=== rank-layer net h3 by volr rank (does rank 1 beat rank 10?) ===")
    for w in WINDOWS:
        d = sig[wmask(sig, w)].dropna(subset=["volr", "fwd3"]).copy()
        d["rk"] = d.groupby("date")["volr"].rank(ascending=False, method="first")
        cells = []
        for rk in (1, 2, 3, 4, 5, 10, 20):
            v = d[d["rk"] == rk]["fwd3"].mean() - COST_RT
            cells.append(f"#{rk}={v*100:+.2f}%")
        print(f"   {w:6s} n={len(d):5d} pool={d['fwd3'].mean()*100-COST_RT*100:+.2f}% | " + " ".join(cells))
    print("S-gap   +212.7%/+40.3%/+14.7% (OOS2/train/valid); long +463.6% CAGR43.1% MDD-8.4 SR3.50")
    # sensitivity on ranking feature and slots
    print("\n=== sensitivity (total%) ===")
    for feat in FEATS:
        row = []
        for w in ("OOS2", "train", "valid", "long"):
            r = portfolio_sim(sig, dates, w, slots=4, rank_feat=feat)
            row.append(f"{w}={r['total']*100:+.0f}%")
        print(f"   rank={feat:7s} " + " ".join(row))


def portfolio_sim(sig, dates, window, slots=4, rank_feat="volr", cost=COST_RT):
    """Slot-aware replay: buy close T, sell close T+3, rank by `rank_feat`, 4 slots x 25%.

    Returns dict(total, cagr, mdd, sharpe, fills, active).
    """
    a, b = WINDOWS[window]
    lo = next((i for i, d in enumerate(dates) if d >= np.datetime64(a)), 0)
    hi = next((i for i, d in enumerate(dates) if d >= np.datetime64(b)), len(dates))
    by_day = {}
    sub = sig[(sig["date"] >= a) & (sig["date"] < b)].dropna(subset=[rank_feat, "fwd3"])
    for d, g in sub.groupby("date"):
        recs = g.sort_values(rank_feat, ascending=False)[["ts", "fwd1", "fwd2", "fwd3"]].to_dict("records")
        by_day[np.datetime64(d)] = recs

    cash = 1.0
    open_pos = []  # dict(alloc, entry_idx, fwd=(1,2,3))
    nav = []
    fills = 0
    realized = []
    for i in range(lo, hi):
        d = dates[i]
        # exits at T+3
        still = []
        for p in open_pos:
            if i - p["entry_idx"] >= 3:
                cash += p["alloc"] * (1 + p["fwd"][2]) - p["alloc"] * cost / 2
                realized.append((1 + p["fwd"][2]) * (1 - cost))
                fills += 1
            else:
                still.append(p)
        open_pos = still
        # mark to market
        def marked():
            v = cash
            for p in open_pos:
                j = i - p["entry_idx"]  # 1 or 2
                v += p["alloc"] * (1 + p["fwd"][j - 1])
            return v
        equity = marked()
        # entries
        free = slots - len(open_pos)
        if free > 0 and d in by_day:
            held = {p["ts"] for p in open_pos}
            for rec in by_day[d]:
                if free <= 0:
                    break
                if rec["ts"] in held:
                    continue
                alloc = 0.25 * equity
                cash -= alloc + alloc * cost / 2
                open_pos.append({"ts": rec["ts"], "alloc": alloc, "entry_idx": i,
                                 "fwd": (rec["fwd1"], rec["fwd2"], rec["fwd3"])})
                free -= 1
        nav.append(marked())
    nav = np.array(nav)
    rets = np.diff(nav) / nav[:-1]
    total = nav[-1] / 1.0 - 1
    ndays = len(nav)
    cagr = (1 + total) ** (242.0 / max(ndays, 1)) - 1
    peak = np.maximum.accumulate(nav)
    mdd = (nav / peak - 1).min()
    sharpe = (rets.mean() / rets.std() * np.sqrt(242)) if rets.std() > 0 else float("nan")
    fill_net = (np.mean(realized) - 1) if realized else float("nan")
    return dict(total=total, cagr=cagr, mdd=mdd, sharpe=sharpe, fills=fills, ndays=ndays, fill_net=fill_net)


if __name__ == "__main__":
    main()
