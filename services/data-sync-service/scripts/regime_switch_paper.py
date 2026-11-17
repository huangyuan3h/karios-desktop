#!/usr/bin/env python3
"""Forward paper log for the 2-state regime switch (read-only, no orders).

Rule (2-state):
  - month-end 000300 close > MA200 -> starship_robust (H2-a25)
  - otherwise -> starport (w=0.2)

The script records the signal and the strategy's daily NAV from the frozen
timeline (not a real account). Run daily after close, or weekly; it fills the
gap from the last logged date.

Usage:
  PYTHONPATH=src python3 scripts/regime_switch_paper.py --status
  PYTHONPATH=src python3 scripts/regime_switch_paper.py --seed
  PYTHONPATH=src python3 scripts/regime_switch_paper.py --append
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LOG_DEFAULT = ROOT / "data" / "backtest_reports" / "regime_paper" / "paper_log.csv"
COST = 0.0010  # 10bp per switch


def _index(start: str, end: str) -> pd.Series:
    import psycopg

    from data_sync_service.config import get_settings

    s = get_settings()
    with psycopg.connect(s.database_url) as conn:
        df = pd.read_sql(
            "select trade_date, close from index_daily "
            "where ts_code='000300.SH' and trade_date between %s and %s order by trade_date",
            conn,
            params=(start, end),
        )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date")["close"].astype(float)


def _states(idx: pd.Series) -> pd.Series:
    ma200 = idx.rolling(200).mean()
    return pd.Series(np.where(idx > ma200, "bull", "bear"), index=idx.index)


def _month_target(st: pd.Series, dates: pd.DatetimeIndex) -> pd.Series:
    ser = pd.Series(dates, index=dates)
    me = set(ser.groupby([ser.dt.year, ser.dt.month]).last().values)
    out, last = [], None
    for d in dates:
        if d in me:
            last = st.loc[d]
        out.append(last)
    return pd.Series(out, index=dates).ffill()


def _timeline(start: str, end: str, strategy: str) -> pd.DataFrame:
    from data_sync_service.api import backtest_routes as br
    from data_sync_service.service.starport import blend_starport_timeline

    if strategy == "starport":
        hp, _ = br._get_or_build_timeline(start, end, strategy="homeport")
        sat, _ = br._get_or_build_timeline(start, end, strategy="starship")
        r = blend_starport_timeline(hp, sat, sat_weight=0.2)
    else:
        r, _ = br._get_or_build_timeline(start, end, strategy=strategy)
    rows = r.get("rows") or []
    return pd.DataFrame(
        {
            "date": pd.to_datetime([x.get("date") for x in rows if x.get("date")]),
            "nav": [float(x.get("navSingle") or 1.0) for x in rows if x.get("date")],
        }
    ).set_index("date").sort_index()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=None)
    ap.add_argument("--log", default=str(LOG_DEFAULT))
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--seed", action="store_true")
    ap.add_argument("--append", action="store_true")
    args = ap.parse_args()

    asof = pd.Timestamp(args.asof) if args.asof else pd.Timestamp.today().normalize()
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    lookback = (asof - pd.Timedelta(days=500)).strftime("%Y-%m-%d")
    end = asof.strftime("%Y-%m-%d")
    idx = _index(lookback, end)
    st = _states(idx)
    dates = idx.index
    tg = _month_target(st, dates)
    state = tg.iloc[-1]
    target = "starship_robust" if state == "bull" else "starport"

    if args.status:
        print(
            f"asof={end} 000300={idx.iloc[-1]:.2f} MA200={idx.rolling(200).mean().iloc[-1]:.2f} "
            f"state={state} target={target}"
        )
        if log_path.exists():
            log = pd.read_csv(log_path, parse_dates=["date"])
            print(f"log rows={len(log)} last={log.date.iloc[-1].date()} nav={log.nav.iloc[-1]:.4f}")
        return

    if args.seed:
        log_path.write_text(
            "date,state,target,ret,nav,m30_ret,m30_nav,h2a25_ret,h2a25_nav,switch,note\n"
            f"{end},{state},{target},0,1.0,0,1.0,0,1.0,0,seed\n"
        )
        print(f"seeded {log_path} at {end} state={state} target={target}")
        return

    if not args.append:
        ap.error("use --status / --seed / --append")
    if not log_path.exists():
        ap.error(f"log not found: {log_path}; run --seed first")

    log = pd.read_csv(log_path, parse_dates=["date"]).sort_values("date")
    last = log.date.iloc[-1]
    start = (last - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    if pd.Timestamp(start) > asof:
        print(f"no new dates after {last.date()}")
        return

    nav_s = _timeline(start, end, target)
    nav_m = _timeline(start, end, "homeport_m30")
    nav_h = _timeline(start, end, "starship_robust")
    idx2 = _index(start, end)
    st2 = _states(_index((pd.Timestamp(start) - pd.Timedelta(days=500)).strftime("%Y-%m-%d"), end))
    tg2 = _month_target(st2, idx2.index)
    m30 = nav_m["nav"].reindex(idx2.index).ffill()
    h2a25 = nav_h["nav"].reindex(idx2.index).ffill()
    target_nav = nav_s["nav"].reindex(idx2.index).ffill()

    nav = float(log.nav.iloc[-1])
    m30_nav = float(log.m30_nav.iloc[-1])
    h2a25_nav = float(log.h2a25_nav.iloc[-1]) if "h2a25_nav" in log.columns else 1.0
    rows = []
    prev_target = log.target.iloc[-1]
    for d in idx2.index:
        if d <= last:
            continue
        state_d = tg2.loc[d]
        tgt = "starship_robust" if state_d == "bull" else "starport"
        r = 0.0
        pos = target_nav.index.get_loc(d)
        if pos > 0:
            a, b = target_nav.iloc[pos], target_nav.iloc[pos - 1]
            if b and np.isfinite(a) and np.isfinite(b):
                r = a / b - 1.0
        m_pos = m30.index.get_loc(d)
        mr = 0.0
        if m_pos > 0:
            a, b = m30.iloc[m_pos], m30.iloc[m_pos - 1]
            if b and np.isfinite(a) and np.isfinite(b):
                mr = a / b - 1.0
        h_pos = h2a25.index.get_loc(d)
        hr = 0.0
        if h_pos > 0:
            a, b = h2a25.iloc[h_pos], h2a25.iloc[h_pos - 1]
            if b and np.isfinite(a) and np.isfinite(b):
                hr = a / b - 1.0
        switch = int(d.month != pd.Timestamp(last).month and tgt != prev_target)
        r_net = r - (COST if switch else 0.0)
        nav *= 1.0 + r_net
        m30_nav *= 1.0 + mr
        h2a25_nav *= 1.0 + hr
        rows.append(
            {
                "date": d,
                "state": state_d,
                "target": tgt,
                "ret": r_net,
                "nav": nav,
                "m30_ret": mr,
                "m30_nav": m30_nav,
                "h2a25_ret": hr,
                "h2a25_nav": h2a25_nav,
                "switch": switch,
                "note": "",
            }
        )
        prev_target = tgt
        last = d

    if rows:
        pd.concat([log, pd.DataFrame(rows)], ignore_index=True).to_csv(log_path, index=False)
    print(f"appended {len(rows)} rows; nav={nav:.4f} m30={m30_nav:.4f} h2a25={h2a25_nav:.4f} target={prev_target}")


if __name__ == "__main__":
    main()
