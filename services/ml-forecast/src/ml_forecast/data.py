"""PiT data loader for N-day trend forecast.

- Source: local Postgres (same DB as data-sync-service)
- Calendar from `daily` distinct trade_date
- Bars from `daily` (qfq close already)
- Labels: N-day forward return, point-in-time, no leakage
- Outputs: DataFrame per sample = (day, ts_code, label_reg, label_cls, close)
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

# load env like data-sync-service
load_dotenv(Path(__file__).resolve().parents[3] / ".env")
load_dotenv(Path(__file__).resolve().parents[2] / ".env")


def get_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    # psycopg needs postgresql://
    if url.startswith("postgresql+"):
        url = url.replace("postgresql+psycopg://", "postgresql://")
    return psycopg.connect(url)


def load_calendar(start: str, end: str) -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM daily WHERE trade_date>=%s AND trade_date<=%s ORDER BY trade_date",
                (start, end),
            )
            rows = cur.fetchall()
    return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in rows]


def load_bars(start: str, end: str, lookback_days: int = 90) -> pd.DataFrame:
    """Load daily bars with lookback for feature window."""
    d0 = (date.fromisoformat(start) - timedelta(days=lookback_days + 20)).isoformat()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, trade_date, open, high, low, close, vol, amount
                FROM daily
                WHERE trade_date >= %s AND trade_date <= %s AND close > 0
                ORDER BY ts_code, trade_date
                """,
                (d0, end),
            )
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"])
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    # filter B shares etc? keep all then filter by amount later
    return df


def load_rs_rank_map(start: str, end: str) -> dict[str, dict[str, float]]:
    """Optional: load RS rank for feature (whole-market percentile)."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # same query as backtest _load_rs_ranks but simplified
                cur.execute(
                    """
                    SELECT trade_date, ts_code, close FROM daily
                    WHERE trade_date >= %s AND trade_date <= %s AND close>0
                    """,
                    ((date.fromisoformat(start) - timedelta(days=30)).isoformat(), end),
                )
                rows = cur.fetchall()
        # compute 20d return rank per day in python (simpler than SQL lag)
        df = pd.DataFrame(rows, columns=["trade_date", "ts_code", "close"])
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values(["ts_code", "trade_date"])
        # need per ts lag 20
        df["prev20"] = df.groupby("ts_code")["close"].shift(20)
        df["ret20"] = df["close"] / df["prev20"] - 1
        df = df.dropna(subset=["ret20"])
        # rank per day
        out: dict[str, dict[str, float]] = {}
        for day, g in df.groupby("trade_date"):
            if str(day) < start or str(day) > end:
                continue
            s = g["ret20"]
            ranks = s.rank(pct=True)
            out[str(day)] = dict(zip(g["ts_code"], ranks))
        return out
    except Exception:
        return {}


def load_extra_maps(start: str, end: str) -> tuple[dict, dict, dict]:
    """Load score, mv, turnover for feat+ (PiT, may be sparse)."""
    # score: watchlist_score_daily -> ts_code
    score_map: dict[str, dict[str, float]] = {}
    mv_map: dict[str, dict[str, float]] = {}
    turnover_map: dict[str, dict[str, float]] = {}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # scores: need to map CN:000001 -> 000001.SZ/SH
                # heuristic: 60* -> SH, else SZ (covers 00/30/68)
                cur.execute(
                    "SELECT symbol, trade_date, score FROM watchlist_score_daily WHERE trade_date>=%s AND trade_date<=%s",
                    ((date.fromisoformat(start)-timedelta(days=70)).isoformat(), end),
                )
                for sym, d, sc in cur.fetchall():
                    if not sym or not sc: continue
                    s = str(sym)
                    if not s.startswith("CN:"): continue
                    code = s.split(":")[1]
                    ts = code + (".SH" if code.startswith("60") or code.startswith("68") and code.startswith("688") else ".SZ")
                    # 688 should be .SH as STAR, fix: 688* -> SH
                    if code.startswith("688") or code.startswith("689"):
                        ts = code + ".SH"
                    elif code.startswith("60"):
                        ts = code + ".SH"
                    else:
                        ts = code + ".SZ"
                    day = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                    try: v=float(sc)
                    except: continue
                    score_map.setdefault(day, {})[ts]=v
                # mv + turnover from stock_dailybasic
                cur.execute(
                    "SELECT ts_code, trade_date, total_mv, turnover_rate FROM stock_dailybasic WHERE trade_date>=%s AND trade_date<=%s",
                    ((date.fromisoformat(start)-timedelta(days=70)).isoformat(), end),
                )
                for ts, d, mv, tr in cur.fetchall():
                    day = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                    if mv is not None:
                        try: mv_map.setdefault(day, {})[str(ts)] = float(mv)
                        except: pass
                    if tr is not None:
                        try: turnover_map.setdefault(day, {})[str(ts)] = float(tr)
                        except: pass
    except Exception as e:
        print(f"load_extra_maps warn {e}")
    return score_map, mv_map, turnover_map


def build_samples(
    bars: pd.DataFrame,
    calendar: list[str],
    n_forward: int = 20,
    x_pct: float = 8.0,
    min_avg_amount: float = 0.7,
) -> pd.DataFrame:
    """Build per (day, ts) samples with label = N-day forward return.

    - calendar: trading days sorted
    - bars: DataFrame with ts_code, trade_date, close, vol, amount etc
    - forward return computed on calendar index, not fixed timedelta (PiT)
    """
    cal_index = {d: i for i, d in enumerate(calendar)}
    # pivot close for fast lookup
    close_map: dict[str, dict[str, float]] = {}
    for _, r in bars.iterrows():
        close_map.setdefault(r["ts_code"], {})[r["trade_date"]] = float(r["close"])
    # liquidity filter placeholder — amount is Decimal, skip heavy calc, filter later via vol
    samples = []
    for day in calendar:
        idx = cal_index[day]
        if idx + n_forward >= len(calendar):
            continue
        fwd_day = calendar[idx + n_forward]
        for ts, closes in close_map.items():
            c0 = closes.get(day)
            c1 = closes.get(fwd_day)
            if c0 is None or c1 is None or c0 <= 0:
                continue
            # need L=60 history for features
            if idx < 60:
                continue
            # need at least 50 closes in last 60 trading days (allow suspensions)
            window_days = calendar[idx - 60 + 1 : idx + 1]
            cnt = sum(1 for d in window_days if closes.get(d) is not None)
            if cnt < 50:
                continue
            label_reg = (c1 / c0 - 1) * 100
            # liquidity: avg amount 60d > threshold? use bars
            # quick lookup: get bars rows for window
            # skip ST/B? ST names not in bars, ignore
            label_cls = 1 if label_reg > x_pct else 0
            samples.append((day, ts, float(c0), float(c1), float(label_reg), int(label_cls)))
    df = pd.DataFrame(samples, columns=["trade_date", "ts_code", "close_t", "close_fwd", "label_reg", "label_cls"])
    return df
