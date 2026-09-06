"""Factor signals service — daily scan for morphology factors.

Currently: strong_scoop_exhaustion (exhaustion top).
Uses the validated detector in services/ml-forecast but re-implemented here
in Python/numpy to avoid cross-venv import (ml-forecast venv vs data-sync venv).
Logic mirrors services/ml-forecast/src/ml_forecast/morphology.py and the
validated thresholds from docs/designs/pattern-factor-validation.md §2.4.
"""

from __future__ import annotations

import numpy as np

from data_sync_service.db import get_connection
from data_sync_service.db.factor_signals import upsert_rows


def _rollmean(a: np.ndarray, w: int) -> np.ndarray:
    cs = np.cumsum(np.concatenate([[0.0], np.asarray(a, float)]))
    out = np.full(len(a), np.nan)
    idx = np.arange(w - 1, len(a))
    out[idx] = (cs[idx + 1] - cs[idx - w + 1]) / w
    return out


def _symbol_for_ts(ts_code: str) -> str:
    """Market-disambiguated symbol (OPT-146).

    HK tickers (``*.HK``, 5 digits) must never be labeled ``CN:`` — a 5-digit
    ``CN:00004`` collides with nothing real but poisons every symbol join
    (Shenzhen 000004 is 6 digits). No-suffix codes default to CN (legacy).
    """
    ts = str(ts_code or "")
    code = ts.split(".")[0]
    suffix = ts.split(".")[-1].upper() if "." in ts else ""
    market = "HK" if suffix == "HK" else "CN"
    return f"{market}:{code}"


def _probability(ret60: float, vol_ratio: float) -> float:
    # thresholds from deep-dig table (out-of-sample stable)
    if ret60 > 0.50 and vol_ratio > 1.2:
        return 0.922
    if ret60 > 0.40 and vol_ratio > 1.2:
        return 0.894
    if ret60 > 0.30 and vol_ratio > 1.2:
        return 0.854
    if ret60 > 0.50:
        return 0.865
    if ret60 > 0.40:
        return 0.830
    return 0.787  # ret60>0.30 base


def scan_strong_scoop_exhaustion(trade_date: str) -> int:
    """Scan one trade_date for strong_scoop_exhaustion signals and persist.

    Returns number of signals written for that date (replaces same key).
    """
    from data_sync_service.db.stock_basic import get_connection as _unused  # noqa: F401
    # load 80 days of daily up to trade_date for 60-day lookback
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code, trade_date, open, high, low, close, vol, amount
                FROM daily
                WHERE trade_date <= %s::date
                  AND trade_date >= (%s::date - interval '180 days')
                ORDER BY ts_code, trade_date
                """,
                (trade_date, trade_date),
            )
            rows = cur.fetchall()
            # stock_basic for name/industry/board
            cur.execute("SELECT ts_code, name, industry, market FROM stock_basic")
            basic = {r[0]: {"name": r[1], "industry": r[2], "board": r[3]} for r in cur.fetchall()}

    if not rows:
        return 0

    # group by ts_code
    from collections import defaultdict
    by_ts: dict[str, list] = defaultdict(list)
    for r in rows:
        by_ts[r[0]].append(r)
    signals: list[dict] = []
    for ts_code, lst in by_ts.items():
        # lst sorted by trade_date (query order)
        dates = [str(x[1])[:10] for x in lst]
        try:
            t = dates.index(trade_date)
        except ValueError:
            continue
        if t < 60:
            continue
        closes = np.array([float(x[5]) for x in lst])
        highs = np.array([float(x[3]) for x in lst])
        lows = np.array([float(x[4]) for x in lst])
        vols = np.array([float(x[6]) for x in lst])
        ma20 = _rollmean(closes, 20)
        ma60 = _rollmean(closes, 60)
        # uptrend gate at t
        if not (ma20[t] > ma60[t] and closes[t - 30] > ma60[t - 30]):
            continue
        ph = highs[t - 40:t - 20].max()
        bw = lows[t - 20:t + 1]
        bottom = bw.min()
        bi = t - 20 + int(np.argmin(bw))
        depth = (ph - bottom) / ph if ph > 0 else 0
        if not (0.05 <= depth <= 0.18):
            continue
        if not (closes[t] >= bottom * 1.03 and closes[t] >= ma20[t] * 0.99):
            continue
        if bi < t - 15:
            continue
        scoop_vol = vols[t - 20:t + 1].mean()
        vr = float(vols[t] / scoop_vol) if scoop_vol > 0 else 1.0
        ret60 = closes[t] / closes[t - 60] - 1 if closes[t - 60] > 0 else 0
        if ret60 <= 0.30:
            continue
        # strong scoop exhaustion requires ret60>0.30; we already filtered weak
        prob = _probability(float(ret60), float(vr))
        # only emit if at least base threshold (ret60>0.30); caller may filter higher
        entry = float(closes[t])
        target = float(bottom * 0.99)
        stop = float(ph * 1.02)
        symbol = _symbol_for_ts(ts_code)
        meta = basic.get(ts_code, {})
        board_map = {"主板": "主板", "创业板": "创业板", "科创板": "科创板", "北交所": "北交所"}
        signals.append(dict(
            trade_date=trade_date, symbol=symbol, factor_name="strong_scoop_exhaustion",
            direction="short", entry_price=entry, target_price=target, stop_price=stop,
            probability=prob, hold_days=20, status="pending",
            ret60=float(ret60), vol_ratio=float(vr),
            industry=meta.get("industry"), board=board_map.get(str(meta.get("board") or ""), str(meta.get("board") or "")) or None,
            symbol_name=meta.get("name"),
        ))
    if signals:
        upsert_rows(signals)
    return len(signals)


def repair_hk_symbols() -> dict:
    """One-shot repair for the 2025-06-16 backfill mislabeling (OPT-146).

    A 5-digit ``CN:xxxxx`` can never be a real A-share code (6 digits) — it
    is a Hong Kong ticker that went through the old ``f"CN:{code}"`` path.
    Rewrite to ``HK:xxxxx``. If an ``HK:`` twin already exists for the same
    (trade_date, factor_name) key, drop the mislabeled duplicate instead of
    violating the primary key. History is preserved (rows relabeled, not
    deleted-except-duplicates), and the count is reported.
    """
    repaired = 0
    dropped = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, symbol, factor_name FROM factor_signals "
                "WHERE symbol ~ '^CN:[0-9]{5}$'"
            )
            bad = [(str(r[0])[:10], str(r[1]), str(r[2])) for r in cur.fetchall()]
            for trade_date, symbol, factor_name in bad:
                hk_symbol = "HK:" + symbol[3:]
                cur.execute(
                    "SELECT 1 FROM factor_signals "
                    "WHERE trade_date = %s AND symbol = %s AND factor_name = %s",
                    (trade_date, hk_symbol, factor_name),
                )
                if cur.fetchone():
                    cur.execute(
                        "DELETE FROM factor_signals "
                        "WHERE trade_date = %s AND symbol = %s AND factor_name = %s",
                        (trade_date, symbol, factor_name),
                    )
                    dropped += 1
                else:
                    cur.execute(
                        "UPDATE factor_signals SET symbol = %s "
                        "WHERE trade_date = %s AND symbol = %s AND factor_name = %s",
                        (hk_symbol, trade_date, symbol, factor_name),
                    )
                    repaired += 1
        conn.commit()
    return {"repaired": repaired, "dropped_duplicates": dropped}


def sync_for_range(start_date: str, end_date: str) -> dict:
    """Backfill factor signals for a date range (inclusive). Uses calendar trading days."""
    from datetime import date

    from data_sync_service.db.trade_calendar import get_open_dates
    dates = get_open_dates("SSE", date.fromisoformat(start_date), date.fromisoformat(end_date))
    total = 0
    for d in dates:
        total += scan_strong_scoop_exhaustion(d.isoformat())
    return {"start": start_date, "end": end_date, "signals": total}
