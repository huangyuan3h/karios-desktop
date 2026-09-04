#!/usr/bin/env python3
"""S4 diagnostic battery: which BUY-side exclusions deserve a walk-forward?

Selection windows OOS2+train; valid NOT touched. Same eligible set as S2
(gate + skip_t1 + C1 upside cap). Forward 3-day net (1430 -> day-3 1430).

Dimensions (each hypothesis-led):
  turn   amount/avg20 bins: huge-turnover gaps = distribution top?
  board  60/00 vs 3/68: 20cm gaps too wild to hold 3 days?
  mv     daily size tercile: small-cap gaps = pump-and-dump?
  age    listing age <1y / 1-3y / >3y: fresh-listing gap noise?
  idxgap 000001.SH open gap bins: chasing gaps on overheated opens?
  breadth gate-marginal 0.5-0.6 / 0.6-0.75 / >0.75: marginal days worse?

Rule: BOTH windows must agree + worst bin reliably negative + mechanism,
else killed. At most ONE walk-forward. All six reported (no cherry-pick).
Read-only vs Postgres. Prints tables, saves nothing.
"""

from __future__ import annotations

import sys
from datetime import date as _date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

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
TURN_BINS = ("<1x", "1-2x", "2-4x", ">4x")
IDX_BINS = ("idx<0%", "0-1%", ">1%")
BR_BINS = ("0.50-0.60", "0.60-0.75", ">0.75")
AGE_BINS = ("<1y", "1-3y", ">3y")


def _turn_bin(t: float) -> str:
    if t != t:
        return TURN_BINS[1]
    if t < 1:
        return TURN_BINS[0]
    if t < 2:
        return TURN_BINS[1]
    if t < 4:
        return TURN_BINS[2]
    return TURN_BINS[3]


def _idx_bin(g: float) -> str:
    if g < 0:
        return IDX_BINS[0]
    if g < 0.01:
        return IDX_BINS[1]
    return IDX_BINS[2]


def _br_bin(b: float) -> str:
    if b < 0.60:
        return BR_BINS[0]
    if b < 0.75:
        return BR_BINS[1]
    return BR_BINS[2]


def _load_list_dates() -> dict[str, str]:
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[str, str] = {}
    try:
        conn = psycopg.connect(get_settings().database_url)
        cur = conn.cursor()
        cur.execute("SELECT ts_code, list_date FROM stock_basic WHERE list_date IS NOT NULL")
        for ts, ld in cur.fetchall():
            out[str(ts)] = str(ld)[:10]
        conn.close()
    except Exception:
        pass
    return out


def _load_index_open_gaps() -> dict[str, float]:
    """000001.SH open gap per day from index_daily (prior close as base)."""
    import psycopg

    from data_sync_service.config import get_settings

    out: dict[str, float] = {}
    try:
        conn = psycopg.connect(get_settings().database_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT trade_date, open, close FROM index_daily "
            "WHERE ts_code = '000001.SH' AND trade_date >= '2024-07-01' "
            "ORDER BY trade_date"
        )
        rows = [(str(d)[:10], o, c) for d, o, c in cur.fetchall()]
        conn.close()
        prev: float | None = None
        for d, o, c in rows:
            if prev and o and o > 0:
                out[d] = o / prev - 1
            prev = c if c and c > 0 else prev
    except Exception:
        pass
    return out


def main() -> int:
    print("loading context ...", flush=True)
    ctx = load_sgap_context("2024-08-01", "2026-02-01")
    per_ts = ctx["per_ts"]
    date_idx = ctx["date_idx"]
    mv_map = ctx["mv_map"]
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    list_dates = _load_list_dates()
    print(f"list_dates for {len(list_dates)} names", flush=True)
    idx_open_gap = _load_index_open_gaps()

    acc: dict[str, dict[str, dict[str, list[float]]]] = {
        "turn": {w: {b: [] for b in TURN_BINS} for w in WINDOWS},
        "board": {w: {"main": [], "20cm": []} for w in WINDOWS},
        "mv": {w: {"small": [], "mid": [], "big": []} for w in WINDOWS},
        "age": {w: {b: [] for b in AGE_BINS} for w in WINDOWS},
        "idxgap": {w: {b: [] for b in IDX_BINS} for w in WINDOWS},
        "breadth": {w: {b: [] for b in BR_BINS} for w in WINDOWS},
    }
    for w, (s, e) in WINDOWS.items():
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
            # index open gap (preloaded from index_daily)
            idx_gap = idx_open_gap.get(day, float("nan"))
            # mv tercile cutoffs for the day
            mvs = sorted(v for v in (mv_map.get(day) or {}).values() if v and v > 0)
            q1 = mvs[len(mvs) // 3] if mvs else 0
            q2 = mvs[2 * len(mvs) // 3] if mvs else 0
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
                    ts=ts, px=px, open_px=bar.get("open"), pre_close=bar.get("pre_close"),
                    skip_t1_limit=True, max_open_to_1430_pct=0.03, near_limit_buffer_pct=None,
                )
                if reason or not px or px <= 0:
                    continue
                px_exit = _intraday_px(ctx, ts, exit_day, "1430")
                if not px_exit or px_exit <= 0:
                    continue
                if not bar.get("open") or not bar.get("pre_close"):
                    continue
                fwd = px_exit / px - 1 - COSTS_ROUNDTRIP
                # T-1 turnover: yesterday's amount / trailing avg (fully known
                # at 14:30; same-day amount is incomplete intraday -> lookahead).
                t1_turn = float("nan")
                if di > 20:
                    amts = [r.get("amount") for r in series[di - 21: di] if r.get("amount")]
                    if len(amts) >= 15 and amts[-1]:
                        avg = sum(amts[:-1]) / max(len(amts) - 1, 1)
                        t1_turn = amts[-1] / avg if avg else float("nan")
                acc["turn"][w][_turn_bin(t1_turn)].append(fwd)
                code = ts.split(".")[0]
                acc["board"][w]["20cm" if code.startswith(("3", "68")) else "main"].append(fwd)
                mv = (mv_map.get(day) or {}).get(ts) or 0
                acc["mv"][w]["small" if mv < q1 else ("mid" if mv < q2 else "big")].append(fwd)
                ld = list_dates.get(ts)
                if ld:
                    age_y = (_date.fromisoformat(day) - _date.fromisoformat(ld)).days / 365.25
                    acc["age"][w]["<1y" if age_y < 1 else ("1-3y" if age_y < 3 else ">3y")].append(fwd)
                if idx_gap == idx_gap:
                    acc["idxgap"][w][_idx_bin(idx_gap)].append(fwd)
                acc["breadth"][w][_br_bin(breadth)].append(fwd)

    def _show(title: str, table: dict[str, dict[str, list[float]]], cols: tuple[str, ...]) -> None:
        print(f"\n## {title}")
        print("| window | " + " | ".join(f"{c}" for c in cols) + " |")
        print("|" + "|".join(["------"] * (1 + len(cols))) + "|")
        for w in WINDOWS:
            cells = []
            for c in cols:
                v = table[w][c]
                m = float(np.mean(v)) * 100 if v else 0.0
                hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
                cells.append(f"{m:+.2f}%/{hit:.0f}%/{len(v)}")
            print(f"| {w} | " + " | ".join(cells) + " |")

    _show("turnover (amount/avg20)", acc["turn"], TURN_BINS)
    _show("board", acc["board"], ("main", "20cm"))
    _show("mv tercile", acc["mv"], ("small", "mid", "big"))
    _show("listing age", acc["age"], AGE_BINS)
    _show("index open gap", acc["idxgap"], IDX_BINS)
    _show("breadth level", acc["breadth"], BR_BINS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
