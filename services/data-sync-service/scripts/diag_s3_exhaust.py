#!/usr/bin/env python3
"""S3-exhaust diagnostic: split frozen S-3 CN realized trades by entry flag.

Selection windows OOS2+train; valid NOT touched (pre-reg §2).
Flag = strong_scoop_exhaustion at signal day S (last trading day before
entry_date, inside the entry decision info set, zero added lookahead),
thresholds verbatim from factor_signals_service (ret60>0.40 & vr>1.2,
scoop shape gates; t<89 forced False). Metric = realized net pnl_pct
(engine's own exits). Each BacktestTrade row (incl. pyramid adds, each
with its own entry_date) is one observation; documented, not filtered.

Read-only vs Postgres (via engine's own loaders). Saves nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from run_walk_forward import S3_CONFIG  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
}


def _num(sym: str) -> str:
    s = str(sym).upper()
    if ":" in s:
        s = s.split(":")[-1]
    return s.split(".")[0].zfill(6)


def _is_exhausted_at(dates: list[str], closes: np.ndarray, highs: np.ndarray,
                     lows: np.ndarray, vols: np.ndarray, t: int) -> bool:
    if t < 89 or t >= len(dates):
        return False
    if np.any(closes[t - 60:t + 1] <= 0):
        return False
    ma20 = float(np.mean(closes[t - 19:t + 1]))
    ma60 = float(np.mean(closes[t - 59:t + 1]))
    ma60_m30 = float(np.mean(closes[t - 89:t - 29]))
    if not (ma20 > ma60 and closes[t - 30] > ma60_m30):
        return False
    ph = float(np.max(highs[t - 40:t - 20]))
    if ph <= 0:
        return False
    win = lows[t - 20:t + 1]
    if np.any(win <= 0):
        return False
    bottom = float(np.min(win))
    bi = t - 20 + int(np.argmin(win))
    depth = (ph - bottom) / ph
    if not (0.05 <= depth <= 0.18):
        return False
    if not (closes[t] >= bottom * 1.03 and closes[t] >= ma20 * 0.99):
        return False
    if bi < t - 15:
        return False
    sv = float(np.mean(vols[t - 20:t + 1]))
    if sv <= 0 or vols[t] <= 0:
        return False
    vr = float(vols[t] / sv)
    ret60 = float(closes[t] / closes[t - 60] - 1)
    return bool(ret60 > 0.40 and vr > 1.2)


def main() -> int:
    print("S3-exhaust diagnostic (frozen CN S-3, realized trades split by entry flag)\n")
    for w, (s, e) in WINDOWS.items():
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)  # type: ignore[arg-type]
        print("  loading BacktestData ...", flush=True)
        data = BacktestData(cfg)
        print(f"  universe: {len(data.ts_codes)} names, calendar {len(data.calendar)} days", flush=True)
        print("  simulating ...", flush=True)
        run = simulate(cfg, data)
        trades = run.trades
        print(f"  trades rows: {len(trades)}", flush=True)
        # per-ts arrays from engine bars (date, open, high, low, close, volume)
        series: dict[str, dict] = {}
        for ts, bars in data.bars_by_ts.items():
            dd, cc, hh, ll, vv = [], [], [], [], []
            for b in bars:
                try:
                    c = float(b[4])
                except (TypeError, ValueError):
                    continue
                if c <= 0:
                    continue
                dd.append(str(b[0]))
                cc.append(c)
                try:
                    hh.append(float(b[2])); ll.append(float(b[3]))
                except (TypeError, ValueError):
                    hh.append(0.0); ll.append(0.0)
                try:
                    vv.append(float(b[5]))
                except (TypeError, ValueError, IndexError):
                    vv.append(0.0)
            if len(dd) > 90:
                series[_num(ts)] = {
                    "idx": {d: i for i, d in enumerate(dd)},
                    "c": np.array(cc), "h": np.array(hh),
                    "l": np.array(ll), "v": np.array(vv),
                }
        exh_pnl: list[float] = []
        cln_pnl: list[float] = []
        exh_hold: list[int] = []
        cln_hold: list[int] = []
        n_noseries = 0
        for tr in trades:
            key = _num(tr.symbol)
            st = series.get(key)
            if not st:
                n_noseries += 1
                continue
            ei = st["idx"].get(str(tr.entry_date)[:10], -1)
            if ei <= 0:
                n_noseries += 1
                continue
            flag = _is_exhausted_at(
                list(st["idx"].keys()), st["c"], st["h"], st["l"], st["v"], ei - 1
            )
            # NOTE: dates list rebuilt per trade is O(n^2)-ish; windows are
            # small (<=~150 rows) so this stays cheap. Keep code simple.
            if flag:
                exh_pnl.append(float(tr.pnl_pct))
                exh_hold.append(int(tr.holding_days or 0))
            else:
                cln_pnl.append(float(tr.pnl_pct))
                cln_hold.append(int(tr.holding_days or 0))
        def _show(name: str, v: list[float], h: list[int]) -> None:
            m = float(np.mean(v)) if v else 0.0
            hit = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
            ah = float(np.mean(h)) if h else 0.0
            print(f"  {name:<10} mean {m:+.2f}%  hit {hit:.0f}%  n {len(v)}  avgHold {ah:.1f}d")
        _show("exhausted", exh_pnl, exh_hold)
        _show("clean", cln_pnl, cln_hold)
        tot = len(exh_pnl) + len(cln_pnl)
        print(f"  coverage: {len(exh_pnl)}/{tot} ({100.0*len(exh_pnl)/max(1,tot):.1f}%), no-series {n_noseries}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
