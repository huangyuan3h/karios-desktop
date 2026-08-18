"""Entry-lag measurement (2026-08-12): how much does buying NEXT DAY's open
differ from the backtest's signal-day close?

The backtest buys at the signal-day CLOSE. The user often buys intraday on
appearance, or — for EOD (17:30) candidates — the next session. This script
measures the realized price gap on every closed backtest trade:

  gap_open  = (next_session_open  - signal_close) / signal_close
  gap_close = (next_session_close - signal_close) / signal_close

Output: avg / median / p90 / p10 / win-rate impact for both gaps, per window.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    _resolve_ts_code,
    simulate,
)


def _next_session_open(ts_code: str, signal_date: str) -> tuple[float | None, float | None]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open, close FROM daily
                WHERE ts_code = %s AND trade_date > %s
                ORDER BY trade_date LIMIT 1
                """,
                (ts_code, signal_date),
            )
            row = cur.fetchone()
    return (float(row[0]), float(row[1])) if row else (None, None)


def main() -> int:
    base = {k: v for k, v in S3_CONFIG.items()}
    for name, (start, end) in WINDOWS.items():
        if name == "long":
            continue
        cfg = BacktestConfig(start_date=start, end_date=end, **base)
        run = simulate(cfg)
        open_gaps: list[float] = []
        close_gaps: list[float] = []
        for t in run.trades:
            parsed = _resolve_ts_code(t.symbol)
            if parsed is None:
                continue
            n_open, n_close = _next_session_open(parsed[1], t.entry_date)
            if n_open is None or n_open <= 0:
                continue
            entry = float(t.entry_price)
            open_gaps.append((n_open - entry) / entry * 100.0)
            close_gaps.append((n_close - entry) / entry * 100.0)
        if not open_gaps:
            print(f"[{name}] no data")
            continue
        def stats(vals: list[float]) -> str:
            vals = sorted(vals)
            n = len(vals)
            avg = sum(vals) / n
            med = vals[n // 2]
            p10 = vals[int(n * 0.10)]
            p90 = vals[int(n * 0.90)]
            worse = sum(1 for v in vals if v > 0) / n * 100
            return f"均值 {avg:+.2f}% · 中位 {med:+.2f}% · p10 {p10:+.2f}% · p90 {p90:+.2f}% · 高开占比 {worse:.0f}%"
        print(f"[{name}] {len(open_gaps)} 笔")
        print(f"  次日开盘 vs 信号日收盘: {stats(open_gaps)}")
        print(f"  次日收盘 vs 信号日收盘: {stats(close_gaps)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
