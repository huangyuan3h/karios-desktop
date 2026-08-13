"""Backtest the trendok structural exit signals vs S-3 exits (2026-08-12).

Question (OPT-095 follow-up): the watchlist main table exits on trendok
structure breaks (EMA5<EMA20, close<EMA20, momentum exhaustion) while the
health card / S-3 backtest only uses price-time rules (stop/trail/max_hold).
Which rule set is right? The structural signals have NEVER been validated
in the backtest.

Method (per-trade counterfactual, close-based, same slippage/cost as the
engine):
  For every S-3 trade, replay the holding period day by day with as-of
  EMA5/EMA20/MACD-hist/volume signals (120-day lookback). On the FIRST
  signal day, "exit at that day's close" and record what the trade would
  have made. Compare with the trade's actual backtest pnl.

  caveat: per-trade isolation (no sleeve re-entry effects) — direction
  signal only; a positive result would then go through real engine
  parameterization + walk-forward.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    _resolve_ts_code,
    simulate,
)

REPORT_FILE = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "trend_exit_latest.json"


def _ema(values: list[float], span: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (span + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _macd_hist(closes: list[float]) -> list[float]:
    if len(closes) < 35:
        return []
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    dif = [a - b for a, b in zip(ema12, ema26, strict=False)]
    dea = _ema(dif, 9)
    return [d - e for d, e in zip(dif, dea, strict=False)]


def _bars(ts_code: str, start: str, end: str) -> list[tuple[str, float, float]]:
    """(date, close, vol) ascending, inclusive."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, close, vol FROM daily
                WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (ts_code, start, end),
            )
            rows = cur.fetchall()
    out = []
    for r in rows:
        try:
            out.append((str(r[0]), float(r[1]), float(r[2] or 0.0)))
        except (TypeError, ValueError):
            continue
    return out


def _structural_signal_days(
    bars: list[tuple[str, float, float]],
    entry_idx: int,
) -> list[tuple[str, str]]:
    """Days >= entry with a structure-break signal, close-based (as-of)."""
    closes = [b[1] for b in bars]
    vols = [b[2] for b in bars]
    if len(closes) < 40:
        return []
    ema5 = _ema(closes, 5)
    ema20 = _ema(closes, 20)
    hist = _macd_hist(closes)
    out: list[tuple[str, str]] = []
    for i in range(entry_idx + 1, len(bars)):
        reasons: list[str] = []
        if ema5[i] < ema20[i]:
            reasons.append("ema5<ema20")
        if closes[i] < ema20[i]:
            reasons.append("close<ema20")
        if i >= 3 and len(hist) > i:
            h4 = hist[i - 3 : i + 1]
            if h4[0] > h4[1] > h4[2] > 0.0 and h4[3] < 0.0:
                avg5 = sum(vols[max(0, i - 4) : i + 1]) / 5.0
                avg30 = sum(vols[max(0, i - 29) : i + 1]) / min(30, i + 1)
                if avg30 > 0 and avg5 < avg30:
                    reasons.append("momentum_exhaust")
        if reasons:
            out.append((bars[i][0], ",".join(reasons)))
            break  # first signal only
    return out


def run_market(market: str, start: str, end: str) -> dict:
    base = S3_CONFIG if market == "CN" else HK_S3_CONFIG
    cfg = BacktestConfig(
        start_date=start,
        end_date=end,
        market=market,
        **{k: v for k, v in base.items() if k not in ("market", "start_date", "end_date")},
    )
    run = simulate(cfg)

    base_stats = {"n": 0, "win": 0, "total": 0.0}
    sig_stats = {"n": 0, "win": 0, "total": 0.0}
    signal_only_stats = {"n": 0, "win": 0, "total": 0.0}
    per_signal: dict[str, dict] = {}
    slip = cfg.slippage_pct
    costs = 0.3  # CN round-trip cost % (engine costs_pct)
    if market == "HK":
        costs = 0.6

    for t in run.trades:
        base_stats["n"] += 1
        base_stats["total"] += t.pnl_pct
        if t.pnl_pct > 0:
            base_stats["win"] += 1

        # entry day index inside bars: lookback 120d before entry
        start_lookback = (date.fromisoformat(t.entry_date) - timedelta(days=180)).isoformat()
        end_extra = (date.fromisoformat(t.close_date) + timedelta(days=10)).isoformat()
        resolved = _resolve_ts_code(str(t.symbol))
        if not resolved or resolved[0] != market:
            continue
        ts_code = resolved[1]
        bars = _bars(ts_code, start_lookback, end_extra)
        if not bars:
            continue
        entry_idx = next((i for i, b in enumerate(bars) if b[0] == t.entry_date), None)
        if entry_idx is None:
            continue

        # Baseline holding-period signals (regardless of actual close reason)
        sig_days = _structural_signal_days(bars, entry_idx)
        if not sig_days:
            continue
        sig_date, sig_reasons = sig_days[0]
        sig_day_bar = next((b for b in bars if b[0] == sig_date), None)
        if sig_day_bar is None:
            continue
        sig_close = sig_day_bar[1]
        entry_px = t.entry_price
        cost_px = entry_px * (1 + slip / 100.0)
        gross = (sig_close * (1 - slip / 100.0) - cost_px) / cost_px * 100.0
        net = gross - costs

        sig_stats["n"] += 1
        sig_stats["total"] += net
        if net > 0:
            sig_stats["win"] += 1

        # Would the signal exit beat the actual exit?
        actual = t.pnl_pct
        if sig_date < t.close_date:
            signal_only_stats["n"] += 1
            signal_only_stats["total"] += actual
            if actual > 0:
                signal_only_stats["win"] += 1
            for r in sig_reasons.split(","):
                d = per_signal.setdefault(r, {"n": 0, "sumActual": 0.0, "sumSignal": 0.0, "winActual": 0, "winSignal": 0})
                d["n"] += 1
                d["sumActual"] += actual
                d["sumSignal"] += net
                if actual > 0:
                    d["winActual"] += 1
                if net > 0:
                    d["winSignal"] += 1

    def fmt(s: dict) -> str:
        n = s["n"]
        wr = f"{s['win']/n:.0%}" if n else "—"
        avg = f"{s['total']/n:.2f}" if n else "—"
        return f"n={n:<4} 胜率={wr:<4} 均={avg}% 总={s['total']:.0f}%"

    print(f"\n=== {market} {start}~{end} ===")
    print(f"全部交易      : {fmt(base_stats)}")
    print(f"触发过信号    : {fmt(sig_stats)}   (信号日退出口径)")
    print(f"其中信号早于原退出: {fmt(signal_only_stats)}   (原退出收益=实际回测收益)")
    if per_signal:
        print("按信号类型（原退出 vs 信号退出 收益对比）:")
        for r, d in sorted(per_signal.items(), key=lambda kv: -kv[1]["n"]):
            print(
                f"  {r:<16} n={d['n']:<4} 原:胜率{d['winActual']/d['n']:.0%} 总{d['sumActual']:.0f}% "
                f"| 信号:胜率{d['winSignal']/d['n']:.0%} 总{d['sumSignal']:.0f}% "
                f"| 差 {d['sumSignal']-d['sumActual']:+.0f}%"
            )
    return {
        "market": market,
        "window": f"{start}~{end}",
        "all": base_stats,
        "signal_exit": sig_stats,
        "signal_earlier_than_actual": signal_only_stats,
        "bySignal": per_signal,
    }


def main() -> int:
    report: dict = {"generatedAt": date.today().isoformat(), "markets": {}}
    for market, start, end in (
        ("CN", "2021-08-01", "2026-08-11"),
        ("CN", "2024-08-01", "2025-08-01"),   # OOS2
        ("CN", "2025-08-01", "2026-02-01"),   # train
        ("CN", "2026-03-01", "2026-08-07"),   # valid
        ("HK", "2024-08-01", "2026-08-07"),
    ):
        report["markets"][f"{market}:{start}"] = run_market(market, start, end)
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nreport -> {REPORT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
