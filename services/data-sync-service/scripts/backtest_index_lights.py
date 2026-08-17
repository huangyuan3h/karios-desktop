"""Backtest the index traffic lights (红绿灯) against S-3 trades (2026-08-12).

Question: do the index-light definitions (red/yellow/green/deep_green) and
their heuristic position hints (0-10% / 30% / 50-60% / 80-100%) actually
separate good S-3 entry days from bad ones?

Method:
  1. Replay get_index_signals(as_of_date=day, include_breadth=False) for
     every trading day in the window -> per-day market light (tighter of
     the CN trio / HK pair).
  2. Simulate the S-3 strategy over the same window (S3_CONFIG / HK_S3_CONFIG
     — same code as live) to get every entry's trade_date.
  3. Bucket trades by the light on their entry day; compare win-rate / avg
     net pnl / total pnl across buckets vs the overall baseline.

Output: console table + data/backtest_reports/index_light_backtest_latest.json
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    simulate,
)
from data_sync_service.service.market_regime import get_index_signals  # noqa: E402

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REPORT_FILE = REPORT_DIR / "index_light_backtest_latest.json"

CN_NAMES = {"沪深300", "中证500", "创业板指"}
HK_NAMES = {"恒生指数", "恒生科技指数"}

SIGNAL_RANK = {"deep_green": 4, "green": 3, "yellow": 2, "red": 1, "unknown": 0}


def _tighter(signals: list[dict], names: set[str]) -> str:
    lights = [
        str(s.get("signal") or "unknown")
        for s in signals
        if str(s.get("name") or "") in names
    ]
    if not lights:
        return "unknown"
    return min(lights, key=lambda x: SIGNAL_RANK.get(x, 0))


def replay_lights(start: str, end: str, names: set[str]) -> dict[str, str]:
    """Per-day tighter light for the given index set (as-of replay, no
    realtime, no breadth → no look-ahead)."""
    out: dict[str, str] = {}
    d = date.fromisoformat(start)
    last = date.fromisoformat(end)
    while d <= last:
        iso = d.isoformat()
        try:
            signals = get_index_signals(as_of_date=iso, include_breadth=False)
        except Exception:  # noqa: BLE001
            out[iso] = "unknown"
            d += timedelta(days=1)
            continue
        out[iso] = _tighter(signals, names)
        d += timedelta(days=1)
    return out


def bucket_stats(trades, lights: dict[str, str]) -> dict:
    buckets: dict[str, list[float]] = {}
    for t in trades:
        light = lights.get(t.entry_date, "unknown")
        buckets.setdefault(light, []).append(float(t.pnl_pct or 0.0))
    stats: dict[str, dict] = {}
    for light, pnls in sorted(buckets.items(), key=lambda kv: SIGNAL_RANK.get(kv[0], 0), reverse=True):
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        stats[light] = {
            "trades": n,
            "winRate": round(wins / n, 3) if n else None,
            "avgPnlPct": round(sum(pnls) / n, 2) if n else None,
            "totalPnlPct": round(sum(pnls), 2),
        }
    return stats


def run_market(market: str, start: str, end: str, names: set[str]) -> dict:
    base = S3_CONFIG if market == "CN" else HK_S3_CONFIG
    cfg = BacktestConfig(
        start_date=start,
        end_date=end,
        market=market,
        **{k: v for k, v in base.items() if k not in ("market", "start_date", "end_date")},
    )
    run = simulate(cfg)
    all_pnls = [float(t.pnl_pct or 0.0) for t in run.trades]
    baseline = {
        "trades": len(all_pnls),
        "winRate": round(sum(1 for p in all_pnls if p > 0) / len(all_pnls), 3) if all_pnls else None,
        "avgPnlPct": round(sum(all_pnls) / len(all_pnls), 2) if all_pnls else None,
        "totalNetPnlPct": round(sum(all_pnls), 2),
    }
    lights = replay_lights(start, end, names)
    stats = bucket_stats(run.trades, lights)
    covered = sum(s["trades"] for s in stats.values())
    return {
        "market": market,
        "window": f"{start} ~ {end}",
        "baseline": baseline,
        "byLight": stats,
        "lightsCovered": round(covered / len(all_pnls), 3) if all_pnls else 0,
        "daysReplayed": len(lights),
    }


def main() -> int:
    report: dict = {"generatedAt": date.today().isoformat(), "markets": {}}
    for market, start, end, names in (
        ("CN", "2021-08-01", "2026-08-11", CN_NAMES),
        ("HK", "2024-08-01", "2026-08-07", HK_NAMES),
    ):
        t0 = time.time()
        m = run_market(market, start, end, names)
        m["seconds"] = round(time.time() - t0, 1)
        report["markets"][market] = m
        _print_market(m)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nreport -> {REPORT_FILE}")
    return 0


def _print_market(m: dict) -> None:
    print(f"\n=== {m['market']}  {m['window']} (回放 {m['daysReplayed']} 天, 覆盖 "
          f"{m['lightsCovered'] * 100:.0f}% 入场) ===")
    b = m["baseline"]
    print(f"基线   : {b['trades']} 笔 · 胜率 {b['winRate']} · 均盈 {b['avgPnlPct']}% · 总盈 {b['totalNetPnlPct']}%")
    print(f"{'灯':<10}{'笔数':>6}{'胜率':>8}{'均盈%':>9}{'总盈%':>10}")
    for light, s in m["byLight"].items():
        wr = f"{s['winRate'] * 100:.0f}%" if s["winRate"] is not None else "—"
        print(f"{light:<10}{s['trades']:>6}{wr:>8}{s['avgPnlPct']:>9}{s['totalPnlPct']:>10}")


if __name__ == "__main__":
    raise SystemExit(main())
