"""C4: paper-vs-backtest report (2026-08-12).

Reconciles every CLOSED S-3 paper trade (source S3/S3HK) against the
backtest engine's twin trade: same symbol, same entry date, same S-3
config, one small per-trade window (entry - lookback ~ close + buffer).

Per-trade diff (entry price, close price, pnl, reason) attributes the gap
to execution vs rules; the summary compares paper win-rate / avg pnl to
the backtest's matched trades. Output:
  - console table
  - data/backtest_reports/paper_vs_backtest_latest.json

TODO (C4 completion, >=20 closed trades): add statistical verdict
(win-rate/pnl confidence bands) — the framework is sample-agnostic and
just gets more meaningful as the paper book closes more trades.
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
    simulate,
)

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REPORT_FILE = REPORT_DIR / "paper_vs_backtest_latest.json"
LOOKBACK_DAYS = 25
BUFFER_DAYS = 5


def _closed_s3_trades() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, entry_date, entry_price, close_date, close_price,
                       pnl_pct, holding_days, close_reason, source, market
                FROM paper_trades
                WHERE status = 'closed' AND source IN ('S3', 'S3HK')
                ORDER BY close_date
                """
            )
            rows = cur.fetchall()
    return [
        {
            "symbol": str(r[0]),
            "entryDate": str(r[1]),
            "entryPrice": float(r[2]),
            "closeDate": str(r[3]),
            "closePrice": float(r[4]) if r[4] is not None else None,
            "pnlPct": float(r[5]) if r[5] is not None else None,
            "holdingDays": int(r[6]) if r[6] is not None else None,
            "closeReason": str(r[7] or ""),
            "source": str(r[8] or ""),
            "market": str(r[9] or "CN"),
        }
        for r in rows
    ]


def _full_window(trades: list[dict]) -> tuple[str, str]:
    """One continuous window per market so sleeve state is realistic
    (a short window starts fully-loaded from a batch entry, falsely
    sleeve-blocking later paper entries)."""
    first = min(date.fromisoformat(t["entryDate"]) for t in trades)
    last = max(date.fromisoformat(t["closeDate"]) for t in trades)
    start = (first - timedelta(days=60)).isoformat()
    end = (last + timedelta(days=BUFFER_DAYS)).isoformat()
    return start, end


def _find_twin(run, trade: dict) -> dict | None:
    """Backtest trade for (symbol, entry_date), else same-symbol nearest."""
    exact = [t for t in run.trades if t.symbol == trade["symbol"] and t.entry_date == trade["entryDate"]]
    if exact:
        return exact[0]
    same = sorted(
        (t for t in run.trades if t.symbol == trade["symbol"]),
        key=lambda t: abs(
            (date.fromisoformat(t.entry_date) - date.fromisoformat(trade["entryDate"])).days
        ),
    )
    return same[0] if same else None


def _reason_map(reason: str) -> str:
    return {
        "stop_hit": "stop_loss",
        "trailing_stop": "trailing_stop",
        "swapped": "swapped",
        "max_hold": "max_hold",
        "pool_exit": "pool_exit",
        "end_of_window": "end_of_window",
    }.get(reason, reason)


def main() -> int:
    trades = _closed_s3_trades()
    report: dict = {
        "generatedAt": date.today().isoformat(),
        "sampleCount": len(trades),
        "verdict": "样本 <20 笔：结论待积累（C4 未定案）" if len(trades) < 20 else "样本充足：可作统计对照",
        "rows": [],
    }
    paper_total = 0.0
    paper_wins = 0
    twin_total = 0.0
    twin_wins = 0

    by_market: dict[str, list[dict]] = {}
    for trade in trades:
        by_market.setdefault(trade["market"], []).append(trade)
    runs: dict[str, object] = {}
    for market, m_trades in by_market.items():
        base = S3_CONFIG if market == "CN" else HK_S3_CONFIG
        start, end = _full_window(m_trades)
        config = BacktestConfig(
            start_date=start,
            end_date=end,
            market=market,
            **{k: v for k, v in base.items() if k not in ("market", "start_date", "end_date")},
        )
        runs[market] = simulate(config)

    for trade in trades:
        run = runs[trade["market"]]
        twin = _find_twin(run, trade)
        if trade["pnlPct"] is not None:
            paper_total += trade["pnlPct"]
            paper_wins += 1 if trade["pnlPct"] > 0 else 0
        row: dict = {
            "symbol": trade["symbol"],
            "market": trade["market"],
            "entryDate": trade["entryDate"],
            "closeDate": trade["closeDate"],
            "paper": {
                "entryPrice": trade["entryPrice"],
                "closePrice": trade["closePrice"],
                "pnlPct": round(trade["pnlPct"], 2) if trade["pnlPct"] is not None else None,
                "holdingDays": trade["holdingDays"],
                "closeReason": trade["closeReason"],
            },
        }
        if twin is None:
            row["backtest"] = None
            row["note"] = "回测未入场（分数/闸门/时点差异）"
        else:
            twin_pnl = float(twin.pnl_pct or 0.0)
            twin_total += twin_pnl
            twin_wins += 1 if twin_pnl > 0 else 0
            entry_diff = (
                (float(twin.entry_price) - trade["entryPrice"]) / trade["entryPrice"] * 100.0
                if trade["entryPrice"]
                else None
            )
            pnl_diff = twin_pnl - (trade["pnlPct"] or 0.0)
            row["backtest"] = {
                "entryDate": twin.entry_date,
                "closeDate": twin.close_date,
                "entryPrice": round(float(twin.entry_price), 4),
                "closePrice": round(float(twin.close_price), 4),
                "pnlPct": round(twin_pnl, 2),
                "holdingDays": twin.holding_days,
                "closeReason": twin.close_reason,
            }
            row["diff"] = {
                "entryPriceDiffPct": round(entry_diff, 2) if entry_diff is not None else None,
                "pnlDiffPct": round(pnl_diff, 2),
            }
            row["note"] = (
                "一致"
                if twin.entry_date == trade["entryDate"] and _reason_map(trade["closeReason"]) == twin.close_reason
                else "存在差异"
            )
        report["rows"].append(row)

    n = len(trades)
    matched = [r for r in report["rows"] if r["backtest"] is not None]
    report["summary"] = {
        "paper": {
            "closed": n,
            "winRate": round(paper_wins / n, 3) if n else None,
            "avgPnlPct": round(paper_total / n, 2) if n else None,
        },
    }
    report["summary"]["backtestMatched"] = {
        "closed": len(matched),
        "winRate": round(twin_wins / len(matched), 3) if matched else None,
        "avgPnlPct": round(twin_total / len(matched), 2) if matched else None,
    }

    print(f"C4 paper-vs-backtest · 样本 {n} 笔（<20 未定案）\n")
    print("| symbol | 入场 | paper pnl | paper reason | 回测 pnl | 回测 reason | 入场价差% | 备注 |")
    print("|--------|------|-----------|--------------|----------|-------------|-----------|------|")
    for r in report["rows"]:
        bt = r["backtest"] or {}
        print(
            f"| {r['symbol']} | {r['entryDate']} | "
            f"{r['paper']['pnlPct'] if r['paper']['pnlPct'] is not None else '—'} | "
            f"{r['paper']['closeReason']} | "
            f"{bt.get('pnlPct') if bt else '—'} | {bt.get('closeReason') if bt else '—'} | "
            f"{r['diff']['entryPriceDiffPct'] if r.get('diff') else '—'} | {r['note']} |"
        )
    print("\n汇总：")
    print(f"  paper:      {report['summary']['paper']}")
    print(f"  回测匹配:   {report['summary']['backtestMatched']}")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nreport -> {REPORT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
