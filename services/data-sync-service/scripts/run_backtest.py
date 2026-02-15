from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from data_sync_service.testback.engine import (
    BacktestParams,
    DailyRuleFilter,
    ScoreConfig,
    UniverseFilter,
    run_backtest,
)
from data_sync_service.testback.strategies import get_strategy_class


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backtest with default settings.")
    parser.add_argument("--strategy", default="ma_crossover")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2026-01-01")
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--slippage-rate", type=float, default=0.0)
    parser.add_argument("--adj-mode", default="qfq")
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument("--min-volume", type=float, default=100000)
    parser.add_argument("--exclude-keywords", default="ST")
    parser.add_argument("--min-list-days", type=int, default=60)
    parser.add_argument("--out", default="backtest_log.txt")
    parser.add_argument("--out-html", default="backtest_log.html")
    parser.add_argument("--max-selected", type=int, default=10)
    parser.add_argument("--max-days", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    strategy_cls = get_strategy_class(args.strategy)
    params = BacktestParams(
        start_date=args.start,
        end_date=args.end,
        initial_cash=args.initial_cash,
        fee_rate=args.fee_rate,
        slippage_rate=args.slippage_rate,
        adj_mode=args.adj_mode,
    )
    universe = UniverseFilter(
        market="CN",
        exclude_keywords=[k.strip() for k in args.exclude_keywords.split(",") if k.strip()],
        min_list_days=args.min_list_days,
    )
    rules = DailyRuleFilter(
        min_price=args.min_price,
        min_volume=args.min_volume,
    )
    scoring = ScoreConfig(
        top_n=args.top_n,
        momentum_weight=1.0,
        volume_weight=0.2,
        amount_weight=0.1,
    )
    result = run_backtest(
        strategy_cls=strategy_cls,
        params=params,
        universe_filter=universe,
        daily_rules=rules,
        score_cfg=scoring,
    )
    out_path = Path(args.out)
    html_path = Path(args.out_html)
    max_selected = max(1, int(args.max_selected))
    max_days = max(0, int(args.max_days))
    lines: list[str] = []
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    summary = result.get("summary", {})
    daily_log = result.get("daily_log", [])
    lines.append(f"生成时间={generated_at}")
    lines.append(f"摘要={summary}")
    lines.append(f"日志天数={len(daily_log)}")
    lines.append("")
    if max_days > 0:
        daily_log = daily_log[-max_days:]
    for day in daily_log:
        selected = day.get("selected", [])[:max_selected]
        selected_str = ",".join(
            f"{s.get('ts_code')}:{s.get('score'):.4f}@{s.get('avg_price'):.4f}"
            for s in selected
        )
        orders = day.get("orders", [])
        orders_str = ",".join(
            f"{o.get('status')}:{o.get('action')}:{o.get('ts_code')}:"
            f"qty={_fmt_num(o.get('exec_qty') or o.get('qty'))}:"
            f"price={_fmt_num(o.get('exec_price'))}:{o.get('reason') or ''}"
            for o in orders
        )
        lines.append(
            f"{day.get('date')} 现金={day.get('cash'):.2f} 权益={day.get('equity'):.2f} "
            f"选股=[{selected_str}] 指令=[{orders_str}]"
        )
    out_path.write_text("\n".join(lines), encoding="utf-8")
    _write_html(
        html_path=html_path,
        generated_at=generated_at,
        summary=summary,
        daily_log=daily_log,
        max_selected=max_selected,
    )
    print("回测摘要:", summary)
    print("日志天数:", len(daily_log))
    print("日志文件:", out_path.resolve())
    print("页面文件:", html_path.resolve())


def _write_html(
    html_path: Path,
    generated_at: str,
    summary: dict,
    daily_log: list[dict],
    max_selected: int,
) -> None:
    rows: list[str] = []
    for day in daily_log:
        selected = day.get("selected", [])[:max_selected]
        selected_str = "<br/>".join(
            f"{s.get('ts_code')} | 分数 {s.get('score'):.4f} | 均价 {s.get('avg_price'):.4f}"
            for s in selected
        )
        orders = day.get("orders", [])
        orders_str = "<br/>".join(
            f"{o.get('status')} {o.get('action')} {o.get('ts_code')} "
            f"qty={_fmt_num(o.get('exec_qty') or o.get('qty'))} "
            f"price={_fmt_num(o.get('exec_price'))} {o.get('reason') or ''}"
            for o in orders
        )
        rows.append(
            "<tr>"
            f"<td>{day.get('date')}</td>"
            f"<td>{day.get('cash'):.2f}</td>"
            f"<td>{day.get('equity'):.2f}</td>"
            f"<td>{selected_str}</td>"
            f"<td>{orders_str}</td>"
            "</tr>"
        )
    html = f"""<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8"/>
  <title>回测日志</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    h1 {{ margin-bottom: 6px; }}
    .meta {{ color: #555; margin-bottom: 12px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px; vertical-align: top; }}
    th {{ background: #f5f5f5; position: sticky; top: 0; }}
    .search {{ margin: 12px 0; }}
  </style>
</head>
<body>
  <h1>回测日志</h1>
  <div class="meta">生成时间: {generated_at}</div>
  <div class="meta">摘要: {summary}</div>
  <div class="search">
    <label>筛选关键字: <input id="filter" placeholder="例如 000001 或 buy"/></label>
  </div>
  <table id="logTable">
    <thead>
      <tr>
        <th>日期</th>
        <th>现金</th>
        <th>权益</th>
        <th>选股(Top{max_selected})</th>
        <th>指令</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows)}
    </tbody>
  </table>
  <script>
    const input = document.getElementById('filter');
    const table = document.getElementById('logTable');
    input.addEventListener('input', () => {{
      const q = input.value.trim().toLowerCase();
      const rows = table.querySelectorAll('tbody tr');
      rows.forEach(row => {{
        const text = row.innerText.toLowerCase();
        row.style.display = text.includes(q) ? '' : 'none';
      }});
    }});
  </script>
</body>
</html>
"""
    html_path.write_text(html, encoding="utf-8")


def _fmt_num(val) -> str:
    if val is None:
        return "-"
    try:
        return f"{float(val):.4f}"
    except Exception:
        return str(val)


if __name__ == "__main__":
    main()
