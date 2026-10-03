"""Factor cold-storage vault (Karios 因子冷库 display layer, read-only).

One table of ALL factors/strategies that were once effective, with a uniform
health check from the H2k revival rule (H2k_sgap_revival.md Sec 3-4).

Uniform rule (frozen, same for every factor, documented here and in the UI):
- Rolling last 40 trades (or rebalance periods for monthly legs): percentile
  vs a same-period random baseline, and net return after costs.
- State:
    冷库 (cold)   : enough data AND (percentile < 50 OR net <= 0)
    观察 (watch)   : 50 <= percentile < 75, OR not enough trades (< 20)
    复活 (revived): percentile >= 75 AND net > 0 (needs confirmations below)
  Precedence: revived-check first, then insufficient-trades -> watch,
  then cold, else watch. Where a true same-period random percentile was never
  run (most pilots), percentile is None ("--" in UI) and a factor can never
  show 复活 (needs a real >= 75 percentile); status then falls back to
  net + trades (net <= 0 with >= 5 trades -> cold, else watch).
- Suggested position steps: 0% -> 10% -> 20% (cap 20%, H2j A25 cap).
  Step-up needs TWO consecutive revived windows (double confirmation against
  single-window spikes, fixed, not a free param) AND >= 20 trades since the
  last change. Step-down is immediate one level per window below 50
  (20 -> 10 -> 0, no jump). Drop to 0 on percentile < 50 or any breaker
  (forward 60d -10% / single-trade -8% / 3 same-day full losses or weekly -5%
  halve / data-link alarm / holdout to -25%; FINAL E5 + H2j Sec 3).
  Paper-20 + monthly-execution delays from H2k Sec 4 are enforced live
  (paper ~4/20 unmet pins the live line at 0%); this display replays the
  rule without the paper gate so revivals stay visible, and the UI labels
  the live prerequisite separately. 星舰B stays the live baseline; this
  module never changes live strategy params or any order/broker logic.

Data (frozen, read-only, no DB/network/clock/broker calls):
- S-gap / 星舰B family: per-trade h2f_trades.json (1130 trades) where
  available; else frozen window aggregates below.
- Harbor S-3 stock leg: per-trade h2d_blotter_*.json (416 long trades).
- All other pilots: frozen audit window aggregates (FINAL_REPORT.md Sec 2,
  H2_backtests.md Sec 2-4, G_portfolio_100w.md Sec 2, H2b/H2d, H2j, H2k).
  Percentiles are true placebo values where the audits ran them
  (F 15000 draws for 星舰B, H2d 500/window for Harbor, H2 500/window for
  H01/H02/H03/H05/H06/H11-relative); otherwise None (never revived).
- Costs are after-fee: satellite 32.28bp/trade, transfer/B-leg 5bp/side,
  monthly rebalance included in NAV totals. contrib is satellite points
  (0.25 * (ret - 32.28bp) * 100); harbor blotter pnl is additive points
  (ret% * pos); monthly legs are NAV %.

Purity: no DB, no network, no clock, no broker/order calls.
"""

from __future__ import annotations

import math
from typing import Any

# Frozen H2k revival params (H2k Sec 3-4, same as sgap_decay.py).
REVIVAL_N: int = 40
REVIVAL_X: float = 75.0
REVIVAL_OFF: float = 50.0
REVIVAL_STEP_GAP: int = 20
MIN_TRADES: int = 20
POSITION_STEPS: tuple[int, ...] = (0, 10, 20)

# S-gap F long R1 calibration (H2k Sec 3, same as sgap_decay.py).
SGAP_MU: float = 0.0178
SGAP_SIGMA: float = 1.2175

# Harbor S-3 stock-leg calibration from H2d Sec 3.2 placebo (long 416 trades:
# random mean 40.3 / p50 38.8 / p95 76.7 -> sigma_total ~22.1).
HARBOR_MU: float = 0.097
HARBOR_SIGMA: float = 1.08

SOURCE_LABEL = "karios-audit-2026-10 frozen windows (FINAL/H2/G/H2b/H2d/H2j/H2k)"

# Window end dates (H2k frozen cut) for the history axis.
WINDOW_DATES: dict[str, str] = {
    "OOS2": "2025-08-07",
    "train": "2026-02-27",
    "valid": "2026-08-07",
    "holdout": "2026-09-30",
}

# Each factor: frozen window aggregates. net = window total after costs
# (satellite points for S-gap family / additive points for Harbor blotter /
# NAV % for monthly legs); pct = true placebo percentile where the audits ran
# it, else None; win = win rate 0-1 or None; n = trades/periods in window.
# Sources are FINAL_REPORT.md Sec 2 unless noted.
FACTOR_DEFS: tuple[dict[str, Any], ...] = (
    {
        "id": "starship_b",
        "name": "星舰B默认",
        "family": "星舰B族",
        "source": "S-gap>3% + amp前1/3 + body=3 独立信号",
        "doc": "FINAL Sec 2 + H2k (live基线, rebaseline post-dense)",
        "unit": "点",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 221.9, "pct": 99.9, "win": 0.633, "n": 293},
            "train": {"net": 71.4, "pct": 100.0, "win": 0.55, "n": 124},
            "valid": {"net": 5.9, "pct": 50.2, "win": 0.519, "n": 52},
            "holdout": {"net": -19.8, "pct": 10.9, "win": 0.288, "n": 52},
            "long": {"net": 729.9, "pct": 100.0, "win": 0.56, "n": 1078},
        },
    },
    {
        "id": "starship_a25",
        "name": "H2-a25",
        "family": "星舰B族参数档",
        "source": "同S-gap信号 × 25%H2+75%B3 (非独立源)",
        "doc": "FINAL Sec 2 (K3 FAIL, REJECT底)",
        "unit": "点",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 225.8, "pct": None, "win": None, "n": 293},
            "train": {"net": 76.7, "pct": None, "win": None, "n": 124},
            "valid": {"net": 4.0, "pct": None, "win": None, "n": 52},
            "holdout": {"net": -20.1, "pct": None, "win": None, "n": 52},
            "long": {"net": 803.3, "pct": None, "win": None, "n": 1078},
        },
    },
    {
        "id": "starship_b3",
        "name": "纯B3组合",
        "family": "星舰B族对照",
        "source": "同S-gap信号 × 纯B3停放 (非独立源)",
        "doc": "FINAL Sec 2 (对照)",
        "unit": "点",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 225.2, "pct": None, "win": None, "n": 293},
            "train": {"net": 69.3, "pct": None, "win": None, "n": 124},
            "valid": {"net": 3.5, "pct": None, "win": None, "n": 52},
            "holdout": {"net": -19.3, "pct": None, "win": None, "n": 52},
            "long": {"net": 713.4, "pct": None, "win": None, "n": 1078},
        },
    },
    {
        "id": "starship_h2",
        "name": "星舰H2 100%",
        "family": "星舰B族激进对照",
        "source": "同S-gap信号 × 100%H2 (非独立源)",
        "doc": "FINAL Sec 2 (证伪, 不进Live)",
        "unit": "点",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 224.3, "pct": None, "win": None, "n": 293},
            "train": {"net": 100.4, "pct": None, "win": None, "n": 124},
            "valid": {"net": 1.3, "pct": None, "win": None, "n": 52},
            "holdout": {"net": -22.7, "pct": None, "win": None, "n": 52},
            "long": {"net": 1040.4, "pct": None, "win": None, "n": 1078},
        },
    },
    {
        "id": "starship_cap50",
        "name": "容量版星舰50M",
        "family": "星舰B族执行版",
        "source": "同S-gap信号 × 日成交≥50M过滤4槽 (非独立源)",
        "doc": "FINAL Sec 2 + H2b Sec 2.2 (200w执行版, holdout q19.6)",
        "unit": "点",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 79.0, "pct": 99.5, "win": None, "n": 160},
            "train": {"net": 32.2, "pct": 92.4, "win": None, "n": 86},
            "valid": {"net": -6.4, "pct": 6.6, "win": None, "n": 40},
            "holdout": {"net": -16.5, "pct": 19.6, "win": None, "n": 41},
            "long": {"net": 341.1, "pct": 100.0, "win": None, "n": 592},
        },
    },
    {
        "id": "h11",
        "per_trade": True,
        "name": "H11 S-gap liquid",
        "family": "S-gap远亲 (观察)",
        "source": "绝对50M + amp<4% (相对分位版已证伪)",
        "doc": "H2 Sec 2 (holdout binding失败 n=127 t-4.8, PBO0.83)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.90, "pct": None, "win": None, "n": 1934},
            "train": {"net": 0.62, "pct": None, "win": None, "n": 762},
            "valid": {"net": 1.22, "pct": None, "win": None, "n": 1079},
            "holdout": {"net": -1.96, "pct": 0.0, "win": None, "n": 127},
            "long": {"net": 0.58, "pct": None, "win": None, "n": 6598},
        },
    },
    {
        "id": "h08",
        "per_trade": True,
        "name": "H08 跳空追涨hold10",
        "family": "事件追涨",
        "source": "跳空高开追涨独立信号",
        "doc": "H2 Sec 4 (long显著正t10.5, holdout显著负t-4)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.30, "pct": None, "win": None, "n": 1200},
            "train": {"net": 0.73, "pct": None, "win": None, "n": 600},
            "valid": {"net": 1.22, "pct": None, "win": None, "n": 500},
            "holdout": {"net": -0.69, "pct": None, "win": None, "n": 200},
            "long": {"net": 0.36, "pct": None, "win": None, "n": 5000},
        },
    },
    {
        "id": "h06",
        "per_trade": True,
        "name": "H06 上升三法hold20",
        "family": "形态",
        "source": "上升三法独立形态",
        "doc": "H2 Sec 3 (Bonferroni后ns, holdout n=8待n>2000)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 3.44, "pct": None, "win": None, "n": 201},
            "train": {"net": 0.84, "pct": None, "win": None, "n": 100},
            "valid": {"net": 3.00, "pct": None, "win": None, "n": 60},
            "holdout": {"net": -5.38, "pct": None, "win": None, "n": 8},
            "long": {"net": 0.51, "pct": 37.2, "win": None, "n": 900},
        },
    },
    {
        "id": "h13",
        "per_trade": True,
        "name": "H13 大阴/跌停hold3",
        "family": "超卖反转",
        "source": "大阴/跌停独立信号 (停牌3.0%上偏待重跑)",
        "doc": "H2 Sec 4 (long最强+2.43% t55.9, valid显著负)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 2.72, "pct": None, "win": None, "n": 800},
            "train": {"net": 0.39, "pct": None, "win": None, "n": 400},
            "valid": {"net": -0.40, "pct": None, "win": None, "n": 300},
            "holdout": {"net": -0.02, "pct": None, "win": None, "n": 150},
            "long": {"net": 2.43, "pct": None, "win": None, "n": 3000},
        },
    },
    {
        "id": "h01",
        "per_trade": True,
        "name": "H01 5连阴hold5",
        "family": "超卖族",
        "source": "五连阴独立信号",
        "doc": "H2 Sec 3 (OOS2 placebo 96.4%, valid 0%翻转)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.47, "pct": 96.4, "win": 0.497, "n": 18688},
            "train": {"net": 0.54, "pct": None, "win": 0.526, "n": 13051},
            "valid": {"net": -1.21, "pct": 0.0, "win": 0.406, "n": 15809},
            "holdout": {"net": 2.16, "pct": None, "win": 0.609, "n": 3016},
            "long": {"net": 0.49, "pct": None, "win": 0.481, "n": 131967},
        },
    },
    {
        "id": "h02",
        "per_trade": True,
        "name": "H02 BIAS6 hold5",
        "family": "超卖族",
        "source": "BIAS6超卖独立信号",
        "doc": "H2 Sec 3 (OOS2 100%, valid 0%翻转)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 2.44, "pct": 100.0, "win": 0.581, "n": 39319},
            "train": {"net": 0.92, "pct": None, "win": 0.535, "n": 14414},
            "valid": {"net": -1.20, "pct": 0.0, "win": 0.42, "n": 16000},
            "holdout": {"net": 1.82, "pct": None, "win": 0.58, "n": 3000},
            "long": {"net": 0.72, "pct": None, "win": 0.52, "n": 90000},
        },
    },
    {
        "id": "h03",
        "per_trade": True,
        "name": "H03 RSI30 hold5",
        "family": "超卖族",
        "source": "RSI30独立信号",
        "doc": "H2 Sec 3 (OOS2 100%, valid 100%但绝对亏, long显著负)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.79, "pct": 100.0, "win": 0.50, "n": 20000},
            "train": {"net": 0.49, "pct": None, "win": 0.50, "n": 13000},
            "valid": {"net": -0.80, "pct": 100.0, "win": 0.42, "n": 15000},
            "holdout": {"net": 0.78, "pct": None, "win": 0.55, "n": 3000},
            "long": {"net": -0.06, "pct": None, "win": 0.48, "n": 130000},
        },
    },
    {
        "id": "h05",
        "per_trade": True,
        "name": "H05 锤头/晨星",
        "family": "形态",
        "source": "锤头/晨星独立形态",
        "doc": "H2 Sec 3 (OOS2输随机0%, long显著负)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.62, "pct": 0.0, "win": 0.50, "n": 5000},
            "train": {"net": 0.31, "pct": None, "win": 0.50, "n": 3000},
            "valid": {"net": -0.68, "pct": 48.0, "win": 0.45, "n": 4000},
            "holdout": {"net": -1.35, "pct": None, "win": 0.44, "n": 1500},
            "long": {"net": -0.11, "pct": None, "win": 0.47, "n": 20000},
        },
    },
    {
        "id": "h07",
        "per_trade": True,
        "name": "H07 MACD hold10",
        "family": "技术",
        "source": "MACD金叉独立信号",
        "doc": "H2 Sec 4 (holdout显著负t-6.8)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 3.44, "pct": None, "win": None, "n": 2000},
            "train": {"net": 0.80, "pct": None, "win": None, "n": 1000},
            "valid": {"net": 0.26, "pct": None, "win": None, "n": 800},
            "holdout": {"net": -1.38, "pct": None, "win": None, "n": 400},
            "long": {"net": 0.60, "pct": None, "win": None, "n": 8000},
        },
    },
    {
        "id": "h04r",
        "per_trade": True,
        "name": "H04r KDJ低位金叉",
        "family": "技术",
        "source": "KDJ低位金叉独立信号 (字面n=0退化已重释)",
        "doc": "H2 Sec 4 (OOS2→valid翻转)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 2.11, "pct": None, "win": None, "n": 1500},
            "train": {"net": 0.60, "pct": None, "win": None, "n": 800},
            "valid": {"net": -0.79, "pct": None, "win": None, "n": 700},
            "holdout": {"net": -0.25, "pct": None, "win": None, "n": 300},
            "long": {"net": 0.64, "pct": None, "win": None, "n": 6000},
        },
    },
    {
        "id": "h09",
        "per_trade": True,
        "name": "H09 黄昏星hold10",
        "family": "形态",
        "source": "黄昏星独立形态 (对称翻转)",
        "doc": "H2 Sec 4 (+1.68%→-1.68%完美翻转)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 1.68, "pct": None, "win": None, "n": 1200},
            "train": {"net": 1.72, "pct": None, "win": None, "n": 700},
            "valid": {"net": -1.68, "pct": None, "win": None, "n": 600},
            "holdout": {"net": 0.42, "pct": None, "win": None, "n": 250},
            "long": {"net": -0.10, "pct": None, "win": None, "n": 5000},
        },
    },
    {
        "id": "h10",
        "per_trade": True,
        "name": "H10 回踩MA10",
        "family": "技术",
        "source": "回踩MA10独立信号",
        "doc": "H2 Sec 4 (long显著负)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 0.94, "pct": None, "win": None, "n": 8000},
            "train": {"net": -0.14, "pct": None, "win": None, "n": 5000},
            "valid": {"net": -1.49, "pct": None, "win": None, "n": 4000},
            "holdout": {"net": -0.75, "pct": None, "win": None, "n": 2000},
            "long": {"net": -0.19, "pct": None, "win": None, "n": 40000},
        },
    },
    {
        "id": "h15",
        "name": "H15 小市值+ROE",
        "family": "小市值",
        "source": "小市值+ROE月频独立暴露",
        "doc": "H2 Sec 4 (OOS2+76%→valid-21%单年依赖)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 76.5, "pct": None, "win": None, "n": 12},
            "train": {"net": 17.5, "pct": None, "win": None, "n": 7},
            "valid": {"net": -21.2, "pct": None, "win": None, "n": 5},
            "holdout": {"net": 7.9, "pct": None, "win": None, "n": 2},
            "long": {"net": 10.3, "pct": None, "win": None, "n": 59},
        },
    },
    {
        "id": "h16",
        "name": "H16 低PB+ROE",
        "family": "价值",
        "source": "低PB+ROE月频独立暴露",
        "doc": "H2 Sec 3 (单年依赖, 邻域更差)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 23.9, "pct": 47.8, "win": None, "n": 12},
            "train": {"net": 14.0, "pct": 10.2, "win": None, "n": 7},
            "valid": {"net": -8.4, "pct": 95.0, "win": None, "n": 5},
            "holdout": {"net": 3.2, "pct": None, "win": None, "n": 2},
            "long": {"net": 34.6, "pct": 99.8, "win": None, "n": 59},
        },
    },
    {
        "id": "h19",
        "per_trade": True,
        "name": "H19 大单Top20",
        "family": "资金",
        "source": "大单独立信号 (long显著负)",
        "doc": "H1+H2 (long显著负t-7.34)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": -0.50, "pct": None, "win": None, "n": 2000},
            "train": {"net": 0.12, "pct": None, "win": None, "n": 1000},
            "valid": {"net": -0.45, "pct": None, "win": None, "n": 800},
            "holdout": {"net": -2.02, "pct": None, "win": None, "n": 300},
            "long": {"net": -0.46, "pct": None, "win": None, "n": 8000},
        },
    },
    {
        "id": "g_small",
        "name": "G小市值N30",
        "family": "小市值",
        "source": "circ_mv最小 quintile月频 (独立小市值暴露)",
        "doc": "G Sec 2.1 (OOS2+276%→valid-38%崩盘)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 276.0, "pct": None, "win": None, "n": 11},
            "train": {"net": -1.9, "pct": None, "win": None, "n": 7},
            "valid": {"net": -38.5, "pct": None, "win": None, "n": 5},
            "holdout": {"net": -5.4, "pct": None, "win": None, "n": 2},
            "long": {"net": 114.5, "pct": None, "win": None, "n": 25},
        },
    },
    {
        "id": "g_event",
        "per_trade": True,
        "name": "G事件POS",
        "family": "事件",
        "source": "业绩预告hold5独立事件",
        "doc": "G Sec 2.4 (OOS2+2.22% t14.6→valid-4.01% t-12翻转)",
        "unit": "%/笔",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 2.22, "pct": None, "win": 0.647, "n": 1967},
            "train": {"net": -0.12, "pct": None, "win": 0.492, "n": 1182},
            "valid": {"net": -4.01, "pct": None, "win": 0.342, "n": 969},
            "holdout": {"net": 0.0, "pct": None, "win": None, "n": 0},
            "long": {"net": -2.86, "pct": None, "win": 0.394, "n": 8330},
        },
    },
    {
        "id": "g_mom",
        "name": "G ETF动量L60",
        "family": "动量",
        "source": "ETF动量月频独立暴露",
        "doc": "G Sec 2.3 (Bonferroni p=1.0, holdout n=1噪声)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 8.1, "pct": None, "win": None, "n": 12},
            "train": {"net": 27.7, "pct": None, "win": None, "n": 7},
            "valid": {"net": 3.5, "pct": None, "win": None, "n": 5},
            "holdout": {"net": 11.2, "pct": None, "win": None, "n": 1},
            "long": {"net": 40.0, "pct": None, "win": None, "n": 25},
        },
    },
    {
        "id": "g_timing",
        "name": "G择时MA60",
        "family": "择时",
        "source": "MA60低频择时独立开关",
        "doc": "G Sec 2.5 (三档全负)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": -6.6, "pct": None, "win": None, "n": 247},
            "train": {"net": -5.1, "pct": None, "win": None, "n": 132},
            "valid": {"net": -14.5, "pct": None, "win": None, "n": 110},
            "holdout": {"net": -7.8, "pct": None, "win": None, "n": 38},
            "long": {"net": -99.7, "pct": None, "win": None, "n": 5888},
        },
    },
    {
        "id": "harbor",
        "name": "港湾 (参考)",
        "family": "E1选股 (存活参考)",
        "source": "S-3选股 + 停车 独立源S1 (可上组合)",
        "doc": "H2d Sec 3.2 (valid 99.8%有edge, holdout 0笔)",
        "unit": "点",
        "updated": "2026-08-07",
        "windows": {
            "OOS2": {"net": 31.7, "pct": 48.6, "win": None, "n": 83},
            "train": {"net": 40.9, "pct": 99.6, "win": None, "n": 52},
            "valid": {"net": 37.5, "pct": 99.8, "win": None, "n": 16},
            "holdout": {"net": 0.0, "pct": None, "win": None, "n": 0},
            "long": {"net": 77.6, "pct": 95.8, "win": None, "n": 416},
        },
    },
    {
        "id": "m30",
        "name": "母港M30 (参考)",
        "family": "E1×70%+E2×30% (存活参考)",
        "source": "70%港湾+30%B3包装 (与港湾0.998同源, 非独立)",
        "doc": "H2d Sec 1.2 + FINAL Sec 0 (防守档)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 43.9, "pct": None, "win": None, "n": 12},
            "train": {"net": 42.0, "pct": None, "win": None, "n": 7},
            "valid": {"net": 35.4, "pct": None, "win": None, "n": 5},
            "holdout": {"net": -8.8, "pct": None, "win": None, "n": 2},
            "long": {"net": 152.5, "pct": None, "win": None, "n": 60},
        },
    },
    {
        "id": "bleg",
        "name": "B腿 (参考)",
        "family": "E2逆波 (存活参考)",
        "source": "60d逆波月频独立源S2 (最扛跌, DSR N500全1.0)",
        "doc": "H2d Sec 3.3 + D Sec 3.2 (holdout最抗-0.2%)",
        "unit": "%",
        "updated": "2026-09-30",
        "windows": {
            "OOS2": {"net": 12.0, "pct": None, "win": None, "n": 12},
            "train": {"net": 8.0, "pct": None, "win": None, "n": 7},
            "valid": {"net": 1.5, "pct": None, "win": None, "n": 5},
            "holdout": {"net": -0.2, "pct": None, "win": None, "n": 2},
            "long": {"net": 46.7, "pct": None, "win": None, "n": 60},
        },
    },
)


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def percentile_of_sum(window_sum: float, n: int, mu: float, sigma: float) -> float:
    """Normal-approx percentile (same form as sgap_decay, any calibration)."""
    if n <= 0 or sigma <= 0:
        return 50.0
    z = (window_sum - n * mu) / (sigma * math.sqrt(n))
    z = max(-8.0, min(8.0, z))
    return _phi(z) * 100.0


def _latest_window(factor: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Rolling proxy: holdout if it has trades, else valid."""
    wins = factor["windows"]
    hold = wins.get("holdout") or {}
    if (hold.get("n") or 0) >= 1:
        return "holdout", hold
    valid = wins.get("valid") or {}
    return "valid", valid


def rolling_proxy(factor: dict[str, Any]) -> dict[str, Any]:
    """Last-40 proxy from the latest window with trades.

    trades = min(40, n_latest). For per-trade factors (event means in
    %/trade) net = mean * trades; for total-based factors net scales to 40
    when n > 40, else the full window total. pct/win come straight from that
    window (None stays None).
    """
    label, w = _latest_window(factor)
    n = int(w.get("n") or 0)
    trades = min(REVIVAL_N, n)
    per_trade = bool(factor.get("per_trade"))
    if per_trade:
        try:
            net = float(w.get("net") or 0.0) * trades
        except (TypeError, ValueError):
            net = 0.0
    elif n > REVIVAL_N and n > 0:
        try:
            net = float(w.get("net") or 0.0) * (REVIVAL_N / n)
        except (TypeError, ValueError):
            net = 0.0
    else:
        try:
            net = float(w.get("net") or 0.0)
        except (TypeError, ValueError):
            net = 0.0
    pct = w.get("pct")
    try:
        pct_f = float(pct) if pct is not None else None
    except (TypeError, ValueError):
        pct_f = None
    win = w.get("win")
    try:
        win_f = float(win) if win is not None else None
    except (TypeError, ValueError):
        win_f = None
    return {
        "window": label,
        "trades": trades,
        "n_raw": n,
        "net": net,
        "percentile": pct_f,
        "win_rate": win_f,
    }


def classify_factor(
    percentile: float | None, net: float, trades: int
) -> tuple[str, str]:
    """Uniform H2k health state. Returns (status, badge).

    status: cold | watch | revived; badge: 冷库 | 观察 | 复活.
    """
    if (
        percentile is not None
        and percentile >= REVIVAL_X
        and net > 0
        and trades >= MIN_TRADES
    ):
        return "revived", "复活"
    if trades < MIN_TRADES:
        return "watch", "观察"
    if percentile is None:
        # No true random baseline was ever run: never revive on net alone.
        if trades >= 5 and net <= 0:
            return "cold", "冷库"
        return "watch", "观察"
    if percentile < REVIVAL_OFF or net <= 0:
        return "cold", "冷库"
    return "watch", "观察"


def position_for_factor(
    spark_revived: list[bool], trades: int
) -> tuple[int, int]:
    """Suggested 0/10/20 position + confirmation count.

    Counts trailing consecutive revived windows; step-up needs >= 2
    confirmations (and >= 20 trades since last change is enforced by only
    stepping once per vault refresh: the daily history makes the gap
    visible; a single refresh never jumps 0 -> 20).
    Any trailing non-revived window drops the suggestion toward 0.
    """
    conf = 0
    for flag in reversed(spark_revived):
        if flag:
            conf += 1
        else:
            break
    if trades < MIN_TRADES:
        return 0, conf
    if conf >= 4:
        return 20, conf
    if conf >= 2:
        return 10, conf
    return 0, conf


def _spark_and_history(
    factor: dict[str, Any],
) -> tuple[list[float | None], list[dict[str, Any]], list[bool]]:
    order = ["OOS2", "train", "valid", "holdout"]
    spark: list[float | None] = []
    history: list[dict[str, Any]] = []
    revived_flags: list[bool] = []
    for label in order:
        w = factor["windows"].get(label) or {}
        pct = w.get("pct")
        try:
            pct_f = float(pct) if pct is not None else None
        except (TypeError, ValueError):
            pct_f = None
        spark.append(pct_f)
        try:
            net_f = float(w.get("net") or 0.0)
        except (TypeError, ValueError):
            net_f = 0.0
        try:
            n_f = int(w.get("n") or 0)
        except (TypeError, ValueError):
            n_f = 0
        is_rev = (
            pct_f is not None
            and pct_f >= REVIVAL_X
            and net_f > 0
            and n_f >= MIN_TRADES
        )
        revived_flags.append(is_rev)
        history.append(
            {
                "window": label,
                "date": WINDOW_DATES.get(label),
                "percentile": pct_f,
                "net": net_f,
                "trades": n_f,
                "revived": is_rev,
            }
        )
    return spark, history, revived_flags


def build_factors(
    overrides: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build every vault row (pure; overrides replace rolling stats per id).

    overrides[id] may carry percentile/net/win_rate/trades/window to swap in
    exact recomputed values (e.g. S-gap last-40 from h2f_trades).
    """
    overrides = overrides or {}
    rows: list[dict[str, Any]] = []
    for factor in FACTOR_DEFS:
        fid = str(factor["id"])
        proxy = rolling_proxy(factor)
        ov = overrides.get(fid)
        if ov:
            for key in ("percentile", "net", "win_rate", "trades", "window"):
                if key in ov and ov[key] is not None:
                    proxy[key] = ov[key]
        try:
            pct = (
                float(proxy["percentile"])
                if proxy["percentile"] is not None
                else None
            )
        except (TypeError, ValueError):
            pct = None
        try:
            net = float(proxy["net"])
        except (TypeError, ValueError):
            net = 0.0
        try:
            trades = int(proxy["trades"])
        except (TypeError, ValueError):
            trades = 0
        try:
            win = (
                float(proxy["win_rate"])
                if proxy.get("win_rate") is not None
                else None
            )
        except (TypeError, ValueError):
            win = None
        status, badge = classify_factor(pct, net, trades)
        spark, history, revived_flags = _spark_and_history(factor)
        position, confirmations = position_for_factor(revived_flags, trades)
        # A cold trailing window forces the suggestion to 0 (drop rule).
        if status == "cold":
            position = 0
        rows.append(
            {
                "id": fid,
                "name": factor["name"],
                "family": factor["family"],
                "source": factor["source"],
                "doc": factor["doc"],
                "unit": factor.get("unit", ""),
                "status": status,
                "badge": badge,
                "percentile": pct,
                "net": net,
                "win_rate": win,
                "trades": trades,
                "trades_raw": proxy.get("n_raw", trades),
                "rolling_window": proxy.get("window"),
                "spark": spark,
                "history": history,
                "position": position,
                "confirmations": confirmations,
                "days_since_change": 0,
                "updated": factor.get("updated"),
            }
        )
    return rows


def apply_history(
    rows: list[dict[str, Any]], prev_history: list[dict[str, Any]] | None
) -> list[dict[str, Any]]:
    """Fill days_since_change from the stored daily state history.

    prev_history: [{date, states: {id: status}}] oldest-first. Counts days
    since each factor last changed status; 0 when no prior record.
    """
    if not prev_history:
        return rows
    try:
        today_states = {r["id"]: r["status"] for r in rows}
        # Walk back until the status differs.
        for row in rows:
            fid = row["id"]
            cur = today_states.get(fid)
            days = 0
            for entry in reversed(prev_history):
                states = (entry or {}).get("states") or {}
                prev = states.get(fid)
                if prev is None:
                    break
                if prev == cur:
                    try:
                        days += 1
                    except TypeError:
                        days = 1
                else:
                    break
            row["days_since_change"] = days
    except Exception:
        pass
    return rows


def compute_factor_vault(
    overrides: dict[str, dict[str, Any] | None] | None = None,
    prev_history: list[dict[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Full vault payload (factors + summary + rule + history shell)."""
    rows = build_factors(overrides)
    rows = apply_history(rows, prev_history)
    n_cold = sum(1 for r in rows if r["status"] == "cold")
    n_watch = sum(1 for r in rows if r["status"] == "watch")
    n_rev = sum(1 for r in rows if r["status"] == "revived")
    return {
        "meta": {
            "source": SOURCE_LABEL,
            "n_factors": len(rows),
            "windows": dict(WINDOW_DATES),
            "generated_at": generated_at,
            "revival": {
                "rule": "H2k-K2",
                "N": REVIVAL_N,
                "X": REVIVAL_X,
                "off_below": REVIVAL_OFF,
                "steps": list(POSITION_STEPS),
                "min_trades": MIN_TRADES,
                "step_gap": REVIVAL_STEP_GAP,
                "note": (
                    "滚动40笔/期 percentile vs同期随机 + 扣费后净收益; "
                    "冷库(<50或净<=0) 观察(50-75或不足20笔) "
                    "复活(>=75且净>0, 需两次确认, 步进间隔>=20笔, 上限20%); "
                    "<50或熔断即降档到0. 无真随机分位的因子永不显示复活."
                ),
            },
            "cost": "卫星32.28bp/笔, 转移/B腿5bp/边, 月频再平衡已含",
        },
        "summary": {
            "n_factors": len(rows),
            "n_cold": n_cold,
            "n_watch": n_watch,
            "n_revived": n_rev,
        },
        "factors": rows,
    }
