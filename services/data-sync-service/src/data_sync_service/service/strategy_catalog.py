"""Frozen strategy-family catalog for the Backtest page overview (display layer).

Single source for the 2026-09-14 clean caliber numbers (costs included) shown in
the UI. Every row cites its authoritative doc; numbers must be updated here when
a new audit re-freezes them (see the audit doc's §0 comparison table).

Live stays 港湾 — this catalog is read-only display data, never order wiring.
"""

from __future__ import annotations

from typing import Any

UPDATED = "2026-09-14"

# Windows: total% (含成本) / CAGR% / maxDD% / Sharpe. Frozen 2026-09-14 clean.
_STRATEGIES: list[dict[str, Any]] = [
    {
        "key": "harbor",
        "name": "港湾",
        "structure": "S-3 择强核心 + 闲置现金 ETF 停车场（mom60+MA200 argmax、因果 trail8）",
        "status": "live",
        "statusLabel": "Live",
        "timelineStrategy": "harbor",
        "doc": "docs/backtests/stable/etf-parking-baseline-2026-09-13.md",
        "tag": "harbor-p1-20260913",
        "windows": {
            "OOS2": {"total": 55.2, "cagr": 58.0, "mdd": -14.3, "sharpe": 1.73},
            "train": {"total": 52.2, "cagr": 138.2, "mdd": -8.0, "sharpe": 3.01},
            "valid": {"total": 50.3, "cagr": 156.6, "mdd": -21.8, "sharpe": 2.14},
            "long": {"total": 201.5, "cagr": 25.7, "mdd": -22.8, "sharpe": 1.00},
        },
        "pros": ["四窗全正、绝对收益最强（long +201.5%）", "Live=回测决策单源已对账 100%（473/473）"],
        "cons": ["long Sharpe 仅 1.00、回撤深（−22.8%）", "无分散器/风险预算层，闲置资金只停 ETF"],
    },
    {
        "key": "homeport",
        "name": "母港",
        "structure": "港湾 × B3 风险预算 50/50（300/500/黄金/纳指/国债，60d 逆波动率，月初再平衡 5bp/边）",
        "status": "product_candidate",
        "statusLabel": "产品候选",
        "timelineStrategy": "homeport",
        "doc": "docs/backtests/stable/harbor-riskbudget-2026-09-13.md",
        "tag": "h-mix-20260913",
        "windows": {
            "OOS2": {"total": 36.5, "cagr": 38.3, "mdd": -9.0, "sharpe": 1.90},
            "train": {"total": 35.0, "cagr": 85.9, "mdd": -5.0, "sharpe": 3.52},
            "valid": {"total": 25.6, "cagr": 69.4, "mdd": -11.4, "sharpe": 2.26},
            "long": {"total": 116.3, "cagr": 17.4, "mdd": -11.6, "sharpe": 1.18},
        },
        "pros": [
            "四窗 Sharpe/MDD 全面优于港湾（回撤近腰斩）",
            "过 2022–23 压测 + 20bp 成本敏感（K1–K5 全过）",
        ],
        "cons": ["收益近半（long +116.3 vs +201.5）——买保险的产品选择", "分散红利 regime 依赖（2021–26 股弱金债强）"],
    },
    {
        "key": "starport",
        "name": "星港",
        "structure": "母港 × 卫星 1/3 曝露（有仓日 core + w×(sat−core)，H-B3-SAT chosen=1/3）",
        "status": "product_candidate_increment",
        "statusLabel": "产品候选增量",
        "timelineStrategy": "starport",
        "doc": "docs/backtests/stable/harbor-b3-sat-2026-09-14.md",
        "tag": "h-b3-sat-20260914",
        "windows": {
            "OOS2": {"total": 89.4, "cagr": 94.5, "mdd": -5.0, "sharpe": 4.45},
            "train": {"total": 40.7, "cagr": 102.3, "mdd": -3.9, "sharpe": 4.54},
            "valid": {"total": 20.7, "cagr": 54.6, "mdd": -11.4, "sharpe": 1.93},
            "long": {"total": 198.1, "cagr": 25.4, "mdd": -11.2, "sharpe": 1.84},
        },
        "pros": [
            "OOS2/long 收益与 Sharpe 大幅提升且 MDD 更低（long −11.2%）",
            "换基座吸收 2/3 valid 拖累（斜率 −42.4×w → −14.6×w），曝露封在风险预算内",
        ],
        "cons": ["valid 窗相对母港 −4.9pt、踩线（K1 余量 0.1pt；稳健区间 0.15–0.25）", "依赖卫星执行（审计已过：90bps +310%/SR 2.45、容量 ≤5M）；holdout 描述弱"],
    },
    {
        "key": "starship",
        "name": "星舰",
        "structure": "习惯 S-gap 卫星 standalone（14:30 买/第 3 日 14:30 卖、amp_1430、gate_1430、4×25% 槽、30bps）",
        "status": "aggressive_pending",
        "statusLabel": "激进 · 前置未满",
        "timelineStrategy": "starship",
        "doc": "docs/backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md",
        "tag": "starship",
        "windows": {
            "OOS2": {"total": 212.7, "cagr": 227.8, "mdd": -3.3, "sharpe": 7.44},
            "train": {"total": 40.3, "cagr": 101.1, "mdd": -8.1, "sharpe": 3.50},
            "valid": {"total": 14.7, "cagr": 37.3, "mdd": -6.5, "sharpe": 2.06},
            "long": {"total": 463.6, "cagr": 43.1, "mdd": -8.4, "sharpe": 3.50},
        },
        "pros": [
            "long +463.6%/SR 3.50/MDD −8.4%，年度全正、与核心低相关",
            "执行审计 ✅（2026-09-14）：90bps 仍 +310%/SR 2.45、贴板 0、容量 ≤5M",
        ],
        "cons": [
            "进 Live 前置未满：paper 3/20 + 用户风险授权（执行审计已过）",
            "多重检验 + regime 依赖（2026 仅 +5.8%、valid +14.7%）——数字当上限看",
        ],
    },
    {
        "key": "twin_star",
        "name": "双子星",
        "structure": "港湾核心 × 卫星 50/50（无仓日 100% 核心；并行对照档，保留不退役）",
        "status": "parallel_candidate",
        "statusLabel": "并行对照",
        "timelineStrategy": "twin_star",
        "doc": "docs/backtests/stable/twin-star-parking-refit-2026-09-13.md",
        "tag": "b12",
        "windows": {
            "OOS2": {"total": 149.7, "cagr": 159.3, "mdd": -9.2, "sharpe": 4.54},
            "train": {"total": 52.2, "cagr": 138.0, "mdd": -6.8, "sharpe": 3.98},
            "valid": {"total": 29.1, "cagr": 80.5, "mdd": -21.8, "sharpe": 1.49},
            "long": {"total": 291.9, "cagr": 32.8, "mdd": -20.8, "sharpe": 1.45},
        },
        "pros": ["（历史）OOS2 +149.7/long +291.9，低相关卫星叠加显著"],
        "cons": [
            "valid 窗相对港湾核心 Δ−21.2 / Δsr−0.65（未过 K1/K2，clean 口径）",
            "两腿叠加港湾任何权重都无解（H-SAT-W 死区）——三腿版为星港",
        ],
    },
]


def strategy_catalog() -> list[dict[str, Any]]:
    """Return the frozen catalog (fresh copies; callers may annotate)."""
    return [
        {**row, "windows": {k: dict(v) for k, v in row["windows"].items()},
         "pros": list(row["pros"]), "cons": list(row["cons"]), "updated": UPDATED}
        for row in _STRATEGIES
    ]
