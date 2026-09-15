"""Frozen strategy-family catalog for the Backtest page overview (display layer).

Single source for the 2026-09-14 clean caliber numbers (costs included) shown in
the UI. Every row cites its authoritative doc; numbers must be updated here when
a new audit re-freezes them (see the audit doc's §0 comparison table).

Live stays 港湾 — this catalog is read-only display data, never order wiring.
"""

from __future__ import annotations

from typing import Any

UPDATED = "2026-09-15"

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
        "pros": [
            "Live 基线（tag harbor-p1-20260913）：四窗 +55.2/+52.2/+50.3/+201.5（SR 1.73/3.01/2.14/1.00）；闲置现金全量停 ETF（long 增量 +118.8pt）",
            "2024–2026 连续大年（+29.9/+51.6/+47.0）；月度胜率 56%、中位 +1.2%；决策单源对账 100%（473/473）",
            "指数上行/动量 up 时强：日均 +0.173/+0.168%（年化 +41~42%）；低/高波动区 +0.185/+0.168%/日",
        ],
        "cons": [
            "行情依赖：2021–2023 三年几乎白干（−5.1/+8.4/+1.2）；指数 ≤MA200 时日均 +0.045%（年化 +11%）",
            "中波动区为负（日均 −0.033%）；long MDD −22.8%、SR 仅 1.00",
            "停车场把 valid MDD 从 −6.1 拉到 −21.8（空仓期满仓 ETF + trail 出场无冷却再进）",
        ],
        "regime": {
            "fit": [
                "指数 >MA200（日均 +0.173% ≈ 年化 +42%）",
                "指数 20 日动量 up（+0.168%/日）",
                "低/高波动区（+0.185/+0.168%/日）",
            ],
            "unfit": [
                "指数 <MA200（+0.045%/日 ≈ 年化 +11%）",
                "20 日动量 down（+0.054%/日）",
                "中波动区为负（−0.033%/日）",
                "2021–2023 低收益空窗（−5.1/+8.4/+1.2）",
            ],
            "evidence": [
                {"label": "2021", "value": "−5.1（MDD 15.2 / SR −1.64）"},
                {"label": "2022", "value": "+8.4（15.9 / 0.58）"},
                {"label": "2023", "value": "+1.2（16.3 / 0.16）"},
                {"label": "2024", "value": "+29.9（22.4 / 1.03）"},
                {"label": "2025", "value": "+51.6（10.9 / 1.90）"},
                {"label": "2026(至8月)", "value": "+47.0（20.8 / 1.81）"},
                {"label": "月度", "value": "胜率 34/61 = 56% · 中位 +1.2% · 最差 2024-08 −11.1% / 2021-10 −7.9%"},
                {"label": "市况", "value": "MA200 上/下 +0.173/+0.045%/日 · 动量 up/down +0.168/+0.054% · 波动 高/中/低 +0.168/−0.033/+0.185%"},
            ],
            "note": "只描述、不作闸门；long 窗 2021-08~2026-08 连续回放。",
        },
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
            "港湾降波版（月初再平衡 5bps/边）：年度 MDD 6–12%（港湾 15–22%）、四窗 SR 全升、2022–23 压测 + 20bp 成本全过",
            "年度近全正（−1.0/+3.3/+3.9/+24.4/+30.6/+25.2）；月度胜率 61%、最差月 −5.8%（2024-08）",
            "高/低波动区均 +0.11%/日（年化 +26~27%）",
        ],
        "cons": [
            "收益近半（long +116.3 vs 港湾 +201.5）——买保险的产品选择",
            "同样行情依赖：指数 ≤MA200 +0.039%/日、动量 down +0.043%/日；中波动区 ≈ 0（−0.014%/日）",
            "分散红利 regime 依赖（2021–26 股弱金债强）；2021 年 −1.0",
        ],
        "regime": {
            "fit": [
                "指数 >MA200（+0.107%/日 ≈ 年化 +26%）· 动量 up（+0.098%/日）",
                "高/低波动区（+0.111/+0.107%/日）",
                "2022–2023 弱市仍正（+3.3/+3.9）",
            ],
            "unfit": [
                "指数 <MA200（+0.039%/日 ≈ 年化 +9%）· 动量 down（+0.043%/日）",
                "中波动区 ≈ 0（−0.014%/日）",
                "2021（−1.0）",
            ],
            "evidence": [
                {"label": "2021", "value": "−1.0（MDD 9.0 / SR −1.58）"},
                {"label": "2022", "value": "+3.3（8.3 / 0.40）"},
                {"label": "2023", "value": "+3.9（7.9 / 0.42）"},
                {"label": "2024", "value": "+24.4（11.6 / 1.40）"},
                {"label": "2025", "value": "+30.6（6.1 / 2.10）"},
                {"label": "2026(至8月)", "value": "+25.2（11.2 / 1.86）"},
                {"label": "月度", "value": "胜率 37/61 = 61% · 中位 +0.6% · 最差 2024-08 −5.8% / 2022-01 −3.9%"},
                {"label": "市况", "value": "MA200 上/下 +0.107/+0.039%/日 · 动量 up/down +0.098/+0.043% · 波动 高/中/低 +0.111/−0.014/+0.107%"},
            ],
            "note": "只描述、不作闸门；long 窗 2021-08~2026-08 连续回放。",
        },
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
            "全候性最好：年度全正（+17.3/+14.7/+7.4/+30.9/+40.2/+12.3）、月度胜率 69%、最差月仅 −3.8%、各市况桶全正",
            "换基座吸收 2/3 valid 拖累（−42.4×w → −14.6×w）；四窗 +89.4/+40.7/+20.7/+198.1（SR 4.45/4.54/1.93/1.84）",
            "指数强/弱、动量 up/down 均正（+0.108/+0.066、+0.110/+0.066%/日）",
        ],
        "cons": [
            "收益上限受母港约束（long +198.1 vs 星舰 v2 +884.9）",
            "valid 相对母港 −4.9pt 踩线（稳健区间 0.15–0.25）；依赖卫星执行（审计已过：90bps +310%、容量 ≤5M）",
            "卫星弱年拖累：2026 仅 +12.3（卫星 v1 +5.8）；卫星抽搐期最差月 2026-07 −3.8%",
        ],
        "regime": {
            "fit": [
                "几乎全档为正：高波 +0.124%/日 · 低波 +0.116% · MA200 上/下 +0.108/+0.066%",
                "指数动量 up/down 均正（+0.110/+0.066%/日）",
                "2021–2023 弱势年仍正（+17.3/+14.7/+7.4）",
            ],
            "unfit": [
                "无全负档；最弱 = 中波动（+0.020%/日 ≈ 年化 +5%）",
                "2026 卫星弱年（+12.3）· 卫星抽搐期（最差月 2026-07 −3.8%）",
            ],
            "evidence": [
                {"label": "2021", "value": "+17.3（MDD 7.6 / SR −1.17）"},
                {"label": "2022", "value": "+14.7（6.7 / 1.64）"},
                {"label": "2023", "value": "+7.4（7.4 / 0.81）"},
                {"label": "2024", "value": "+30.9（6.9 / 2.07）"},
                {"label": "2025", "value": "+40.2（4.3 / 3.14）"},
                {"label": "2026(至8月)", "value": "+12.3（11.2 / 1.11）"},
                {"label": "月度", "value": "胜率 42/61 = 69% · 中位 +1.2% · 最差 2026-07 −3.8% / 2021-10 −3.5%"},
                {"label": "市况", "value": "MA200 上/下 +0.108/+0.066%/日 · 动量 up/down +0.110/+0.066% · 波动 高/中/低 +0.124/+0.020/+0.116%"},
            ],
            "note": "只描述、不作闸门；long 窗 2021-08~2026-08 连续回放。",
        },
    },
    {
        "key": "starship",
        "name": "星舰",
        "structure": (
            "v2（2026-09-15 用户拍板）= 习惯 S-gap 卫星（14:30 买/第 3 日 14:30 卖、amp_1430、"
            "gate_1430、4×25% 槽、30bps）+ 闲置现金因果停放 ETF 停车场（mom60+MA200 argmax、"
            "trail8、5bps/边、真实现金权重 T−1）"
        ),
        "status": "aggressive_pending",
        "statusLabel": "激进 · 前置未满",
        "timelineStrategy": "starship",
        "doc": "docs/backtests/stable/sat-idle-parking-2026-09-15.md",
        "tag": "starship",
        "windows": {
            "OOS2": {"total": 235.8, "cagr": 253.0, "mdd": -9.2, "sharpe": 5.27},
            "train": {"total": 67.7, "cagr": 190.8, "mdd": -9.9, "sharpe": 4.29},
            "valid": {"total": 11.5, "cagr": 28.7, "mdd": -28.0, "sharpe": 0.77},
            "long": {"total": 884.9, "cagr": 60.7, "mdd": -28.5, "sharpe": 2.09},
        },
        "pros": [
            "long +884.9%（15bps 3× 成本仍 +829.5/MDD −28.6）· OOS2/train 双升（+23.1/+27.4）；平均停车 84.1%，空闲现金不再空转",
            "停放收益可归因：黄金 +27.8%/yr（493 天）· 原油 +32.8%（69 天）；卫星腿执行审计 ✅（1036/1036 落日线区间、90bps +310%/SR 2.45、容量 ≤5M）",
            "各市况日均全正：高波动 +0.196%/日（年化 +48%）、指数 ≤MA200 +0.182%/日——卫星停手期停车腿吃金/债趋势",
        ],
        "cons": [
            "回撤/Sharpe 显式上移：MDD −8.4→−28.5、SR 3.50→2.09、valid −3.2（house K2 不过，用户显式风险预算覆盖）",
            "尾部 = 停车资产抽搐：2026-04 −8.8% / 06 −6.9% / 07 −5.6%（最差 8 月占 4）；2026 年内 MDD −28.5 全来自停车腿（卫星 +0.7pt / 停车 −34.6pt）",
            "regime 依赖：套筒平年（2021–22）增量仅 +1.4/+0.3pt；低波动区增量薄（+0.033%/日）；进 Live 前置未满（paper 3/20 + 授权）",
        ],
        "regime": {
            "fit": [
                "高波动 + 指数弱于 MA200（v2 日均 +0.196% ≈ 年化 +48%、Δvs v1 +0.085%/日）",
                "停车资产趋势年（2023/2025/2026：套筒 +20.3/+24.0/+18.0%，主体黄金 +27.8%/yr）",
                "指数动量 down（Δvs v1 +0.076%/日）",
            ],
            "unfit": [
                "停车资产单日 ±10% 抽搐（2026-04/06/07 = 最差月 −8.8/−6.9/−5.6%，年内 MDD −28.5 全来自停车腿）",
                "套筒平年（2021-08~2022：增量 +1.4/+0.3pt，v2≈v1 但多付 5bps/边）",
                "低波动 + 指数强势（v2 日均 +0.153% ≈ 年化 +37%，增量最薄）",
            ],
            "evidence": [
                {"label": "2021(8–12)", "value": "v1 +68.5 / v2 +70.7 / Δ+2.2（MDD −5.6）"},
                {"label": "2022", "value": "v1 +43.8 / v2 +43.7 / Δ−0.1（−6.9）"},
                {"label": "2023", "value": "v1 +15.3 / v2 +32.9 / Δ+17.6（−7.5）"},
                {"label": "2024", "value": "v1 +43.7 / v2 +52.5 / Δ+8.8（−14.8）"},
                {"label": "2025", "value": "v1 +32.6 / v2 +66.0 / Δ+33.4（−11.2）"},
                {"label": "2026(至8月)", "value": "v1 +5.8 / v2 +19.3 / Δ+13.5（−28.5）"},
                {"label": "月度", "value": "胜率 44/61 = 72% · 中位 +2.8% · 最差 2026-04 −8.8% / 2026-06 −6.9%"},
                {"label": "市况", "value": "MA200 上/下 +0.174/+0.182%/日 · 动量 up/down +0.184/+0.176% · 波动 高/中/低 +0.196/+0.192/+0.153%"},
                {"label": "套筒按持有", "value": "黄金 +27.8%/yr · 原油 +32.8%（n=69）· 纳指 ~+6% · 国债/回购 ≤0"},
            ],
            "note": "只描述、不作闸门；long 窗 2021-08~2026-08 连续回放。",
        },
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
        "pros": [
            "牛年弹性最大（2021 +22.3 / 2022 +24.5 / 2025 +66.8）；卫星低相关（long 日相关 0.09–0.10）是放大来源",
            "月度胜率 69%；高/低波动区统一 +0.161%/日（年化 +39%）",
            "四窗 +149.7/+52.2/+29.1/+291.9（OOS2 SR 4.54）",
        ],
        "cons": [
            "卫星弱年拖累：2023 +5.8 / 2026 +7.7；最差月 −7.9%（2021-10）/ 2026-07 −7.2%",
            "valid 相对港湾核心 Δ−21.2 / Δsr −0.65（未过 K1/K2，clean 口径）；两腿叠加港湾任何权重无解（H-SAT-W 死区）——保留为并行对照档，不进 Live",
            "中波动区 ≈ 0（+0.012%/日）",
        ],
        "regime": {
            "fit": [
                "指数强/弱均正（+0.134/+0.086%/日）",
                "动量 up/down 均正（+0.144/+0.084%/日）",
                "高/低波动区统一 +0.161%/日（年化 +39%）",
            ],
            "unfit": [
                "中波动区 ≈ 0（+0.012%/日 ≈ 年化 +3%）",
                "卫星弱年（2023 +5.8、2026 +7.7）",
                "卫星抽搐期（2026-06 −6.9% / 2026-07 −7.2% 最差月）",
            ],
            "evidence": [
                {"label": "2021", "value": "+22.3（MDD 12.7 / SR −1.11）"},
                {"label": "2022", "value": "+24.5（9.4 / 1.73）"},
                {"label": "2023", "value": "+5.8（12.9 / 0.44）"},
                {"label": "2024", "value": "+35.4（13.0 / 1.54）"},
                {"label": "2025", "value": "+66.8（8.1 / 3.02）"},
                {"label": "2026(至8月)", "value": "+7.7（20.8 / 0.54）"},
                {"label": "月度", "value": "胜率 42/61 = 69% · 中位 +2.0% · 最差 2021-10 −7.9% / 2026-07 −7.2%"},
                {"label": "市况", "value": "MA200 上/下 +0.134/+0.086%/日 · 动量 up/down +0.144/+0.084% · 波动 高/中/低 +0.161/+0.012/+0.161%"},
            ],
            "note": "只描述、不作闸门；long 窗 2021-08~2026-08 连续回放。",
        },
    },
]


def strategy_catalog() -> list[dict[str, Any]]:
    """Return the frozen catalog (fresh copies; callers may annotate)."""
    out: list[dict[str, Any]] = []
    for row in _STRATEGIES:
        fresh = {
            **row,
            "windows": {k: dict(v) for k, v in row["windows"].items()},
            "pros": list(row["pros"]),
            "cons": list(row["cons"]),
            "updated": UPDATED,
        }
        reg = row.get("regime")
        if reg:
            fresh["regime"] = {
                **reg,
                "fit": list(reg["fit"]),
                "unfit": list(reg["unfit"]),
                "evidence": [dict(e) for e in reg["evidence"]],
            }
        out.append(fresh)
    return out
