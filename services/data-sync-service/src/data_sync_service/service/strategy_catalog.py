"""Frozen strategy-family catalog for the Backtest page overview (display layer).

Single source for the clean caliber numbers (costs included) shown in the UI.
Every row cites its authoritative doc; numbers must be updated here when a new
audit re-freezes them (see the audit doc's §0 comparison table).

Text style (2026-09-15): human-facing copy is plain language (大白话) — what it
does, what it earned, when it works. Jargon stays in the真值 docs.

Live stays 港湾 — this catalog is read-only display data, never order wiring.

Three-tier lineup (2026-09-16): 星舰 offense / 星港 balanced / 母港M30 defense,
plus 港湾 sunset baseline (Live keeps running; no new development).
Order below is the display order everywhere (catalog panel, tabs, mode bar).
"""

from __future__ import annotations

from typing import Any

UPDATED = "2026-09-17"

# Windows: total% (含成本) / CAGR% / maxDD% / Sharpe. Frozen 2026-09-14 clean.
_STRATEGIES: list[dict[str, Any]] = [
    {
        "key": "starship",
        "name": "星舰",
        "structure": (
            "v2（2026-09-15 定稿，2026-09-16 起闲置按 H2 迟滞套筒停放）：卫星打法——"
            "每天下午 2:30 挑“跳空高开 + 波动小”的股票，最多 4 只、每只 25% 的钱，"
            "持有 3 天后卖出；大盘太弱时自动停手。没出手时的现金拿去买趋势最好的"
            "一个 ETF，换仓需 2pt 动量领先（过滤噪音换仓）。"
        ),
        "status": "aggressive_pending",
        "statusLabel": "激进 · 前置未满",
        "role": "offense",
        "roleLabel": "进攻",
        "timelineStrategy": "starship",
        "doc": "docs/backtests/stable/sleeve-tune-2026-09-16.md",
        "tag": "starship-h2-20260916",
        "windows": {
            "OOS2": {"total": 242.7, "cagr": 260.5, "mdd": -8.6, "sharpe": 5.08},
            "train": {"total": 76.8, "cagr": 224.6, "mdd": -9.6, "sharpe": 4.55},
            "valid": {"total": -0.3, "cagr": -0.6, "mdd": -35.9, "sharpe": 0.23},
            "long": {"total": 975.0, "cagr": 63.6, "mdd": -30.5, "sharpe": 2.15},
        },
        "pros": [
            "长期总收益 +975%（15bps 下仍 +921%；旧停车口径 +925%）",
            "换仓降噪有增量：train +76.8（比旧停车 +10.6）、long +975（+50.1）",
            "卫星本身执行审计已过：1036 笔成交全部落在当天最高最低价区间内；500 万以内好成交",
            "六个年份全部正收益（+74%/+34%/+33%/+61%/+77%/+22%）",
        ],
        "cons": [
            "回撤仍大：长期从高点最多跌 30.5%；valid 窗内有 −35.9% 的 episode",
            "2022 是迟滞弱年（停车腿 −6.9% vs 旧口径 −0.1%）；valid 与旧口径持平（−0.3%）",
            "单月最差 −8.7%；要进实盘还差两步：paper 先跑满 20 笔平仓 + 你正式授权",
        ],
        "regime": {
            "fit": [
                "六个年份全部正收益：2021 +74%、2022 +34%、2023 +33%、2024 +61%、2025 +77%、2026 +22%",
                "约 74% 的月份赚钱（61 个月里 45 个月赚钱）；中位 +3.5%",
                "弱市 + 波动大时卫星脉冲最肥（OOS2 +243%）",
            ],
            "unfit": [
                "单月最差 −8.7%；valid 窗内 −35.9% 的 episode",
                "趋势窗卫星挨饿时全靠停车腿（valid 卫星有仓仅 27%）",
            ],
            "evidence": [
                {"label": "2021 年（8–12月）", "value": "+74.1%"},
                {"label": "2022 年", "value": "+34.3%"},
                {"label": "2023 年", "value": "+33.1%"},
                {"label": "2024 年", "value": "+60.6%"},
                {"label": "2025 年", "value": "+76.8%"},
                {"label": "2026 年至 8 月", "value": "+21.5%"},
                {"label": "每月胜率", "value": "74%（61 个月里 45 个月赚钱）；中位 +3.5%；最差 −8.7%"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
        },
    },
    {
        "key": "starport",
        "name": "星港",
        "structure": "母港 + 卫星 0.2 曝露（当前数据下过验收的最大权重）。想稳一点、又不想只吃港湾的收益时用它。",
        "status": "product_candidate_increment",
        "statusLabel": "产品候选增量",
        "role": "balanced",
        "roleLabel": "均衡",
        "timelineStrategy": "starport",
        "doc": "docs/backtests/stable/harbor-b3-sat-2026-09-14.md",
        "tag": "h-b3-sat-20260914",
        "windows": {
            "OOS2": {"total": 65.3, "cagr": 68.8, "mdd": -6.5, "sharpe": 3.25},
            "train": {"total": 38.2, "cagr": 95.1, "mdd": -4.4, "sharpe": 4.16},
            "valid": {"total": 21.0, "cagr": 55.4, "mdd": -11.4, "sharpe": 1.97},
            "long": {"total": 169.0, "cagr": 22.8, "mdd": -11.2, "sharpe": 1.62},
        },
        "pros": [
            "六个年份全部赚钱（+10%/+9%/+5%/+31%/+35%/+19%）",
            "约 65% 的月份赚钱；最差的一个月只亏 4.3%",
            "弱市年进攻 + 趋势年跟住：OOS2 +65%、长期 +169%，回撤只有 −11%",
        ],
        "cons": [
            "有一段比母港少赚 4.6%（余量很薄，数据一漂就挂——1/3 档已挂）",
            "卫星不行时会被拖累；卫星腿执行有容量边界（约 500 万以内舒服）",
            "均衡不免费：趋势窗仍比港湾少赚约三成",
        ],
        "regime": {
            "fit": [
                "几乎什么行情都能赚：2021–2023 弱势年份 +10%/+9%/+5%",
                "弱市年进攻（OOS2 +65%），趋势年跟住（valid +21%）",
                "约 65% 的月份赚钱，中位 +0.9%",
            ],
            "unfit": [
                "没有全亏的阶段；最弱是卫星抽搐期",
                "单月最差 −4.3%（港湾 −11.1%）",
            ],
            "evidence": [
                {"label": "2021 年", "value": "+9.7%（年内最多跌 8.1%）"},
                {"label": "2022 年", "value": "+9.0%（最多跌 6.6%）"},
                {"label": "2023 年", "value": "+5.1%（最多跌 7.2%）"},
                {"label": "2024 年", "value": "+30.8%（最多跌 8.4%）"},
                {"label": "2025 年", "value": "+34.7%（最多跌 5.1%）"},
                {"label": "2026 年至 8 月", "value": "+18.5%（最多跌 11.2%）"},
                {"label": "每月胜率", "value": "65%（60 个月里 39 个月赚钱）；中位 +0.9%；最差 −4.3%"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
        },
    },
    {
        "key": "homeport",
        "name": "母港",
        "structure": (
            "港湾的减震版（M30 档）：七成钱跟港湾走，三成按风险预算买一篮子"
            "（沪深300/中证500/黄金/纳指/国债），每月调一次仓。防守档：回撤比港湾浅，"
            "长期年化 21%。"
        ),
        "status": "product_candidate",
        "statusLabel": "产品候选",
        "role": "defense",
        "roleLabel": "防守",
        "timelineStrategy": "homeport_m30",
        "doc": "docs/backtests/stable/homeport-weight-tune-2026-09-16.md",
        "tag": "h-mix-m30-20260916",
        "windows": {
            "OOS2": {"total": 43.9, "cagr": 46.1, "mdd": -11.0, "sharpe": 1.81},
            "train": {"total": 42.0, "cagr": 106.3, "mdd": -6.3, "sharpe": 3.25},
            "valid": {"total": 35.4, "cagr": 101.6, "mdd": -15.7, "sharpe": 2.18},
            "long": {"total": 152.5, "cagr": 21.2, "mdd": -16.4, "sharpe": 1.10},
        },
        "pros": [
            "防守也有收益：四段测试总收益 +44%/+42%/+35%/+153%（含手续费），长期年化 21%（M50 只有 17.6%）",
            "回撤仍浅：四段最多跌 11%/6%/16%/16%（港湾 14%/8%/22%/23%）；夏普四段全不低于港湾",
            "成本不敏感：20bp 成本下数字几乎不动；2022–2023 弱市压测收益风险双优于港湾",
        ],
        "cons": [
            "长期收益仍只有港湾约七成（+153% vs +208%）——保险总有价格",
            "大盘特别强的年份涨幅打折（70% 权重决定了上限）",
            "B3 那三成靠“股票弱、黄金债券强”的环境吃饭，环境反转会拖累（与 M50 同源）",
        ],
        "regime": {
            "fit": [
                "大盘向上的年份跟着港湾赚：2024 +28%、2025 +38%、2026 至 8 月 +35%",
                "弱市年不亏反赚：2022 +4.7%、2023 +1.7%，回撤一直在 16% 以内",
                "约六成月份赚钱（60 个月里 34 个月赚钱）；中位 +0.8%，最差 −7.9%",
            ],
            "unfit": [
                "2021 年 −2.6%（开局弱，年内最多跌 11.5%）",
                "单月最差 −7.9%（港湾 −11.1%）",
                "B3 环境反转（股强金债弱）时三成仓位拖累",
            ],
            "evidence": [
                {"label": "2021 年", "value": "−2.6%（年内最多跌 11.5%）"},
                {"label": "2022 年", "value": "+4.7%（最多跌 11.4%）"},
                {"label": "2023 年", "value": "+1.7%（最多跌 11.2%）"},
                {"label": "2024 年", "value": "+28.1%（最多跌 16.4%）"},
                {"label": "2025 年", "value": "+37.6%（最多跌 8.0%）"},
                {"label": "2026 年至 8 月", "value": "+34.8%（最多跌 15.2%）"},
                {"label": "每月胜率", "value": "57%（60 个月里 34 个月赚钱）；中位 +0.8%；最差 −7.9%"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
        },
    },
    {
        "key": "harbor",
        "name": "港湾",
        "structure": (
            "现在实盘用的就是它：买 A 股里最强的一批股票（S-3 选强），没买股票的钱"
            "不闲着，拿去买趋势最好的一个 ETF（黄金/原油/纳指/国债里挑一个）。"
            "（日落模式：维持运行，不再开发。）"
        ),
        "status": "live",
        "statusLabel": "Live · 日落",
        "role": "live",
        "roleLabel": "Live 底座",
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
            "实盘基线。四段测试总收益 +55%/+52%/+50%/+202%（含手续费）",
            "2024–2026 连年大赚（+30%/+52%/+47%）；约 56% 的月份在赚钱，最差一个月 −11%",
            "大盘走强时最能赚：大盘在 200 日均线上方，平均每天 +0.17%（≈ 一年 +42%）",
        ],
        "cons": [
            "看天吃饭：2021–2023 三年基本白干（−5%/+8%/+1%）；大盘跌破 200 日均线时，每天只剩 +0.045%（≈ 一年 +11%）",
            "大盘横着磨、波动不大不小的阶段最难受：平均每天 −0.03%",
            "长期最大回撤 −23%（从高点最多跌 23%），性价比（夏普 1.0）一般",
        ],
        "regime": {
            "fit": [
                "大盘在 200 日均线上方（大趋势向上）：每天约 +0.17%，≈ 一年 +42%",
                "大盘最近 20 天在涨：每天约 +0.17%",
                "波动特别大或特别小都能赚：每天 +0.168% / +0.185%",
            ],
            "unfit": [
                "大盘跌破 200 日均线（弱势）：每天只剩 +0.045%，≈ 一年 +11%",
                "大盘最近 20 天在跌：每天约 +0.05%",
                "大盘横着磨（波动不大不小）：每天 −0.03%",
                "2021–2023 这类没行情的年份（−5%/+8%/+1%）",
            ],
            "evidence": [
                {"label": "2021 年", "value": "−5.1%（年内最多跌 15.2%）"},
                {"label": "2022 年", "value": "+8.4%（最多跌 15.9%）"},
                {"label": "2023 年", "value": "+1.2%（最多跌 16.3%）"},
                {"label": "2024 年", "value": "+29.9%（最多跌 22.4%）"},
                {"label": "2025 年", "value": "+51.6%（最多跌 10.9%）"},
                {"label": "2026 年至 8 月", "value": "+47.0%（最多跌 20.8%）"},
                {"label": "每月胜率", "value": "56%（61 个月里 34 个月赚钱）；中位 +1.2%；最差 2024-08 −11.1%"},
                {"label": "大盘环境", "value": "均线上/下 +0.173% / +0.045%；20 天涨/跌 +0.168% / +0.054%；波动 大/中/小 +0.168% / −0.033% / +0.185%（每天）"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
        },
    },
    {
        # 2026-09-17 audit: restored as the parallel comparison entry
        # (removing it from the product surface was too aggressive; user
        # decision 2026-09-14: keep it, catalog badge = 并行对照).
        "key": "twin_star",
        "name": "双子星",
        "structure": "港湾核心和卫星打法各放一半钱。行情好时冲得最猛，作为“并行对照”保留观察，不进实盘。",
        "status": "parallel_candidate",
        "statusLabel": "并行对照",
        "role": "balanced",
        "roleLabel": "对照",
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
            "行情好的年份冲得最猛：2021 +22%、2022 +24%、2025 +67%",
            "港湾和卫星相关性低（每天涨跌几乎不同步），所以两边一起能叠加收益",
            "69% 的月份赚钱；波动大或小的行情统一 +0.161%/天（≈ 一年 +39%）",
        ],
        "cons": [
            "卫星状态差的年份很平庸：2023 只 +5.8%、2026 只 +7.7%",
            "最差一个月 −7.9%；卫星抽搐时连累明显（2026-06 −6.9%、2026-07 −7.2%）",
            "有一段比港湾核心少赚 21.2%，没通过内部验收——保留它只为了对照观察，不进实盘",
        ],
        "regime": {
            "fit": [
                "大盘强或弱都赚：每天 +0.134% / +0.086%",
                "大盘涨或跌都赚：每天 +0.144% / +0.084%",
                "波动大或小都好：统一 +0.161%/天（≈ 一年 +39%）",
            ],
            "unfit": [
                "大盘横着磨（波动不大不小）：约 0（每天 +0.012%）",
                "卫星状态差的年份（2023 +5.8% / 2026 +7.7%）",
                "卫星抽搐期（2026 年 6 月 −6.9%、7 月 −7.2%）",
            ],
            "evidence": [
                {"label": "2021 年", "value": "+22.3%（年内最多跌 12.7%）"},
                {"label": "2022 年", "value": "+24.5%（最多跌 9.4%）"},
                {"label": "2023 年", "value": "+5.8%（最多跌 12.9%）"},
                {"label": "2024 年", "value": "+35.4%（最多跌 13.0%）"},
                {"label": "2025 年", "value": "+66.8%（最多跌 8.1%）"},
                {"label": "2026 年至 8 月", "value": "+7.7%（最多跌 20.8%）"},
                {"label": "每月胜率", "value": "69%（61 个月里 42 个月赚钱）；中位 +2.0%；最差 2021-10 −7.9% / 2026-07 −7.2%"},
                {"label": "大盘环境", "value": "均线上/下 +0.134% / +0.086%；20 天涨/跌 +0.144% / +0.084%；波动 大/中/小 +0.161% / +0.012% / +0.161%（每天）"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
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
