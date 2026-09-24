"""Frozen strategy-family catalog for the Backtest page overview (display layer).

Single source for the clean caliber numbers (costs included) shown in the UI.
The current robust Starship row is the H2-a25 report and explicitly carries
its K3 risk; every row cites its authoritative doc and numbers must be updated
when a new audit re-freezes them (see the audit doc's §0 comparison table).

Text style (2026-09-15): human-facing copy is plain language (大白话) — what it
does, what it earned, when it works. Jargon stays in the真值 docs.

Live stays 港湾 — this catalog is read-only display data, never order wiring.

Three-tier lineup (2026-09-24): 星舰 offense / 稳健星舰 H2-a25 balanced / 星港 balanced /
母港M30 defense, plus 港湾 sunset baseline (Live keeps running; no new development).
Order below is the display order everywhere (catalog panel, tabs, mode bar).
"""

from __future__ import annotations

from typing import Any

from data_sync_service.service.state_bucket_track import A25_TAG, STARSIP_B_TAG

UPDATED = "2026-09-21"

# Windows: total% (含成本) / CAGR% / maxDD% / Sharpe. Frozen 2026-09-14 clean.
_STRATEGIES: list[dict[str, Any]] = [
    {
        "key": "starship",
        "name": "星舰",
        "structure": (
            "激进对照：卫星 4 槽 ×25% + 闲置现金 100% 停 H2 趋势 ETF"
            "（mom60+MA200，领先 2pt 才换仓）；不接 Live 自动下单。"
        ),
        "status": "aggressive_pending",
        "statusLabel": "激进 · 前置未满",
        "role": "offense",
        "roleLabel": "进攻",
        "timelineStrategy": "starship",
        "doc": "docs/backtests/stable/sat-h2-a25-2026-09-24.md",
        "tag": "starship-h2-20260924",
        "updated": "2026-09-24",
        "variant": {
            "sleeveMode": "h2",
            "hystBand": 0.02,
            "sleeveWeight": 1.0,
            "b3Weight": 0.0,
        },
        "windows": {
            "OOS2": {"total": 240.7, "cagr": 258.36, "mdd": -8.6, "sharpe": 5.06},
            "train": {"total": 75.9, "cagr": 221.0, "mdd": -9.6, "sharpe": 4.5},
            "valid": {"total": -0.6, "cagr": -1.32, "mdd": -36.1, "sharpe": 0.21},
            "long": {"total": 962.7, "cagr": 63.26, "mdd": -30.5, "sharpe": 2.14},
        },
        "pros": [
            "H2 迟滞后 long +962.7%，纯套筒臂长期收益最高",
            "卫星执行审计已过：成交价格落在日内区间；容量约 500 万以内",
            "适合单独研究卫星脉冲和收益上限",
        ],
        "cons": [
            "回撤极深：long MDD -30.5%，valid MDD -36.1%",
            "valid -0.6%，弱窗卫星与停车腿都缺乏有效收益",
            "只固化到研究/展示；Live 前置仍为 paper 20 笔 + 风险授权",
        ],
        "regime": {
            "fit": [
                "卫星脉冲和趋势窗进攻",
                "long 端提供最高收益上限",
            ],
            "unfit": [
                "卫星弱窗和大幅回撤期",
                "需要严格风险授权，不适合作为稳健基线",
            ],
            "evidence": [
                {"label": "长窗（2021-08~2026-08）", "value": "+962.7% / MDD -30.5% / SR 2.14"},
                {"label": "OOS2（2024-08~2025-08）", "value": "+240.7% / MDD -8.6%"},
                {"label": "train（2025-08~2026-02）", "value": "+75.9% / MDD -9.6%"},
                {"label": "valid（2026-03~2026-08）", "value": "-0.6% / MDD -36.1%"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关。",
        },
    },
    {
        "key": "starship_robust",
        "name": "稳健星舰 H2-a25",
        "structure": (
            "唯一现行稳健研究/展示档：卫星 4 槽 ×25% + cashShare(T−1) 的闲置现金；"
            "闲置现金按 25% H2 趋势 ETF（mom60+MA200，领先 2pt 才换仓）+ 75% B3 风险预算"
            "（5 资产 inverse-vol，月调权重）配置。H2-a25，不接 Live 自动下单。"
        ),
        "status": "product_candidate",
        "statusLabel": "现行 canonical · K3 风险",
        "role": "balanced",
        "roleLabel": "稳健",
        "timelineStrategy": "starship_robust",
        "doc": "docs/backtests/stable/sat-h2-a25-2026-09-24.md",
        "tag": A25_TAG,
        "updated": "2026-09-24",
        "canonical": True,
        "variant": {
            "sleeveMode": "h2",
            "hystBand": 0.02,
            "sleeveWeight": 0.25,
            "b3Weight": 0.75,
        },
        "risk": "K3 failed: long MDD -8.4% vs pure B3 -6.8% (delta -1.6pt)",
        "windows": {
            "OOS2": {"total": 238.4, "cagr": 255.83, "mdd": -5.0, "sharpe": 6.32},
            "train": {"total": 55.9, "cagr": 150.4, "mdd": -7.5, "sharpe": 4.15},
            "valid": {"total": 3.2, "cagr": 7.5, "mdd": -10.8, "sharpe": 0.46},
            "long": {"total": 738.5, "cagr": 55.43, "mdd": -8.4, "sharpe": 3.5},
        },
        "pros": [
            "H2 2pt 迟滞减少无效换仓，收益路径接近 0pt 经典臂但保留现行产品逻辑",
            "三窗相对纯 B3 的收益增量为 +1.9/+6.3/+0.2pt，long +84.2pt",
            "回撤仍显著低于激进套筒：long -8.4% vs 纯套筒 -30.5%",
        ],
        "cons": [
            "K3 未通过：long MDD 相对纯 B3 恶化 -1.6pt（预注册门槛 -1.0pt），不能写成全门 PASS",
            "valid 只有 +3.2%，卫星弱窗仍是主要短板",
            "仅固化到研究/展示/人工操作；Live 自动执行仍为港湾，前置仍是 paper 20 笔 + 风险授权",
        ],
        "regime": {
            "fit": [
                "弱市/高波时卫星脉冲较肥（OOS2 +238.4%，MDD -5.0%）",
                "H2 2pt 迟滞下换仓更少，操作噪声低于 0pt 经典臂",
                "闲置资金仍有 75% B3 分散，long MDD 比纯套筒浅 22.1pt",
            ],
            "unfit": [
                "K3 风险：H2-a25 long MDD 比纯 B3 深 1.6pt",
                "valid 弱窗只有 +3.2%，卫星腿仍是主要收益波动来源",
                "报告裁决为 REJECT（仅 K3 失败），不是全门 PASS",
            ],
            "evidence": [
                {"label": "长窗（2021-08~2026-08）", "value": "+738.5% / MDD -8.4% / SR 3.50"},
                {"label": "OOS2（2024-08~2025-08）", "value": "+238.4% / MDD -5.0%"},
                {"label": "train（2025-08~2026-02）", "value": "+55.9% / MDD -7.5%"},
                {"label": "valid（2026-03~2026-08）", "value": "+3.2% / MDD -10.8%"},
                {"label": "门控", "value": "K1/K2/K4/K5 PASS；K3 FAIL（MDD delta -1.6pt）"},
            ],
            "note": "这些只是“什么行情下好用”的记录，不是买卖开关；K3 风险需持续观察。",
        },
    },
    {
        "key": "starship_b",
        "name": "星舰 B",
        "structure": (
            "卫星 4 槽 ×25% 不变；闲置现金 100% 停 {国债+黄金+纳指} 逆波动率（3 腿月频，"
            "因果 T−1，5bps/边）。相比 H2-a25：长期收益略低、回撤更浅、2022–23 熊市更强。"
            "研究/展示/人工操作档，不接 Live 自动下单。"
        ),
        "status": "product_candidate",
        "statusLabel": "稳健备选 · 3 腿停放",
        "role": "balanced",
        "roleLabel": "稳健",
        "timelineStrategy": "starship_b",
        "doc": "docs/backtests/stable/starship-b-2026-09-24.md",
        "tag": STARSIP_B_TAG,
        "updated": "2026-09-24",
        "windows": {
            "OOS2": {"total": 231.7, "cagr": 248.55, "mdd": -4.0, "sharpe": 6.65},
            "train": {"total": 51.5, "cagr": 135.95, "mdd": -8.2, "sharpe": 4.01},
            "valid": {"total": 5.4, "cagr": 12.87, "mdd": -10.0, "sharpe": 0.66},
            "long": {"total": 669.6, "cagr": 52.69, "mdd": -5.5, "sharpe": 3.9},
        },
        "pros": [
            "回撤更浅（long MDD -5.5% vs H2-a25 -8.4%），Sharpe 更高（3.90 vs 3.50）",
            "2022–23 熊市显著更强（stress +124.0% / -7.4% vs H2-a25 +103.5% / -10.1%）",
            "停放腿只有 3 只 ETF（国债+黄金+纳指），比 H2+B3 好复制",
        ],
        "cons": [
            "长期收益低于 H2-a25（long +669.6% vs +738.5%，少 69pt）",
            "valid 仍弱（+5.4%），卫星弱窗是共同短板",
            "仅研究/展示/人工操作；Live 自动执行仍为港湾",
        ],
        "regime": {
            "fit": [
                "熊市/压力段（stress +124.0%）优于 H2-a25",
                "卫星空仓时停放的 3 腿更分散、回撤更浅",
            ],
            "unfit": [
                "追逐最高长期收益时不如 H2-a25",
                "valid 弱窗收益仍薄",
            ],
            "evidence": [
                {"label": "长窗（2021-08~2026-08）", "value": "+669.6% / MDD -5.5% / SR 3.90"},
                {"label": "OOS2（2024-08~2025-08）", "value": "+231.7% / MDD -4.0%"},
                {"label": "train（2025-08~2026-02）", "value": "+51.5% / MDD -8.2%"},
                {"label": "valid（2026-03~2026-08）", "value": "+5.4% / MDD -10.0%"},
                {"label": "stress（2022-01~2023-12）", "value": "+124.0% / MDD -7.4%"},
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
            "OOS2": {"total": 64.8, "cagr": 68.24, "mdd": -6.6, "sharpe": 3.22},
            "train": {"total": 38.0, "cagr": 94.44, "mdd": -4.5, "sharpe": 4.14},
            "valid": {"total": 20.9, "cagr": 55.12, "mdd": -11.4, "sharpe": 1.96},
            "long": {"total": 163.3, "cagr": 22.24, "mdd": -11.2, "sharpe": 1.57},
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
            "OOS2": {"total": 43.4, "cagr": 45.54, "mdd": -11.0, "sharpe": 1.79},
            "train": {"total": 41.9, "cagr": 105.95, "mdd": -6.4, "sharpe": 3.24},
            "valid": {"total": 35.4, "cagr": 101.43, "mdd": -15.7, "sharpe": 2.18},
            "long": {"total": 146.6, "cagr": 20.59, "mdd": -16.4, "sharpe": 1.06},
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
            "OOS2": {"total": 54.4, "cagr": 57.23, "mdd": -14.3, "sharpe": 1.71},
            "train": {"total": 52.0, "cagr": 137.48, "mdd": -8.2, "sharpe": 3.0},
            "valid": {"total": 50.2, "cagr": 156.13, "mdd": -21.8, "sharpe": 2.13},
            "long": {"total": 197.1, "cagr": 25.34, "mdd": -22.9, "sharpe": 0.98},
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
        # 2026-09-21 user decision: fully revived as a first-class comparison
        # strategy (was a "parallel reference" badge after the 09-17 audit).
        # Display/research only — Live orders stay 港湾 (S-3 + parking).
        "key": "twin_star",
        "name": "双子星",
        "structure": "港湾核心和卫星打法各放一半钱（无仓日 100% 港湾）。行情好时冲得最猛；五档正式并行对比（展示口径，非 Live）。",
        "status": "product_candidate",
        "statusLabel": "并行 · 正式",
        "role": "balanced",
        "roleLabel": "均衡",
        "timelineStrategy": "twin_star",
        "doc": "docs/backtests/stable/twin-star-parking-refit-2026-09-13.md",
        "tag": "twin-star-parking-2026-09-13",
        "windows": {
            "OOS2": {"total": 144.6, "cagr": 153.8, "mdd": -9.2, "sharpe": 4.22},
            "train": {"total": 52.2, "cagr": 138.0, "mdd": -6.6, "sharpe": 3.93},
            "valid": {"total": 24.5, "cagr": 66.1, "mdd": -21.8, "sharpe": 1.31},
            "long": {"total": 299.8, "cagr": 33.3, "mdd": -20.8, "sharpe": 1.48},
        },
        "pros": [
            "行情好的年份冲得最猛：2021 +22%、2022 +24%、2025 +67%",
            "港湾和卫星相关性低（每天涨跌几乎不同步），所以两边一起能叠加收益",
            "69% 的月份赚钱；波动大或小的行情统一 +0.161%/天（≈ 一年 +39%）",
        ],
        "cons": [
            "卫星状态差的年份很平庸：2023 只 +5.8%、2026 只 +7.7%",
            "最差一个月 −7.9%；卫星抽搐时连累明显（2026-06 −6.9%、2026-07 −7.2%）",
            "valid 窗比港湾核心少赚 25.7pt（趋势窗卫星有仓仅 27%），是它唯一短板——作为正式并行档保留，不进 Live",
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
            "updated": row.get("updated", UPDATED),
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
