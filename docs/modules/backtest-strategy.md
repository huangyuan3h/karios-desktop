# 回测策略研究档案（已归档 · 2026-08-09 冻结）

> ⚠️ **本文件已于 2026-08-28 归档至 `docs/archive/modules-legacy/backtest-strategy-legacy.md`。**
> 当前内容冻结于 **2026-08-09**，此后 2026-08-12~23 的多轮固化（mp10、D2/D3/E2、
> neutral_block、entry_style=auto、Strong-only ATR 止损、drawdown_circuit=-25、qfq 重灌、
> universe 扩全市场）**均未回写本文件**，故本页与现行策略已脱节。
>
> **现行唯一真值请移步：**
> - 运行参数真值 → [`modules/strategy-params.md`](./strategy-params.md)
> - 回测引擎 / 冻结基线 → `services/data-sync-service/data/backtest_reports/walk_forward_baseline.json`
>   （mp10 · sha256 `40ef4cd0d6a5…` · `git tag s3-baseline-20260822-realism`）
> - 实验全记录 → [`backtests/README.md`](../backtests/README.md)
>
> **重要口径提示**：本归档页 §6.5/6.6/6.7 的 S-3 数字（全年 155.1% / 121.7% / 154.1% 等）
> 为 **mp20 / 200% 杠杆未约束**口径，已被 `strategy-params.md` 标记为「封存」。现行实盘口径
> （mp10 + 现金≤100%）为：过去一年 66.6% / OOS2 43.1% / train 34.3% / valid 43.3%。
> **请勿把本页的 120%+ 数字当作当前 S-3 实盘口径。**
