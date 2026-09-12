# 预注册：GARP（质量 + 便宜）长持 sleeve pilot（只读 · 不进 Live）

> 状态：草稿 2026-09-12，跑之前冻结。不改 Live / 双子星 / S-3。
> **执行结果（2026-09-12）：`CLOSE`（质量无增量）。** BPS 反推过校验（Spearman 0.976）；GARP +2.6%/年（71% 胜）
> ≈ value 臂 +2.6%（年度超额相关 +0.93），quality 只 +0.5%/年 → GARP 只是"便宜"的换皮。
> 结果见 [`../backtests/factors/garp-sleeve-2026-09-12.md`](../backtests/factors/garp-sleeve-2026-09-12.md)。
> 前置：L1 长持找几倍股 REJECT（高增长已 price in）→ 下一步 GARP（`todo.md` P0-12/L1）。
> 脚本：`services/data-sync-service/scripts/incubate/garp_sleeve.py`（只读）。
> 结果回写 `../backtests/factors/garp-sleeve-2026-09-12.md`。P0-12 纪律：自有基线、单假设、零网格。

## 0. 自查（最可能死在哪）

| 死因 | 风险 |
|------|------|
| 共线 | 最高。value 腿=慢价值 V1（已 PASS）、quality 腿=ROE（P0-11 已测：高 ROE=贵、控市值后死）；GARP 可能只是这两个的加总，无新信息 |
| 样本 | 高。估值数据：`cn_financial` 仅 2019+ 全量 → 必须用报表反推 BPS 才够长；反推失败则只 5 个 formation（underpowered） |
| 单窗 | 高。2020-2024 是极端风格轮动（成长崩/价值起），短窗好看无意义 |
| 风格 | 高。A 股"便宜+质量"很可能 = 大盘价值 beta，非独立 alpha |

## 1. 机制假设（一句话）

在流动池内、行业中性后，**高 ROE + 高现金含量（质量）× 高 E/P、B/P（便宜）** 的股票，
未来 12 个月相对流动池等权有稳定正超额；且**质量对纯价值有增量**（不是只买便宜货）。

## 2. 数据与 PIT（防前视）

- 基本面 as-of：`cn_income_stmt` / `cn_balance_sheet` / `cn_cashflow_stmt`（2007+，`ann_date ≤` 形成日）。
- `raw_price(t) = qfq_close(t) × adj_factor(latest) / adj_factor(t)`（还原当时真实股价；qfq 价含未来复权，直接当价格有前视）。
- **BPS 反推校验（gate）**：`shares_est = n_income_attr_p / basic_eps`（basic_eps>0），`bps_est = equity/shares_est`；
  与 `cn_financial.bps`（2019+ 真值）比 **Spearman ≥0.8** 才用长史（2008–2024）；否则退化为 **2020–2024 pilot** 并在结果里标 `⚠️underpowered`。
- 收益率：qfq 5 月到次年 5 月（qfq 收益无复权错位）；流动性 = 当年 2–4 月日均成交额（as-of）。

## 3. 冻结因子与组合（零网格）

- `EP = basic_eps/raw_price`（仅 `basic_eps>0`，负值记 NaN）。
- `BP = bps_est/raw_price`。
- `ROE = n_income_attr_p/equity`（equity>0）。
- `CCR = n_cashflow_act/n_income_attr_p`（n_income>0 才有意义）。
- 各因子先转 pct-rank，再**行业内**（东财，UNK 兜底）转 pct-rank。
- 宇宙：流动池 ≥ 中位；持仓 = 头档（quintile）等权、12 个月；成本 = 换手 × 30bp。
- **四个臂（预注册对照，不是网格）**：
  1. `value` = mean(EP, BP)
  2. `quality` = mean(ROE, CCR)
  3. **`garp` = mean(EP, BP, ROE, CCR)**（主臂）
  4. 基线 = 流动池等权；次基线 = 中证500。
- 形成年：2008–2024（若校验退化为 2020–2024）。

## 4. 判据与 kill 线（跑之前写死）

- 主指标 = 年度净超额 vs 流动池等权 + 胜率；OOS 切分 **2008–2015 / 2016–2024**（退化档则不切，标 underpowered）。
- **INTERESTING**（才进下一步）需全部满足：
  1. `garp` 全窗超额 > 0 且年胜率 ≥ 55%；
  2. `garp` 全窗超额 **> value 臂 且 > quality 臂**（质量对纯价值有增量，否则只是 value/quality 的代理）；
  3. 两期内 `garp` 超额都 > 0（退化档此条按可用期报）。
- 任一不满足 → **CLOSE（parked）**，不组合、不进 paper、不调参。
- 补充：若 `garp` 与 `value` 超额几乎相等 → 判"质量无增量"，明确关闭 GARP 而保留 V1。

## 5. 必报审计

- BPS 反推校验（Spearman / 中位相对误差 / 覆盖率）。
- 每年持仓数、GARP 与 value/quality 的年度超额序列（相关性）。
- 成本压力 30/50bp；容量（头档 Feb–Apr 日均额×10%，亿元）。
- 行业分布描述。
- 诚实边界：反推 BPS 有噪；非 PIT 行业；A 股 long-only 高波动结构性问题。

## 6. 不做什么

- 不加 growth 原始增速项（L1 已证高增长=陷阱）；不加 S-3 任何 gate；不进 Live。
- 不做参数网格（因子子集/分位/阈值不扫，四臂是预注册对照）。
- 不把绝对 CAGR 当 KPI；判据是相对自有基线的超额。
