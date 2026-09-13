# 预注册：CB 双低（价格+低溢价）（H-CB3 · 2026-09-12）

> 跑前冻结。单假设、零网格。前置数据已就位：PIT 转股溢价率（akshare `bond_zh_cov_value_analysis`，见 [cb-mom20](../backtests/cb/cb-mom20-2026-09-12.md) 同批数据）。CB 日线横截面只剩这一条缝。

## 假设

**H-CB3**：**双低**（转债价格 + 转股溢价率(百分数) 之和，取最低 20%）跑赢转债等权宇宙，扣成本后成立。

**一句话机制**：低价给债底保护（下行有限），低溢价给股性弹性（上行跟随正股）——"下有保底、上不封顶"，是转债最经典的散户策略；T+0、免印花，费低。

**最可能死因**：#2 共线（双低≈低价，且 2024-08 转债信用事件里低价债集体崩 → regime/beta，不是独立 alpha）/ #4 单窗 / #6 成分。

## 冻结口径

| 项 | 值 |
|----|----|
| 宇宙 | `data/cb/cb_daily.csv`（价/量）+ `data/cb/cb_valuation.csv`（PIT 溢价）；每券当日 amount ≥ 40 分位；price>0；溢价非空 |
| 价格 | akshare `close_em`，缺则用 1-min 聚合 close |
| 信号 | `双低 = price + conv_prem_pct`，取**最低 20%**；对照：仅低溢价、仅低价 |
| 权重 | 等权，周频（每 5 交易日） |
| 执行 | 主 close-to-close；稳健 next-open |
| 成本 | 主 20bp 往返；压力 40bp |
| 基准 | 同日同流动性门**等权全宇宙** |
| 窗口 | OOS2/train/valid/past_year + **long（2021-01-01~2026-08-07）** + 分年 |

## 拒收线（跑前冻结）

- **K1**：long 窗 `双低 − EW` ≤ 0 → REJECT。
- **K2**：三窗 WF 不足 ≥2 窗为正 → REJECT。
- **K3**：40bp 下 long 转负 → REJECT。
- **K4**：正收益集中 1–2 年、其余 ≤0 → 标成分/regime 依赖，不采纳。
- 邻域（低溢价/低价、next-open）只作平台形状检查，不挑最优。

## 结果（跑后补）

**REJECT**（K1 +18.3 ✅ / K2 WF 1/3 ❌ / K3 40bp −12.7 ❌ / K4 4/6）。见 [`cb-doublelow-2026-09-12.md`](../backtests/cb/cb-doublelow-2026-09-12.md)。
