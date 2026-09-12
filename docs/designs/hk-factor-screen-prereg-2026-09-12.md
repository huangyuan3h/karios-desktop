# 预注册：HK（H 股）Alpha101 + GTJA191 L0 速筛（只读 · 不进 Live）

> 状态：草稿 2026-09-12，跑之前冻结。不改 Live / 双子星 / S-3。
> 脚本：`services/data-sync-service/scripts/hk_factor_screen.py`（只读，复用两套公式）。
> 结果：`../backtests/factors/hk-alpha101-gtja191-l0-2026-09-12.md`。
> 动机：回答"A 股价量因子结论是否市场特有"——先在香港实测（仓库有数据），US/加拿大待数据。

## 0. 自查

| 死因 | 风险 |
|------|------|
| 成本 | 最高。HK 印花税 0.1%×双边 ≈ 20bp + 费/佣金/滑点 → 往返 ~40bp（比 A 股高） |
| 数据 | 高。`daily` 的 `adj_factor` 为 NULL → **原始价**（拆股/合股会污染）；无 HK 行业/市值 |
| 信号 | 中高。HK 散户换手低于 A → 短反转信号预期更弱 |
| 共线 | 同 A：价量相关家族 |

## 1. 机制假设（一句话）

若 A 股结论（唯一活的是价量相关家族、成本杀死）是**市场结构**（不能做空 + 成本）导致，则 HK 应**复现且更差**（成本更高、信号更弱）；若 A 股特有，则 HK 表现应显著不同。

## 2. 口径

- 宇宙：`stock_basic.market='HK'`，剔 ETF/基金；20 日均额 ≥ **1000 万 HKD**、价 ≥1 HKD。
- 数据：`daily` 中 `ts_code LIKE '%.HK'`；`vwap = amount/vol`；`volume = amount`（成交额口径）。
- 复权：无 `adj_factor` → 原始价；**剔除窗口内任一 |1 日收益|>60% 的标的**（拆合股/异动代理）。
- 行业：无 HK 映射 → `groups='UNKNOWN'`（`IndNeutralize` ≈ 横截面去均值）；`cap=NaN`（A56/GTJA056 → NO-DATA）。
- 基准：GTJA075/182 用 **HSI**（`global_index_daily`）。
- 窗口：OOS2/train/valid 同 A 股；h=1/5/10，主判 h=5。
- 成本：**40bp 往返**（HK 现实口径）；三窗净 = 毛 − 换手×0.004×5。

## 3. 判据与 kill 线（同 A 股 L0）

三窗 IC 同号 + |ICIR|≥0.5 + ≥2/3 单调 + n≥100/avgN≥100 + net>0 → PASS；同号 + 中位|ICIR|≥0.3 → CANDIDATE；否则 REJECT。

## 4. 必报

- 完整判定表 + 家族归类；与 A 股同族因子的 IC/ICIR/net 逐条对照。
- 诚实边界：原始价（未复权）、无行业/市值、成本口径为近似。

## 5. 不做什么

- 不改 Live；不因 HK 结果开参数网格；不与 A 股混宇宙。
- US / 加拿大**不在本轮**（无数据）；本档只给 HK 实测 + 机制推断。
