# 预注册：Alpha Incubator 趋势卡前瞻性诊断（只读 · 不进 Live）

> 状态：草稿 2026-09-07。任何结果（有价值/无价值）都不改 Live 参数；最好结局是转前瞻 paper 记录。
> 先过 `first-principles-2026-09-05.md` 自查（见 §0），再跑 `scripts/diag_alpha_trend_forward.py`。

## 0. 自查（先答“最可能死在哪类”）

| 死因类 | 本诊断的风险 |
|--------|--------------|
| 样本 | 最高。as-of 安全历史仅 2026-06-01 起（~3 个月，~80 批）；grade×driver×持有期一切全是小格子，按 `n<100 underpowered` 纪律，主判据只看 grade 梯度 pooled+批聚类双口径，driver 只描述不判定 |
| 单窗 | 高。06-09 全是牛市年单 regime，绝对收益天然偏乐观——所以判据用 **market-relative 超额**，不用绝对收益 |
| 共线 | 高。映射多为行业龙头（中际旭创/恒瑞/紫金矿业类）= 主线 beta + RS 动量，与现有 alpha 大概率重合；梯度检验本身就是在测“分级有没有独立信息” |
| 方向 | 中。若 S>A>B 梯度不存在，说明 LLM 分级不携带定价信息，关闭该方向 |

## 1. 机制假设（一句话）

Incubator 趋势卡的分级（S/A/B）+ A 股映射，对映射票未来 10/20 日有相对全市场的超额（注意力/事件驱动资金跟随）。

## 2. as-of 口径（防前视铁律）

- 信号时刻 = `alpha_radar_trends.created_at`（batch 多在盘后，如 16:56CST）→ 入场统一为**下一交易日 open**。
- 映射用 **`trend_json.a_share_mapping` 出生中文名**（创建时冻结），不用现 `cn_symbols`（支持事后 Remap，有前视嫌疑）。
- 中文名 → `stock_basic`  current master 只取 A 股（SH/SZ）；同名多 match 记 ambiguous 并列入审计；映射不上记未映射率。
- academic 类天然 timeless，不进样本（trends 均为 RSS batch 产物，此处注明无 academic）。
- 样本窗 = created 2026-06-01 起，截止到“20 日 forward 在最新日线内完整”的最后出生日；**主判据 10 日，20 日次要**（保证 n）。
- 基准 = 同窗全 A 等权（entry open → exit close），每窗一条 SQL 聚合。
- 成本 = 往返 0.3%（`COSTS_ROUNDTRIP` 同卫星口径）。

## 3. 判据与 kill 线（跑之前写死）

- **K1（梯度）**：S/A/B(/C) 10 日净 market-relative 均值，无单调 `S>A>B`（pooled 与按批聚类任一口径）→ **REJECT**。
- **K2（绝对超额）**：S 腿净 market-relative 均值 ≤ 0 → **REJECT**。
- 任一触发即判“无独立定价价值”，关闭该方向，不补网格（机制证伪按 D3 条件单先例）。
- 双过 → “有限 PASS”：仍不进 Live，转前瞻 paper 记录（从当日记出生，满 60 sessions 或 20 fills 重验）。

## 4. 必报审计数

Remap 漂移率（现 `cn_symbols` 名集合 vs 出生名集合不一致的卡片占比）+ 未映射率 + ambiguous 率 + 批数/批内相关 + 各 grade n。
