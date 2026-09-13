# 预注册：大宗溢价动量中性化复验（H-BLK-B · 2026-09-12）

> **单假设 · 零网格 · 拒收线跑前冻结。** 这是 H-BLK-A 的**唯一复活条件**（见 [block-premium](../backtests/inst/block-premium-2026-09-12.md) §4）。
> **关键词**：大宗溢价 动量中性 共线 复验 预注册

## 0. 一句话机制（§四 S1）
H-BLK-A 发现"溢价大宗 > 大折价"的前瞻超额（N10 +0.82%、N20 +1.44%）。**但它可能是动量的换皮**——溢价大宗常发生在**已上涨**的票上（买方追价接货）。本实验问：**在同动量档内，溢价 vs 大折价的超额是否仍存在？**

## 1. first-principles 自查（跑前）
- `§五 结构吸收律 维1 动量`：单票动量已被 RS/主线闸**全吸收** → 若溢价超额 = 动量代理，则**无独立增量**（`#2 共线`）。
- `#1 截右尾` / `median 负`（H-BLK-A）：先验偏"右尾/动量"，本实验是**判决性去伪**。

## 2. 口径（冻结）
- 事件：同 H-BLK-A（`data/block/block_trade.csv`，A 股，T 日盘后披露 → T+1 开盘买）。
- 分组：**只保留 `premium`（prem>0）与 `big_disc`（prem≤−5%）** 两组（K2 对）。
- **动量 `mom20(T)`** = `close(T)/close(T−20) − 1`（`daily`，T 及之前，因果）。
- **动量档（固定 3 档）**：`down` mom20<0 / `mid` 0≤mom20<10% / `up` mom20≥10%。
- 前瞻超额：vs 中证500，T+1 open → T+1+10 close（**主 N=10**，与 H-BLK-A 的 K2 同口径）；N5/N20 仅上下文。
- 窗口：OOS2/train/valid/long（同前）。

## 3. 指标
- `Δ_b` = mean(excess|premium,档 b) − mean(excess|big_disc,档 b)，主 N=10。
- `Δ_neutral` = 按档内样本量加权的 `Δ_b` 平均（档内配对差）。
- 混杂见证：P(动量档 | premium) vs P(动量档 | big_disc) 分布。

## 4. 判定（冻结）
- **K1（中性后仍在）**：`Δ_neutral(long) > 0` 且 OOS2/train/valid **≥2/3** 为正。
- **K2（非单档）**：long 下 **≥2/3** 动量档 `Δ_b > 0`。
- K1 & K2 皆过 → **复活**（值得开引擎回放，仅限"溢价大宗"腿，且需与动量正交）。
- 任一不过 → **REJECT**（溢价=动量换皮），**永久关闭**，不重开。
- **不扫**：动量档边界/窗口/持有 N/超额基准。

## 5. 死因预判（跑前写死）
- 主：**#2 共线**（溢价超额被动量解释 → Δ_neutral 塌向 0 或反号）。
- 次：`#3 方向证伪`（档内翻转）、样本（溢价&某档可能稀）。

## 6. 产出
- 脚本：`scripts/diag_block_momneutral.py` → `data/backtest_reports/block_momneutral_2026-09-12.json`
- 结论落 `docs/backtests/inst/block-momneutral-2026-09-12.md` + SUMMARY 一行。

## 结果（跑后补）
**PASS → REVIVE**：K1 ✅（Δ_neutral long +0.82，四窗全正 +0.29/+1.57/+1.61/+0.82）、K2 ✅（动量三档 Δ 全正 +0.98/+0.70/+0.69）；动量分布显示溢价组**偏下跌档**（45.7% vs 折价 39.7%），"溢价=追强势票"直觉不成立。见 [`block-momneutral-2026-09-12.md`](../backtests/inst/block-momneutral-2026-09-12.md)。

*冻结于 2026-09-12，跑数前。*
