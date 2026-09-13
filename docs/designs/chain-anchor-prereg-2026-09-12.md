# 预注册：产业链 anchor → A 股概念传导（H-CHAIN-A · 2026-09-12）

> **单假设 · 零网格 · 拒收线跑前冻结。** B3 产业链推理链的第一刀（设计稿 `industry-chain-signal-2026-09-12.md`）。
> **关键词**：产业链 anchor 概念板块 传导 隔夜跳空 漂移 预注册

## 0. 一句话机制（§四 S1）
海外龙头（anchor）的基本面/量价强势 → 沿供应链传导 → A 股对应概念板块**中期（数周）漂移**。**关键分辨**：传导是"隔夜一次性兑现"（`§一.14` 不可交易）还是"慢漂移"（可交易）。

## 1. first-principles 自查（跑前）
- `§一.14 宏观信息消费端律`：海外信息在美股时段兑现，A 股开盘 gap 补上——**本实验正是该律的判决**：若效应全在 gap、`open→close`≈0、远期≈0 → 判死。
- `§五 维5 行业/主题 全吸收`：静态板块动量已被主线闸吸收 → 只有"**跨市场 anchor 领先**"这一格未测。

## 2. 数据（冻结）
- 概念板块指数（同花顺 `stock_board_concept_index_ths`）→ `data/chain/board_index.csv`（12 板）。
- 配对（curated，冻结）：苹果概念←AAPL / 消费电子←AAPL / 英伟达概念←NVDA / CPO←NVDA / 液冷服务器←NVDA / PCB←NVDA / 铜缆高速连接←NVDA / 数据中心AIDC←NVDA / 存储芯片←MU / 机器人概念←TSLA / 人形机器人←TSLA / 光刻机←ASML。
- Anchor 日线 `data/chain/anchor_us.csv`（AAPL/NVDA/MU/TSLA/ASML/AVGO/AMD/TSM）。

## 3. 对齐与口径（冻结）
- `anchor_ret(D)` = US 收盘 close(D)/close(D−1)−1（US 日 D）。
- **反应日 t** = 板块日历中**第一个 > D 的交易日**（US 夜盘信息在 A 股 t 开盘可见）。
- `gap(t)` = board open(t)/board close(t−1) − 1（隔夜兑现部分）。
- `intraday(t)` = board close(t)/board open(t) − 1（日内，可交易部分）。
- `fwd_N(t)` = board close(t+N)/board close(t) − 1（事件日收盘后起的漂移，N=5/10/20）。
- `anchor_mom20(D)` = anchor close(D)/close(D−20)−1。
- 窗口：OOS2/train/valid/long（板块可用历史为准，英伟达系从 2023-05 起）。

## 4. 指标 / 判定（冻结）
- **A 反应分解**：corr(`anchor_ret`, `gap`) 与 corr(`anchor_ret`, `intraday`)——量化"信息在谁时段兑现"。
- **B 慢漂移**：按 `anchor_mom20` 分**三档**（低/中/高），看 board `fwd_20` 高档−低档差值（pooled + 分窗）。
- **K1（传导存在）**：pooled corr(`anchor_ret`, `gap`) **> 0.2**（隔夜确实反应）。
- **K2（可交易性 / §一.14 判决）**：pooled corr(`anchor_ret`, `intraday`) **≥ 0.1**（日内有延续）**或** fwd20 高档−低档 pooled **> 0**。
- K1 ✅ 且 K2 ✅ → **CANDIDATE**（有慢漂移/日内延续，值得进引擎）。
- K1 不过 或 K2 不过（效应全在 gap）→ **REJECT**（`§一.14` 确认，本体系内不重开）。
- **不扫**：配对清单/动量档界/持有 N/anchor 定义。

## 5. 死因预判（跑前写死）
- 主：**`§一.14`**（效应全在隔夜 gap → 不可交易）。
- 次：`#2 共线`（概念=主题动量代理）、`#6 成分`（板块成分非 PIT，幸存者偏差）、样本（部分板 2023+）。

## 6. 产出
- 脚本：`scripts/diag_chain_anchor.py` → `data/backtest_reports/chain_anchor_2026-09-12.json`
- 结论落 `docs/backtests/chain/chain-anchor-2026-09-12.md` + SUMMARY 一行。

*冻结于 2026-09-12，跑数前。*
