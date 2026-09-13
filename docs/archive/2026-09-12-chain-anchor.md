# P0-13 B3 产业链 anchor→A股概念传导（H-CHAIN-A） · 归档于 2026-09-12

## 当时的目标（todo 链接）
- `docs/todo.md` → **P0-13 P2④ B3 产业链推理链**。用户直觉："大 V 真正的点是产业链推理链（上下游传导）——英伟达服务器出货多→上游采购多；苹果带动果链。"
- 设计稿 `docs/designs/industry-chain-signal-2026-09-12.md` 预判头号风险 = `§一.14`（海外事件→A股隔夜 gap）。

## 实际做了什么
- 数据构建 `scripts/sync_chain.py` → `data/chain/`（gitignored）：**12 条配对**（同花顺概念指数 ← 美股龙头）——苹果←AAPL、消费电子←AAPL、英伟达←NVDA、CPO/液冷/PCB/铜缆/AIDC←NVDA、存储←MU、机器人/人形←TSLA、光刻机←ASML；anchor 美股 `stock_us_daily`（8 只）。
- 只读诊断 `scripts/diag_chain_anchor.py`（预注册 `docs/designs/chain-anchor-prereg-2026-09-12.md`）：对每个 board-day 映射最近的 anchor 美股 session，分解**隔夜 gap / 日内 intraday / 远期漂移**。

## 验证 / 数据
- **REJECT（§一.14 确认）**：`anchor_ret → gap` **corr +0.318**（传导存在，逐配对 0.23~0.41 全正）；`anchor_ret → intraday` **corr −0.01**（逐配对 ≈0）；`anchor_mom20 → 概念 fwd20` high−low **−0.11%**（long；OOS2 +3.41/train −0.58/valid +4.63/long −0.32）。
- **结论**：产业链信息 = **A 股消费的海外信息**，在 A 股开盘**一次性 gap 兑现**，盘中零延续、无慢漂移 → 日线级**不可交易**。
- **§一.14 增补证据**：本律**不限于宏观**——海外龙头/产业信息同理（写进 first-principles）。

## 后续影响 / 留给谁
- **B3 概念层关闭**（本体系内不重开）。设计稿中"慢漂移"假设被否；效应 100% 在 gap。
- **深挖前置（parked）**：唯一可能存活的是"**个股级供应链映射**（识别二阶/未被映射的供应商）"，需 **PIT 供应链数据**（概念成分历史 / 主营关联），当前不可得且幸存者偏差大；先验更弱。设计稿 `industry-chain-signal-2026-09-12.md` 保留。
- 新增只读数据资产：`data/chain/{board_index,anchor_us,meta}.csv`。
- 无新 OPT/TIP；不改 Live/schema。

## 关联档
- 实验：`docs/backtests/chain/chain-anchor-2026-09-12.md`
- 预注册：`docs/designs/chain-anchor-prereg-2026-09-12.md` · 设计稿 `docs/designs/industry-chain-signal-2026-09-12.md`
- 脚本：`scripts/sync_chain.py` · `scripts/diag_chain_anchor.py` · 报告 `data/backtest_reports/chain_anchor_2026-09-12.json`
