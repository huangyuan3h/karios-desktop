# B18 游资解剖（头部席位 × 封板质量 × 情绪周期） · 归档于 2026-09-15

## 当时的目标（todo 链接）
- `docs/todo.md` P0-13 B18（2026-09-15 立）：回答"头部杀出来的游资怎么赚钱、我们能不能跟"——只读理解性研究，不进 Live。
- 预注册：`docs/designs/hotmoney-anatomy-prereg-2026-09-15.md`（E1–E5 + D0–D5 数据准备）。

## 实际做了什么
- 数据：席位级龙虎榜 `top_inst` 2021+（1,186,154 行/1,380 日）· 精确涨跌停价 `stk_limit`（7,137,212 行）· 全天 5min 事件集（本地 vendor CSV 259 万行 + baostock 长窗脚本就绪）· 周期变量从 daily+涨停价重建。
- 实验（全部按预注册冻结线）：
  - **E1 席位次日分解**（118.6 万营业部记录 → gap/intraday/d3）：**REJECT（K2+K3 FAIL）**。
  - **E2 席位时间结构**（隔夜 vs 接力）：**REJECT（披露截断）**。
  - **E4 情绪相位**（ON/MID/OFF）：**REJECT（K3 挂）**，相位梯度干净。
  - **E3 封板质量**（41,032 涨停日 × 全天 5min）：**PASS（K1/K2/K3 全过）**。
  - **E5 可执行缝**（回封 / 高质量次日）：**双 REJECT**。
- 基期修正：首跑 `daily`(qfq) × `stk_limit`(raw) 混用（同族第 3 次）；逐日匹配数异常抓出，`hotmoney_lib` raw 重建后 E1/E3/E4 重跑 + 事件集重导。新增不变量 `first-principles §二.8 基期单一律`。

## 验证 / 数据
- **E3（唯一 PASS）**：first_seal G1/G3 `gap_ex` OOS2 **+3.69/+1.15** · train +3.40/+1.45 · valid +2.77/+1.06；sealed_share G3/G1 **+4.39/+0.54** · +3.76/+0.92 · +2.92/+0.71；剔买不进一字后梯度保留；高质量档扣 30bp 仍 +1.9~2.3%。
- **E1**：head `gap_ex` long +0.23 < inst +0.43（三窗差值 −0.03/−0.25/−0.43）；剔一字后 head 转负（long −0.15）；**拉萨（散户）组 −2.23**。
- **E2**：头部席位 T+1 卖出可见率 17.5%（inst 15.0），20/20 席位 mixed → 退出不可观测。
- **E4**：`gap_ex` ON +0.27 / MID −0.27 / OFF −0.91（ON−OFF 三窗 +1.49/+1.53/+1.08）；d3 全相位负。
- **E5**：E5a 回封 −2.92（三窗 −2.51/−2.59/−0.69）；E5b 高质量次日 −1.63（三窗 −1.83/−1.60/−0.91）。
- 结论一句话：**头部游资的钱 = 自己封板 + 吃自己制造的隔夜缺口（E3）；公开数据三面封死——进（E1/E5）、出（E2）、相位只解释缺口大小（E4）。**

## 后续影响 / 留给谁
- 报告：`docs/backtests/hotmoney/`（6 份：seat-flow / seat-timing / cycle / board-quality / board-quality-phase1（作废留档）/ exec-probe）；SUMMARY 5 行。
- 工具/数据：`scripts/hotmoney_lib.py`（qfq→raw）、5 个 sync/import 脚本、4 个 diag 脚本；`data/lhb/lhb_seats.csv`、`data/limit/stk_limit.csv`、`bar_5min` 事件集。
- **遗留（不影响结论）**：E3 long 窗 2021–23 全天 5min 待 baostock 限流解除（脚本 `backfill_5min_event_days.py` 修正版就绪，35,364 日/8,410 job）；D3 EM 涨停池向前快照未启动（历史仅剩 ~2 周，只能为未来攒）。
- 纪律沉淀：`first-principles §二.8 基期单一律`（qfq 系列不得直接对 raw 价格水平做等值比较；先逐日自检匹配数）。
