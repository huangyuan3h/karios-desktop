# 港湾停车统一到 H2 · 预注册（H-H2-UNIFY · 2026-09-18）

> **单假设 · 跑前冻结 · 用户 2026-09-18 已拍板方向。**
> 把 ETF 闲置停车的**唯一实现**收敛为 **H2 迟滞**（换仓需新目标 `mom60` 领先现持 ≥2pt；被挡＝保留旧腿），
> 让 **Live / paper / 回测 / Timeline / watchlist / recon 全部走这一套**；canonical（立即换仓）降级为**研究参考**。
> **关键词**：港湾 停车 H2 统一 单源 Live paper watchlist 预注册

---

## 1. 决策依据（已有证据，不在本稿重开）

- **多窗综合（2026-09-18 · `h2_vs_canonical_windows.json`）**：9 个额外时间段 + 原三窗，港湾线 H2 vs canonical：
  收益 5胜4负（均值 **+3.4pt CAGR**）、夏普 5胜4负、回撤 **3胜6负**；用户权重（收益0.5/夏普0.3/回撤0.2）综合 **+0.022（平局偏 H2）**。
- **可操作性**：H2 停车换手 **−52%**（9/9 窗口都更少；full 127→60），真实账户人工执行 → 少一半买卖/盯盘/手误。
- **用户价值取向（2026-09-18 明确）**：**收益 > 夏普 > 回撤** + **整体容易操作**。
- **现状操作负担**：停车有 **3 套实现**（`harbor.parking_replay` / `parking_sleeve.hysteresis_parking_replay` / 旧 `third_asset_sleeve`），
  正是「不好操作」与 2026-09-17 H2 机制 bug 的根源。

**结论**：在「收益优先 + 易操作」下，H2 三维占优，唯一代价是 long MDD 略深（用户最低优先级）。**这不是数据变了，是用新的价值取向重裁 HK3（原差 0.1pt 被拒）。**

## 2. 冻结口径（本稿锁死，不可网格）

| 项 | 值 |
|---|---|
| 迟滞带 `HYST_BAND` | **0.02（2pt）**，不扫 1/3/5（H3/H5 已 REJECT） |
| 规则 | trail8 优先（出场回 REPO，不当日再入）；换仓需 `want.mom60 − held.mom60 ≥ 2pt`，否则**保留旧腿**（不卖、不空窗、峰值续算）；无候选/破 MA200 → REPO |
| 成本 | 5bp/边（回测）/ paper 0.1% 往返 |
| 覆盖面 | Live 港湾 + 母港/星港/双子星底座 + 星舰 + Timeline + watchlist + paper + recon + eval 默认档 |
| canonical | 保留为**研究参考**（显式 `hyst_band=0.0` 传入），不再是任何 Live/展示默认 |
| T6 `third_asset_sleeve` | 退役（OPT-179） |
| H2 影子账本 | 退役（Live 即 H2，影子无意义） |

## 3. 预期影响（引用既有数字，本稿不预测新值）

- 港湾线：OOS2 +55.2→**+57.6** / train +52.2→**+61.5** / valid +50.3→**+49.7** / long +207.9→**+219.2**（MDD −22.8→−23.9）。
- 星舰：A2_true +924.9 → A2_H2 **+975.0**（已在用）。
- 换手：full 127→60。

## 4. 验证门（代码改完必须全过，否则回退）

- **V1 单源**：`git grep` 确认停车状态机只有一处（`harbor.parking_replay` + `hyst_band`）；`hysteresis_parking_replay` 仅为薄委托。
- **V2 Live 对账**：`verify_harbor_live_vs_backtest.py` **必须 100%**（Live 决策 vs 回测 Timeline 决策）。
- **V3 测试**：`pytest tests/test_harbor_parking.py tests/test_parking_sleeve.py tests/test_multi_asset_* tests/test_parking_universe.py tests/test_sleeve_* tests/test_harbor_h2_shadow.py`（后者随退役调整）+ 全量。
- **V4 DB 干净**：`python3 scripts/db_rows_baseline.py check` OK（27 表零变化）。
- **V5 展示一致**：Timeline/paper/recon/watchlist 对同一日 pick 一致（决策单源）。

## 5. Live 红线与回滚

- 本次为 **Live 策略变更**：paper 继续镜像观察；**若 paper 累计落后旧口径 >2pt 或 MDD 深 >2pt → 回滚 canonical**（沿用影子账本阈值语义）。
- 变更需记入 `strategy-params.md` 变更表 + `strategy-recipes.md` §0.3/§1–§5 + `optimization-checklist.md`。
- 用户已授权方向；落地后补 `docs/backtests/stable/` 结果档。

## 6. Kill criteria（任何一条挂 → 不落）

- verify 对账 <100%；
- 全量 pytest 红；
- `db_rows_baseline.py check` 非 OK；
- 单源检查发现第二处停车状态机。

---

## 7. 落地记录（2026-09-18）

**已实现**：`harbor.parking_replay` 加 `hyst_band`（产品 `HYST_BAND=0.02`，默认 0.0 保留研究复现）+ `held_mom`；
`build_harbor_timeline` / `apply_parked_display` / `multi_asset_sleeve`（Live/paper/watchlist/recon）全走产品 band；
`parking_sleeve.hysteresis_parking_replay` 降为薄委托（唯一状态机）。**验证全过**：`verify_harbor_live_vs_backtest` 473/473=100%、
全量 pytest 4378 passed/3 skipped、`db_rows_baseline.py` 零变化、ruff clean。详见 [结果档](../backtests/stable/harbor-h2-unify-2026-09-18.md)。

**本稿 §2 中「H2 影子账本退役」未在本轮执行**（纯展示/冗余，行为不变）→ 另立 **OPT-226**；旧 T6 退役仍归 OPT-179。
