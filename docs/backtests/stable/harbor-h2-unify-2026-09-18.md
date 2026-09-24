# 港湾停车统一到 H2（H-H2-UNIFY · 2026-09-18 · 已落地）

> **一句话**：把 ETF 闲置停车收敛为**唯一实现** `harbor.parking_replay(hyst_band=HYST_BAND=2pt)`，
> **Live / paper / Timeline / watchlist / recon 全部走它**；canonical（任何变化就换）降级为研究参考。
> 依据 = 多窗综合「平局偏 H2」+ **换手 −52%** 的可操作性 + 用户价值取向（收益>夏普>回撤+易操作）。
> **关键词**：港湾 停车 H2 统一 单源 Live paper watchlist 换手

**预注册**：[`docs/designs/harbor-h2-unify-prereg-2026-09-18.md`](../../designs/harbor-h2-unify-prereg-2026-09-18.md)
**脚本/报告**：`scripts/diag_h2_vs_canonical_windows.py` · `data/backtest_reports/h2_vs_canonical_windows.json`

---

## 1. 决策依据（多窗 + 可操作性）

- **9 个额外时间段 + 原三窗**（`h2_vs_canonical_windows.json`）：
  - 收益 5胜4负（均值 **+3.4pt CAGR**）、夏普 5胜4负、回撤 **3胜6负**（H2 略深，用户最低优先级）。
  - 用户权重（收益0.5/夏普0.3/回撤0.2）综合 **+0.022** = **平局偏 H2**；加入可操作性后明确胜。
- **可操作性（决定性）**：H2 停车换手 **−52%**（9/9 窗口都更少；full 127→60、train 23→7）——真实账户人工执行，少一半买卖/盯盘/手误。
- **实现收敛**：原 3 套（`harbor.parking_replay` / `parking_sleeve` / 旧 T6）→ 1 套；2026-09-17 H2 机制 bug 的根因（两份实现不一致）被结构性消除。

## 2. 代码变更（单一状态机）

| 文件 | 变更 |
|---|---|
| `service/harbor.py` | `parking_replay(..., hyst_band=0.0)` 新增迟滞闸；新增 `HYST_BAND=0.02`、`held_mom()`；`build_harbor_timeline` 传 `HYST_BAND`；记录加 `hyst_blocked` |
| `service/parking_sleeve.py` | `hysteresis_parking_replay` 降为薄委托（删除重复循环/`_held_mom`） |
| `service/multi_asset_sleeve.py` | Live 决策（watchlist/paper/recon）换仓前比对 `held_mom`，不足 2pt → `HOLD`（`hystBlocked`）；新增 `_held_leg_mom` |
| `service/state_bucket_track.py` | `apply_parked_display` 统一走 `harbor.parking_replay(hyst_band=HYST_BAND)`（`sleeve_mode` 仅兼容保留） |

`parking_replay` 默认 `hyst_band=0.0`：**研究脚本/冻结实验不受影响**（canonical 可复现）；产品路径显式传 `HYST_BAND`。

## 3. 验证（全过）

- **Live 对账**：`verify_harbor_live_vs_backtest.py` → **473/473 = 100.0%**（OOS2/train/valid）。
- **测试**：全量 `pytest` **4378 passed / 3 skipped**；新增 `test_extra_sleeve_h2_blocks_noise_rotation`（H2 阻断噪音换仓）。
- **DB**：`db_rows_baseline.py` save→全量→check **OK**（零变化；先前一次 FAIL 系 live dev 后端写入，非测试）。
- **ruff**：changed files clean。

## 4. 边界 / 回滚

- 这是 **Live 策略变更**：paper 继续镜像；**若 paper 累计落后旧 canonical >2pt 或 MDD 深 >2pt → 回滚 `hyst_band=0`**（阈值沿用原 H2 影子语义）。
- **冗余清理已完成（OPT-226 / OPT-179，2026-09-18）**：`?strategy=harbor_h2` 验证线、`harbor_h2_shadow` 18:35 影子账本与调度、旧 T6 `third_asset_sleeve`（+ `thirdAssetSleeve/Holding` 展示面、`/sleeve-nav`、前端 `SleeveNavCard`/`ThirdAssetHealthBlock`、ai-service 字段）全部退役。验证：后端全量 4345 绿、shared 87 绿、前端/ai-service typecheck + lint 0 error（2 例前端失败为 HEAD 既有、与本次无关）。
- 五策略记分卡的「港湾」行、`eval_etf_parking_baseline` 的 P1 臂仍为 **canonical 参考值**；重跑切 H2 见 **OPT-227**。
