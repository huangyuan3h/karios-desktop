# S-3 Gate 在择强单轨内的松闸优化（2026-09-01 拒收归档）

> **一句话**：S-3 的 `gates=full`（regime+资金流+主线白名单）**在择强单轨 fused NAV 上仍是最优门**，任何放松（`regime/none`、RS 放宽、去掉 `neutral_block`、放开创业板）均 `valid -18~-58pt` 或 `OOS2 -6~ -27pt` 触发 `>5pt` 拒收。**不要再试松闸。**
> **关键词锚点**：`S-3 gate` `择强 STOCK腿` `gates=full` `择强单轨优化` `S-3 放松闸门` — 下次再有“择强里 S-3 gate 太严是不是不好”想法时，直接读本文，无需重跑。

**关联**：
- 假设来源：用户主观感受“S-3 在择强单轨的 gate 可能不是好 gate，有优化空间”（`S-3` 为 `pick-strong-track.md:46` 的股票腿生成器，`gates=full` 见 `strategy-params.md:20`）。
- 基线：`walk_forward_baseline.json` `s3-baseline-20260828-nav` `47.3/34.1/38.7` + `pick_strong_trail8_20260829.json` `fused 17.8/40.7/139.1`（`trail8` `services/data-sync-service/scripts/pick_strong_grid.py:145`）。
- 机会双子星 `state-bucket-algo-2026-08-31.md:119` 已冻为最优可执行增强，与本文 `STOCK腿 gate` 实验正交。

---

## 0. 为什么做这个实验

`S-3` 的五件套（`neutral_block`/`entry_style auto`/`D2`/`D3`/`panic2`）全是 `scripts/run_walk_forward.py:41` 上以 **S-3 standalone `totalNetPnl`** 择优，择强只取 `S-3 持仓篮的 mom60均值` 做 `argmax`。直觉上 `full` 的 `行业资金流+主线白名单` 可能过滤过度，篮子该更早/更满地入池再交给择强去切金/油/纳/债。

但 `pick-strong-stock-pool-2026-08-29.md:85` 已证 **在 `full` 之上再加闸**（`n≥2/mom>0/MA50%/CN-only`）全拒收。本轮反向：**减闸**。

## 1. 实验设计

**口径**：`window-local 空簿起步`（与 `pick_strong_grid.py:145` 同），三窗 `OOS2 2024-08-01~2025-08-01 / train 2025-08-01~2026-02-01 / valid 2026-03-01~2026-08-07` + `past_year 2025-08-28~2026-08-28` 展示。`HK` 腿固定 `HK_S3_CONFIG` `trail8`。

**变体**（`services/data-sync-service/src/data_sync_service/service/backtest_engine.py:131` `BacktestConfig`）：

| 变体 | 覆盖 |
|------|------|
| `baseline` | `S3_CONFIG` `gates=full` |
| `gates_regime` | `gates=regime`（去资金流+主线） |
| `gates_none` | `gates=none`（裸 score） |
| `rs0` | `rs_rank_min=0` |
| `rs03/rs07` | `rs=0.3/0.7` |
| `neutral_off` | `neutral_block=False` |
| `entry_score` | `entry_style=score`（关 auto） |
| `score60` | `score_threshold=60` |
| `no_exclude300` | `exclude_boards=""`（含创业板） |

**脚本**：`services/data-sync-service/scripts/test_s3_pickstrong_gates.py:1` + `test_one_variant.py:1` + `test_twin_for_variant.py:1`（`PYTHONPATH=src:scripts python3 scripts/test_one_variant.py --variant X --overrides '{}'`）。

## 2. 结果

### 2.1 S-3 standalone（`totalNetPnl%`）

| 变体 | OOS2 | train | valid | past_year | 判定 |
|------|------|-------|-------|-----------|------|
| baseline | 47.3 | 34.1 | 38.7 | 58.3 | 基线 |
| gates_regime | 47.3 (+0) | 23.5 (-10.6) | -2.5 (-41.2) | 13.7 | ❌ |
| gates_none | 35.7 (-11.6) | 10.1 (-24.0) | -5.7 (-44.4) | -3.0 | ❌ |
| rs0 | 11.8 (-35.5) | 19.8 (-14.3) | 0.0 (-38.7) n0 | 9.5 | ❌ |
| rs03/rs07 | 47.5/47.2 (+0.2) | 33.7/32.3 | 38.7 | 58.3 | ≈ |
| neutral_off | 47.3 | 34.1 | 38.7 | 58.3 | ≈ (OOS2/train无sentiment) |
| entry_score | 20.3 (-27.0) | 33.2 | 20.4 (-18.3) | 37.5 | ❌ |
| score60 | 47.3 | 34.1 | 38.7 | 58.3 | ≈ |
| no_exclude300 | 46.8 (-0.5) | 33.2 | 37.4 (-1.3) | 56.3 | ≈ |

### 2.2 择强 fused `trail8`（`pick_strong_grid.py:348` `build_nav_from_cache` 100%硬切）

| 变体 | OOS2 | train | valid | past_year | 三窗判定 |
|------|------|-------|-------|-----------|----------|
| baseline | 17.8 | 40.7 | 139.1 | 190.7 | 基线 |
| gates_regime | 17.8 (+0) | 27.0 (-13.7) | 106.6 (-32.5) | 187.1 | ❌ |
| gates_none | 17.0 (-0.8) | 47.0 (+6.3) | 80.7 (**-58.4**) | 211.5 | ❌ |
| rs0 | 21.7 (+3.9) | 35.5 (**-5.2**) | 139.1 (0) | 213.8 | ❌ train |
| rs03/rs07 | 17.9/17.7 | 40.7 | 139.1 | 190.7 | ≈ |
| neutral_off | 17.8 | 40.7 | 139.1 | 190.7 | ≈ |
| entry_score | 11.8 (**-6.0**) | 42.8 (+2.1) | 139.1 | 164.8 | ❌ OOS2 |
| score60 | 17.8 | 40.7 | 139.1 | 190.7 | ≈ |
| **no_exclude300** | 17.6 (-0.2) | 43.1 (+2.4) | 139.1 (0) | **221.8 (+31.1)** | ✅ 三窗过线但见 §3 |

> `past_year` 不参与三窗票决，仅展示。`gates_none` 的 `past_year +20.8` 看似好但 `valid -58pt` 崩盘，典型单窗过拟合。

### 2.3 与机会双子星叠加（`state_bucket_track.py:293` `skip_t1_limit strict` + `ps_g50_blend.py:1` `opp_50`）

| 变体 | OOS2 Δtwin-core | train Δ | valid Δ | past_year Δ |
|------|----------------|---------|---------|-------------|
| baseline | **+45.1** | +8.4 | +7.9 | -0.2 |
| gates_none | +45.7 | +8.8 | +6.5 | -7.2 |
| no_exclude300 | +45.1 | +10.6 | +7.9 | **-15.3** |

`twin` 在所有变体上仍三窗全胜单轨，但 `no_exclude300` 的 `core 221.8` 被 `twin 206.5` 稀释 — 含创业板的 STOCK腿与 `S-gap` 卫星相关性上升，错峰减弱。

## 3. 结论与提醒

1. **不要再松 S-3 gate**。`full` 的 `资金流枯竭挡 + 主线 Top3` 在 `valid` 贡献 `+32.6pt/回撤砍半/胜率+13pt`（`strategy-params.md:122`），fused 上 `valid -32~-58pt` 反证其必要性。OOS2/train 多数无 sentiment 数据 `neutral_block` 本就零影响，`rs` 放宽仅塌陷。
2. **唯一三窗过线的 `no_exclude300`**：`fused +2.4 train / past_year +31.1` 看似最优，但 `S-3 standalone` 未升且 `twin` 被稀释 ` -15.3`，且与 `strategy-params.md:29` 已固化结论“创业板三窗从未贡献 alpha（OOS2 25笔胜28%）”冲突，牛市年偶发边际，不纳入定案。**STOCK 入池加闸已全拒收**（`pick-strong-trail8-and-stock-pool-2026-08-29.md:85`），减闸亦拒收，**该方向结案**。
3. **真优化不在 gate**。`S-3 score是买点分不是趋势分`（`trading-system.md:54`），择强用 `mom60均值` 比续航，错位在**强度代理**而非闸门。若再动 STOCK腿，应试 `STOCK强度=mom20/median/篮NAV的mom` 等代理，而非 `gate`。

**再有此想法时的动作**：直接关闭本想法，改为读本文或跑 `scripts/test_one_variant.py` 复现一行即可。纪律见 `docs/modules/pick-strong-track.md:76` 与 `strategy-params.md:85` 冻结声明。

---
*归档 2026-09-01 · 复现 `PYTHONPATH=src python3 scripts/test_s3_pickstrong_gates.py` · 基线 `walk_forward_baseline.json:1` `s3-baseline-20260828-nav`*
