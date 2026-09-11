# H-SAT-RANK：桶内阶段重排（预注册 · PASS 2026-09-11）

> **H-SAT-RANK**：primary 桶不变（top-1/3 by amp，同尺寸），桶内按
> `(0 if S2&climax else 1 if S2|climax else 2, amp)` 重排，每日照拿满，开仓率 ~100%。
> **通过线（冻结）**：三窗 ≥2 窗收益超冻结基线。

## 1. 结果

| 窗 | 基线收益/笔 | 重排收益/笔/均值/命中 |
|---|---|---|
| OOS2 | +101.6 / 271 | +94.3 / 270 / +1.70 / 53.7% |
| train | +21.6 / 127 | **+43.1** / 126 / +1.67 / 54.0% |
| valid | −6.1 / 84 | **−2.4** / 84 / +0.19 / 50.0% |

wins=2/3 → **PASS**。自检：复刻器三窗 fills 与引擎逐笔一致。

## 2. 解读

* train 翻倍、valid 亏损收窄 60%，代价是 OOS2 让出 ~7pt——重排把 OOS2 的
  一些非阶段赢家挤掉了，风格更集中，方差结构变化。
* 开仓率 270/271、126/127、84/84——门不饿死人，这是它能过而 STAGE 双门
  不能过的全部原因。
* 有效成分：tier0/1 优先占据了满仓的 4 个槽位，把 tier2（S1/cool）的槽位
  挤掉——不是"选得更准"，是"烂票少占槽"。

## 3. 落地（done 2026-09-11）

* 冻结配方 t1430_b3（amp 排序）保持为基线不动。
* Live 推送（`twin_star_intraday` 14:30）primary 桶按 `stage_rank_key`
  重排（S2×climax → 任一 → 其他，amp tiebreak；池子尺寸/锁单/C1 都不动），
  每行带 `stage`/`stageTier`；`sat_push_log.stage` 同步记录（migration 0047）。
* 标签唯一真值收进 `state_bucket_track.stage_labels/stage_tier`，
  回测脚本改从 src 引，不再各写一份。

脚本：`scripts/compare_sat_rank.py`（自带复刻自检）；报告：`data/backtest_reports/h_sat_rank.json`。
