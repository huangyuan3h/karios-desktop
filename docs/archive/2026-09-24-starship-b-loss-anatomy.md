# B22 星舰 B 卫星左尾解剖 + H-SAT-AMP-CAP · 归档于 2026-09-24

## 当时的目标（todo 链接）

- `docs/backtests/stable/starship-b-loss-analysis-2026-09-24.md`（todo P0-13 B22）：用户 2026-09-24 提出——"星舰 B 选作 Live 方向，分析它亏钱的原因和天数"→"有没有避免的可能"。
- 序列 S1（进场干净信号）→ S2（篮子结构）→ S3（篮子级早退）。

## 实际做了什么

- **亏损解剖**（`analyze_starship_b`）：long +644.0% / MDD −5.1% / SR 3.79 / 胜率 61%；亏损=卫星脉冲 3 日持有左尾（477/1080 亏、全 `body_exit`、无止损）；停放腿只护闲置现金。
- **聚簇**（`diag_sat_cluster`/`diag_sat_mkt_trend`）：离散非 regime；同日 4 只**≈一个赌注**（n=4 全亏 2.6× 独立、组内 std 0.49）；持续性只在 lag1–2（=body=3 重叠持有）、lag6+ 噪声；入场在大盘均线上方但均线**不预测亏损**。
- **S1**（`diag_sat_entry`）：gap 关闭（corr −0.01）；**clean `amp1430` 存活**——corr −0.175、三窗一致 −0.31/−0.29/−0.17、**同日截面 −0.165**（真进场质量）。
- **S1b 回放**（`eval_sat_ampcap`，预注册 `designs/h-sat-amp-cap-prereg-2026-09-24.md`）：臂 A `bucket_q=4` ≈ no-op；臂 B `max_amp_1430_pct=1.0%` 右尾保留 ✅、valid/train/OOS2 回撤+Sharpe 改善，**但 long total −68pt → K1/K2 FAIL**。引擎新增可选 `max_amp_1430_pct`（默认 None，Live 不传；`test_state_bucket_track` 66 绿）。
- **S2**（`diag_sat_industry`）：306 个多成交篮子**仅 1 个全同行业** → 行业不集中，单题材上限无效，关闭。
- **S3**（`diag_sat_cohort_exit`）：篮子级 day-2 联合走弱提前出 **Δ −24~−63pt**（red@mid 的票 mid→exit 仍 +0.40%，砍在坑里），关闭。

## 验证 / 数据

- **结论：无免费午餐**。S1–S3 全结，唯一可落地产物 = **臂 B（amp≤1%）= 降波动/降左尾换 ~11% 长期收益**的防守档（非 PASS，待用户拍板风险偏好）。
- 预注册两处：`designs/vix-regime-prereg-2026-09-24.md`（B21，另案）、`designs/h-sat-amp-cap-prereg-2026-09-24.md`（B22）。
- 复现脚本全部在 `services/data-sync-service/scripts/`（见档 §7）。

## 后续影响 / 留给谁

- LIVE **不动**（仍=港湾；星舰 B 为研究/展示/人工档）。臂 B 若要落地是**资金结构/风险偏好**决定，需用户授权，不自动进 Live。
- 顺带交付：VIX + CN/US 收益率曲线接每日增量（`sync_vix`/`sync_bond_yields`，B21 同批）。
- 不需要补 OPT/TIP。
