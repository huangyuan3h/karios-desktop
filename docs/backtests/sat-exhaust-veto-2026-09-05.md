# 耗尽顶否决 E-veto：用强股勺型耗尽跳过卫星追高（2026-09-05 启动）

> **一句话**：习惯双子星局部邻域已搜尽（C1/桶/R-wide/卖点/body/排名全到单峰，见 §0），下一刀不再拧旧旋钮。新假设只加一条**机制先验否决**：T-1 已出现 `strong_scoop_exhaustion`（ret60>0.40 & 放量>1.2x，形态文档冻结阈值，原样照抄，不扫网格）的缺口候选，卫星不买、strict 不补、空槽回核心。单假设、零网格、三窗 + 熊市回放验收，不过就关方向。
> **关键词**：习惯双子星 超越计划 E-veto 耗尽顶 预注册 单假设
> **状态**：计划已冻结，待跑。结论（PASS 或 REJECT 都留档）进本档 §4；任何情况下不自动进 Live。

**基线**：冻结习惯配方 `C1 3% + same_1430 + body=3 + 第3日14:30卖 + strict 4×12.5% + opp_50`，三窗 twin vs 核心 OOS2 **+76.3/+2.22/−1.9** · train **+14.5/+1.53/−2.7** · valid **+2.7/+0.21/0**（`sat-live-caliber-2026-09-04.md`），熊市回放 21/22/23 年 Δ **+25.9/+18.5/+21.1**（`sat-bear-replay-2026-09-05.md`）。
**预注册**：本档 §2–§3 即预注册（跑前冻结，跑中不改）。参考格式 `designs/sat-weight-6040-prereg-2026-09-05.md`。

---

## 0. 为什么是这条路（读完 48+ 次失败才定的方向）

局部邻域结论（全部三窗实测，不重开）：

| 已搜 | 结论 | 档 |
|------|------|----|
| C1 网格 2/3/4/5% | 3% 平顶 | `sat-c1-grid` |
| 桶 1/2 vs 1/3 | 1/3 维持 | `sat-bucketq` |
| R-wide 0.4/0.5/0.6 | 0.5 单峰，0.6 是过拟合陷阱 | `sat-rwide` |
| 第 3 日收盘/10:00/14:30 | 14:30 唯一 beats_core | `sat-exit-hhmm` |
| body=4 / 换分钟 | 占槽税 −16pt / 无更佳分钟 | `sat-habit-clock` |
| 无前视排名键 | gap 升序 OOS2 −96pt 永不重开 | `sat-rank-hhmm` |
| D3 条件单 | 三窗全拒，回吐≠反转 | `sat-exit-d3trail` |
| C3 下跌过滤 | 组合冗余 | `sat-c3-fade` |
| CHURN 4x | PASS/worse，记候选不进 Live | `sat-churn` |
| 市况加权 60/40 | V1 按门 PASS 但除 OOS2 外 ≤+1.7pt，V2 证伪；70/30 不开 | `sat-weight-6040-2026-09-05` |

推论：**再拧旧旋钮 = 拿单窗噪音当信号**（用户 09-05 已拍板 70/30 不开、滑动权重不动）。超越必须正交、低自由度、机制先行。

候选正交方向只剩两条活路（其余已死）：

1. **E-veto（本实验）**：形态因子库唯一 ≥80% 项——强股勺型耗尽做空 `ret60>0.40 & 放量 89.4% / n24k`，四时间桶 89–91% 稳定（`designs/pattern-factor-validation.md §2.4`）。卫星买的是“缺口 + 低波尾”，耗尽顶买的是“强动量派发点”，两者在强股区正面相撞。用耗尽作**跳过式否决**（不是加分项），自由度=1 个二值开关。
2. （备选，不在本档开）：核心 STOCK 腿同否决。先做卫星侧（核心冻结，对照干净），卫星侧 PASS 才另开新档做核心侧。

不做的方向：行业（`industry-style-plan` 144 行业正交拒收）、松 S-3 闸（10 变体全拒）、trail/−5%/砍篮（OOS2 崩）、波动率加权（G1 证伪：OOS2 高波最赚、holdout 高波全亏，不稳定）。

---

## 1. 手上有什么资源（本实验只用这些，不引入新数据）

### 1.1 数据表（Postgres，全部已入库）

| 表 | 本实验用法 | 现状 |
|----|-----------|------|
| `daily`（腾讯 qfq） | ret60、MA20/60、勺底/前高、vol；C1/开盘口径同习惯档 | 2021+ 全量，qfq 统一后残留 258 个真跳空（已知） |
| `bar_5min` | 习惯成交：入场/出场真 14:30 bar（入场 100% 真 bar，出场 ~5% 收盘回退，`sat-live-caliber §3`） | 56M 行，2021–2026 尾盘七根；三年 `skipNoPrint1430` 全 0 |
| `stock_dailybasic` / `stock_basic` | mv 过滤、无 mv 剔除、ST/BJ/退市过滤 | 21–23 已补数（含 revert 审计，`sat-bear-replay §4.3`） |
| `index_daily`（000001.SH） | 不用（本实验禁用市况维，防 G1 后再加闸） | — |
| `factor_signals` + `service/factor_signals_service.py:scan_strong_scoop_exhaustion` | 检测器已实现（ret60/vol/MA/勺深 5–18%/底近 15 日/收复逻辑与形态文档同口径，`_probability` 含 0.894 档） | 生产 stale（仅 2025-06-16 77 条 backfill，无日调度，`scoop-exhaustion-oos-check-2026-09-04`）——本实验**不依赖生产表**，回放内用同函数逐日重算，避免 stale 污染 |
| `etf_daily`（GOLD/BOND10/NASDAQ/OIL） | 核心腿冻结不动 | 21–22 只有 GOLD/BOND10（残缺核心已知，不影响卫星对照） |

诚实注脚：`scoop-exhaustion-oos-check` 判“做空 OOS 方法缺失”（detection-close 稻草人必止损），**本实验不借 89% 当证据**，只借“强股 + 圆回踩 + 放量 = 派发”的机制先验；否决式 skip-long 必须自己过三窗，不过就关。

### 1.2 引擎与脚本（只读复用，不改冻结引擎）

- 卫星回放：`service/state_bucket_track.py:replay_sgap_from_context`（已支持 `max_open_to_1430_pct` C1 / `max_t1_turnover_mult` CHURN / `rank_key` / `r_wide` / `exit_hhmm`；E-veto 新增实验参数 `skip_exhausted` 走同一守卫：非 `same_1430` 传参抛错，Live 默认 None）。
- 核心：`scripts/fused_timeline_walk.py --mode mom_compare`（trail8 冻结，不动）。
- 合成：`opp_50`（`satActive` 含退出日，无仓 100% 核心，strict 不补）。
- 三窗：`scripts/run_walk_forward.py`（OOS2/train/valid 固定切分，>5pt 劣化自动拒收）。
- 熊市回放：`scripts/replay_bear_habit.py`（2021/22/23 只读诊断）。
- 诊断电池：`scripts/diag_sat_exclusions.py`（S4 方法论：OOS2+train 双窗一致才进回放，最多带一个）。
- Holdout：`scripts/holdout_habit_check.py`（只读，弱不重开已拒、强不开新变体）。

---

## 2. 预注册冻结细则（跑之前定死，跑中不改）

- **假设一句话**：缺口低波候选里混入了强股耗尽顶，跳过它们能让卫星少买派发点，twin 超习惯基线。
- **否决定义（逐字抄形态文档，不调）**：T-1 收盘已知信息判定 `strong_scoop_exhaustion` 且 `ret60 > 0.40` 且 `vol_ratio > 1.2`（对应 `_probability 0.894` 档）。判定用 T-1 及更早日线（ret60 用 T-1/ T-61 收盘，vol 用 T-1/勺均量），**14:30 前完全已知，零前视**。qfq 内一致（收盘/收盘比值，量不受复权影响），无 C1 式的跨空间污染。
- **执行**：`fill_mode=same_1430` 下，桶内 top-1/3 命中否决 → skip（记 `skip_exhausted`，blotter 可审计），strict 不补，空槽回核心。C1/skip_t1/R-wide/body/卖点全部保持冻结习惯值。**只测这一个变体**，不加 `ret60 0.3/0.5`、`vol 1.0/1.5`、`prob` 网格（开了等于重走 H2/C1-grid 老路，预先禁止）。
- **基线**：同窗同配方冻结习惯 `c1_x1430`（gross；沿用熊市档 base/stress 抖动预算列示，不进引擎）。
- **窗口**：`OOS2/train/valid` 为拒收闸；`2021/2022/2023` 熊市回放为软闸（诊断）；`holdout 2026-08-08~` 只读确认，不参与判定。
- **口径**：window-local 空簿、clip4 4×12.5%、`opp_50`、成本 `COSTS_ROUNDTRIP=0.003` 与习惯档一致。

---

## 3. 拒收线（任一触发即 REJECT，关方向不补网格）

1. `OOS2/train/valid` 任一窗 twin-tot 相对习惯基线差 `< −5pt`。
2. 任一窗 sharpe 相对基线差 `< −0.3`。
3. `2022` 熊市年相对基线差 `< −5pt`（软闸：耗尽否决在熊市不能比躺平差一截）。
4. valid 相对**核心**转负（loses_core_tot）：即使超基线，只要 valid twin−core ≤ 0，仍判 REJECT（valid 缓冲只有 +2.7，不接受“比基线好但输核心”的变体进候选）。
5. 诊断电池未过（OOS2+train 无一致梯度）→ 连回放都不进，直接 REJECT（省算力，沿 S4 纪律）。

PASS 线：三窗 tot/sharpe 不差于基线（PASS）且至少一窗 tot **+2pt 以上**（PASS+ 候选；+2pt 以下如 weight-V1 的薄增益只记候选，不推 Live）。**任何情况不自动进 Live**，进不进用户另拍（沿 weight 档 §3）。

---

## 4. 执行步骤（一次一刀）

1. **诊断电池**（只看 OOS2+train，不碰 valid）：候选桶内命中否决的 fill 后续 3 日 pnl vs 未命中，同 S4 六维表格式。双窗同向为负才进 2。
2. **三窗回放**：新脚本 `scripts/compare_sat_exhaust.py --save-report`（复用 `compare_sat_churn.py` 结构），输出 twin tot/sr/dd vs 基线 + fills/skip 口径 + fillSrc 血统。
3. **熊市回放**：`replay_bear_habit` 同口径重跑 2021/22/23（含 stress 10/30bps 列示）。
4. **收尾**：本档补 §5 结果表 + 判定（PASS/REJECT）+ 是否记候选；`SUMMARY.md §1` 追一行；holdout 满 60 sessions 后重验（选参窗不变）。

复现（待第 2 步落地后填实测 hash）：

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_exclusions.py
PYTHONPATH=src:scripts python3 scripts/compare_sat_exhaust.py --save-report
PYTHONPATH=src:scripts python3 scripts/replay_bear_habit.py --save-report
```

---

## 5. 结果（2026-09-05 诊断电池实测 · REJECT，未进回放）

> **判定：REJECT/方向证伪**。耗尽候选在两窗都**好于**干净候选（不是差于），跳过它们等于跳过赢家。按预注册 §3.5 关闭方向，不补网格，不进三窗回放。

**脚本**：`services/data-sync-service/scripts/diag_sat_exhaust.py`（只读；口径同 S4：gate + skip_t1 + C1 3%，fwd = 14:30→第3日14:30 − 成本；valid 未碰）
**实现备忘**：`state_bucket_track` 上下文只有 amount 无 vol，第一版诊断全零命中；已用单查询 vol map 按 `factor_signals_service` 逐字重算，并在 2025-06-16 的 7 个 strict 信号（000506/603139/688386/002951/300682/301024/002104，ret/vr 与落库值逐数一致）上 bit 级验证通过。t<89 强制 False（MA60[t−30] 不存在）。

| 窗口 | exhausted 均值/胜率/n | clean 均值/胜率/n | 差（exh−clean） | 覆盖 |
|------|----------------------|------------------|----------------|------|
| OOS2 | −1.61%/40%/252 | −2.52%/31%/10458 | **+0.91pp**（反号） | 2.4% |
| train | +1.15%/42%/78 | −0.19%/43%/3133 | **+1.34pp**（反号） | 2.4% |

读法：

1. **方向反了，且两窗一致**——不是噪音，是机制错配：耗尽顶的 edge 在 20 天尺度（`hold_days=20`，目标=勺底下方），3 天脉冲窗里动量延续压倒派发，exhausted 反而少亏/多赚。拿 20 天的顶去否决 3 天的票， horizon 对不上。
2. 覆盖仅 2.4%，即使方向对也只能动 ±1pt 量级（CHURN 级），现在方向反，更无价值。
3. **Live 不动**（本来就没动）；引擎零改动，无需 revert；`factor_signals` 生产 stale 问题仍在（`scoop-exhaustion-oos-check`），与本结论无关。
4. 备选“核心 STOCK 腿同否决”**不开**：同一检测器、同一 horizon 错配嫌疑，且核心持有 60 天与 20 天信号更接近——但那是新假设，需另开预注册，不能拿本档的 REJECT 当 PASS 借尸还魂。

```bash
cd services/data-sync-service
PYTHONPATH=src:scripts python3 scripts/diag_sat_exhaust.py
```
