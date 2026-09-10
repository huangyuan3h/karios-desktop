# Karios 交易逻辑改进 Checklist

> 业务执行栈（TIP/V6/V7）：交易 / 业务规则与策略校准怎么做（非纯工程性能）。
> **正文只留 3 条未完成**（TIP-010 blocked、TIP-014 尾巴、TIP-015-M5）；已完成全部按主题归档到 [`archive/`](archive/)（见文末索引表）。
> **不做**：用东方财富条件选股替换 TV 第一层（评估结论：无必要；东财继续承担行业资金 / 主线）。

---

## 如何使用

1. 按 ID 顺序执行下面 3 条（TIP-010 等用户数据，平时只看 014 尾巴 / 015-M5）。
2. 每项完成后将 `[ ]` 改为 `[x]`，填写 **完成日期**，并按主题归档（仿 git 历史）+ 正文替换为索引行。
3. 「预期收益」是相对卫星仓纪律系统的定性评估，不是回测保证收益。
4. 若实施中方案有变，**就地更新本文件**，不要另起文档；策略文案同步更新 `docs/modules/`。

### Agent 任务模板（见仓库根 AGENTS.md → Scoped optimization tasks；本文件只补一条）

> 开独立会话，把对应条目整段粘贴为 scope；只改列出的文件 / 配置范围（含对应业务文档）；完成后标 `[x]` + 完成日期；不扩 scope 到其他 TIP。

---

## 未完成（4 条）

### TIP-017：跨市场风险状态传感器族（广度闸 / 国家队代理 / 散户共振 · 数据基建落地）

**状态**：[x] 数据基建 + A/B 实验完成 2026-09-09；[x] C Phase 0/1 诊断完成 2026-09-10（REJECT/无候选进 Phase 2）；[x] **B 已进冻结 2026-09-10**（用户拍板）
**来源**：2026-09-09 用户直觉——"6-9 月国家队撤退、情绪没恢复，但系统还在买可能亏的资产；
缺的是'谁在买'的核心数据；散户共振等'无法描述但重要'的信息想被量化"。
**预注册**：[`backtests/risk-state-sensors-2026-09-09`](backtests/risk-state-sensors-2026-09-09.md)
**裁决（2026-09-09）**：**A 广度闸 no-op REJECT**（分散化 6 资产宇宙的 MA200 广度跌不到 3——
债券/黄金/油总有上的；唯一触发变体 cooldown -36pt；且 2021~2023-10 双 ETF 未上市不可评估）。
**B 国家队闸协议 PASS → 已进冻结（2026-09-10 用户拍板）**：long **+20.0pt**（2021~2023-04 资金流旱段跳过 2022 熊市主跌段），
三窗全 +0.0（现代完全惰性——国家队持续申购），past_year +0.0。接线：引擎 `national_team_gate` + live `paper_s3._national_team_blocked`
+ S3_CONFIG 三处镜像；long 复现 78.6→98.6（逐数一致）；基线重固化 `s3-cn-baseline-20260910`（sha `5ea07cb964be`）。
**C 预注册已起草**（2026-09-10 · [`backtests/factors/candidate-c-flow-resonance-2026-09-10`](backtests/factors/candidate-c-flow-resonance-2026-09-10.md)）：**方法学修正**——市场级时序不能用 TIP-013 截面 Rank IC，改为条件前瞻收益 + 共线诊断 + gate walk-forward；主假设 = 两融去杠杆低分位 + 趋势破位 → 核心腿前瞻负向（fail-open 休眠闸）；三窗数据全覆盖（2024-08 起）。**Phase 0/1 已完成（`scripts/diag_candidate_c.py`）：REJECT/无候选进 Phase 2**——主假设（两融）三窗翻转证伪；共振三维同向覆盖≈0；北向被趋势共线 0.76 杀；唯一三窗一致 = 国家队 20dΔ 低分位（上涨中撤离，指数>MA200）跑输基线但幅度小、n 小 → 降级显示/监测信号，不进闸。复活条件：前瞻 paper ≥20 个样本仍负 或 holdout 独立复现。
**基建**：4 新表（cn_etf_share 2018+/cn_margin_total 2021+/cn_moneyflow_hsgt 2021+/global_index_daily 2018+）
+ alembic 0042 + 每日 job `risk_state_sync`（18:50，含 margin_detail/moneyflow 补漏）+ 前端 riskState 组。
**实验工具 bug**：`_above_flags` lookback 收盘未参与 MA（每窗前 200 日冷启动伪影）——首轮数字作废，
修复 + 回归测试后重跑（修正数字见预注册档 §6）。

### TIP-010：备用宽宇宙实验（东财形态仅对照，不替换）

**状态**：[ ]  
**完成日期**：  
**备注 / PR**：

#### 问题

需实证「东财多头排列 vs TV Pullback」进同一后滤后重叠度；避免团队再争论换源。

#### 目标

选 ≥5 个交易日：

1. 导出东财「均线多头排列」（或等价）标的列表  
2. 对同一列表跑 **回撤（可选）+ TrendOK**（与系统同口径）  
3. 与当日 TV 漏斗结果算 Jaccard / 交集人数  

**结论写入本条目**：预期「交集有限、东财更宽」——用于关闭「换东财」议题，而不是上线替换。

#### 预期收益

- **低直接收益**；**高决策收益**（停止错误方向投入）。

#### 验证

- [ ] 表格落在本文件或 `docs/modules/screener.md` 附录

---

## TIP-014 — 环境×买入风格适配（主体已落地 · 尾巴 2 项暂缓）

**状态**：[x] 主体落地 2026-08-14/15（原"[ ] Phase 1 进行中"已过期，2026-09-06 正名）；[ ] 尾巴暂缓见下
**落地**：neutral_block（弱/中性日禁开仓，valid +10.7pt/DD 12.1→2.7）· entry_style auto（RS0.7+dip3%，valid +4.7pt）·
D2 环境持有期 45（valid +11.4pt）· D3 环境仓位 1.25/0.75（三窗 +24.6/+19.5/+26.4）· HK 线不适用（维持 score）· Phase 3 分钟线 2026-09-03 接通
**明细**：[`backtests/s3/experiments-tip014.md`](./backtests/s3/experiments-tip014.md) · [`backtests/s3/experiments-d-pool.md`](./backtests/s3/experiments-d-pool.md)（D2/D3） ·
[`backtests/s3/tip014-dip-retry-2026-08-22.md`](./backtests/s3/tip014-dip-retry-2026-08-22.md)（dip 放宽重试已闭环：全劣）
**尾巴（暂缓）**：① 电风扇日回调样本不足 → 分钟线积累后重看；② 板块画像 ROI 低暂缓

---

## TIP-015 — 决策 Agent 闭环（2026-08-06 立 · M1–M4 已落地，M5 待定）

**状态**：[x] M1 会话持久化 + M2 分层 context + M3 归档检索反馈 + M4 分析视图 + 建议追踪（2026-08-06 全落地）；[ ] M5 外部导出旁路待定
**来源**：用户决策流程——Dashboard → Copy All → 外部决策 agent（5–10 轮对话）→ 手动回填执行。痛点：复制时点冻结（TIP-013/014 只是补丁）、外部 agent 每会话失忆（无反馈闭环）、复制动作本身是成长环节不能消灭。
**设计稿**：[`designs/tip-015-decision-agent-loop.md`](./designs/tip-015-decision-agent-loop.md)（三层 Context：活跃层每轮注入 / 对话窗口滑动 / 10 天归档按需检索）

**M1 会话持久化（2026-08-06）**：`db/decision.py`（3 表 + CRUD）+ alembic 0020 + `api/decision_routes.py`（6 端点）+ DecisionPage 基础聊天；测试后端 4 用例。
**M2 分层 context + Inspector（2026-08-06）**：`buildDecisionActiveLayer`（P0 操作表/P1 战情/P2 背景）+ ContextInspector（P0/P1/P2 徽标 + token 预算条）+ freshness 状态条；实现偏差：装配放前端（ai-service 保持无状态）。
**M3 归档 + 检索 + 反馈（2026-08-06）**：`decision_snapshot_job`（交易日 18:00）+ `decision_outcome_job`（盘后回灌）+ ai-service `/decision`（tools 检索归档）；V7.9 决策合同原文注入，不维护第二份。
**M4 分析视图（2026-08-06）**：AnalysisView「分析」tab——开火归因分桶（TIP-011 口径）+ 模拟盘胜负统计 + 注入审计会话列表。
**建议追踪（2026-08-06）**：`decision_actions` 表（alembic 0021）+ `/decision/extract-actions` + `match_executions()`（3 天内同 symbol+action=executed）+ `track_action_outcomes()`（后 1/3/5 日涨跌）+ `decision_action_tracking`（工作日 18:30）+ 前端「建议追踪」区块。
**M5 待定**：保持 Copy All 外部旁路，对比两路胜率，再决策是否去复制化。
**关联**：TIP-011（开火归因）/ TIP-013（新鲜度）/ TIP-014（强制刷新）/ V7.9 downstream-ai-prompt（决策合同）/ OPT-065 周度复盘（L3-P4 M2 v0：TIP-015 时点问答升级为数据驱动周报）/ L3-P4 决策 Agent。

**验证**：后端 test_decision 8 用例；全量后端 1323 / 前端 478 / ai-service typecheck+lint clean。

---

---

## 已完成索引（按主题归档 · 只读）

| 日期 | 条目 | 标题 | 归档 |
|------|------|------|------|
| 2026-07-22 | TIP-001–006 | TV 双宇宙 / 漏斗仪表 / Falcon 降级 / 进池闸 / 清池 / 版本合同 | [2026-07-22-tip-001-006-funnel-gates.md](archive/2026-07-22-tip-001-006-funnel-gates.md) |
| 2026-07-23 | TIP-007–008 | 动量通道 / 复盘字段 | [2026-07-23-tip-007-008-momentum.md](archive/2026-07-23-tip-007-008-momentum.md) |
| 2026-07-24 | V6.2-01–03 | 尾盘锁 / 防守袖子 / Zero-Pos | [2026-07-24-tip-v62-hub.md](archive/2026-07-24-tip-v62-hub.md) |
| 2026-07-27 | V6.3-01–02 | 溢出豁免 / Alpha S | [2026-07-27-tip-v63-overflow.md](archive/2026-07-27-tip-v63-overflow.md) |
| 2026-08-02 | V6.4-01 | ETF 资金确认因子 | [2026-08-02-tip-v64-etf-confirm.md](archive/2026-08-02-tip-v64-etf-confirm.md) |
| 2026-08-04 | TIP-009/011 | 映射抽检 / 开火归因 | [2026-08-04-tip-009-011-mapping.md](archive/2026-08-04-tip-009-011-mapping.md) |
| 2026-08-05 | V7.0-01–03/TIP-012 | 相关性热力网 / 风险平价 / 护城河排除 / 研报通道 | [2026-08-05-tip-v70-sizing.md](archive/2026-08-05-tip-v70-sizing.md) |
| 2026-08-22 | TIP-013 | IC 验证（有效因子清单空，S-3 择时唯一超额源） | [2026-08-22-tip-013-ic.md](archive/2026-08-22-tip-013-ic.md) |
| 2026-09-09 | TIP-016 | 策略级止损：B（HK 熔断 -25）进冻结；W1/W2/E/A 拒收（A 水位节流 long 一开 22 个月，6/6 拒） | [2026-09-09-tip-016-strategy-level-stop.md](archive/2026-09-09-tip-016-strategy-level-stop.md) |

> 旧当时总结（非完整条目）：`archive/2026-08-04-tip-011-*`、`2026-08-05-tip-012-*`、`2026-08-06-tip-013-014-*`——查完整条目走上表。

---

## 验收总标准（计划是否「做完」）

全部 P0/P1 勾选且满足：

- [ ] TV 主宇宙与回踩进池 thesis 一致（或 Momentum 分支明确分离）
- [ ] 任意交易日可回答漏斗四层数字
- [ ] 主 screener 0 票日有降级或明确告警策略
- [ ] Alpha 进池有轻量闸；非 S 可被三日 GC
- [ ] 业务文档（screener / watchlist / alpha）与线上行为一致

P2/P3 为增强项，不阻塞「基础改进完成」声明。
