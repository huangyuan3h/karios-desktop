# Designs（设计概念 & 未完成工作）

> **容器性质**：本目录放**还没落地 / 还在构思 / 等新一轮评估**的设计稿。
> 落地后要么迁出到 `modules/`（成为真值），要么迁去 [`../archive/`](../archive/)（作为历史快照）。
> **不要长期堆**——这里的每份草稿应能回答"还需要谁拍板才能动"。

---

## 何时用这个目录

| 情况 | 用本目录 |
|------|----------|
| 想写一个 idea 草稿但还没在 `todo.md` 排上 | ✅ |
| 把 `todo.md` 里的某条展开成几张图/几张表 | ✅ |
| 反思某个子系统要不要重做 | ✅ |
| 已经是落地真值的文档 | ❌ 改去 `../modules/` |
| 已经被废弃/被合并的设计 | ❌ 改去 `../archive/` |

---

## 当前文件（2026-09-06 全量 · 状态只写有档可查的，其余标待分类）

### 回测 / 策略（结论已定 · 看 `../backtests/`，本目录只留预注册与过程）

| 文件 | 状态 |
|------|------|
| [`twin-star-ops-phase-2026-09-02.md`](./twin-star-ops-phase-2026-09-02.md) | **方向已拍板 2026-09-02**；实盘默认 clip4；按 OPT 落地（todo P0-0） |
| [`sat-entry-filter-phase1-2026-09-03.md`](./sat-entry-filter-phase1-2026-09-03.md) | **C1 已三窗** [sat-entry-c1](../backtests/sat/sat-entry-c1-2026-09-03.md)；valid tot 仍 −3.3（后随 habit 转正） |
| [`third-asset-sleeve.md`](./third-asset-sleeve.md) | **提示已落地 2026-08-19**；自动配置/paper 层待拍板（最优=纳指ETF+200dMA） |
| [`state-bucket-slice-stock-leg.md`](./state-bucket-slice-stock-leg.md) | **结论已迁** `docs/backtests/state-bucket-algo-2026-08-31.md` |
| [`pattern-factor-validation.md`](./pattern-factor-validation.md) | **首跑 done 2026-09-04**（todo P0-6 #6：8 形态 7 拒，不进 S-3） |
| [`scout-breakthrough-2026-09-05.md`](./scout-breakthrough-2026-09-05.md) | **收兵 2026-09-05**（S/K 电池全杀，突破口暂无） |
| [`sat-weight-6040-prereg-2026-09-05.md`](./sat-weight-6040-prereg-2026-09-05.md) | 预注册；结果见 [sat-weight-6040](../backtests/sat/sat-weight-6040-2026-09-05.md)（薄增益，不进 Live） |
| [`hedge-twin-short-2026-09-05.md`](./hedge-twin-short-2026-09-05.md) | 关联 [hedge-twin](../backtests/hedge/hedge-twin-2026-09-05.md)（实现证伪，关闭） |
| [`regime-allocation-incubator-prereg-2026-09-11.md`](./regime-allocation-incubator-prereg-2026-09-11.md) | **P0-12 R1 预注册**：regime 配置腿（趋势×慢价值，换发动机非加 gate）；Phase 0 诊断未跑 |
| [`longhold-tenbagger-prereg-2026-09-11.md`](./longhold-tenbagger-prereg-2026-09-11.md) | **P0-12 L1 预注册 + pilot**：长持集中找几倍股；**REJECT-frozen**（1 年高增长=盈利脉冲陷阱）；下一步 durability/扩数据 |

### API / 外部打通

| 文件 | 状态 |
|------|------|
| [`api-contract.md`](./api-contract.md) | 活文档（被 `../api/*.md` 引用） |
| [`webhook-event-subscription.md`](./webhook-event-subscription.md) | 待分类 |
| [`tv-capture-data-source-2026-08.md`](./tv-capture-data-source-2026-08.md) | 待分类 |
| [`miniqmt-xtquant-live-trading.md`](./miniqmt-xtquant-live-trading.md) | L4-P1 券商研究，未落地 / 待拍板（需券商文字确认费率与权限） |
| [`freelancer-architecture.md`](./freelancer-architecture.md) | 待分类 |

### 部署 / 工程 / 数据

| 文件 | 状态 |
|------|------|
| [`cloud-deployment-options.md`](./cloud-deployment-options.md) | 部署走向决策，待拍板 |
| [`cloudflare-tunnel-setup.md`](./cloudflare-tunnel-setup.md) | 待分类 |
| [`mac-mini-deployment.md`](./mac-mini-deployment.md) | 待分类 |
| [`db-backup-and-migrate-2026-08.md`](./db-backup-and-migrate-2026-08.md) | 待分类 |
| [`db-direction-2026-08.md`](./db-direction-2026-08.md) | 待分类 |
| [`stability-audit-2026-09-01.md`](./stability-audit-2026-09-01.md) | 被 optimization-checklist 引用，待分类 |
| [`data-gap-backfill-2026-08.md`](./data-gap-backfill-2026-08.md) | 待分类 |
| [`data-source-audit-2026-08.md`](./data-source-audit-2026-08.md) | 待分类 |

### 演进 / 远期（未排期）

| 文件 | 状态 |
|------|------|
| [`l3-l4-evolution-roadmap.md`](./l3-l4-evolution-roadmap.md) | **方向已拍板 2026-08-07**；L3 五里程碑全部完成，L4 待排期 |
| [`tip-015-decision-agent-loop.md`](./tip-015-decision-agent-loop.md) | ⚠️ 编号待确认（checklist 最大 TIP-014，无 TIP-015 条目） |
| [`ml-n-day-trend-forecast.md`](./ml-n-day-trend-forecast.md) | 待分类 |
| [`ego-lite-spike-2026-08.md`](./ego-lite-spike-2026-08.md) | 待分类 |
| [`small-agile-track-2026-08.md`](./small-agile-track-2026-08.md) | 待分类 |
| [`family-hub-2027.md`](./family-hub-2027.md) | 远期 |
| [`karios-longevity-2026-08.md`](./karios-longevity-2026-08.md) | 待分类 |
| [`mobile-redesign-2027.md`](./mobile-redesign-2027.md) | 远期 |

> 已归档：[`l4-gate-audit`](../archive/2026-08-08-l4-gate-audit.md)（L4 准入 Gate 全清 2026-08-08）。
