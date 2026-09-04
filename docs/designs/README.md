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

## 当前文件

| 文件 | 关联 todo 节 | 状态 |
|------|--------------|------|
| [`l3-l4-evolution-roadmap.md`](./l3-l4-evolution-roadmap.md) | §16 升级方向（L3 → L4） | **方向已拍板 2026-08-07**；L3 五里程碑全部完成，L4 待排期 |
| [`l4-gate-audit.md`](./l4-gate-audit.md) | §17 L4 准入 Gate（全模块排查加固） | **计划已立 2026-08-07**；P0 未清不启动 L4 |
| `cloud-deployment-options.md` | §5 / §4 部署走向决策 | 待拍板 |
| [`miniqmt-xtquant-live-trading.md`](./miniqmt-xtquant-live-trading.md) | §16 L4-P1 券商研究 | 未落地 / 待拍板（需券商文字确认费率与权限） |
| [`state-bucket-slice-stock-leg.md`](./state-bucket-slice-stock-leg.md) | 择强 STOCK 腿 · slice / PS-G50 实验 | **结论已迁** `docs/backtests/state-bucket-algo-2026-08-31.md` 文首口径铁律 + v3 冻结 |
| [`twin-star-ops-phase-2026-09-02.md`](./twin-star-ops-phase-2026-09-02.md) | 机会双子星下一阶段（工程 / 业务对齐 / 回测可分析） | **方向已拍板 2026-09-02**；实盘默认 clip4；按 OPT-128+ 落地 |
| [`sat-entry-filter-phase1-2026-09-03.md`](./sat-entry-filter-phase1-2026-09-03.md) | 卫星 14:30 入场过滤（不是 early stop） | **C1 已三窗** [sat-entry-c1](../backtests/sat-entry-c1-2026-09-03.md)；valid tot 仍 −3.3，不改 Live |
| [`third-asset-sleeve.md`](./third-asset-sleeve.md) | §8 回测（T6 第三资产套筒） | **提示已落地 2026-08-19**；自动配置/paper 层待拍板（最优=纳指ETF+200dMA） |
