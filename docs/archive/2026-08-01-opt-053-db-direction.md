# OPT-053 / §12 #10 · DB 走向决策 · 归档于 2026-08-01

> **关联 todo**：[`docs/todo.md §4 工程与部署 P0` / `§12 实施清单 #10`](../../todo.md)
> **决策文档真值**：[`docs/designs/db-direction-2026-08.md`](../../docs/designs/db-direction-2026-08.md)（**新**——长期真值，不迁移至 archive）
> **上下文**：[`docs/designs/cloud-deployment-options.md`](../../docs/designs/cloud-deployment-options.md) + [`docs/designs/freelancer-architecture.md`](../../docs/designs/freelancer-architecture.md)（已链到本文档）

## 当时的目标

§12 #10："DB 走向决策文档 · 关掉'要不要上云'的反复讨论"——0.5 天拍板 + 半年期强制复审。

## 实际做了什么

### A. 决策文档（`docs/designs/db-direction-2026-08.md`）

9 节结构：

| 节 | 内容 |
|----|------|
| §0 TL;DR | 一行版本："Postgres 留在本地 Mac。触发条件未满足前不上 RDS / Neon / Supabase。日常成本 = $0。" |
| §1 为什么单独写 | 现状盘点：DB 决策散落在 3 份文档（cloud-deployment / freelancer-arch / todo），新需求来时讨论成本高 |
| §2 现状盘点 | DB 在哪 / 数据量级（1-2 GB，远未到 5 GB 触发线）/ 访问模式（DB 永远不直接对外暴露） |
| §3 选项对比 | 5 维度对比：本地 / RDS ($20-40/月) / Neon ($19/月) / DigitalOcean VPS ($6-12/月) / 拒绝 NoSQL |
| §4 决策 | 现状决策 + 备份策略（每日/每周/每月 3 副本）+ 监控（轻量 cron）|
| §5 触发条件 | 6 条精确触发线（满足任一即重开文档，不允许默默"先上了再说"）|
| §6 反方案 | 4 个常见反方案的更优替代（开 read-only 账号 / 加内存 / 脚本化备份 / Cloudflare Access 邀请制）|
| §7 已知风险 | 7 风险 + 概率 + 影响 + 缓解 |
| §8 复审日历 | 2027-02-01 半年期强制重读 |
| §9 文档引用关系 | 与 cloud-deployment / freelancer-arch / api-contract 的依赖图 |

### B. 同步指引（3 处交叉引用）

| 文件 | 改动 |
|------|------|
| `docs/designs/freelancer-architecture.md` | §5 "何时回退" 加一行："**DB 维度具体触发条件 + 备份策略见 [db-direction-2026-08.md]**" |
| `docs/designs/cloud-deployment-options.md` | 顶部加：**DB 维度具体决策（备份 / HA / 触发条件 / 复审日历）见 [db-direction-2026-08.md]** |
| `docs/todo.md` §12 #10 行 | 标 ✅ + 链到 designs/db-direction-2026-08.md |

### C. 设计取舍（这次是真论据，按权重排）

| # | 论据 | 强度 | 旧版 vs 新版 |
|---|------|------|--------------|
| 1 | **DB 永远不在公网路径上 = homelab 核心安全姿态** | **强** | 旧版没说；新版 §0 第一条 |
| 2 | **盘前批量 `/v1/explain` 5ms vs 100ms 决定 API 可用性** | **强** | 旧版没说；新版 §0 第二条 |
| 3 | **Tunnel 失效 ≠ DB 暴露 = 全栈解耦** | **强** | 旧版没说；新版 §0 第三条 |
| 4 | **迁移成本几乎不可逆**（Neon → 本地回退 3-5 天） | 中 | 旧版没说；新版 §0 第四条 |
| 5 | 省钱 $228-360/年 | **弱** | 旧版**当大数挡你**，新版明确标注**最弱论据** |

### D. v2 增量（本会话内 review 后加固）

1. **§3.6 新增** —— "Neon + 本地双轨"作为**真正该花的 $19/月**（只读副本分担外部流量）
2. **§3.7 新增** —— Tailscale Funnel 替代 Cloudflare Tunnel 的对比
3. **§6 反方案扩充** —— 5 个反方案的更优替代
4. **§7 风险量化** —— 之前"低 / 中"拍脑袋，现在给年化概率 + SLA 数字
5. **§7.5 新增** —— "反例：什么时候我的论据会**不**成立"——让决策**可证伪**，避免自洽闭环

### E. 为什么 §7.5 重要（自洽性风险）

> 一个不可证伪的决策是**信仰**，不是**决策**。

§7.5 列出 5 个会让我改口的反向触发——这等于告诉未来 review：
- 这些场景来时，**结论可能翻转**
- 不是"我现在说啥都对"

**自洽闭环 = 文档杀手**。文档要让 reader 能反驳，作者要先反驳自己。

## 验证 / 数据

| 测试文件 | 状态 |
|----------|------|
| `services/data-sync-service` 全部 208/208 ✅ + 1 skip | 回归零影响（决策文档无代码改动）|
| `ai-service` 126/126 ✅ | 同上 |

**注**：本 OPT 是**纯文档决策**，**无代码改动**——这是 §12 #10 设计意图（0.5 天拍板）。

## 后续影响 / 留给谁

### 给未来 review

- **2027-02-01**（半年期强制）：重读本文件 + cloud-deployment-options.md + freelancer-architecture.md
- **任何时点**：满足 §5 触发条件任一 → 重开本文件 + cloud-deployment-options.md
- **新需求来时**（如"AI 助手迁云端"、"给朋友开账号"）：**先读本文件**——不要再讨论"要不要上云"，答案是"不"。直接讨论 §5 触发条件是否满足。

### 给 Karios 本身

- **不动 schema**——决策对 schema 是 no-op
- **不动备份脚本**——本地 dump cron 已存在；新规则需要的话扩 `data-source-healthcheck.sh`
- **不动 /v1/* 契约**——Tunnel 只代理 HTTP，DB 永远不在公网路径上

### 给外部 AI 助手

- 集成契约不变——AI 助手永远走 `/v1/*` HTTP，**不会直连 DB**
- 决策不影响 `/v1/*` 任何 endpoint

## 沉淀数据

| 项 | 值 |
|----|----|
| 新增文件 | 2（decision + archive summary）|
| 改动文件 | 3（freelancer-arch.md + cloud-deployment-options.md + todo.md）|
| 总测试 | 208/208 ✅ + 1 skip（决策文档无代码改动）|
| 工期 | 1 个会话 |
| 预算 | **$0/月**（vs Neon $19/月 或 RDS $20-40/月）|