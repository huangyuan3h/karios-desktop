# DB 走向决策 · 2026-08

> **关联 todo**：[`docs/todo.md §4 工程与部署 P0` / `§12 实施清单 #10`](../../todo.md)（"DB 走向决策文档 · 关掉'要不要上云'的反复讨论"）
> **上下文**：[`cloud-deployment-options.md`](./cloud-deployment-options.md)（已说"DB 完全本地"——本文档**只**拉出 DB 维度的细节决策）
> **决议日**：2026-08-01
> **下次复审**：2027-02-01（半年期；或满足"触发条件"任一即重开）

---

## 0. TL;DR（一行版本）

> **Postgres 留在本地 Mac。** 触发条件未满足前不上 RDS / Neon / Supabase。日常成本 = `$0`。
>
> **决策核心**（按权重排，不是按省钱排）：
>
> 1. **DB 永远不在公网路径上 = homelab 核心安全姿态**（$19/月买不到）
> 2. **盘前批量 `/v1/explain` 5ms vs 100ms = 决定 API 可用性**
> 3. **Tunnel 失效 ≠ DB 暴露 = 全栈解耦**
> 4. **迁移成本几乎不可逆**——Neon → 本地回退 3-5 天
> 5. **省钱 $228-360/年**（最弱论据——别靠这个说服自己）

| 决策点 | 选择 | 关键理由 |
|--------|------|----------|
| 部署位置 | **本地 Postgres 14+**（Mac 同机） | DB 永远不暴露公网；同机 5ms 延迟；Tunnel 失效 ≠ DB 暴露 |
| 备份策略 | **`pg_dump` cron + iCloud Drive + 本地外接盘 3 副本** | 不买 AWS S3 备份桶；Mac 自带外接盘足够 |
| HA / 复制 | **不做**（单机足够） | 单人 + 单进程 + 偶发断电 ≤ 30min/UPS 撑住 |
| 多区域 | **不复制 DB 到云**（Karios 只暴露 `/v1/*` 给外面读） | 避免"本地和云不一致"的悖论 + 避免 DB 公网暴露 |
| 迁移触发 | **见 §5 触发条件**——满足任一即重开本文件 | 不让"上云冲动"无成本地反复发生 |
| 费用 | **`$0/月`** | **附加价值**——不是核心论据 |

---

## 1. 为什么单独写一份（不动 cloud-deployment-options）

2026-08 之前的现状：

- `cloud-deployment-options.md` 写了"DB 完全本地 Postgres"
- `freelancer-architecture.md` 提到"评估 Vercel + Neon"作为触发条件
- **`docs/todo.md §4` / `§6` / `§11` / `§12 #9` 多处出现"要不要上云 DB"的讨论**

每次新需求来（比如"AI 助手迁到云端"、"要给朋友开账号"、"Mac 备份做烦了"），就要**重读两份文档 + 自己拼**——讨论成本高。本文档目的：

1. **DB 单维度独立决策**——不上云总决策的"DB 切片"
2. **触发条件精确化**——满足任一即重开，避免冲动
3. **备份 / HA / 监控细节**——总决策没覆盖的部分
4. **复审日历**——半年期强制重读，防"一次性决定"

---

## 2. 现状盘点（截止 2026-08）

### 2.1 DB 在哪

```bash
$ grep DATABASE_URL .env
DATABASE_URL=postgresql://admin:admin123@localhost:5432/karios-desktop
```

- Postgres 14+（macOS brew install）
- 数据库名 `karios-desktop`
- 用户 `admin` / 密码 `admin123`（本地开发用，**未对外暴露**）
- 同机进程：`pnpm dev`（Next.js）+ `python -m data_sync_service`（FastAPI）+ Postgres

### 2.2 数据量级

| 表 | 估算行数 | 备注 |
|----|----------|------|
| `daily` | ~5000 票 × 1500 天 ≈ **7.5M 行** | 主力，CN daily K 线 |
| `hk_daily` | ~3000 票 × 1000 天 ≈ **3M 行** | OPT-041 起 |
| `daily_basic` | 每日全量 ~5000 行 | Tushare 增量 |
| `alpha_radar_trends` | ~100/月 × 12 = **~1200 行** | 增量小 |
| `paper_trades` | OPT-049 v0，~50/月 | CN only |
| 其他（watchlist / journal / screener 等） | **~10K 行** | 元数据 |

总数据量估：**~1-2 GB**（含索引）。**远未到 5 GB 触发线**。

### 2.3 当前访问模式

| 来源 | 频率 | 说明 |
|------|------|------|
| FastAPI 同机直连 | **95%** | `127.0.0.1:5432`，毫秒级 |
| Next.js（同机） | **4%** | 同上 |
| Cloudflare Tunnel → 外部 AI 助手 | **<1%** | 走 `/v1/*`，**不直连 DB**——Tunnel 只代理 HTTP |
| 远程 SSH（iPhone / iPad） | 偶发 | Tailscale SSH → `psql` 临时查 |

**关键事实**：DB **从未直接对外暴露**。所有外部访问都走 FastAPI → `/v1/*` HTTP。这是 homelab 架构的核心安全姿态。

---

## 3. 选项对比

### 3.1 选项 A：本地 Postgres（当前）

| 维度 | 评估 |
|------|------|
| 月成本 | **$0**（macOS 自带 / brew） |
| 延迟 | **<5ms**（同机） |
| 可靠性 | 单点故障（依赖 Mac 不断电） |
| 备份 | `pg_dump` cron + iCloud + 外接盘 |
| 数据源兼容性 | **极高**（本机 IP） |
| 运维负担 | 中（升级 / 调参自己来）|
| 多区域 | 不支持 |
| HA | 不支持 |

### 3.2 选项 B：AWS RDS（托管 Postgres）

| 维度 | 评估 |
|------|------|
| 月成本 | **$15-30**（db.t3.micro）+ 备份 $5-10 = **$20-40/月** |
| 延迟 | **15-50ms**（同区）/ 100ms+（跨区） |
| 可靠性 | 99.95% SLA / 自动备份 / 自动小版本升级 |
| 数据源兼容性 | **低**（云 IP，Tushare / EM 易被风控） |
| 运维负担 | 低（managed） |
| 多区域 | 支持（read replica） |
| HA | 同区 multi-AZ |

**为何不选**：成本 + **数据源 IP 风控**——已实测过几次云 IP 拉黑。Tushare 共享 IP 池对 AWS 段不友好。

### 3.3 选项 C：Neon / Supabase（Serverless Postgres）

| 维度 | 评估 |
|------|------|
| 月成本 | **$19/月 Launch**（Neon）/ Supabase 类似 |
| 延迟 | **50-200ms**（冷启动可达秒级） |
| 可靠性 | 高（serverless 自动恢复） |
| 数据源兼容性 | **低**（同 RDS） |
| 运维负担 | **极低** |
| 多区域 | 支持（Neon branching） |
| HA | 内置 |

**为何不选**：冷启动 + 共享 IP + 没有比 RDS 显著优势。

### 3.4 选项 D：自建云 VM（DigitalOcean / Lightsail）

| 维度 | 评估 |
|------|------|
| 月成本 | **$6-12/月**（Lightsail 1GB） |
| 延迟 | **30-100ms** |
| 可靠性 | 自己管 |
| 数据源兼容性 | 中（共享 IP 池仍有一定风险） |
| 运维负担 | 高（OS / 安全补丁 / 备份） |
| 多区域 | 支持 |
| HA | 不支持（单 VM） |

**为何不选**：比 RDS 便宜但**仍不是本机**；数据源 IP 风险仍在；多了一个远程 OS 要维护。

### 3.5 选项 E（拒绝评估）：DynamoDB / MongoDB / 其他 NoSQL

Karios schema 是关系型 + 时序（K 线 + 日增量），PG 是天然选择。NoSQL 迁移成本**远高于省下的 $19/月**。

### 3.6 选项 F（真正该花的 $19/月）：Neon + 本地双轨

```
本地 PG = 权威源（写 / cron / backup）
   ↓ 每日 22:00 异步 dump（pg_dump → psql restore）
Neon    = 只读副本（外部 AI 助手读 / Tunnel 出站后命中）
```

| 维度 | 评估 |
|------|------|
| 月成本 | **Neon Launch $19** |
| 延迟 | 本地 5ms（Karios 自用）/ Neon 100ms（外部只读）|
| 安全姿态 | Neon 公网暴露**只读 + 独立账号 + IP 白名单**；写仍只能在本地 |
| 备份 | 本地 3 副本**保留**（Neon 自动备份是 bonus）|
| 价值 | 外部 AI 助手走 Neon 不拖累本地 PG；本地 PG 仍是 cron / FastAPI 同机快路径 |

**何时选**：外部 AI 助手流量 > 内部 cron 流量**且**对 Neon 100ms 延迟可接受。
**何时不选**：外部流量稀少（当前阶段就是这种）；$19/月不是问题，**架构复杂度才是**。

> **强建议**：当前**不选**。理由：外部流量还在早期，外部 AI 助手今天主要是轮询（不密集）+ 解释（不频繁），本地 PG 完全可以扛。等外部流量真起来（§5 触发条件满足），再升级到双轨。

### 3.7 选项 G：Tailscale Funnel 替代 Cloudflare Tunnel

| 维度 | Cloudflare Tunnel | Tailscale Funnel |
|------|-------------------|------------------|
| 月成本 | $0 | $0 |
| 配置 | 域名 + DNS | Tailscale 账号 + ACL |
| 域名 | 自定义（`karios.{your-domain}`） | `<node>.ts.net`（丑） |
| 延迟 | 5-50ms (CF edge) | 20-100ms (DERP relay) |
| WAF | **内置** | 无 |
| 适用 | 公开域名 + 防御扫描 | **私密邀请制**（朋友 / 家人） |
| 账号依赖 | Cloudflare | Tailscale |

**何时选**：要给朋友 / 家人**邀请制**访问，且不想走 CF 域名。
**何时不选**：要公开域名 + 抗扫描——CF WAF 内置是 Tailscale 给不了的。

---

## 4. 决策

### 4.1 现状决策（2026-08 → 2027-02）

> **Postgres 留在本地 Mac。** 不动 schema、不动备份策略、不上云。
>
> **触发条件未满足前不再讨论上云**——本文档即结论。

### 4.2 备份策略（具体动作）

| 频率 | 方法 | 存储位置 | 保留 |
|------|------|----------|------|
| **每日 02:00** | `pg_dump -Fc` 压缩 | `~/backups/karios-YYYY-MM-DD.dump` | 7 天 |
| **每周日 03:00** | 上面 dump | iCloud Drive `/KariosBackups/` | 4 周 |
| **每月 1 号 04:00** | 上面 dump | 外接 USB 盘 `/Volumes/Backup/karios-monthly/` | 12 月 |

**验证**：每月 1 号恢复一次到 `karios_restore_test` DB，确认 dump 可用。

**Cron 配置**（参考，本地 macOS）：

```bash
# /usr/local/bin/karios-db-backup.sh
#!/usr/bin/env bash
set -euo pipefail
TS=$(date +%Y-%m-%d)
BACKUP_DIR=$HOME/backups
mkdir -p "$BACKUP_DIR"
pg_dump -Fc -d karios-desktop -f "$BACKUP_DIR/karios-$TS.dump"
# 7 天滚动
find "$BACKUP_DIR" -name "karios-*.dump" -mtime +7 -delete
# 同步到 iCloud
cp "$BACKUP_DIR/karios-$TS.dump" "$HOME/Library/Mobile Documents/com~apple~CloudDocs/KariosBackups/"
```

### 4.3 监控（cheap & local）

- **`pg_stat_activity` 日志扫描**（每小时 cron）：
  - 长查询（> 5min）→ Slack / Telegram 不发（避免噪音）→ 写到 `~/karios-monitor.log`
- **磁盘空间**（每日 cron）：
  - DB 大小 > 4 GB → 警告（仍远低于 5 GB 触发线）
  - 备份盘满 → 警告

不接 Prometheus / Grafana——homelab 不需要那套。**`scripts/data-source-healthcheck.sh`（OPT-050 已写）扩展一项即可**。

---

## 5. 触发"重新评估上云"的条件

满足任一即**重开本文件 + cloud-deployment-options.md**——不允许默默"先上了再说"：

- [ ] **个人 AI 助手迁到云端，不愿连回本地 Tunnel**（Tunnel 延迟 / 可用性不耐受）
- [ ] **移动端（iPad / 手机）使用频率 > 3 次/日** + **对延迟不可接受**（> 200ms 不可用）
- [ ] **要给家庭成员 / 朋友开账号**（多人协作，需要 read replica / 多用户隔离）
- [ ] **电脑经常断电 / 网络不稳**，"永远开机"假设**持续**不成立（不是偶发）
- [ ] **本地 Postgres 数据 > 5 GB**，备份 / 恢复**成为日常痛点**（不是月痛点）
- [ ] **公开 demo URL** 需求（无 Tunnel 域名）

> **关键**：触发后**先重读本文档**——不要直接动 schema。先评估"能不能不迁 DB 而是迁别的"（如只迁 FastAPI 出去 + 远程连回 DB）。

---

## 6. 反方案：什么时候选什么

| 场景 | 推荐 | 不推荐 |
|------|------|--------|
| 想给朋友开 read-only 账号 | **Tunnel + API Key 只读 scope** | 上 Neon（杀鸡用牛刀） |
| iPhone 慢 | **本地加内存 / UPS** | 迁 DB 到云（延迟更差） |
| 备份做烦了 | **脚本化 + iCloud 自动** | 买 S3 备份桶（再开 $1-5/月） |
| 想 public demo | **Cloudflare Access 邀请制 + Tunnel** | 上 Vercel + Neon（$39/月） |
| 多人协作（家庭账户） | **多用户 FastAPI 账号 + Row-Level Security** | RDS（schema 改造巨大） |

---

## 7. 已知风险 + 缓解（这次我真量化了）

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| Mac 硬盘故障 | 低（年化 <2%）| 数据全失 | 三副本备份（本地 + iCloud + 外接盘）；每月 1 号 dry-run restore |
| Mac 突然断电 | 中（取决于你家供电） | 当前 cron 失败 / DB corruption | UPS 撑 30min+；macOS `caffeinate` 防自动休眠；Postgres WAL checkpoint 每 5min |
| iCloud 同步丢数据 | 极低（iCloud SLA 99.9%） | 周备份丢 | 外接盘独立保留，**不依赖 iCloud 作主备份** |
| `pg_dump` 失败 | 低 | 当日无备份 | cron 输出捕获 + `~/karios-monitor.log` 告警；每周日手动演练 |
| Postgres 大版本升级（14 → 17）破坏 schema | 低 | 升级失败 | 先在备份 dump 上 dry-run upgrade；新 macOS 用户级 PG 16 默认 |
| 同机进程 OOM | 极低（Mac 16GB+） | 进程被杀 | Postgres 不参与前端热路径，独立进程；监控 `pg_stat_activity` 长查询 |
| **Tunnel 失效 ≠ DB 暴露**（核心论据）| — | — | **架构解耦**：Tunnel 只代理 HTTP，DB 永远在 127.0.0.1；Tunnel 挂 = 你看不到 ≠ 数据失 |
| macOS 系统更新强制重启 | 中（每年 4-6 次） | 服务停 5-10min | UPS 撑过 + 重启脚本（`§12 #7` Docker 一键 起 中处理）|
| 你忘了改 .env 密码本地默认 `admin123` | 中 | 本地安全姿态下降 | 改 .env；密码 16+ 字符 |

---

---

## 7.5 反例：什么时候我的论据会**不**成立（诚实反向触发）

为了让本决策可信，列出**会让我改口的场景**：

| 反例场景 | 我的回应 |
|----------|----------|
| **你的 Karios ROI 起来后，月成本从隐性视角看真不算事** | §3.6 双轨方案变首选——本地 + Neon 只读副本 |
| **你真要给朋友 / 家人 / 公开 demo** | §3.7 Tailscale Funnel + CF Access 邀请制；不上 Neon |
| **盘前 `/v1/explain` 你不再用本地，AI 助手全走云** | 那本地 PG 5ms 延迟优势消失；§3.6 双轨合理 |
| **你想做多区域 / 跨设备实时同步** | 单机 PG 天生不行；此时不是"DB 上云"问题，是"产品形态变了"——重开 todo §2 |
| **macOS 14/15 PG 14 被 EOL** | 升级 PG 14 → 17；不涉及 DB 走向 |

---

## 8. 复审日历

| 日期 | 触发 | 必读 |
|------|------|------|
| **2027-02-01** | 半年期强制 | 本文件 + `cloud-deployment-options.md` |
| 任何时点 | 满足 §5 任一触发条件 | 同上 |

---

## 9. 与其他文档的引用关系

```
docs/designs/db-direction-2026-08.md  (本文档 · DB 单维度真值)
        │
        ├─► cloud-deployment-options.md  (整体上云/不上云真值)
        │
        └─► freelancer-architecture.md  (Tunnel + /v1/* 架构真值)
                │
                └─► api-contract.md  (/v1/* 接口契约)
```

> **下次需要讨论"DB 走向"时**：先读本文档。**不要再讨论"要不要上云"——答案是"不"。** 直接讨论 §5 触发条件是否满足。