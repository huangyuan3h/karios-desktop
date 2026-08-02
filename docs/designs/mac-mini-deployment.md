# Karios · Mac mini 长期部署方案

> **决议日**：2026-08-01
> **触发条件**：用户获得一台长期开机的 Mac mini（或同等小型 Mac 设备）
> **关联 todo**：[`docs/todo.md` §13 Longevity · §12 #7 Docker](../../todo.md)
> **关联设计稿**：[`docs/designs/karios-longevity-2026-08.md`](./karios-longevity-2026-08.md)（系统级痛点真值）
>
> **本文档目标**：把"长期稳定部署"的目标、决策、架构、触发条件、迁移步骤写清楚——当用户拿到 Mac mini 那一天，按本文档执行即可。

---

## 1. 决策摘要（一句话）

> **未来 Mac mini 时代**：用 Docker 把整个 Karios 栈（data-sync + ai-service + desktop-ui）打包成单一可启动系统，自动开机启动，UPS 断电保护，**单一 Postgres（本地 PG）作为唯一数据源**——`pnpm dev` 和 `docker compose up` 共用同一份数据。

| 维度 | 决策 |
|------|------|
| **部署形态** | Docker compose（5 个 app service + 1 个 pgadmin）|
| **数据源** | **本地 Postgres**（Postgres.app 或 brew services），**单一数据源** |
| **自动启动** | macOS LaunchAgent → `docker compose up -d` |
| **断电保护** | UPS + nut/apcupsd → `pmset shutdown` |
| **远程访问** | Cloudflare Tunnel（OPT-048 已通） |
| **远程访问兜底** | Tailscale Funnel（§13 #2，暂缓） |
| **代码更新** | `git pull` + `scripts/docker-up.sh --rebuild` |
| **数据备份** | pg_dump cron（OPT-053 已设计）→ 本地 3 副本 + 异地 iCloud Drive |

---

## 2. 为什么是这个方案（vs 其他）

### 2.1 vs 当前现状（pnpm dev + 本地 PG）

| 维度 | 当前 pnpm dev | Mac mini Docker |
|------|---------------|-----------------|
| 换电脑 | `git clone` + `pnpm install:all` + `pnpm dev`（手动跑）| LaunchAgent 自动起（开机即用）|
| 断电恢复 | 开机后**什么都不跑**——你得 SSH 进 Mac mini 手动 `pnpm dev` | LaunchAgent 跑 `docker compose up -d`（自动） |
| 系统升级 | 各组件独立升级（PG、Node、Python）| 容器化升级（重建 image）|
| 数据源 | **当前已经在用的本地 PG**（用户原话："db 用现在的"）| 同一个本地 PG（不切换）|
| 远程访问 | Tunnel 挂 `pnpm dev` 端口（已 OK） | Tunnel 挂 docker 端口（一致）|
| 资源占用 | 轻（每个进程独立）| 重（5 个容器 + PG） |

**关键 insight**：Docker 不是为了"省事"——是为了**机器重启 / 断电后能自动恢复**，让用户**完全不干预**。

### 2.2 vs 全云方案（Neon 主库 + Vercel/Cloud Run）

| 维度 | Mac mini 本地 | 全云 |
|------|---------------|------|
| 数据所有权 | 你硬盘上 = **绝对所有权** | 云厂商 SLA 99.9% |
| 月成本 | $0 + 电费 | $20-50/月 |
| 维护量 | 0（自动）+ 偶尔换硬盘 | 偶尔看账单 + 平台升级 |
| 离网可用 | 是 | 否（你 Mac 不在路径上但云平台在）|
| 退休友好 | 差（依赖 Mac mini 在线）| 好（自动）|

**用户原话**："可能是未来很久以后还 mac mini 长期开机部署的方案， 对这个项目真的能养活我之后的事情吧"

→ **本地 Mac mini 是当前阶段最优**（ownership + cost + 离网）。**全云是退休期方案**（不是现在）。

### 2.3 vs Tauri 桌面形态

Tauri 是把整个 Next.js 嵌进桌面 app。Mac mini + Docker 比 Tauri 好的地方：
- 多个客户端可以同时访问（家人 / 你在外面用手机）
- 不依赖桌面环境（macOS GUI 卡了不影响 backend）
- 远程访问更自然

Tauri 适合：笔记本 + 单人 + 出差用。Mac mini + Docker 适合：**家里 7×24 跑 + 多端访问**。

---

## 3. 架构（未来态）

```
┌─────────────────────────────────────────────────────────────┐
│  Mac mini · 7×24 在线 · UPS 保护                              │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  单一 Postgres（系统级 brew services postgresql@16）    │ │
│  │  - localhost:5432                                       │ │
│  │  - 数据目录：~/Library/Application Support/Postgres/...│ │
│  │  - 单源 = pnpm dev + docker compose 共用                │ │
│  └────────────────────────────────────────────────────────┘ │
│           ▲                                  ▲              │
│           │ host.docker.internal:5432        │ localhost:5432│
│           │                                  │              │
│  ┌────────┴──────────┐            ┌──────────┴─────────┐    │
│  │ Docker compose stack│            │ pnpm dev (罕见)    │    │
│  │ - data-sync        │            │ - desktop-ui HMR   │    │
│  │ - ai-service       │            │ - data-sync uvicorn│    │
│  │ - desktop-ui       │            │ - ai-service tsx   │    │
│  │ - pgadmin          │            └────────────────────┘    │
│  └────────────────────┘                                     │
│           ▲                                                  │
│           │ docker compose up -d                              │
│           │                                                  │
│  ┌────────┴────────────────────────────────────────────────┐ │
│  │  macOS LaunchAgent                                       │ │
│  │  com.karios.docker-up.plist                              │ │
│  │  - RunAtLoad: true                                       │ │
│  │  - 开机登录后自动跑 scripts/docker-up.sh                │ │
│  └────────────────────────────────────────────────────────┘ │
│           ▲                                                  │
│           │ docker compose down                              │
│  ┌────────┴────────────────────────────────────────────────┐ │
│  │  UPS hook (nut/apcupsd)                                  │ │
│  │  - 检测到 low battery                                    │ │
│  │  - 调 scripts/ups-shutdown.sh → pmset shutdown now       │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                ▲
                │ Cloudflare Tunnel (OPT-048)
                ▼
        远程设备 / 家人 / 手机
```

**关键约束**：
- **没有 docker postgres service**——compose 里**移除** postgres 容器，让 data-sync 直连本地 PG
- **Alembic 迁移**仍要跑（一次性 init，跑在本地 PG 上）
- **桌面 UI** = nginx 反代 `/api/` → data-sync:4330、`/ai/` → ai-service:4310、`/` → Next 静态
- **`host.docker.internal:5432`** = data-sync 在容器内连宿主 PG 的标准方式

---

## 4. 与现状的差异（执行清单）

未来切换到 Mac mini 部署时，对当前代码做以下改动：

### 4.1 docker-compose.yml 改动

```diff
  services:
-   postgres:
-     image: postgres:16-alpine
-     container_name: karios-postgres
-     ...
-     healthcheck:
-       test: ['CMD-SHELL', 'pg_isready ...']

-   migrate:
-     build: ./services/data-sync-service
-     command: ['alembic', 'upgrade', 'head']
-     environment:
-       DATABASE_URL: postgresql://admin:admin123@postgres:5432/...
-     depends_on:
-       postgres: { condition: service_healthy }
-     restart: 'no'

    data-sync:
      ...
      environment:
-       DATABASE_URL: postgresql://admin:admin123@postgres:5432/karios-desktop
+       DATABASE_URL: postgresql://admin:admin123@host.docker.internal:5432/karios-desktop
-     depends_on:
-       postgres: { condition: service_healthy }
-       migrate: { condition: service_completed_successfully }
+     depends_on: []
+       # 不依赖 docker postgres；连本地 PG
+     extra_hosts:
+       - 'host.docker.internal:host-gateway'
```

### 4.2 alembic 迁移：手动跑（不再用 migrate service）

```bash
# 在 Mac mini 上（不是容器内）：
cd ~/Projects/karios-desktop/services/data-sync-service
PYTHONPATH=src uv run alembic upgrade head
```

→ 把这步做成 `scripts/db-migrate.sh`，纳入 `docker-up.sh` 的 pre-flight。

### 4.3 LaunchAgent 不变（已就绪）

`scripts/install-launchd.sh` 已存在，跑一次即可。

### 4.4 UPS hook 不变（已就绪）

`scripts/ups-shutdown.sh` 已存在，由 nut/apcupsd 调。

### 4.5 .env 改动

```diff
  POSTGRES_USER=admin
  POSTGRES_PASSWORD=admin123
- POSTGRES_PORT=5432
+ # POSTGRES_PORT removed — Mac mini uses local PG on default 5432
+ # POSTGRES_HOST=host.docker.internal  # if docker needs to reach local PG
  DATABASE_URL=postgresql://admin:admin123@localhost:5432/karios-desktop
+ # ↑ 这个 URL 给 pnpm dev 用；docker 里改写成 host.docker.internal:5432
```

---

## 5. 实施时序（Mac mini 拿到那天）

### Phase 1：基础设施（1 天）

```bash
# 1. 安装工具链
brew install pnpm node@22 uv postgresql@16 cloudflared
brew services start postgresql@16    # 系统级 PG 服务
# 验证：psql -U admin -d karios-desktop -c '\dt'

# 2. 初始化 schema（一次性）
createdb -U admin karios-desktop
git clone <repo-url> ~/Projects/karios-desktop
cd ~/Projects/karios-desktop
pnpm install && pnpm install:all
PYTHONPATH=services/data-sync-service/src uv run \
    --project services/data-sync-service \
    alembic upgrade head

# 3. 准备 .env
cp .env.example .env
$EDITOR .env  # 填真实 key
```

### Phase 2：Docker 化（半天）

```bash
# 4. 应用本文档 §4 的 docker-compose.yml diff
# 5. 应用 §4.5 的 .env diff

# 6. 首次 build
scripts/docker-up.sh
# 预期：build 5-10 分钟 + 启动 + 全部 healthz 通过

# 7. 验证
scripts/docker-status.sh
# 期望：7 service 全 OK，但 postgres 容器不在（用的是本地 PG）

# 8. 浏览器访问
open http://127.0.0.1:8080
```

### Phase 3：自动启动 + 远程（半天）

```bash
# 9. LaunchAgent（开机自启 docker stack）
scripts/install-launchd.sh

# 10. Cloudflare Tunnel（远程访问）
brew install cloudflared
scripts/setup-named-tunnel.sh    # 见 OPT-048 文档
# 完成后 <your-domain>.com 直连 Mac mini

# 11. UPS hook（断电保护）
brew install nut
# 配置 /etc/nut/ups.conf（按 nut 文档）
# /etc/nut/upsmon.conf:
#   SHUTDOWNCMD "/Users/<you>/Projects/karios-desktop/scripts/ups-shutdown.sh"
```

### Phase 4：验收（半天）

| 检查项 | 命令 |
|--------|------|
| 重启后 docker stack 自动起 | `sudo shutdown -r now` + 5 分钟后再开浏览器 |
| 断电后正常关机 | 拔电源模拟（小心） |
| Tunnel 远程访问 | 用 4G 网络访问 <your-domain>.com |
| 数据备份 cron | `crontab -l` 含 pg_dump 行 |
| 异地备份 | 检查 iCloud Drive / Dropbox 同步 |

---

## 6. 日常维护（极少）

| 频率 | 动作 | 命令 |
|------|------|------|
| **代码改了想看效果** | rebuild + restart | `scripts/docker-up.sh --rebuild` |
| **每周** | 检查 backup cron 是否成功 | `cat ~/Library/Logs/karios/*.log` |
| **每月** | 检查磁盘空间 + PG vacuum | `pg_dump --schema-only` 看大小 |
| **每季度** | OS 更新 + Docker Desktop 更新 | 常规 |
| **每年** | 硬盘健康检查 + 备份验证 | `pg_restore` 试恢复到 test DB |

**不要**：
- 不要 `pnpm dev` 在 Mac mini 上跑（除非调试）
- 不要手动改 schema（必须 Alembic revision）
- 不要让 docker postgres service 复活（会跟本地 PG 抢 5432）

---

## 7. 触发条件（什么时候做这个迁移）

**今天不需要做**。等以下任一触发：

| 触发 | 紧迫度 |
|------|--------|
| 你获得了 Mac mini（或同等 7×24 设备） | **立即** |
| 你当前的 Mac 出现健康问题（电池 / 风扇 / 硬盘 SMART warning）| 1-2 周 |
| 你想给家人开账号 / 远程 demo | 1 周 |
| 你想给 AI agent 全天候 /v1/ 访问 | 1 周 |
| 你的"daily dev"生活变了（远程为主）| 1 个月 |
| 任何时点你不想天天维护 | 立即 |

**反触发**（不需要做）：
- 你现有的 Mac 跑得很好 + 你每天都在用
- 你只是想要个 backup（pg_dump cron 够了）
- 远程访问需求不强（Tunnel + pnpm dev 够用）

---

## 8. 失败模式与恢复

| 失败 | 检测 | 恢复 |
|------|------|------|
| **Mac mini 物理损坏** | 你访问不到 | 新 Mac：git clone + pg_restore 备份 → 4 小时 |
| **PG 数据损坏** | 启动报错 | pg_dump 备份 restore → 1 小时 |
| **Docker image 坏** | container 起不来 | `docker compose pull` + rebuild → 30 分钟 |
| **Cloudflare 账号被封** | 域名 502 | 启 Tailscale Funnel（§13 #2）→ 1 小时 |
| **UPS 失灵** | 意外断电 | LaunchAgent 重启后跑 `docker compose up -d` → 5 分钟 |
| **网络断** | 远程访问失败 | 修网络 → Tunnel 自动恢复 |
| **代码有 bug** | Karios 行为异常 | `git checkout` + `scripts/docker-up.sh --rebuild` |

---

## 9. 反例（什么时候**不**做这个方案）

| 情况 | 该用别的方案 |
|------|-------------|
| 你只有笔记本 + 经常出差 | **Tauri 桌面**（便携） |
| 你完全不打算养 Mac mini | **全云**（Neon + Vercel） |
| 你只想 pnpm dev 不折腾 | **当前现状**（什么都不改）|
| 你要部署到 Linux 服务器 | **直接照搬**（架构一致；只需改 brew → apt）|
| 你的 Karios 只跑 1-2 个服务 | **不需要 Docker**（直接 host process）|

---

## 10. 与其他文档的关系

```
docs/designs/mac-mini-deployment.md  (本文档 · Mac mini 时代部署方案)
        │
        ├─► karios-longevity-2026-08.md  (3 个真痛点)
        ├─► db-direction-2026-08.md       (本地 PG + 备份策略)
        ├─► docker-one-click.md           (当前 docker compose 实施)
        ├─► cloud-deployment-options.md   (云 vs 本地架构)
        └─► freelancer-architecture.md    (Tunnel + /v1/* 集成)
```

> **未来 review**：
> - 任何关于"Mac mini 上怎么跑 Karios"的讨论 → 先读本文件
> - 任何关于"换电脑 / 长期生命力"的讨论 → [`karios-longevity-2026-08.md`](./karios-longevity-2026-08.md)
> - 任何关于"DB 备份策略"的讨论 → [`db-direction-2026-08.md`](./db-direction-2026-08.md)
> - 任何关于"代码更新"的讨论 → [`docker-one-click.md`](../setup/docker-one-click.md)

---

## 11. 决议日 + 复审

| 日期 | 触发 | 行动 |
|------|------|------|
| **2026-08-01** | 决议 | 本文档立 |
| **2027-02-01** | 半年期强制 | 检查是否已具备 Mac mini；如无，§7 触发条件是否成熟 |
| **任何时点** | 用户拿到 Mac mini | 按 §5 时序执行 |

---

最后更新：2026-08-01