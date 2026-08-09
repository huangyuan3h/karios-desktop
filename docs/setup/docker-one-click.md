# Docker 一键起 + UPS 自动恢复

> todo §12 #7 · §13 · OPT-056 · [`mac-mini-deployment.md`](../designs/mac-mini-deployment.md)
>
> 目的：让 Karios 在 **Mac mini 长期开机部署**场景下自动启动 + 断电恢复，**不依赖云**。
>
> **使用边界**：日常开发仍用 `pnpm dev`（见 §「与开发模式的区别」）。Docker 栈是为 **Mac mini 时代** 准备的——本文档配套 [`mac-mini-deployment.md`](../designs/mac-mini-deployment.md) 一起读。

## 概览

```
┌─────────────────────────────────────────────────────────────────┐
│  Karios Docker stack                                              │
│                                                                  │
│  postgres:16-alpine  ─┐                                          │
│                        │                                          │
│  data-sync-service  ──┤── karios-net bridge                      │
│  (FastAPI · 4330)     │       │                                  │
│                        │       │                                  │
│  ai-service         ──┤       ▼                                  │
│  (Hono · 4310)        │                                          │
│                        │   migrate (一次性 Alembic 跑完即退出)  │
│  desktop-ui         ──┤                                          │
│  (nginx · 8080)       │                                          │
│                        │                                          │
│  rsshub            ───┤  (optional · 1200)                      │
│  pgadmin           ───┘  (optional · 8081)                      │
└─────────────────────────────────────────────────────────────────┘
                ▲                                       ▲
                │ host.docker.internal (macOS only)     │
                │ TV Chrome capture 走宿主 Chrome       │
                └───────────────────────────────────────┘
                          ▲
                          │ pmset / cloudflared / launchd
                          │
                  ┌───────────────────────┐
                  │  macOS host           │
                  │  - Docker Desktop      │
                  │  - LaunchAgent         │
                  │    com.karios.docker-up│
                  │    (RunAtLoad)         │
                  │  - UPS hook (optional) │
                  └───────────────────────┘
```

## 前置条件

- **macOS**（Apple Silicon 或 Intel）
- **Docker Desktop for Mac** 24+（含 `docker compose` v2）
- **Git**（克隆仓库）
- 8 GB 内存建议（postgres + data-sync + ai-service + desktop-ui 静态服务）
- 可选：**Cloudflare 账户**（远程访问走 Tunnel，OPT-048）
- 可选：**UPS + 监控软件**（`nut` / `apcupsd`）—— 自动断电保护

## 一键启动

```bash
# 1. 克隆（如果你在新电脑上）
git clone <repo-url> karios-desktop
cd karios-desktop

# 2. 安装依赖（pnpm + uv）
pnpm install
pnpm install:all           # 含 services/data-sync-service 的 uv sync

# 3. 准备 .env
cp .env.example .env
# 编辑 .env：填 TU_SHARE_API_KEY / OPENAI_API_KEY / GOOGLE_GENERATIVE_AI_API_KEY 等

# 4. 一键起！
scripts/docker-up.sh

# 5. （可选）安装开机自启 LaunchAgent
scripts/install-launchd.sh
```

第一次 `docker-up.sh` 会 build 3 个 image（data-sync / ai-service / desktop-ui），约 5-10 分钟。后续启动 < 30 秒。

启动后：

| URL | 服务 |
|-----|------|
| http://127.0.0.1:8080 | Web UI（nginx 静态 + 反向代理） |
| http://127.0.0.1:4330 | data-sync FastAPI（含 `/healthz` + `/v1/*`） |
| http://127.0.0.1:4310 | ai-service Hono（含 `/healthz`） |
| http://127.0.0.1:8081 | pgAdmin（数据库管理） |
| http://127.0.0.1:1200 | RSSHub（Alpha Radar 中文 RSS） |

## 状态检查

```bash
scripts/docker-status.sh          # 容器状态 + healthz
scripts/docker-status.sh --logs   # 含最近 50 行日志
```

## 停机

```bash
scripts/docker-down.sh            # 保留数据卷（postgres data 不丢）
scripts/docker-down.sh --volumes  # 同时删数据卷（谨慎！）
```

## 换电脑 / 新机器

新机器上重复上面 **「一键启动」** 的 5 步即可。docker-compose + .env 是声明式的，无需手工复制数据库（除非你想这么做）：

```bash
# 从旧机器导出 postgres 数据
docker exec karios-postgres pg_dump -U admin karios-desktop > backup.sql

# 在新机器上导入
docker exec -i karios-postgres psql -U admin -d karios-desktop < backup.sql
```

或者更简单：用 OPT-053 决定的方案（见 `docs/designs/db-direction-2026-08.md`）做异地备份。

## 升级迁移（旧版用户）

如果你之前用的是老 `docker-compose.yml`（含 `postgres-db / pgadmin-web / karios-rsshub` 三个容器），新 compose 用了同名端口。**先迁移再起**：

```bash
scripts/docker-up.sh --migrate
```

这会停止旧容器后再启动新栈。**Postgres 数据通过同名 volume 保留**——老 volume `karios-desktop_postgres-data` 与新 volume `karios_postgres-data` 是不同的 volume，所以需要按上面的 `pg_dump` / `psql` 流程手动迁移。

> 临时方案：如果你不想迁移数据，直接 `docker volume rm karios-desktop_postgres-data` 然后起新栈（数据会丢，但配置还在 `.env` 里）。

## UPS 自动恢复（可选）

### 原理

```
┌────────────┐  lowbattery   ┌──────────────────────┐
│ UPS 设备    │──────────────▶│ UPS monitor           │
│ (APC 等)   │               │ nut / apcupsd          │
└────────────┘               └──────────┬─────────────┘
                                          │ exec SHUTDOWNCMD
                                          ▼
                                ┌──────────────────────┐
                                │ scripts/ups-shutdown.sh│
                                │  1. docker compose down│
                                │  2. pmset shutdown now │
                                └──────────────────────┘
                                          │
                                          ▼
                                ┌──────────────────────┐
                                │  Mac 关机              │
                                │  供电恢复后自动开机     │
                                │  LaunchAgent 自动起栈  │
                                └──────────────────────┘
```

### 安装 nut（推荐）

```bash
brew install nut
# 配置 /etc/nut/ups.conf + upsmon.conf（参见 nut 官方文档）
# 在 upsmon.conf 里设置：
SHUTDOWNCMD "/path/to/karios-desktop/scripts/ups-shutdown.sh"
```

### 安装 apcupsd（APC 专用）

```bash
brew install apcupsd
# 配置 /etc/apcupsd/apcupsd.conf
# 修改 /etc/apcupsd/apccontrol 的 doshutdown 函数为：
#   /path/to/karios-desktop/scripts/ups-shutdown.sh
```

### 不做 UPS 监控

如果暂时没 UPS，可以不装。UPS 这部分纯外挂，Karios 本身 **不** 监测电池电量。

## 开机自启（macOS LaunchAgent）

`scripts/install-launchd.sh` 会创建 `~/Library/LaunchAgents/com.karios.docker-up.plist`：

```xml
<key>RunAtLoad</key><true/>
```

- 每次登录 macOS 后自动跑 `scripts/docker-up.sh`
- 日志在 `~/Library/Logs/karios/docker-up.{out,err}.log`
- 卸载：`scripts/uninstall-launchd.sh`

**前提**：
- Docker Desktop 设置为「Start Docker Desktop when you sign in」
- 系统设置 → 节能 → 「唤醒以供网络访问」开启（否则远程访问场景下 Mac 不会自动唤醒）

## 与开发模式的区别（**重要**）

| 模式 | 命令 | 用途 | 代码改了怎么办 |
|------|------|------|----------------|
| **Docker 一键起**（**跑 Karios**） | `scripts/docker-up.sh` | 实盘 / 看盘 / 远程访问 / 给家里人看 / 验证生产形态 | `scripts/docker-up.sh --rebuild`（5-10 分钟） |
| **dev 热重载**（**改代码**） | `pnpm dev` | 日常 Python / TS 开发，HMR 1-3 秒生效 | 自动 reload（**不用动 Docker**） |

**核心原则**：

- **改代码 → `pnpm dev`**。Docker compose 不参与日常开发（不是为开发设计的）。
- **跑 Karios → `scripts/docker-up.sh`**。代码已定型后用。
- **代码改了想立即跑 Docker**：先 `pnpm dev` 验证 → 满意后 `scripts/docker-up.sh --rebuild`（不重建 image 只重建 `data-sync` / `ai-service` / `desktop-ui` 的 source 层，约 2-3 分钟）。

**为什么不在 Docker 里做热重载**：
- Docker compose 是"运行 Karios"的工具，不是开发工具
- 在容器里跑 `next dev` / `uvicorn --reload` 比 `pnpm dev` 慢（多一层 volume mount + Docker 网络）
- 行业标准做法就是两套：dev mode = 本机 HMR，prod mode = container baked image

**改代码后想跑 Docker，build 实际多久？**

| 改动 | 重新 build | 备注 |
|------|-----------|------|
| 首次 build（全栈） | 5-10 分钟 | 下载 base image + 装所有依赖 |
| Python 文件 | **~10 秒** | 仅 `COPY src` layer 重 build，uv deps 层 CACHED |
| `pyproject.toml` / `uv.lock` | 1-2 分钟 | uv deps 层重装；runtime layer CACHED |
| `package.json` / `pnpm-lock.yaml` | 2-3 分钟 | pnpm install 重跑；static export 增量 build |
| Desktop UI TS 文件 | 30-60 秒 | 仅 `next build` 重跑 |
| 共享 schema (`packages/shared`) | 1-2 分钟 | desktop-ui 重 build（依赖 shared） |
| `Dockerfile` / `docker-compose.yml` | 全栈 | 配置改了一定要 `--rebuild` |

**不要同时跑**：docker-up.sh 占了 4330 / 4310 / 5432 等端口，pnpm dev 也用同样端口；会冲突。跑 docker-up 前先 `pnpm dev` 退出（或反过来）。

## 故障排查

### `docker compose up` 失败：端口被占用

```
bind: address already in use :::5432
```

谁占着：

```bash
lsof -nP -tiTCP:5432 -sTCP:LISTEN
```

一般是老 stack 没清干净。`scripts/docker-up.sh --migrate` 自动停。

### `data-sync /healthz` 一直 FAIL

等 30 秒。Postgres 启动后 data-sync 才连上。

如果一直 FAIL：

```bash
docker compose logs data-sync
docker compose logs migrate
```

看 `migrate` 是否成功退出（`service_completed_successfully`）。

### `desktop-ui /healthz` FAIL，但 data-sync OK

桌面 UI 的 nginx 容器内部重试了，但没起来。看：

```bash
docker compose logs desktop-ui
```

常见原因：desktop-ui 的 build 阶段失败（缺 `@karios/shared`）。重新 `scripts/docker-up.sh --rebuild`。

### Chrome capture 失败

TV capture 走宿主 Chrome（`http://host.docker.internal:9222`）。如果 Mac 上 Chrome 没开 remote-debugging：

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
    --remote-debugging-port=9222 --remote-debugging-address=0.0.0.0
```

或者暂时停用（用 Tushare 等价查询路径，参见 §6 数据源决策 §12 #8.5）。

## 相关文档

- `docs/optimization-checklist.md` OPT-056（实现细节）
- `docs/designs/karios-longevity-2026-08.md` §13 设计真值
- `docs/designs/db-direction-2026-08.md` Postgres 备份策略
- `docs/designs/cloudflare-tunnel-setup.md` 远程访问（OPT-048）
- `scripts/data-source-healthcheck.sh` 数据源健康检查

---

最后更新：2026-08-01