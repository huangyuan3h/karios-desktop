# OPT-056 · Docker 一键起 + UPS 自动恢复

> 2026-08-01 · [todo §12 #7](../todo.md) · [§13 Longevity](../todo.md)
> 真值文档：[`docs/setup/docker-one-click.md`](../setup/docker-one-click.md)
> 关联设计：[`docs/designs/karios-longevity-2026-08.md`](../designs/karios-longevity-2026-08.md) §3.1 / §3.2 / §3.5

## 一句话

Karios 现在可以 `git clone && scripts/docker-up.sh` 一键起完整栈；UPS 断电由外挂（nut/apcupsd）触发 `ups-shutdown.sh` → `pmset shutdown`；重启后 macOS LaunchAgent 自动跑 `docker-up.sh`。**完全本地、零云依赖**。

## 解决的问题

| 痛点（用户原话） | 现状（2026-08-01 前） | 落地后 |
|----------------|----------------------|-------|
| "换电脑也能跑" | 无 Dockerfile，post-up 文档要 1-3 天 | `docker compose up` ~30 秒；首次 build 5-10 分钟 |
| "长期生命力" | 旧栈 6 周没重启就漂移 | 单一 docker-compose.yml + .env 声明式；换机器即复制 |
| 远程部署 / UPS | 不在本次范围（§13 #1-3 暂缓） | UPS 自动恢复独立完成（#0），远程部署仍待 §14 webhook + Cloudflare Tunnel |

## 改了什么

### 新增 11 个文件

| 文件 | 用途 |
|------|------|
| `services/data-sync-service/Dockerfile` | Python 3.13-slim + uv + tini + non-root + healthcheck |
| `services/data-sync-service/.dockerignore` | 排除 tests/、.venv/、init-data/ |
| `apps/ai-service/Dockerfile` | node:22-alpine + pnpm + tini + non-root + healthcheck |
| `apps/ai-service/.dockerignore` | 排除 dist/、node_modules/、tests |
| `apps/desktop-ui/Dockerfile` | 多阶段：node:22 build → nginx:1.27-alpine runtime + non-root + healthcheck |
| `apps/desktop-ui/nginx.conf` | 反代 `/api/` → data-sync:4330、`/ai/` → ai-service:4310、`/` 静态 SPA fallback |
| `.dockerignore`（root） | 排除 .git/、docs/、tests/、.env（!env.example）、node_modules/、dist/、__pycache__/ |
| `.env.example` | 全栈变量模板（含 `KARIOS_UPS_SHUTDOWN_HOOK`） |
| `docs/setup/docker-one-click.md` | 完整 bringup / migration / UPS / launchd / 故障排查 文档 |
| `services/data-sync-service/tests/test_docker_one_click.py` | 57 个 smoke 测试 |
| `scripts/{docker-up,docker-down,docker-status,install-launchd,uninstall-launchd,ups-shutdown}.sh` | 6 个用户入口脚本 |

### 修改 3 个文件

| 文件 | 改了什么 |
|------|---------|
| `docker-compose.yml` | 重写：加 `name: karios`、加 4 service（`migrate`/`data-sync`/`ai-service`/`desktop-ui`）、加 healthcheck（pg_isready / curl / wget）、加 `depends_on: service_healthy` 强顺序、改端口范围避开旧冲突（5432/8080/1200）、保留 `postgres/pgadmin/rsshub` |
| `docs/todo.md` | §12 #7 状态更新（待补） |
| `docs/optimization-checklist.md` | 新增 OPT-056 全条目（含背景/目标/范围/反模式/验证） |

### 1 个副作用改动

`~/Library/LaunchAgents/com.karios.docker-up.plist` 实际安装在用户机器上（已 `plutil -lint` 通过）。如不需要可 `scripts/uninstall-launchd.sh` 卸载。

## 关键设计决策

1. **数据卷分离** — 老 `karios-desktop_postgres-data` 与新 `karios_postgres-data` 是不同的 volume（因为 compose `name:` 变了）。老用户必须按 setup doc 走 `pg_dump` / `psql` 迁移。
2. **HOST=0.0.0.0 强制** — data-sync 容器必须监听所有接口（之前 dev 命令绑定 `127.0.0.1`，容器内只能自连）。
3. **不 bake secret** — 所有 key 通过 `env_file: ./.env` 注入；Dockerfile 用 `${VAR:-default}` 兜底；无 `.env` 时回退到 `.env.example` 默认值（开发模式）。
4. **migrate 作为一次性 init service** — `restart: "no"` + `command: alembic upgrade head`；app services 用 `depends_on: migrate: condition: service_completed_successfully` 强等。**新增 Alembic revision 文件必须重跑 migrate**。
5. **UPS 不做监控** — macOS 无原生 UPS API；只提供 hook 脚本，由 nut/apcupsd 调。
6. **LaunchAgent 用户级** — 不需 sudo；`LimitLoadSessions=1` 防止嵌套会话重复跑。

## 测试覆盖（57 个）

| 类别 | 数量 | 内容 |
|------|-----:|------|
| 脚本存在 + 可执行 | 12 | 6 脚本 × 2 维度 |
| `bash -n` 语法 | 6 | 所有脚本通过 |
| `--help` 返回 Usage | 6 | 所有脚本支持 |
| plist XML 校验 | 1 | `plutil -lint`（仅 macOS） |
| `docker compose config` 解析 | 1 | 7 services 全部识别 |
| compose 服务声明 | 1 | 7 services 全部存在 |
| compose 端口绑定 | 1 | data-sync 必须 `HOST=0.0.0.0` |
| `.env.example` 覆盖 | 12 | 所有 compose 引用 key 都有 |
| Dockerfile 存在 | 3 | data-sync / ai-service / desktop-ui |
| Dockerfile 无 `:latest` | 3 | 全部 pin 死 |
| Dockerfile 端口正确 | 2 | 4330 / 4310 |
| desktop-ui 用 nginx | 1 | `FROM nginx:X.Y-alpine` |
| nginx.conf 反代 | 1 | `/api/` → data-sync:4330，`/ai/` → ai-service:4310 |
| `.dockerignore` 内容 | 1 | 排除 .env + node_modules |
| 文档完整性 | 3 | 7 节齐全、6 脚本全引用 |

## 端到端（用户首跑清单）

```bash
cd karios-desktop

# 1. 依赖
pnpm install
pnpm install:all

# 2. .env
cp .env.example .env
$EDITOR .env       # 填 TU_SHARE_API_KEY / OPENAI_API_KEY

# 3. 起栈（旧用户必加 --migrate）
scripts/docker-up.sh --migrate

# 4. 验证
scripts/docker-status.sh
# 应看到:
#   ✓ data-sync /healthz
#   ✓ ai-service /healthz
#   ✓ desktop-ui /healthz

# 5. 访问
open http://127.0.0.1:8080

# 6. 开机自启（可选）
scripts/install-launchd.sh
```

## 后续动作（不在本次范围）

- **§13 #1/#2/#3**（远程部署 / Neon / Tailscale / 临时 VM）：用户 review 后暂缓，等真需要时再开
- **§14 #2** `/v1/*` rate-limit retry cookbook
- **§14 #3** 决策/告警 webhook
- **§12 #8.5** TV Capture 数据源决策（Tushare vs TV Scanner API）
- Chrome capture 在容器内运行（如要）：需要换 `puppeteer-core` 或自带 Chromium image（独立优化）

## 反模式回顾（OPT 文档已记录）

- ❌ 用 `:latest` 镜像标签 → ✅ 全部 pinned
- ❌ `network_mode: host` → ✅ 用 bridge + 服务名 DNS
- ❌ secret echo 到 stdout → ✅ 测试用 `--quiet` mode，断言 stdout 不含 key
- ❌ launchd plist 用 sudo → ✅ 用户级 `~/Library/LaunchAgents/`
- ❌ 默认开机自启 UPS hook → ✅ 显式 `KARIOS_UPS_SHUTDOWN_HOOK` 才挂
- ❌ 容器内跑 `next dev` → ✅ 多阶段 build 静态 export
- ❌ 改既有 `docker-compose.yml` 的 postgres 凭据 → ✅ 保留 admin/admin123
- ❌ AI service 容器持久化 secret → ✅ 默认不挂 `KARIOS_APP_DATA_DIR`，由用户在 `docker-up.sh` 选择