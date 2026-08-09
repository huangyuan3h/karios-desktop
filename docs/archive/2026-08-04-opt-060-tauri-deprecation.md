# OPT-060 形态迁移 · Tauri 降级 · 归档于 2026-08-04

> 决议：Karios **不再以 Tauri 桌面形态** 作为发布目标。保留 `src-tauri/` Rust 源码 + build 配置（必要时可手动 `cargo build`），但 **不再** 把它接入日常 dev 流程、文档主路径、依赖管理。

## 当时的目标（todo 链接）

- `docs/todo.md §2`：
  - [P0] **Tauri 桌面 vs 固定 URL**：用户明确倾向"固定 URL + 每张页面有专属链接"，桌面打包不是刚需 → 形态决策文档化到 `docs/archive/`
  - [P0] **形态迁移路线**：保持当前 Next.js dev，把"对外可访问性"提到 P0；Tauri 不再作为主线交付形态（**暂保留 build 配置但不做为发布目标**）
- `docs/todo.md §4`：[P2] Tauri 构建降级 — 保留但停止维护 desktop 形态的 bug 修复
- `docs/todo.md §12 #11`：形态迁移（Tauri 降级） · §2 定位 · 1 天 · 长期减少维护面

## 决策（核心一句话）

> **Web 形态 = Karios 的唯一交付形态**（`pnpm dev` / `docker compose`）。
> Tauri 桌面 = **保留源码不维护**：源码 + build 配置留在 `apps/desktop-ui/src-tauri/`，sidecar build 脚本留在 `scripts/build-sidecars-macos.sh`，**未来真要重新启用 Tauri 时，git history + 这份文档足够重建**。

### 为什么是这个选择

| 维度 | 桌面（Tauri） | Web（pnpm dev / Docker） |
|------|---------------|---------------------------|
| **多端访问** | 一台 Mac 一个窗口 | 任意浏览器 / Tunnel 远程 / 家人共享 |
| **数据所有权 + 离网** | 一样 | 一样 |
| **远程访问** | 需要 VNC / 端口映射 | Cloudflare Tunnel 已通（OPT-048）|
| **构建复杂度** | Rust toolchain + Tauri CLI + sidecar 编译（PyInstaller + Bun compile）+ 平台打包 | `pnpm dev` 一行 / `docker compose up -d` 一行 |
| **发布形态数量** | 2（dev + desktop）| 1（web = pnpm dev = docker）|
| **维护面** | Tauri v2 升级、sidecar 同步、跨平台 bundling | 一个 Next.js 工程 |
| **AI agent 打通** | 桌面内嵌 → 难拉数据给外部 AI | `/v1/*` 已通（OPT-045~047） |

**用户原话（2026-08-01）**："我倾向于系统整体稳定部署 docker，然后自动启动，db 用现在的，不用两个"——明确把 **docker 单形态 + 7×24 在线** 作为长期方向；Tauri 桌面与该方向不重合。

`docs/designs/mac-mini-deployment.md §2.3` 已经写过 "Tauri 适合笔记本 + 单人 + 出差；Mac mini + Docker 适合家里 7×24 跑 + 多端访问"——本文档把那条对比**从"设计稿"升格为"已落地决策"**。

## 实际做了什么

### 代码 / 脚本（活跃路径移除）

| 文件 | 改动 |
|------|------|
| `package.json`（根） | 删 `predev:tauri` / `dev:tauri` 两条 script；删 devDep `concurrently`（仅被 `dev:tauri` 使用）|
| `apps/desktop-ui/package.json` | 删 `tauri` / `tauri:dev` / `tauri:build` 三条 script；删 dep `@tauri-apps/api`（src 内零引用）；删 devDep `@tauri-apps/cli`（无 script 调用）|
| `apps/desktop-ui/next.config.ts` | 注释里"Tauri loads static assets" → 改为 "Static export required by Docker nginx"（动机不再为 Tauri 服务）|
| `services/data-sync-service/Dockerfile` | 注释 "matches live/Tauri conventions" → "matches live conventions" |

### 代码（保留 = 不动）

| 路径 | 为什么保留 |
|------|------------|
| `apps/desktop-ui/src-tauri/`（整个目录：`Cargo.toml` / `Cargo.lock` / `tauri.conf.json` / `tauri.backends.conf.json` / `build.rs` / `src/lib.rs` / `src/backends.rs` / `src/main.rs` / `icons/` / `capabilities/` / `gen/` / `sidecars/`) | §2 P0 "**暂保留 build 配置**"；未来真要重启桌面形态，源码 + Cargo.lock + sidecar 已就绪 |
| `scripts/build-sidecars-macos.sh` | 同上；这是 Tauri 侧 sidecar（ai-service + data-sync-service 编译成单文件）的唯一入口；保留 |
| `.gitignore` 行 35 `# Tauri / Rust` + 行 37 `src-tauri/target/` | 仍然适用（手动 `cargo build` 时 target/ 应忽略）|
| `apps/desktop-ui/src-tauri/.gitignore` | 同上 |
| `apps/desktop-ui/eslint.config.mjs` 里的 `src-tauri/target/**` | 仍然适用 |

### 文档（活跃路径更新）

| 文件 | 改动 |
|------|------|
| `README.md` | 行 163 表：`apps/desktop-ui` 行去掉"运行在 Tauri WebView 中" → "前端界面（Next.js；运行形态：dev / Docker compose）"；行 179 删 "Rust 工具链（用于 Tauri，需要 Rust >= 1.83）" 一行 |
| `AGENTS.md` | 表行 `apps/desktop-ui \| Next.js UI (Tauri WebView)` → `Next.js UI (web-only)` |
| `docs/README.md` | 行 59 `Frontend (Next.js / Tauri)` → `Frontend (Next.js)` |
| `docs/setup/docker-one-click.md` | 表中 "dev + tauri" 行删除（dev 模式只剩 `pnpm dev`）|

### 文档（保留 = 不动）

- `docs/designs/mac-mini-deployment.md §2.3 / §9`：保留——这是 §2 "vs 其他方案" 的对比文档，**不是活跃路径**
- `docs/designs/karios-longevity-2026-08.md §3 / §6`：保留——action list 提到 §12 #11；本文档落地后该引用自动转"已完成"，无需手动改
- `docs/designs/freelancer-architecture.md` §4.10：保留——行动清单引用 §12 #11，同上
- `docs/optimization-checklist.md OPT-056`：保留——已 closed 的历史记录，反模式节里的 "代码 / Tauri / Compose 全部统一" / "不**改 Tauri 桌面（Tauri 已在 §13 降级，独立路线）" / "**不**改 `desktop-ui` 的 `next.config.ts` 的 `output: 'export'`（这是 Tauri 与 Docker 唯一的共同基线）" 是当时 scope-bound 的真实约束，**不能回写历史**——但 `output: 'export'` 现状仍合理（Docker nginx 仍需要静态 export），无需调整

### 测试

`apps/desktop-ui/src/lib/tauri-deprecation.test.ts`（新 · 5 tests）：

1. 根 `package.json` 不含 `tauri` 相关 script
2. 根 `package.json` 不依赖 `concurrently`
3. `apps/desktop-ui/package.json` 不含 `tauri` / `tauri:dev` / `tauri:build` script
4. `apps/desktop-ui/package.json` 不依赖 `@tauri-apps/api` / `@tauri-apps/cli`
5. `src-tauri/` 与 `scripts/build-sidecars-macos.sh` 仍存在（build config 保留契约）

## 验证 / 数据

- 前端 typecheck / lint：clean（未动任何被 src 引用文件）
- 前端 vitest：全量跑通（含新 5 tests）
- 后端 pytest：未受影响（不动任何 services/data-sync-service 业务代码，只改一行 Dockerfile 注释）
- `pnpm-lock.yaml`：**未手工改**——下次 `pnpm install` 会自动清掉 `@tauri-apps/*` + `concurrently` 块（用户首次拉新代码后跑 `pnpm install` 即可）

## 后续影响 / 留给谁

- **谁需要重新启用 Tauri**：读本归档 + `apps/desktop-ui/src-tauri/Cargo.toml` + `scripts/build-sidecars-macos.sh`。预计 ≤ 0.5 天接入（装 Rust + tauri-cli + 跑 sidecar 编译 + `pnpm tauri dev`）
- **谁不该做**：日常开发 / CI / 文档 / 部署——Tauri 不再是这些路径的"备选发布形态"
- **未完成的相邻任务**：`docs/todo.md §2 P1 可分享 / 可订阅 URL`（每张关键页面有 stable URL）—— 与本任务**不冲突**，独立推进
- **未来 review 触发**（满足任一即重读本归档）：
  - 用户拿到 Mac mini 后想"在外不靠 Tunnel 用桌面 client" → 评估 Tauri 是否值得复活（[`mac-mini-deployment.md` §9](../designs/mac-mini-deployment.md) "你只有笔记本 + 经常出差" 反例）
  - 家人需要"零配置桌面图标"且不愿装 Docker → 同上
  - 出现真正的"出差多 / 无固定办公环境"使用场景 → 同上

---

最后更新：2026-08-04（OPT-060 落地）
