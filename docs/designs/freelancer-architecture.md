# Freelancer Architecture — 「自由人」系统架构

> **关联 todo**：[§2 形态](../todo.md) · [§3 API](../todo.md) · [§4 工程](../todo.md) · [§8 回测](../todo.md)  
> **配套**：[`cloud-deployment-options.md`](./cloud-deployment-options.md)（不上云 = Tunnel 暴露）  
> **决议日**：2026-08-01

---

## ⚠️ 职责边界（必读）

**Karios 只做三件事**：

1. **生成**：同步数据、计算指标（TrendOK / Score / Gate / funnel）
2. **展示**：UI（Dashboard / Watchlist / Industry Flow 等）
3. **暴露**：OpenAI 兼容 endpoint 给外部 AI 助手读

**Karios 不做**（明确分工，避免功能重合）：

- ❌ Telegram Bot / 推送通知 → **外部 AI 助手做**
- ❌ 日报生成 / 自然语言决策代理 → **外部 AI 助手做**
- ❌ 监控 / 异常报警 → **外部 AI 助手做**
- ❌ 任何"主动"动作（Karios 是被动的数据/endpoint 服务）

`apps/ai-service` 内部的 Chat Panel 是**局部 UI 辅助**（看 + 问），不替代外部 AI 助手的主动代理角色。

---

## TL;DR

把 Mac 当 homelab，让外部 AI 助手当"日报代理" / "推送代理" / "监控代理"，通过 Cloudflare Tunnel 调 Karios 的 `/v1/*` 拿数据。

**这不是"远程操控一切"**——是"AI 替你看盘 + 你拍板 + Mac 自动执行"。

---

## 物理拓扑

```
        钓鱼点 / 咖啡馆 / 旅行中
        ┌──────────────────────┐
        │  iPhone              │ ← 只装 Telegram（收通知）
        │  iPad                │ ← 只装 Safari（看 Dashboard）
        └──────────┬───────────┘
                   │ 4G/5G + TLS
                   ▼
        ┌──────────────────────────────────────┐
        │  Cloudflare Edge（免费）              │
        │  - 终止 Tunnel                       │
        │  - 域名绑 karios.{your-domain}      │
        │  - WAF 默认 / API Key 验证           │
        └──────────┬───────────────────────────┘
                   │ outbound only
                   ▼
   ┌──────────────────────────────────────────────────┐
   │  你的 Mac（永远开机 · UPS 撑 30min+）              │
   │                                                   │
   │   ┌──────────────┐  ┌──────────────┐  ┌─────────┐ │
   │   │ pnpm dev     │  │ Postgres     │  │ ai-svc  │ │
   │   │ (Next.js)    │  │ (本地权威)   │  │ (LLM)   │ │
   │   └──────┬───────┘  └──────┬───────┘  └────┬────┘ │
   │          └─────────────────┴────────────────┘     │
   │                       │                           │
   │              ┌────────┴─────────┐                 │
   │              │ cloudflared       │                 │
   │              │ daemon           │                 │
   │              └────────┬─────────┘                 │
   │                       │                           │
   │              ┌────────┴─────────┐                 │
   │              │ cloudflared      │                 │
   │              │ daemon           │                 │
   │              └────────┬─────────┘                 │
   └───────────────────────┼─────────────────────────────┘
                           │ outbound only · 无须公网 IP
                           ▼
              ┌─────────────────────────┐
              │ Cloudflare Edge (免费)  │
              │  - Tunnel 终止          │
              │  - 域名绑 karios.xxx    │
              │  - Bot 防御 / WAF       │
              └──────────┬──────────────┘
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
       iPhone/iPad   外部 AI 助手   家庭成员 / 朋友
       (看 Dashboard) (Telegram Bot  (只读访客)
                      / 日报代理 /
                      监控代理)
```

---

## 关键组件清单

### 硬件

| 项 | 推荐 | 预算 | 备注 |
|----|------|------|------|
| 主机 | Mac mini M2 / M2 Pro（16GB+） | $599+ | **不要 iMac/MBP**：功耗差 10× |
| UPS | APC Back-UPS 600VA | ~$80 | 撑断电 30min+，自动关机保护 |
| 网络 | 家用宽带 ≥ 100M 上行 | — | 上行比下行重要（出 Tunnel） |
| iPad | 11" iPad Air（M2）| $599 | **比 iPhone 更适合看 Dashboard** |

### 软件栈

| 层 | 工具 | 作用 | 归属 |
|----|------|------|------|
| 系统 | macOS 自动唤醒 / Wake-on-LAN | 断电恢复 + 远程唤醒 | Mac |
| 反向暴露 | `cloudflared`（Cloudflare Tunnel） | outbound only，无须公网 IP | Mac |
| Web | `pnpm dev` 起的 Next.js | 现状不变 | Karios |
| DB | 本地 Postgres | 权威源 | Karios |
| 局部 AI | ai-service（本地 LLM） | Chat Panel（局部 UI 辅助） | Karios |
| **外部 AI** | **用户独立项目** | **写日报 / 推 Telegram / 监控报警** | **AI 助手（不在 Karios 内）** |
| 推送通道 | Telegram Bot / Pushover / 邮件 | 由 AI 助手调用 | AI 助手 |
| 监控 | healthcheck | Mac 死机通知（可选：AI 助手接管） | Mac / AI 助手 |

---

## 「自由人」工作流

### 三个角色

| 角色 | 谁 | 干啥 |
|------|-----|------|
| **数据服务** | **Karios**（Mac + Postgres + cron） | 跑数据同步、算指标、存数据库、暴露 `/v1/*` |
| **决策者** | 你（iPhone/iPad） | 看 Dashboard → 拍板 yes/no |
| **代理** | **外部 AI 助手**（独立项目） | 调 `/v1/*` 拉数据 → 写日报 → 推 Telegram → 异常报警 |

### 一天节奏（你在旅行）

| 时段 | Karios（被动） | 外部 AI 助手（主动） | 你被动做 |
|------|---------------|---------------------|----------|
| 09:30 | TV 抓快照 + Alpha RSS ingest | — | — |
| 11:00 / 14:00 | 数据持续入库 | 调 `/v1/market/snapshot` 巡检 → 异常推 Telegram | 看一眼「有事/没事」 |
| 15:30 | 收盘 sync + sentiment + gate 计算 | — | — |
| 17:00 | Decision Journal 落库 | AI 助手调 `/v1/decision-journal/query` → 写「今日 3 件待拍板」→ 推 Telegram | 30 秒内 yes/no |
| 17:30 | Watchlist 自动化 + funnel 落库 | AI 助手调 `/v1/watchlist/items` 复盘 | — |
| 20:00 | — | AI 助手写日报 PDF + 周报（周五）| 晚上有空翻一下 |
| 异常时 | Trigger / Alpha S 落库 | AI 助手轮询 → 立刻推送 Telegram | 1 秒内响应 |

### 移动端职责边界

| iPhone | iPad |
|--------|------|
| Telegram（**由 AI 助手推**） | Safari Dashboard（**Karios 的 UI**） |
| 1 秒拍板 | 5 分钟深查 |
| 永不开浏览器 | 不开 Telegram |

> **永远不在移动端做**：买卖 / 改仓位 / 改 watchlist。这是不变量。

---

## 实施清单（按 ROI 排序）

> 详细 ROI 表见 [`docs/todo.md §12`](../todo.md)，本节是子集。

| # | 动作 | 工时 | ROI |
|---|------|------|-----|
| 1 | OpenAI 兼容 `/v1/chat/completions` 暴露 | 2-3 天 | ★★★★★ |
| 2 | Cloudflare Tunnel 部署（一行命令） | 0.5 天 | ★★★★★ |
| 3 | paper-trading daily 启动 | 2-3 天 | ★★★★ |
| 4 | API Key 配额 + OpenAPI 文档 | 1-2 天 | ★★★★ |
| 5 | 数据源质量审计 + 决策 | 1 天 | ★★★ |
| 6 | HK Alpha S 自动归类 | 1 天 | ★★★ |
| 7 | Docker 一键 + UPS 自动恢复 | 1-2 天 | ★★★ |
| 8 | ego-lite / 付费 API 调研 | 2-3 天 | ★★ |
| 9 | DB 走向决策文档 | 0.5 天 | ★★ |
| 10 | 形态迁移（Tauri 降级） | 1 天 | ★★ |
| 11 | BacktestPage 重写 | 3-5 天 | ★ |
| 12 | MCP server 暴露 | 1-2 天 | ★ |

> **AI 助手那边该做什么**（不在 Karios todo 里）：
> - Telegram Bot 实现 + 推送规则
> - 日报生成 prompt + 周报排版
> - 监控异常策略
> 详见 AI 助手自己的项目仓库。

---

## 「自动报告代理」设计（Karios 这边的接口约定）

这是整套架构的**杠杆点**：外部 AI 助手通过 Karios 的 `/v1/*` 拉数据，主动生成推送。

**Karios 这边只需保证**（AI 助手能稳定调通）：

### 暴露的接口

#### 4 个**稳定发现性** endpoint（路径不变，AI 助手启动先调这些）

```
GET  /v1/version      → 版本号；major 跳变时 AI 助手主动告警
GET  /v1/schema       → OpenAPI 3.1 JSON（全量 endpoint + 字段 schema + description）
GET  /v1/errors       → 错误码字典（含 recovery_hint）
GET  /v1/changelog?since=X  → 接口变更 diff
```

#### 业务 endpoint（随产品演进）

```
GET  /v1/healthz                 → 在线 + DB 连通
GET  /v1/market/snapshot?symbols=...  → 实时价/分时/技术指标
GET  /v1/watchlist/items         → 当前池 + Action / Trigger / HardStop
GET  /v1/decision-journal/query  → 近期决策 + Why + 触发时间
GET  /v1/screener/snapshots      → TV 池最新快照
GET  /v1/sentiment/current       → 指数红绿灯 + Gate mode
GET  /v1/news/recent             → 近期 news brief
GET  /v1/alpha-radar/recent      → 近期 Alpha S 候选
```

**接口契约细节**（versioning / 字段 description 规范 / 错误码字典 / changelog）见 [`api-contract.md`](./api-contract.md)。

**AI 助手那边的标准流程**：

1. 启动 → `GET /v1/version` 拿当前版本
2. 与缓存版本对比 → 跳 major → **主动告诉用户**「Karios API 升级了」
3. `GET /v1/changelog?since=老版本` 拿 diff
4. `GET /v1/schema` 重生成客户端 SDK（不需要人手维护）
5. 业务调用开始

### 推荐推送规则（给 AI 助手那边参考）

```
每日 17:00  push: 「今日 3 件待拍板」+ 每件 1-2 句
每日 17:30  push: 「盘后自动化摘要」+ funnel / top5 / alphaReject
每周五 20:00 push: 周报 PDF
实时        push: BUY/ADD Trigger · Alpha S · Gate flip · HardStop 命中
不推:       BUY/ADD 已经 trigger 过的、价格小波动、纯 RSS 摘要
```

### 推荐 Telegram 消息模板（极简）

```
🔴 待拍板 #1  300750 宁德时代
   Action=BUY · Score=88 · TrendOK ✅ · 主线=新能源
   触发: 收盘过 EMA20, 13:55
   一句话: 主线回调接 MA10，建议 +3% sleeve

🟡 待拍板 #2  00700 腾讯控股
   Action=HOLD · Score 跌至 72
   触发: 周五收盘 -2.1%, TrendOK 仍 ok

⚪ 跳过      600519 贵州茅台
   已 HOLD 3 日, 本周无操作
```

> **反原则**：不要在 Telegram 里塞完整 Dashboard。30 秒读完，剩下来的回家做。

---

## 风险与缓解

| 风险 | 概率 | 缓解 |
|------|------|------|
| Mac 断电 | 中 | UPS + 自动恢复 + Wake-on-LAN |
| Cloudflare 账号被封 | 极低 | Tailscale Funnel 备胎 |
| Tunnel 被滥用 / 扫描 | 中 | Cloudflare WAF 默认 + `/v1/*` 必须 API Key |
| Telegram Bot Token 泄露（AI 助手侧） | 低 | 限定 chat_id 白名单（在 AI 助手仓库管） |
| 4G 不稳定 | 高 | 移动端只读；本地优先 + 推送异步（AI 助手可本地缓存重试） |
| AI 助手误判 / 漏报 | 中 | 关键 alert 由 Karios 数据 + AI 助手规则双源（不强求）|
| UPS 没电 | 极低 | AI 助手保持最后一条推送在本地缓存 |

---

## 何时回退 / 重新评估

> **DB 维度的具体触发条件 + 备份 / HA / 多区域策略见 [`db-direction-2026-08.md`](./db-direction-2026-08.md)**。本文档只列出架构层面触发；DB 决策以其为准。

- 移动端使用频次 > 3 次/日，且对延迟不耐受 → 评估 Vercel + Neon（**注意：仍只暴露 Karios**，AI 助手继续独立）
- 公开给家庭成员 / 朋友 → 评估 Vercel + Neon（多人场景）
- 电脑经常断电 / UPS 不够撑 → 评估迁移到 VPS（Hetzner €4/月），Karios + AI 助手都迁
- Cloudflare Tunnel 长期不稳 → Tailscale Funnel 自建

---

## 与 todo.md 的关系

本文档是 **§2 / §3 / §4 / §8 跨领域的合并视角**。具体每条 todo 仍在 `todo.md` 维护；本文档只在「自由人视角」下讨论优先级和联动。落地时：

- 先读 `todo.md §12 实施清单（按 ROI）` 找当前要做的 1 条（**只做 Karios 部分**）
- 回本文档看「上下文」（homelab 假设、`/v1/*` 接口约定、AI 助手那边的对接契约）
- 实现 → 勾 OPT/TIP → 标 todo `[done]` → 摘要进 `docs/archive/`

**`/v1/*` 接口契约变更必须同步通知 AI 助手那边**（版本号 + changelog）。