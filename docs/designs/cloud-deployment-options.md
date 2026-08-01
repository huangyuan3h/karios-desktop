# Cloud Deployment Options — 评估与决策

> **关联 todo**：[§2 形态决策](../todo.md) · [§4 工程与部署](../todo.md) · [§5 数据源](../todo.md) · [§13 Longevity](../todo.md)
> **结论**：**不上云**。改为「Mac 当 homelab + Cloudflare Tunnel 暴露 + AI 助手在本地主动推送」的本地优先架构。
> **决议日**：2026-08-01
>
> **DB 维度的具体决策**（备份 / HA / 触发条件 / 复审日历）见 [`db-direction-2026-08.md`](./db-direction-2026-08.md)。本文档只覆盖整体架构。
>
> **系统级痛点真值**（换电脑 / 长期生命力 / 远程访问）见 [`karios-longevity-2026-08.md`](./karios-longevity-2026-08.md)。**先读那个再读本文档**——能避免讨论错位。

---

## TL;DR

| 决策 | 选择 |
|------|------|
| 计算 / Web 托管 | 保持本地（`pnpm dev`） |
| 对外暴露 | **Cloudflare Tunnel**（免费，套域名，无须公网 IP） |
| 主动推送 / 日报 / 监控 | **用户独立的 AI 助手项目**承担（不在 Karios 内） |
| DB | **完全本地 Postgres**（不上 RDS / Neon） |
| 移动端 | **只读 + 通知**，不做密集操作 |
| 远程唤醒 | Wake-on-LAN / Tailscale SSH |

> **职责边界**：Karios 只做生成 + 展示 + 暴露 `/v1/*`。任何"主动推送"由用户独立的 AI 助手项目负责，通过 `/v1/*` 拉数据。详见 [`freelancer-architecture.md`](./freelancer-architecture.md)。

**核心理由**（按用户优先级排）：

1. **收益（优先级 1）**：交易逻辑仍在本地 + 自家 IP 下跑，Tushare / EM push2 不被云厂商 IP 拉黑（实测过几次）
2. **API 打通（优先级 2）**：本地暴露 `/v1/chat/completions` 风格 endpoint，外面 AI / 浏览器走 Tunnel 进来即可
3. **工程 / 部署（优先级 3）**：单 Docker 一键起 + Tunnel，免运维
4. **数据源（优先级 4）**：akshare / EM 这类源对云 IP 不友好；本机 IP 反而不被风控

---

## 三家平台横向对比（已淘汰）

| 维度 | Lambda + S3 | Vercel | Cloudflare Worker |
|------|-------------|--------|--------------------|
| 免费额度 | Lambda 100 万次/月 · S3 5GB | 100GB 带宽 · Serverless 100 万次 | 10 万次/天 · KV |
| Next.js 适配 | 差（冷启动 + 打包麻烦） | **最优** | 凑合（须 edge runtime） |
| 静态托管 | S3+CloudFront 经典 | 一等公民 | Pages 一等公民 |
| DB 接入 | RDS / Aurora（贵） | Neon / Supabase free tier | D1 / Neon（边缘友好） |
| **AWS RDS 单实例月成本** | **$15-30 起步** | n/a | n/a |
| 数据源友好度 | 中（共享 IP 池风险） | 中 | 中 |
| 自家域名 | Route53 + ACM 联动 | 直接绑 | 直接绑 |

**淘汰路径**：

- ❌ **Lambda + S3**：最贵 + Next.js 适配差；如果非要 AWS，用 **Lightsail** 也比 EC2+RDS 划算
- ❌ **Vercel + Neon**：唯一可接受的上云方案，但**只在你决定上云时启用**——满足任一触发条件才考虑
- ⚠️ **Cloudflare Pages + Worker**：作为 Tunnel 落地可接受，作为完整部署不推荐（架构变化太大）

---

## 推荐架构：「Homelab + Relay」

```
┌──────────────────────────────────────────────────────────────┐
│                  用户的 Mac（永远开机）                          │
│                                                              │
│   ┌─────────────────┐  ┌──────────────────┐                  │
│   │  pnpm dev       │  │  Postgres        │                  │
│   │  (Next.js)      │  │  (本地权威数据)   │                  │
│   └────────┬────────┘  └────────┬─────────┘                  │
│            │                    │                            │
│            └────────► cloudflared daemon ◄─── /v1/* endpoint │
│                              │                                │
└──────────────────────────────┼───────────────────────────────┘
                               │ outbound only · 无须公网 IP
                               ▼
                  ┌─────────────────────────┐
                  │ Cloudflare Edge (免费)  │
                  │  - Tunnel 终止          │
                  │  - 域名绑 karios.xxx    │
                  │  - WAF / Bot 防御       │
                  └──────────┬──────────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
       iPhone / iPad    家庭成员 / 朋友     外部 AI 助手
       (浏览器/Telegram) (只读访客)         (独立项目 · 调 /v1/*)
                                          ↓
                                     自己决定
                                  Telegram / 推送 / 日报
```

**关键设计点**：

1. **outbound only**：Cloudflared 主动连出，永远不需要公网 IP / 端口映射
2. **本地 DB**：权威源，云上**不复制**——避免「本地和云不一致」的悖论
3. **Karios 只暴露 `/v1/*` 给外部读**：推送 / 日报 / 监控都由 AI 助手独立项目承担，Karios 不做
4. **域名**：`karios.{your-domain}` 走 Tunnel；其他子域名继续走 Route53（不动）

---

## 触发「重新评估上云」的条件

满足任一即重新打开本文件：

- [ ] 个人 AI 助手迁到云端，不愿连回本地
- [ ] 移动端（iPad / 手机）使用频率超过每日 3 次，且对延迟不可接受
- [ ] 想公开给家庭成员 / 朋友 / 公开 demo
- [ ] 电脑经常断电 / 网络不稳，「永远开机」假设不成立
- [ ] 本地 Postgres 数据超过 5 GB，备份 / 恢复成为日常痛点

---

## 落地步骤（参考，可调）

1. [P0] 把电脑设成"永不睡眠 / 屏幕关" + UPS（至少撑断电 30min）
2. [P0] 安装 `cloudflared`，配置 Tunnel 绑到 `{your-domain}`
3. [P0] Next.js dev server 起在 `127.0.0.1:3000`（避免公网直连），只允许 Cloudflare IP
4. [P0] **暴露 `/v1/chat/completions`**（OpenAI 兼容）给外部 AI 助手调，**只读 scope**
5. [P1] 移动端只装浏览器 + Telegram；Dashboard 看 / 决策在 iPad 完成
6. [P2] 评估 Tailscale Funnel 作为 Tunnel 的 fallback（一键 HTTPS + 不依赖 Cloudflare 账号）
7. [P2] Wake-on-LAN 配置：极端情况下从 iPhone 唤醒 Mac

> **不在 Karios 内做**：Telegram Bot / 推送 / 日报生成 → 由用户的 AI 助手独立项目承担（用 `Karios` 的 `/v1/*` 拉数据）。详见 [`freelancer-architecture.md`](./freelancer-architecture.md)。

---

## 反方案：什么时候才选 Vercel + Neon

如果未来想要：

- 公开 demo URL（无 Tunnel 域名）→ Vercel
- iPhone 不装 Telegram / 浏览器，纯原生 App → Vercel + Edge Functions
- 多人协作（家庭账户共用） → Vercel + Neon

成本估算（**vs 本方案 $0**）：

| 项目 | 月成本 |
|------|--------|
| Vercel Pro | $20 |
| Neon Launch | $19 |
| 域名 / Cloudflare | $0（已有） |
| **合计** | **~$39/月** |

**结论**：除非必要性证明（见触发条件），否则**没必要付这 $39/月**。

---

## 风险与缓解

| 风险 | 概率 | 缓解 |
|------|------|------|
| Mac 断电 / 关机 | 中 | UPS + 远程唤醒（Tailscale SSH） |
| Cloudflare 账号被封 | 极低 | Tunnel 可迁到 Tailscale / FRP 自建 |
| 数据源 IP 被风控 | 中 | 本机 IP 反而风险低；云上才高 |
| Tunnel 被滥用 / 扫描 | 低 | Cloudflare WAF 默认 + `/v1` 必须 API Key |
| 4G / 钓鱼点无网络 | 高 | AI 助手本地定时跑 + 推送异步；离线也能回看 |