# Cloudflare Tunnel · Karios `/v1/*` 对外暴露

> **关联 todo**：[§4 工程与部署](../todo.md) · [§12 实施清单 #2](../todo.md)
> **配套设计**：[`cloud-deployment-options.md`](./cloud-deployment-options.md) · [`freelancer-architecture.md`](./freelancer-architecture.md)
> **决议日**：2026-08-01

---

## 0. 为什么

Karios `/v1/*` 端到端完成（OPT-045/046/047），但仍只在 `127.0.0.1:4310` 监听。外部 AI 助手（Telegram 推送代理 / 钓鱼旅行时）无法跨网调。

**上云 → 拒绝**（DB 贵 + 数据源对云 IP 不友好 + Tushare/EM push2 被云厂商 IP 拉黑）。

**走 Cloudflare Tunnel** → 域名绑本地，零公网 IP、零端口映射、outbound only 防火墙友好。

两种使用方式：

| 场景 | 工具 | 脚本 |
|------|------|------|
| 测试 / 临时 / 旅行 | Quick Tunnel（`*.trycloudflare.com` 随机 URL）| `scripts/start-quick-tunnel.sh` |
| 生产 / 稳定 / 套自有域名 | Named Tunnel + CNAME | `scripts/setup-named-tunnel.sh` |

---

## 1. 安装 cloudflared

**macOS（Homebrew）：**

```bash
brew install cloudflared
```

**macOS（手动）：**

```bash
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-arm64.tgz \
  | tar -xz -C /tmp
sudo mv /tmp/cloudflared /usr/local/bin/
```

**Linux：** 见 [官方文档](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/)。

验证：

```bash
cloudflared --version
# cloudflared version 2024.X.X (...)
```

---

## 2. Quick Tunnel（最快上手）

**用途**：测试 / 临时 / 给朋友看 demo。URL 每次重启会变（`*.trycloudflare.com` 随机子域）。

**启动**：

```bash
cd services/data-sync-service
./scripts/start-quick-tunnel.sh            # 默认 4310 端口
./scripts/start-quick-tunnel.sh --port 3000 # 自定义端口
```

脚本会先验证：
1. cloudflared 已装
2. `http://127.0.0.1:4310/v1/version` 真的可达

然后启动 tunnel，**把 cloudflared 的输出原样转发**到你的终端。找这一行：

```
Your quick tunnel has been created! Visit it at:
  https://<random-32-chars>.trycloudflare.com
```

**测试**：

```bash
# 拿外部 URL 调 /v1/version
curl https://<random>.trycloudflare.com/v1/version
# {"version":"0.1.0","min_compatible":"0.1.0","released_at":"..."}
```

**停止**：Ctrl-C（脚本会 forward SIGTERM 到 cloudflared）。

---

## 3. Named Tunnel（生产 / 套自有域名）

**用途**：稳定 URL、给 AI 助手配置固定地址、跑在 launchd 上不依赖当前 session。

**前置**：
- 1 个 Cloudflare 账号（free tier 够用）
- 1 个域名已挂在 Cloudflare DNS 上（`example.com` → Cloudflare Nameservers）

### 3.1 一次性 setup

```bash
# Step 1: 登录（会开浏览器，选域名）
cloudflared tunnel login

# Step 2: 创建 named tunnel
cloudflared tunnel create karios
# → 输出：Tunnel credentials written to /Users/<you>/.cloudflared/<UUID>.json
# → 输出：Created tunnel karios with id <UUID>

# Step 3: 路由 DNS（CNAME api.example.com → <UUID>.cfargotunnel.com）
cloudflared tunnel route dns karios api.example.com
```

### 3.2 写 config 文件

`~/.cloudflared/config.yml`（**不进 git**）：

```yaml
tunnel: karios
credentials-file: /Users/<you>/.cloudflared/<UUID>.json

ingress:
  # Karios /v1/* 全套（含 FastAPI Swagger UI /docs）
  - hostname: api.example.com
    service: http://127.0.0.1:4310
  # 兜底：所有其他请求 404
  - service: http_status:404
```

> ⚠️ `credentials-file` 路径**必须用绝对路径**，不能用 `~` 展开。

### 3.3 启动

```bash
# 手动启动（开发用）
cloudflared tunnel run karios

# 装为 macOS launchd 服务（开机自启 + 重启自愈）
sudo cloudflared service install
```

### 3.4 测试

```bash
curl https://api.example.com/v1/version
# {"version":"0.1.0",...}

# 试调业务 endpoint
curl -H "Authorization: Bearer $KARIOS_KEY" \
  "https://api.example.com/v1/explain/CN:000001"
```

---

## 4. 验证清单

Tunnel 起来后，按这个清单逐项过：

- [ ] **发现性 endpoint 不鉴权**：
  ```bash
  curl https://api.example.com/v1/version    # 200
  curl https://api.example.com/v1/schema     # 200（返回 OpenAPI JSON）
  curl https://api.example.com/v1/errors     # 200（含 3 个 seed code）
  ```
- [ ] **业务 endpoint 启用鉴权时 401**（如果设了 `KARIOS_API_KEYS`）：
  ```bash
  curl https://api.example.com/v1/watchlist/items    # 401
  curl -H "Authorization: Bearer wrong" ...          # 401
  curl -H "Authorization: Bearer right" ...          # 200
  ```
- [ ] **HTTPS 锁**（Cloudflare 自动给）：浏览器开 `https://api.example.com/v1/schema` 不报不安全
- [ ] **错误码字典在**：`curl /v1/errors | jq '.codes | length'` ≥ 3

---

## 5. 回退方案

如果 Cloudflare 出问题：

| 替代 | 命令 | 代价 |
|------|------|------|
| Tailscale Funnel | `tailscale funnel 4310` | 需装 Tailscale（免费）+ 客户端 |
| ngrok | `ngrok http 4310` | 免费版限速 / 弹验证 |
| 直接 SSH 隧道 | `ssh -R 80:127.0.0.1:4310 user@public-host` | 需要公网 host |

按优先级：**Cloudflare → Tailscale → SSH**。

---

## 6. 安全

| 项 | 推荐 | 备注 |
|----|------|------|
| 域名 | 子域 `api.example.com`（不要用主域）| 减少 impact 域 |
| WAF | Cloudflare 默认开 | 暴力扫描自动拦 |
| Rate limit | Cloudflare 免费版 100k req/day | 超过上 Pro |
| API Key | 始终设 `KARIOS_API_KEYS` | 4 个发现性 endpoint 仍可访问，但其他路由 401 |
| 监听地址 | `127.0.0.1:4310`，不要 `0.0.0.0` | 仅 cloudflared 本地连 |

---

## 7. 反模式

- ❌ 用 `0.0.0.0:4310` 监听（任何 Cloudflare 配置错误都会直接暴露到公网）
- ❌ 把 `~/.cloudflared/<UUID>.json` 写进 git
- ❌ 把 `api.example.com` 用作别的服务（Tunnel 只能一个 ingress）
- ❌ 启用 Cloudflare Access 但不配置（默认放行所有）
- ❌ 在 Docker 容器里跑 cloudflared（应 host 守护 + launchd）

---

## 8. 当前进度（2026-08-01）

- ✅ Quick Tunnel 脚本就绪
- ✅ Named Tunnel 脚本骨架就绪
- ⏳ 真实端到端验证（**等用户装好 cloudflared**）
- ⏳ launchd 服务配置（待用户域名 + UUID 准备好后）

跑通后写 `archive/2026-08-XX-opt-048-cloudflare-tunnel-verified.md`。
