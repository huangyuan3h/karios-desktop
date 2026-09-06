# opt-111-118-mobile-webhook · 归档于 2026-08-14

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文只留 5 条未完成 OPT（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-111：行为对账横幅感知买入闸门（2026-08-14）

**状态**：[x]

**背景（用户："我没有办法做操作的时候就不用告诉我买什么，只告诉我需要卖"）**：
宏观死锁/闸门关闭日（CN panic cooldown、HK regime=Weak 空仓观望），横幅仍列出 13 只
HK"该持没买"——买入被强制拦截时这些建议不可执行，纯噪音。

**改动**：
- `lib/queries/portfolioHealth.ts::isMarketGateClosed`——闸门判断唯一真值
  （regime=Weak / regime 未知 / panicCooldown.active / circuitBlocked）
- `PortfolioHealthCard` 改用共享函数（原内联逻辑消除重复）
- `BehaviorAuditBanner` 复用 `['portfolio-health']` 缓存（不重复请求），按市场
  gateClosed 过滤：**隐藏该市场"该持没买"**（不可执行），保留"该卖没卖/买了不该买"
  （可执行/既成事实）；隐藏条数以一行说明披露；全部被隐藏时横幅转安静态
  （"无待操作提醒 — 已隐藏 N 条该持没买"）

**验收**：前端 765 passed（+2：闸门关闭隐藏/安静态）· tsc/eslint 干净 ·
gates open 时行为不变（原有测试保持）

---

### OPT-112：行为对账自动 cron（2026-08-14）

**状态**：[x]

**背景（"让提醒变成日常"）**：行为对账（OPT-106）只能手动点"刷新对账"（simulate 3-4 分钟）。

**改动**：`scheduler/behavior_audit_job.py`——工作日 18:45（收盘链 17:45 之后）自动跑
`run_registry_and_persist(today)` 落库 → watchlist 横幅免手动刷新；发现不符项
（extra/missing）→ emit `audit_issues` webhook（买不了也提示可操作的卖出项）。
注册 scheduler + SYNC_JOB_TYPES + 前端 catalog（behavior_audit）。

---

### OPT-113：14:00 执行卡（操作卡时点对齐 + webhook 推送 · 2026-08-14）

**状态**：[x]

**背景（"到点主动告诉我买什么/卖什么"）**：操作卡 14:30 生成，晚于用户 14:00 交易时点；
且只存 brief，不推送。

**改动**：
- `trading_brief_job.py`：action 时点 **14:30 → 14:00**（对齐 intraday-lock 14:00 冻结快照）
- `trading_brief.py::generate_trading_brief`：action 分支 emit `execution_card` webhook
  （gate 状态 CN/HK + 买入候选 + EXIT 持仓，每日 dedupe）
- 前端 catalog 文案/时点同步

---

### OPT-114：Webhook 事件目录全量 + 订阅落地指引（2026-08-14）

**状态**：[x]（目录已全量；订阅待用户创建接收端）

**改动**：cookbook §9.1 事件目录补全 8 类事件（job_failed / intraday_drawdown /
near_stop / candidate_diff / recon_missing / execution_card / audit_issues / test）。
订阅 + 接收端示例见 cookbook §9.2/9.3——用户创建接收端点后即全链路打通。

---

### OPT-115：Bark 推送通道（iPhone webhook 接收端 · 2026-08-14）

**状态**：[x]（代码+迁移完成；待用户提供 Bark 设备 key 创建订阅）

**背景（用户选 iPhone + Bark）**：webhook 投递是通用 JSON 格式，Bark 需要
title/body 结构。为 provider='bark' 的订阅增加格式化通道。

**改动**：
- alembic `0033_webhook_provider`：`webhook_subscriptions.provider`
  （'generic' | 'bark'，默认 generic）
- `service/webhook_format.py::format_bark`——8 类事件 → 中文 title/body
  （执行卡/对账/止损/跌穿/新候选/周对账/任务失败/测试）
- `webhook_delivery.py`：provider='bark' 时 body 用 Bark 格式（仍 HMAC 签名）
- 订阅 API 接受 provider 字段（正则校验 generic|bark）

**验收**：formatter 单测 4 例 + delivery bark 用例 + routes 全过；后端 3382 passed；
ruff 干净；服务已重启（provider 字段已生效）。

**用户侧**：装 Bark app → 复制 `https://api.day.app/<key>` → 创建订阅
（provider=bark，事件全选）→ `POST /api/webhook/test` 验证手机收到。

---

### OPT-116：Family Hub Phase 0 — Cloudflare Tunnel + PWA（2026-08-14）

**状态**：[x]（隧道/PWA/常驻完成；Cloudflare Access 待用户在控制台配置）

**背景（用户愿景）**：家庭投资平台统一入口——手机访问 Mac 上的全部软件，
语音控制、数据说话（docs/designs/family-hub-2027.md）。

**改动**：
- **PWA**：`manifest.webmanifest` + 图标（icon-192/512 + apple-touch-icon，
  PIL 生成，深色底金色柱状图=数据说话）+ `sw.js`（静态缓存、导航 network-first、
  跳过 API 拦截）+ layout metadata（manifest/themeColor/appleWebApp）
- **Tunnel**：Cloudflare 命名隧道 `karios`（id 8d60d5d1…），三个子域
  `karios.it-t.xyz`（UI 3000）/ `api-karios.it-t.xyz`（API 4330）/
  `ai-karios.it-t.xyz`（AI 4310）；~/.cloudflared/config.yml ingress；
  launchd 常驻（plist 修正为 `tunnel run karios`）
- **前端动态 base**：`endpoints.ts` 按 hostname 判断——it-t.xyz 走公网子域，
  本地仍 127.0.0.1（手机/本地同一构建，无需注入环境变量）

**验收**：三个子域 curl 全通（UI 200 / API healthz ok / AI healthz ok）；
前端 84 文件全过（+2 endpoints tunnel 测试）；tsc 干净。
**安全（2026-08-14 增强）**：改为**本地 Basic Auth 网关（caddy :8443，launchd 常驻）**——
密码认证在 Mac 本地完成，不依赖 Cloudflare Access（其验证码流程依赖
login.cloudflareaccess.org，国内网络不稳）。单域名架构：`karios.it-t.xyz` 一
个密码覆盖 UI+API+AI（caddy 按路径分流：/api /v1 及无前缀路由→4330，
/ai→4310，/与静态资源→3000）。密码存 `~/.karios/gateway-password.txt`
（chmod 600）。无认证访问返回 401。

---

### OPT-117：MobileShell 手机端独立 UI（2026-08-14）

**状态**：[x]（v1：执行/持仓/对账三 tab；后续按需增强）

**背景（用户反馈"手机太难操作"）**：桌面工作区（sidebar+agent 面板+密集表格）在
手机不可用。方案：**移动端独立视图**，不复用桌面组件。

**改动**：
- `components/mobile/MobileShell.tsx`：手机优先 3-tab（底部导航）
  ① 执行：闸门徽章（A股/港股可买与否）+ 下午 2 点买入清单 + 🚩需要卖出
  ② 持仓：每票卡片（盈亏/止损线/移动线/到期/EXIT 标记/盘中预警）
  ③ 对账：该卖没卖/买了不该买（行为审计偏差）
- `AppShell`：`matchMedia(max-width:768px)` 检测 → MobileShell（hooks 隔离，
  React hooks 规则合规）
- 数据全复用现有 API（portfolio-health / behavior-audit），无后端改动

**验收**：4 个 MobileShell 测试（闸门/候选/持仓/EXIT/对账）+ 771 passed ·
tsc/eslint 干净 · tunnel UI 200。
**说明**：v1 只读展示（看）；操作（买入/卖出/对账刷新）后续按需加。

---

### OPT-118：Gateway 认证改为 X-Karios-Key Header + 前端登录页（2026-08-14）

**状态**：[x]

**背景（用户反馈"手机反复让我登录"）**：caddy Basic Auth 原生弹框在 iOS PWA
standalone 模式下凭据不持久，反复弹框。

**改动**：
- **caddy v3**：去掉 basic_auth——UI/静态资源放行（壳，无数据）；API/AI 全部
  路径校验 `X-Karios-Key` header（环境变量 KARIOS_GATEWAY_KEY，launchd 注入），
  无/错 key → 401 JSON（不弹框）
- **前端**：`lib/auth.ts`（installFetchAuth 全局包装 window.fetch，API 请求带
  header；401 → 清 key + 广播 UNAUTHORIZED_EVENT）+ `AuthGate` 登录页
  （密码存 localStorage，提交后 reload；401 自动回登录页）
- page.tsx 挂载 AuthGate + installFetchAuth

**验收**：UI 200（免认证）；API 无 key 401 / 带 key 200 / 错 key 401；AI 同；
auth 单测 3 例 + AuthGate 4 例；774 passed；tsc/eslint 干净。
