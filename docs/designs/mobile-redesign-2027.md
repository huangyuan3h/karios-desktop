# 手机版重设计方案 — Mobile Redesign 2027

> 状态：**拍板中草稿**（designs/ 容器）。落地后迁出到 `docs/modules/` 或 `docs/archive/`。
> 依据 `AGENTS.md`：`docs/designs/` 是未落地/拍板中的设计草稿容器，落地后迁出。
> 本文件是手机版设计系统的**真值**，供后续 agent 逐页实现时参照。

---

## 0. 背景与问题

当前手机版（OPT-117/118）只是"功能可达"：用 `MobileShell` 把桌面页面（`components/pages/*`）
原样塞进窄屏，存在：

- **无移动设计语言**：桌面多列表格/网格直接压缩，溢出、拥挤、无呼吸感。
- **组件未隔离**：手机复用 desktop 页面组件，桌面一改手机就裂；桌面组件带 desktop 专属 props
  （`onOpenStock` / `onNavigate` 必填），手机只能 cast 强行塞。
- **缺乏设计感**：没有统一的间距/圆角/字阶/涨跌色规范，卡片风格零散。

目标：**手机版从"能用"升级到"美观、有设计感"，且与 desktop 完全隔离**——
手机有独立组件库，desktop 与 mobile 只共享数据层（queries / api / shared 类型 / 主题色 token）。

---

## 1. 设计原则

1. **Mobile-first，触摸优先**：所有可点击元素最小触控区 `44×44px`；不用 hover 依赖的交互。
2. **单手操作**：核心动作落在屏幕下半部（底部固定 tab bar + 底部主操作按钮）。
3. **信息密度降维**：桌面多列表格 → 手机单列卡片流；桌面侧栏 → 底部 tab；桌面弹窗 → 底部 sheet。
4. **主题一致、token 隔离**：颜色沿用 `globals.css` 的 `--k-*` 语义色；**尺寸/间距/圆角/字阶** 另起
   移动端 scale（`--m-*`），不污染 desktop。
5. **共享数据层、隔离视图层**：手机只复用 `lib/queries/*`、`lib/api/*`、`packages/shared` 类型、
   `lib/auth`（X-Karios-Key）。**不复用** `components/pages/*` 与 `components/layout/*`。
6. **PWA / 离线友好**：组件纯展示 + react-query 缓存，断网有骨架/空态，不依赖 SW 缓存 JS。

---

## 2. 设计 Token

### 2.1 颜色（沿用 desktop，零新增语义冲突）

| Token | Light | Dark | 用途 |
|-------|-------|------|------|
| `--k-bg` | `#f7f7f8` | `#0b0b0d` | 页面背景 |
| `--k-surface` | `#ffffff` | `#121216` | 卡片/面板 |
| `--k-surface-2` | `#f3f4f6` | `#1a1a20` | 次级背景（行 hover、输入底） |
| `--k-border` | `rgba(24,24,27,.12)` | `rgba(244,244,245,.12)` | 描边 |
| `--k-text` | `#09090b` | `#f4f4f5` | 主文字 |
| `--k-muted` | `rgba(24,24,27,.6)` | `rgba(244,244,245,.6)` | 次要文字 |
| `--k-accent` | `#4f46e5` | `#818cf8` | 主操作/聚焦环（**语义=可操作/开市**，非涨跌） |

**涨跌色（A 股惯例：红涨绿跌）** — 与 `--k-accent` 严格区分：

| Token | Light | Dark | 用途 |
|-------|-------|------|------|
| `--k-up` | `#e11d48` | `#fb7185` | 上涨（红） |
| `--k-down` | `#16a34a` | `#4ade80` | 下跌（绿） |
| `--k-warn` | `#d97706` | `#fbbf24` | 预警（接近止损、待办） |
| `--k-danger` | `#dc2626` | `#f87171` | 危险/该卖没卖 |

> 约定：开市绿灯/可操作高亮用 `--k-accent`（indigo）；价格涨跌用 `--k-up/--k-down`（红/绿）。
> 二者不混用，避免"绿色=好"的歧义。

### 2.2 移动端 scale（新增 `--m-*`）

> 实现位置：`globals.css` 的 `/* Mobile scale (Mobile Redesign 2027) */` 区块
> （与 `--k-*` 同文件，避免额外 css import 影响 vitest；desktop 不引用 `--m-*`，
> 视图层隔离不受影响）。

```
--m-gap-1: 4px;  --m-gap-2: 8px;  --m-gap-3: 12px; --m-gap-4: 16px;
--m-gap-5: 20px; --m-gap-6: 24px;
--m-radius-sm: 8px; --m-radius-md: 12px; --m-radius-lg: 16px;
--m-radius-xl: 20px; --m-radius-pill: 999px;
--m-text-xs: 11px; --m-text-sm: 12px; --m-text-base: 14px;
--m-text-lg: 16px; --m-text-xl: 18px; --m-text-2xl: 22px;
--m-shadow-sm: 0 1px 2px rgba(0,0,0,.06);
--m-shadow-md: 0 4px 16px rgba(0,0,0,.08);
--m-tap: 44px;            /* 最小触控高度 */
--m-header-h: 52px;       /* 顶部 header 高度 */
--m-tabbar-h: 56px;       /* 底部 tab bar 高度（不含安全区） */
--m-content-pad: 14px;    /* 内容区左右内边距 */
```

- 安全区：所有贴边容器用 `padding-bottom: env(safe-area-inset-bottom)` / `top` 处理刘海/Home 条。
- 基准视口宽度：**375 / 390 / 430px**（iPhone SE / 13 / 14 Pro Max）。设计以 390 为准，375 不溢出、430 不空旷。

---

## 3. 布局骨架

```
AppShell (isMobile via matchMedia max-width:768px, 初始 null→useEffect 解析)
  └─ <MobileApp/>
       ├─ <MobileHeader/>          固定顶，52px：logo/返回 · 闸门状态 · 更多按钮
       │    └─ 更多面板             从 header 下滑（m-panel-enter 动画），分组网格
       ├─ <main> 滚动区：padding 14，gap 12，底部留 tab bar 空间
       └─ <MobileTabBar/>          固定底，56px + safe-area：首页/自选/Agent 三 tab
```

- **IA v2（2026-08-14 用户反馈）**：底部 bar 只放三个最重要的入口——**首页
  （Dashboard）/ 自选（Watchlist）/ Agent（决策 Agent）**；其余全部收进 header 的
  「更多」面板（图标网格，打开带动画）。
- **自选 = 行动中心**：需要卖出 → 下午 2 点买入清单 → 持仓 → 自选列表 → 添加，
  执行与持仓是自选的一部分（用户定论）。
- **Agent = 随时知情**：快捷提问 chips 自动附加当前快照（regime/候选/持仓），
  「数据」按钮可只插入上下文不提问。
- 内容区永不横向滚动；表格类一律改为卡片/列表。
- 子页面导航：`更多` 面板点页面 → 进入该 mobile 页面组件（替换 main 内容），header 显示
  `‹ 返回`。**不再懒加载 desktop 页面**（IA v2 起全部为 mobile 原生页面）。

---

## 4. 基础组件库 — `components/mobile/primitives.tsx`

后续 agent 实现各页面时**必须复用**以下基础组件，禁止各页面自造样式。

| 组件 | Props（概要） | 视觉规范 |
|------|--------------|----------|
| `MobileCard` | `className?`, `onClick?`, `inset?` | surface 底、radius-md、border、shadow-sm；点击态 `active:scale-[.99]`；min 高度自适应 |
| `MobileSection` | `title`, `action?`, `children` | 标题 `--m-text-lg` 字重 600 + 右侧可选 action 链接；下方 gap-3 排 children |
| `StatusPill` | `tone: 'open'\|'closed'\|'up'\|'down'\|'warn'\|'danger'\|'neutral'`, `children` | pill 形、对应色淡底+同色字（如开市=accent 淡底）、`--m-text-xs` |
| `GateBadge` | `market: 'A股'\|'港股'`, `open: boolean` | 复用 StatusPill tone open/closed |
| `PriceText` | `value: number`, `prefix?` | 涨跌色：正 `--k-up` + `▲`、负 `--k-down` + `▼`；`--m-text-base` 字重 600；0 用 muted |
| `PctText` | `value: number` | 同上，后缀 `%` |
| `MobileRow` | `leading`, `trailing?`, `onClick?` | 行高 ≥44px，左右对齐，分隔线 `border-b`；点击态 surface-2 |
| `MobileList` | `children` | 竖向 gap-2 / 或带分隔线；包在 MobileCard 或裸列 |
| `MobileButton` | `variant: 'primary'\|'ghost'\|'danger'`, `size?`, `block?`, `disabled?` | primary=accent 底白字；ghost=透明+accent 字+border；danger=down/red；min-h 44；radius-md |
| `MobileSheet` | `open`, `onClose`, `title?`, `children` | 底部弹出，覆盖 80% 高，backdrop 半透明；圆角 xl 顶部；安全区 bottom |
| `MobileField` | `label`, `children` | 表单行：label muted 上方 + 控件 |
| `SkeletonBlock` | `h?`, `w?` | surface-2 底 + shimmer 动画（移动端专用 keyframes） |
| `EmptyState` | `icon?`, `title`, `hint?` | 居中、muted、图标 28px + 标题 + 提示 |
| `Toast` | 经现有通知系统 | 底部居中、2s 自动消失、surface 底 shadow-md |

所有基础组件支持 `data-theme` 暗色（自动跟随 `html[data-theme]`）。

---

## 5. 各页面手机版设计

> 信息架构 = 桌面页面**降维**后的手机版。每个页面一个 mobile 组件，置于
> `components/mobile/pages/<Name>Page.tsx`，数据取自对应 `lib/queries/*`。

### 5.1 三个核心 tab（首页级，最高优先级，IA v2）

**首页 `DashboardTab`（默认首屏）**
- 「今日状态」卡：闸门 GateBadge + regime/强度 + 候选/持仓计数 + 数据日期；
  情绪/恐慌冷却警告条。
- 「行业资金流 Top 5」+「最新要闻」两个信息卡组。

**自选 `WatchlistTab`（行动中心 — 执行/持仓合并于此）**
- 「需要卖出」：EXIT 持仓卡（danger 红），含止损线/盈亏/到期/理由。
- 「下午 2 点买入清单」：候选卡（名称/代码/行业 + score + alpha grade）。
- 「持仓」：每仓卡（盈亏、止损/移动/到期三格、HOLD/EXIT pill、盘中预警）。
- 「自选」：行情行（名称/代码/现价/涨跌 PctText/score/删除 Trash2）+ 刷新。
- 「添加自选」：输入 + Plus 按钮。

**Agent `AgentTab`（决策 Agent — 随时基于当前情况）**
- 快捷提问 chips（市场怎么看/买入候选/持仓风险/适合加仓）——自动附加当前快照
  （regime/候选/持仓 markdown）后发送。
- 「数据」ghost 按钮：只插入当前快照到对话，不提问。
- 气泡对话流 + 底部输入/发送（流式回复，同桌面端点）。

### 5.2 更多页（14 个，按使用频率分组实现）

**高频**
- **Dashboard** `DashboardPage(m)`：状态概览卡（大盘/闸门）+ 关键数字（持仓收益、待办数）+ 快捷入口。
- **Watchlist** `WatchlistPage(m)`：`MobileList` 自选股行（代码名 / 现价 `PriceText` / 涨跌 `PctText` / 迷你火花线可选 / 右侧「+自选」或操作）；下拉刷新（react-query refetch）。
- **Market** `MarketPage(m)`：顶部搜索框 `MobileField` + 行情 `MobileList`（同 Watchlist 行样式）；行内「加自选」ghost 按钮。
- **Alpha 雷达** `AlphaPage(m)`：候选卡（C1–C4 标签 `StatusPill` + 信号强度条 + 回测胜率 + 决策按钮）。
- **回测** `BacktestPage(m)`：参数 `MobileField` 组 + 结果 `MobileCard` 关键指标（收益/回撤/胜率）替代桌面图表；图表降级为 sparkline 或"查看大图"链接。

**中频**
- **News** `NewsPage(m)`：卡片流（时间 muted + 标题 base + 来源/摘要 sm）；点击展开 Sheet。
- **行业资金流** `IndustryFlowPage(m)`：行业卡（名称 + 净流入 `PriceText` + 主线标签）。
- **决策 Agent** `DecisionPage(m)`：对话式气泡流 + 底部输入框 + 建议卡（经现有 chat store）。
- **指数** `IndexPage(m)`：指数 `MobileList` 行（名称 / 点位 / 涨跌 `PctText`）。
- **设置** `SettingsPage(m)`：分组 `MobileList`（主题切换 / 网关密钥 / 账户），每项 `MobileRow` 右箭头。

**低频（保功能，样式从简但统一）**
- **Broker 条件单** `BrokerPage(m)`：条件单卡列表 + 底部「新建」primary；卡片显示标的/触发价/状态 `StatusPill`。
- **交易日志** `JournalPage(m)`：时间线 `MobileList`（日期 + 标的 + 动作 + 备注）。
- **任务调度** `SchedulerPage(m)`：任务卡（名称 + 下次运行 muted + 开关 `MobileSwitch`）。
- **Webhook** `WebhookPage(m)`：订阅卡（事件 `StatusPill` + 通道 + 开关）。

> 每个页面组件**自带移动布局**，不再接收 desktop 的 `onOpenStock` / `onNavigate` 必填 props。

---

## 6. 隔离策略（工程约束）

| 维度 | 复用（允许） | 隔离（禁止复用） |
|------|--------------|------------------|
| 数据 | `lib/queries/*`、`lib/api/*`、`packages/shared` | — |
| 认证 | `lib/auth.ts`（X-Karios-Key）、`AuthGate` | — |
| 主题色 | `globals.css` 的 `--k-*` | 移动 scale `--m-*` 独立文件 |
| 视图组件 | — | `components/pages/*`、`components/layout/*` |
| 路由 | 复用 Next 路由文件；mobile 渲染独立组件 | 不 import desktop 页面进 mobile |

目录结构：

```
apps/desktop-ui/src/components/mobile/
  globals.css               # --m-* scale + shimmer keyframes（mobile 区块）
  primitives.tsx            # §4 基础组件（已落地）
  MobileShell.tsx           # 壳：header + 内容 + 底部 4 tab（已重构）
  tabs/                     # ExecutionTab / HoldingsTab / ReconcileTab（已落地）
  pages/                    # MobileXxxPage 14 个（已落地）
  *.test.tsx                # 每个组件 vitest 测试
```

---

## 7. 实施顺序（agent 执行清单，每步独立 OPT-xxx 提交）

> 2026-08-14 状态：**Phase A/B/C 已完成并提交**（见 git log mobile redesign 提交）——
> 设计 token、primitives、三个核心 tab、14 个 mobile 页面、MobileShell 集成均已落地，
> 桌面引用已从手机端移除。剩余打磨项归入 Phase D。

- **Phase A — 设计系统基础**：`globals.css` mobile token 区块 + `primitives.tsx`（§4 全部组件）+ 测试。✅
- **Phase B — 核心三 tab**：ExecutionTab / HoldingsTab / ReconcileTab 用 primitives，接真实 queries。✅
- **Phase C — 更多页（14 个）**：`pages/MobileXxxPage.tsx` 全部落地，`MobileShell` 改用 React.lazy
  加载 mobile 页面，删除 `PAGE_LOADERS` 桌面引用。✅
- **Phase D — 打磨（进行中）**：骨架屏/空态已内建；剩余：下拉刷新、进出场动画、
  无障碍（aria）、375/430 走查、Siri Shortcuts 快捷入口。
- **收尾**：确认无 `components/pages/*` 引用残留（已删）；desktop 与 mobile 视图层隔离完成。

---

## 8. 验收标准

1. 在 375 / 390 / 430px 宽度下，**无横向溢出**、无文字截断（超长按 `truncate` + 省略）。
2. 所有可点击元素触控区 ≥ `44×44px`。
3. 亮色 / 暗色主题下颜色一致（自动跟随系统/应用切换）。
4. 核心操作（买/卖/加自选/查看）单手拇指可达（底部 tab / 底部按钮）。
5. 每个手机页面使用 `primitives` 组件，**零** 直接 import `components/pages/*`。
6. 每个新增组件有 `.test.tsx`（渲染 + 暗色 + 窄屏快照），全量 `npm run test` 通过。

---

## 9. 风险与注意

- **涨跌色歧义**：务必区分 accent（可操作）与 up/down（价格），避免红绿语义混乱（§2.1）。
- **数据层一致性**：mobile 页面只读 `lib/queries/*`，不重复写 fetch，避免与 desktop 漂移。
- **性能**：14 个页面按需懒加载（`next/dynamic`），首屏只加载核心三 tab。
- **过渡期**：Phase C 完成前保留 `PAGE_LOADERS` 桌面降级，避免功能断档。
