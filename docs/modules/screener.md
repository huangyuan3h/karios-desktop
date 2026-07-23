# Screener 模块

> 股票筛选器同步模块

---

## 模块定位

Screener 模块负责从 TradingView 网站抓取用户自定义的股票筛选器结果，实现本地数据的同步与历史记录管理。这是 Karios 投资分析系统的**数据入口**之一。

## 核心价值

- **动态抓取**：实时从 TradingView 获取最新的筛选结果
- **Filter 信息保留**：自动提取并保存筛选条件，确保数据上下文完整
- **历史追踪**：按 AM/PM 两个时段记录快照，支持回溯分析
- **AI 集成**：抓取的数据可直接作为 AI 对话的上下文引用

---

## 业务流程

### 日常使用流程

```
1. 配置阶段（一次性）
   ├── 在 Settings 页面添加 TradingView Screener URL
   └── 系统保存配置到数据库

2. 日常同步
   ├── 打开 Screener 页面
   ├── 点击 "Sync" 或 "Sync all" 触发同步
   ├── 系统通过 CDP 连接 Chrome 抓取数据
   └── 结果保存为快照

3. 数据使用
   ├── 查看最新快照的股票列表
   ├── 查看 AM/PM 历史记录
   ├── 复制 Markdown 表格用于分析
   └── 添加到 AI 对话上下文
```

### 数据采集流程

```
用户触发同步
     │
     ▼
连接本地 Chrome (CDP)
     │
     ▼
打开 TradingView Screener 页面
     │
     ▼
等待页面加载完成
     │
     ▼
提取 Filter Pills（筛选条件）
     │
     ▼
滚动加载表格数据（最多 300 行）
     │
     ▼
数据归一化处理
     │
     ▼
保存快照到数据库
```

---

## 核心概念

### Filter Pills

Filter Pills 是 TradingView Screener 页面上方显示的筛选条件标签，例如：

- `Market: US`
- `Price > 10`
- `Volume > 1M`
- `RSI(14) < 30`
- `Analyst Rating: Strong Buy`

这些信息对于理解筛选结果的上下文至关重要。系统会自动提取并保存这些标签。

### AM/PM 快照

系统每天保存两个时段的快照：

- **AM 快照**：上午时段的筛选结果
- **PM 快照**：下午时段的筛选结果

这样可以追踪一天内的变化，例如：
- 上午筛选出的股票下午是否还在列表中
- 新增或减少了哪些股票

### TrendOK 集成

当从 Screener 导入股票到 Watchlist 时，系统会自动进行 TrendOK 检查：

1. **回撤比例过滤**：只保留回撤比例在 -15% 到 -5% 之间的股票
   - 回撤比例 = (当前价 - 52周最高价) / 52周最高价
   - 这个范围代表"从高点回踩但趋势未坏"的股票

2. **技术面检查**：通过 TrendOK 验证趋势健康度
   - EMA 顺序：收盘价 > EMA20 > EMA60
   - MACD > 0
   - RSI 在 50-90 之间
   - 等等

---

## Strategy contracts（TIP-006）

每个 TradingView Screener 视为一份**策略合同**。改 Filter Pills / 改名 / 换 URL = 发版；以本文 + 快照里的 `screenTitle` / Filter Pills 为准。

### 宇宙一览

| 显示名 / 期望 screenTitle 子串 | 类型 | 进池后滤 | 触媒「今日 screener TrendOK」 | 推荐 enabled |
|--------------------------------|------|----------|-------------------------------|--------------|
| **Karios Pullback**（子串 `karios pullback`） | `pullback` | 52W 回撤 ∈ [-15%, -5%] + TrendOK → `source=screener` | 是（标题匹配） | **是（主宇宙）** |
| Falcon Launch / Institutional Trend | `momentum` | 同上回撤窗（未单独分支前） | 是（标题匹配） | **否**（除非显式要动量观察） |
| Legacy Falcon / Legacy Black Horse（空库 seed 名） | `legacy` | 同上 | Falcon Launch 类标题才进；Black Horse **不**进 | **否**（空库 seed 默认 `enabled=false`；已有库请 Settings 禁用） |
| （系统）Industry Top5 空窗降级 | `system` | 仅 TrendOK → `source=screener_fallback` | 否 | n/a（TIP-003 自动） |

触媒标题匹配实现：`apps/desktop-ui/src/lib/screenerExport.ts` → `SCREENER_TITLE_PATTERNS`（`includes` 小写）。

### Karios Pullback（主合同）

| 项 | 约定 |
|----|------|
| TV 显示名 / screenTitle | **Karios Pullback** |
| URL（TIP-001 验收） | `https://www.tradingview.com/screener/m22BmHkT/` |
| Market | CN |
| Pills 原则 | 保留：市值、P/E>0、营收增长、Price>EMA20/50、中长期均线多头、RSI 约 45–75；**不要**：当日涨幅门槛、Rel vol 强势、Perf 3M 大涨等追涨条件 |
| 表格列 | 须含 **Price** 与 **High 52W**（回撤过滤依赖） |
| 进池 | 回撤窗 + TrendOK；空窗见 TIP-003 |

### 空库 seed 说明

`ensure_seeded()` 仅在 **tv_screeners 为空** 时写入 `falcon` / `blackhorse` 两条（显示名 **Legacy Falcon (momentum)** / **Legacy Black Horse**，**`enabled=false`**）。**不会**更新已有库行。生产库请在 Settings 配置并启用 **Karios Pullback**，确认遗留动量屏为禁用。

---

## 与其他模块的关系

```
                    ┌─────────────┐
                    │  Screener   │
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ Watchlist  │  │    AI      │  │  Dashboard │
    │  (导入股票) │  │  (上下文)  │  │ (同步状态) │
    └────────────┘  └────────────┘  └────────────┘
```

### 与 Watchlist 的协作

1. 在 Screener / Watchlist 页面触发 Import（或盘后 Automation）
2. 系统获取所有启用 Screener 的最新快照
3. 应用回撤比例过滤（-15% 到 -5%）+ TrendOK → `source=screener`
4. **空窗降级（TIP-003）**：若 `tvHit==0` 或 `passPullback==0`，改用 5D Top5 非防守行业成分（≤80）只过 TrendOK → `source=screener_fallback`
5. Funnel 写入 Import Debug / automation `meta.funnel`

主宇宙建议配置为 **Karios Pullback**（趋势回踩，非当日强势动量）。

### 与 AI 的协作

1. 点击 "Reference to chat" 按钮
2. 当前快照数据作为上下文添加到 AI 对话
3. AI 可以基于这些数据进行分析：
   - 解释为什么某些股票出现在列表中
   - 对比不同时间点的变化
   - 结合其他数据源进行综合分析

---

## 关键业务规则

### 回撤比例过滤（导入 Watchlist 时）

```
回撤比例 = (当前价 - 52周最高价) / 52周最高价

保留范围：-15% ≤ 回撤比例 ≤ -5%

含义：
- 股票从 52 周高点回踩了 5% 到 15%
- 不是追高（回撤太少）
- 不是趋势破坏（回撤太多）
```

### TrendOK 检查（导入 Watchlist 时）

| 检查项 | 条件 | 说明 |
|--------|------|------|
| EMA 趋势 | Close > EMA20 > EMA60 | 短期均线在长期均线之上 |
| MACD | MACD Line > 0 | 动能向上 |
| MACD 柱 | Histogram > 0 | 动能加速 |
| 近高点 | Close ≥ 0.90 × High(20) | 价格接近近期高点 |
| RSI | 50 ≤ RSI(14) ≤ 90 | 不过热，不超卖 |
| 成交量 | AvgVol(5) ≥ 0.9 × AvgVol(30) | 量能健康 |

---

## 典型使用场景

### 场景一：每日盘前准备

1. 打开 Screener 页面
2. 点击 "Sync all" 同步所有筛选器
3. 查看最新快照，了解当前符合条件的股票
4. 对比昨天的 AM/PM 快照，发现新增股票
5. 将感兴趣的股票添加到 AI 对话，进行深入分析

### 场景二：发现新机会

1. 在 Watchlist 页面点击 "Import from screener"
2. 系统自动筛选符合条件的股票
3. 查看导入结果表，按 Score 排序
4. 将高分股票添加到 Watchlist 进行追踪

### 场景三：策略验证

1. 复制 Screener 数据为 Markdown
2. 在 AI 对话中讨论筛选条件的有效性
3. AI 结合历史数据和市场环境给出建议

---

## 技术要点（非代码层面）

### 为什么使用 CDP 而不是直接 API

- TradingView 的 Screener 数据是动态渲染的
- 筛选条件（Filter Pills）无法通过 URL 参数获取
- 需要 CDP 连接已登录的 Chrome 来复用认证状态

### 数据存储策略

- 每个 Screener 可以有多个历史快照
- 快照按时间戳唯一标识
- 支持增量查询和删除

### 性能考虑

- 表格是虚拟滚动，需要模拟滚动加载更多数据
- 最多抓取 300 行
- 串行同步多个 Screener，避免过载