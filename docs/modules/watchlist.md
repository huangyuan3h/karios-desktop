# Watchlist 模块

> 股票关注列表与交易追踪模块

---

## 模块定位

Watchlist 模块是 Karios 的**核心操作中心**，用于管理用户关注的股票列表，追踪持仓情况，并提供技术面分析和交易建议。它将 Screener 的筛选结果与实际交易决策连接起来。

## 核心价值

- **股票管理**：维护个人关注的股票清单
- **持仓追踪**：记录买入价格、数量、最高价
- **技术分析**：提供 TrendOK 检查和 Score 评分
- **交易建议**：给出买入/等待/回避建议和止损价位

### 「港湾」（Harbor）今日操作（实盘口径 · 2026-09-13 上线）

Watchlist「今日操作」的目标口径 = **港湾**：**S-3 股票核心 + 闲置现金 ETF 停车场**。旧 `strategyMode=twin_star` 卫星路径（14:30 名单、12:30 快照、4×12.5%、第 3 日卖出）已随 B12 **REJECT 下线**，不再产生卫星指令。

- **核心（S-3）**：按冻结参数持有/轮出 A 股 + HK 股票篮（10%×10 名义上限）；今日决策卡列出 S-3 持仓与买卖（score≥65 · RS 前 50% · regime 非 Weak · 主线白名单 · 非持仓 · CN only）。
- **停车场（闲置现金）**：空闲资金每日按 **14:30 口径**在 `mom60+MA200` 的金（518880）/ 油（513350）/ 纳指（513110 / 513100 best）/ 国债（511260）里取 **`argmax`** 停一只；**无候选 → 留现金/REPO**；因果 trail8（t-1 收盘触发）出场后回现金。只在 S-3 闲置现金上作用，不抢核心股票仓。
- **展示**：顶部显示「核心仓位 + 停车场当前 pick（ETF / mom60 / 是否站上 MA200）」；S-3 Exec 硬闸（单票 15% `SIZE_CAP_BLOCK` / 板块 30% `SECTOR_CONC_BLOCK` / 袖子 `SLEEVE_CAP_BLOCK`）不变。数据依赖 = 核心 ETF 日线（`daily`）+ S-3 持仓账本；陈旧走系统自检。
- **无卫星指令**：不再有 14:30 卫星名单、12:30 东财全市场快照、C1 3% 过滤、第 3 日 14:30 卖出、`source=twin_star` paper 簿、`SAT_MAX_POS` / `SAT_SLOT_NAV_PCT` 等；前端/后端残留路径已随 **OPT-178** 清理（✅）。
- **提醒铃铛**：目标只剩核心 S-3 交易与停车场换仓；旧 `mode=twin_star` 通知已随 OPT-178 下线（通知默认 `single_track`）。
- **Live 现状（2026-09-13 起）**：港湾已上线——`paper_twin_star` intake 前视、套筒 trail 记账前视、`_pnl_for` 恒 0、`SELL_TO_A_SHARE` 0 元平仓等**已全部修复/删除**；审计与清单见 [`optimization-checklist.md` OPT-178](../optimization-checklist.md) 与 [B11 档](../backtests/stable/etf-parking-baseline-2026-09-13.md)。

---

## 核心概念

### TrendOK（趋势健康检查）

TrendOK 是一套技术面健康检查系统，用于判断股票的趋势是否健康。

| 检查项 | 条件 | 说明 |
|--------|------|------|
| EMA 趋势 | Close > EMA20 > EMA60 | 短期均线在长期均线之上，多头排列 |
| MACD 线 | MACD Line > 0 | 动能为正 |
| MACD 柱 | Histogram > 0 | 动能加速中 |
| 近高点 | Close ≥ 0.90 × High(20) | 价格接近 20 日高点 |
| RSI | 50 ≤ RSI(14) ≤ 90 | 不超卖，不过热 |
| 成交量 | AvgVol(5) ≥ 0.9 × AvgVol(30) | 量能健康 |

**结果含义**：
- ✅ 所有检查通过，趋势健康
- ❌ 有检查未通过，趋势存在问题
- — 数据不足，无法判断

### Score（综合评分）

Score 是一个 0-100 的确定性评分，用于衡量股票的短期设置质量。

#### 评分组成（V4.0）

| 子评分 | 权重 | 说明 |
|--------|------|------|
| EMA 趋势连贯 | 40% | EMA 结构 + EMA20 日斜率 |
| MACD 动能稳定 | 20% | 水下过滤 + 2 日柱递增 |
| 量能一致性 | 20% | 温和放量区间 [1.2, 2.0] 满分 |
| 突破平滑 | 10% | 贴近/突破近 20 日高 |
| RSI 舒适带 | 10% | 锚定 RSI=65，>80 加速衰减 |

#### 加分项

- EMA20 连续 5 日上升：+5 分

#### Anti-Spike 剥离惩罚

- 日内涨幅 > 6%：−20 分（TIP-007 放宽 Exec 闸时 **仍保留** 此项 Score 惩罚）
- ATR/Close > 5%：按 `(ratio − 0.05) × 1000`  steep 扣分
- 当日量 / AvgVol30 > 3：−15 分
- 收盘 < EMA20：−30 分（一票否决右侧资格）

#### TIP-007 主线动量日内放宽

默认：日内 `>` 6% → Exec `INTRADAY_SURGE_BLOCK`，后端 `buyAction=avoid`。

**例外**（须同时）：Gate=`ATTACK`、东财行业已过主线、`buyMode=B_momentum`、`TrendOK=true`、**扣日内 spike 前** Score≥85（展示分仍含 Anti-Spike −20）、日内 ≤9%。则：

- FE Why=`MOMENTUM_SURGE_ALLOW`（可 BUY/ADD）
- BE 不强制 `avoid`，`buyChecks.momentum_surge_allow=true`
- `>` 9% 仍拦截；弱市跳空闸 / 仓位硬闸不变

#### V6.2 尾盘时间锁 / 防守双轨 / 归零清场

- **TimeLock**：`DEFEND` 或 `Weak` 时，BUY/ADD 仅上海 **14:30–14:50**；Why=`TIME_LOCK_WEAK_REGIME` / `MARKET_CLOSING_LOCK`。`ATTACK`+`Strong` 豁免。
- **Defensive Sleeve**：`DEFEND` 下白名单（石油石化/公用事业/煤炭/银行/有色金属）+ 5D Top3 + Score≥70 + TrendOK → Why=`DEFENSIVE_SLEEVE_ALLOW`（袖子≤10%、单票≤5%）。HardStop=`max(EMA10, px×0.965)`。Beta 硬条件 follow-up。
- **Zero-Pos**：`Pos%` 置 0/空时清 `costPrice` / `maxPrice` / `entryDate`，避免残留 `ENTRY_DATE_MISSING`。

#### V6.3 极端资金流豁免 / TrendOK recovering

- **WEAK_ATTACK**：单板块 1D 净流入 >500 亿 + upCount >4000 + ≥14:30 → Gate 从 DEFEND/HOLD_ONLY 升级；`allowNewEntries=true`，Suggest%≤5%。
- **recovering**：Alpha Max Grade=S + 今日量 ≥2.5×10 日均量 + 大阳线 → `trendStatus=recovering`、`trendOk=true`、Score floor 60；Action 离开 `WATCH_SILENT` → `WATCH`（Why=`TREND_RECOVERING`），非自动 BUY。

#### 行业加分

- 股票所在行业处于热点 Top3：额外加分（详见 industryFlowReasons）

### 买入建议系统

系统会根据技术分析给出买入建议：

#### 买入模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| A_pullback | 回踩买入 | 趋势向上，等待回调到支撑位 |
| B_momentum | 动量买入 | 突破新高，追涨买入 |

#### 买入动作

| 动作 | 说明 |
|------|------|
| buy | 当前价格在买入区间，可以买入 |
| wait | 等待价格进入买入区间 |
| avoid | 技术面走坏，建议回避 |

#### 买入区间

系统会计算建议的买入价格区间：

```
买入区间 = [buyZoneLow, buyZoneHigh]

A_pullback 模式：
- 买入区间通常在 EMA20 附近

B_momentum 模式：
- 买入区间通常在近期高点附近
```

#### 强制买入规则

当满足以下条件时，A_pullback/wait 会被强制改为 buy：

1. Score > 90
2. 市场处于 Green Zone（指数双绿）
3. 行业处于今日 Top3

含义：强势热点不等待，直接买入。

### 止损系统

止损价计算：

```
止损价 = max(final_support - buffer, hard_stop)

其中：
- final_support：最终支撑位（EMA20 或近低点）
- buffer：ATR × 倍数
- hard_stop：硬止损（成本价下方一定比例）
```

#### 立刻离场条件

任一条件触发，建议立刻离场：

| 条件 | 说明 |
|------|------|
| EMA5 < EMA20 | 短期趋势走坏 |
| 收盘价 < EMA20 | 跌破均线支撑 |
| 动能衰竭 + 量能萎缩 | 动量和量能同时恶化 |

#### 警告条件

建议至少卖出一半：

- MACD 柱缩小但未转负

---

## 业务流程

### 日常使用流程

```
1. 添加股票
   ├── 手动输入股票代码添加
   └── 或从 Screener 导入符合条件的股票

2. 查看分析
   ├── 查看 Score 评分
   ├── 查看 TrendOK 检查结果
   ├── 查看买入建议
   └── 查看止损价位

3. 记录持仓
   ├── 输入成本价
   ├── 输入仓位比例
   └── 系统自动追踪最高价

4. 决策执行
   ├── 根据买入建议决定是否买入
   ├── 根据止损价位决定是否卖出
   └── 更新仓位和成本价
```

### 从 Screener 导入流程

```
1. 点击 "Import from screener"
     │
     ▼
2. 加载所有启用的 Screener 最新快照
     │
     ▼
3. 提取股票代码
     │
     ▼
4. 回撤比例过滤（-15% 到 -5%）
     │
     ▼
5. TrendOK 检查
     │
     ▼
6. 只添加通过检查的股票
     │
     ▼
7. 显示导入调试表供查看
```

---

## 数据字段说明

### 基础字段

| 字段 | 说明 |
|------|------|
| Symbol | 股票代码（如 CN:600000） |
| Name | 股票名称 |
| Industry | 东财行业板块名（与 Dashboard 行业资金流同源） |
| HotTop3 | 是否属于当日行业资金流入 Top3（✓ / —） |
| 成本价 | 买入价格（手动输入） |
| 仓位% | 当前仓位占总资金比例 |
| Current | 当前价格（盘中实时 / 最新日线） |
| VWAP | 当日成交量加权均价（盘中由 quote 推导） |
| P&L% | 相对成本价的浮盈亏比例 |

### 分析字段

| 字段 | 说明 |
|------|------|
| Score | 综合评分（0-100） |
| TrendOK | 趋势健康检查结果 |
| 买入 | 买入建议（模式/动作/区间） |
| 止损 | 止损价位 |

### 颜色标记

股票行背景色含义：

- 🟢 绿色背景：Score ≥ 85，TrendOK ✅，买入建议为 buy
- 🔴 红色背景：止损触发"立刻离场"或买入建议为 avoid
- 默认背景：其他情况

---

## 与其他模块的关系

```
                    ┌──────────────────┐
                    │    Watchlist     │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
 ┌────────────┐       ┌────────────┐       ┌────────────┐
 │  Screener  │       │IndustryFlow│       │    AI      │
 │  (导入股票) │       │ (行业加分) │       │ (分析决策) │
 └────────────┘       └────────────┘       └────────────┘
```

### 与 Screener 的关系

- Screener 是股票来源
- 通过回撤比例和 TrendOK 过滤后导入
- 导入的股票自动获得 Screener 的上下文

### 与 Industry Flow 的关系

- Industry Flow 影响股票的行业加分
- 热点行业的股票 Score 更高
- 帮助"选股先选板块"

### 与 AI 的关系

- Watchlist 数据可作为 AI 对话上下文
- AI 可以解释买入/止损建议的原因
- AI 可以结合新闻和市场环境给出建议

---

## 典型使用场景

### 场景一：盘前准备

1. 打开 Watchlist，点击 Refresh 更新数据
2. 查看所有股票的 Score 和 TrendOK
3. 关注评分高的股票
4. 查看买入建议，准备交易计划

### 场景二：盘中监控

1. 盘中自动刷新（每 10 分钟）
2. 关注 Current 列的实时价格
3. 关注止损列是否有红色警告
4. 根据信号执行买卖

### 场景三：盘后复盘

1. 点击 "Copy Markdown" 导出数据
2. 添加到 AI 对话进行分析
3. 更新成本价和仓位
4. 规划明天的操作

---

## 重要业务规则

### 成本价和最高价

- 成本价：手动输入，用于计算盈亏
- 最高价：自动更新，用于追踪持仓期间的最高价
- 卖出时可参考最高价作为止盈参考

### 仓位管理

- 仓位%：当前持仓占总资金的比例
- 用于控制单只股票的风险敞口
- 建议单只股票不超过 10-15%

### 自动刷新

- 盘中每 10 分钟自动刷新
- 只刷新缓存数据，不强制网络请求
- 手动 Refresh 才会强制更新网络数据

### 盘后自动化（17:30）· 策略池（OPT-209，2026-09-15 用户拍板）

每个 A 股交易日 **17:30（Asia/Shanghai）** 自动运行（也可在 Watchlist 页点击 **Run automation** 手动触发）。
自 2026-09-15 起，automation **只维护两条回测腿的策略池**（`run_watchlist_automation`）：

1. **S-3 池**（`source=s3`）：`paper_s3.build_s3_candidates(gate_mode="pool")` —— **score≥65 & RS 过线**（CN RS≥0.5 / HK RS≥0.6、剔除 300 板），**不含 regime 闸**，所以弱市里池仍保留名字（买入仍由 Live 闸门决定）。宽度 = 引擎建仓上限 `S3_MAX_POSITIONS`（每市场 10，按 score→RS 排名）。
2. **星舰池**（`source=satellite`，研究档）：习惯 replay（`state_bucket_track`, `recipe="habit"`）当前持有的 14:30 卫星腿（持 3 个交易日），带 `entryDate/exitDue/daysLeft` 元数据。
3. **失效自动移除**（持仓豁免，`positionPct>0` 不删）：S-3 = **连续 2 日**（`S3_POOL_FAIL_DAYS`）不再出现在池里；星舰 = replay 已出场（不再持有）。
4. **自动应用**：run 写入 registry 即生效（`apply_pool_run` → `upsert_registry` → `ack_run`），前端不再有 pending/ack 步骤，只在下一次 hydrate 时刷新。
5. **盘后补齐（18:40+，OPT-219）**：`bar_5min_close` 存入当日 14:30 分时后自动调用 `refresh_satellite_pool`（run `trigger=post_5min`）重算星舰池——17:30 那次跑在成交面板落地之前，**当日新进的卫星腿原本要迟到一天**才进池。refresh 幂等、只动 `source=satellite` 行（S-3 移除判定跳过），并把当日 17:30 run 的 S-3 计数累加进自己的 meta（UI 每日只显示最新一条已应用 run）。
6. **Fail-open 纪律**：replay 失败时 `compute_satellite_pool` 返回 `None`（区别于"当日无腿"）→ 移除层整段跳过，绝不因一次失败清空星舰行；`post_5min` refresh 失败同样只记日志、不触碰 registry。
7. **Alpha / 研报渠道已停用**（`ALPHA_CHANNEL_ENABLED=False`）：不再从催化/研报提名；历史 `alpha_radar`/`screener*` 行保留不动（手动清理）。

**池变动历史**：`GET /watchlist/automation/runs` 返回每个交易日的 run（`meta.poolAdded/poolRemoved{s3,satellite}` + `meta.s3PoolSize/satellitePoolSize`），Watchlist 诊断面板渲染为「S-3 池 / 星舰 / 加入 / 移除」表。18:40+ 的 `post_5min` refresh 会**累加**当日计数并继承 S-3 尺寸字段（每个交易日仍只有最新一条已应用 run 展示）。

```text
Last pool automation: {time} ({trigger}) | S-3 池 10 (+3 / −2) · 星舰 4 (+4 / −0)
```

| meta 字段 | 摘要展示 | 说明 |
|-----------|----------|------|
| `poolAdded` / `poolRemoved` | `(+a / −r)` 分腿 | 本日新增 / 失效移除数（含 18:40+ refresh 累加） |
| `s3PoolSize` / `s3PoolPrevSize` | `S-3 池 N` | 今日 / 前一交易日（两日失效判定用） |
| `satellitePoolSize` | `星舰 N` | 当前卫星腿数（refresh 后为收盘口径） |
| `poolCaliber` | — | 口径字符串（审计用） |
| `alphaChannel` | — | `off`（渠道停用标记） |
| `industrySync` | `sync ind✓/✗` | 行业流同步失败只进 meta，不中断 run |
| `satellitePoolError` | — | `true` = replay 失败，当日星舰增减整段跳过（fail-open） |

### 历史渠道（2026-09-15 前，已停用）

TV screener 漏斗（`meta.funnel`：`tvHit → passPullback → passTrendOk → +addedNew`）、TIP-003 空窗降级（`source=screener_fallback`）、Alpha Radar / 研报进池（catalystScore >85 & S 级 + 轻量闸）均已停用；相关代码保留在 `watchlist_automation.compute_alpha_additions` / `compute_removals` 供参考，不再被 run 调用。

---

## 注意事项

### 数据依赖

- TrendOK 和 Score 依赖历史数据
- 如果数据缺失，结果可能为 —
- 建议先在 Market 页面同步股票基础数据

### 建议性质

- 买入/止损建议仅供参考
- 不构成投资建议
- 最终决策由用户自己做出

### 本地存储

- Watchlist 数据存储在浏览器本地
- 清除浏览器数据会丢失

---

## 用户使用笔记（2026-08-01 老婆反馈 · 已落实）

> 真实使用者（老婆）亲自使用后的反馈；对应归档快照 [`2026-08-27-todo-full-snapshot.md`](../archive/2026-08-27-todo-full-snapshot.md) §15。全部已落实，记录在此防回归。

| # | 反馈 | 落实 |
|---|------|------|
| 1 | Watchlist table header 参数看不懂，hover 能明白每个参数干什么 | ✅ `lib/watchlist-column-help.tsx` + `ColumnHeader`（2026-08-01） |
| 2 | 新闻模块（尤其 Dashboard）没有财经新闻准 | ✅ News Substrate 2.0 三轨全完成（Tier 分级源 + LLM enrichment + Morning Brief；2026-08-02） |
| 3 | Dashboard 内容重复、参数看不懂 | ✅ `lib/dashboard-card-help.tsx` + DashboardHeader；Last sync 单行化、Index rule 块 hover（2026-08-01） |

### 交易记录闭环（真实卖出请用行内「卖出」按钮）

- 卖出时点 Watchlist 行内「卖出」→ 输入卖出价格 → 记录真实成交（`user_trades` 表）
- 缺成本/入场日的持仓也可记录（2026-08-09 起：可选成本补填，留空=仅记录卖出无盈亏）
- Watchlist 页「交易期望值看板」自动累计：胜率 / 盈亏比 / 每笔期望值 vs 0.3% 成本线 / 分来源
- 期望值/周度复盘依赖这些真实样本——**不在 Karios 记录卖出，闭环就空转**
- 建议定期导出 Markdown 备份