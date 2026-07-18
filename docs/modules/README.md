# Karios 模块文档

本目录包含 Karios 投资分析系统各功能模块的详细业务文档。

---

## 模块概览

| 模块 | 定位 | 核心功能 |
|------|------|----------|
| [Screener](./screener.md) | 数据入口 | 同步 TradingView 筛选器结果 |
| [Industry Flow](./industry-flow.md) | 板块分析 | 追踪行业资金流向，识别热点板块 |
| [Watchlist](./watchlist.md) | 操作中心 | 管理关注股票，提供技术分析和交易建议 |
| [Market Sentiment](./market-sentiment.md) | 仓位管理 | 市场情绪分析，红绿灯信号，仓位建议 |
| [News Brief](./news-brief.md) | 信息获取 | 财经新闻聚合与 AI 摘要 |

---

## 模块协作关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Karios 投资分析系统                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────┐                                                          │
│   │   Screener   │ ─────────────────────────────────────┐                   │
│   │  (数据入口)   │                                      │                   │
│   └──────┬───────┘                                      │                   │
│          │                                              │                   │
│          │ 筛选结果导入                                  │                   │
│          ▼                                              │                   │
│   ┌──────────────┐        行业加分         ┌─────────────┴───┐              │
│   │  Watchlist   │ ◄────────────────────── │  Industry Flow  │              │
│   │  (操作中心)   │                         │   (板块分析)    │              │
│   └──────┬───────┘                         └─────────────────┘              │
│          │                                                                   │
│          │ 买入规则受市场信号影响                                              │
│          ▼                                                                   │
│   ┌──────────────────┐                                                      │
│   │ Market Sentiment │                                                      │
│   │   (仓位管理)      │                                                      │
│   └────────┬─────────┘                                                      │
│            │                                                                 │
│            │ 市场环境参考                                                     │
│            ▼                                                                 │
│   ┌──────────────┐                                                          │
│   │  News Brief  │                                                          │
│   │  (信息获取)   │                                                          │
│   └──────────────┘                                                          │
│                                                                              │
│   所有模块数据均可作为 AI 对话上下文，进行综合分析                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 典型工作流

### 每日盘前准备

```
1. Dashboard 页面
   └── 点击 "Sync all (force)" 同步所有数据

2. 查看市场状态
   ├── Market Sentiment: 确认当前仓位范围
   └── 指数红绿灯: 确认市场强弱

3. 查看板块动态
   ├── Industry Flow: 识别热点行业
   └── 关注主线板块

4. 查看筛选结果
   ├── Screener: 查看符合条件的股票
   └── 关注新增股票

5. 查看新闻动态
   └── News Brief: 了解重要事件
```

### 股票筛选与追踪

```
1. 从 Screener 导入
   ├── Industry Flow 确认行业是否热点
   ├── 回撤比例过滤（-15% 到 -5%）
   └── TrendOK 检查

2. 添加到 Watchlist
   ├── 查看 Score 评分
   ├── 查看买入建议
   └── 设置止损价位

3. 持仓管理
   ├── 记录成本价和仓位
   ├── 监控止损信号
   └── 根据建议调仓
```

### 交易决策

```
1. 市场环境确认
   ├── 指数是否 Green Zone
   └── 确定可用仓位

2. 板块确认
   ├── 股票是否在热点行业
   └── 行业资金流向是否正向

3. 技术确认
   ├── TrendOK 是否通过
   ├── Score 是否达标（≥85）
   └── 买入建议是否为 buy

4. 执行交易
   ├── 在建议买入区间买入
   ├── 设置止损位
   └── 记录到 Watchlist
```

---

## 投资理念

### 资金定位：卫星仓，而非全家资产

Karios 管理的是家庭资产中的**卫星仓**——专门用来博取更高收益的一部分资金，**不是**全家资产的 all-in 通道。

| 层级 | 典型资产 | 与 Karios 的关系 |
|------|----------|------------------|
| 核心仓 | 房产、宽基/债券 ETF、现金等 | 系统外持有，不交给本套波段规则调度 |
| 卫星仓 | 本系统 Watchlist / Screener / 主线交易 | 在核心仓之外，用纪律化规则追求超额 |

含义：

- 红绿灯仓位、Score、强制买入等规则，只约束**卫星仓内部**怎么打，不决定核心仓配比。
- Deep Green 时「可积极参与」指卫星仓可提高使用率，**不等于**把房产变现或卖掉 ETF 全仓进场。
- 卫星仓上限由家庭自行设定（例如占净资产的固定比例）；系统信号再强，也不应突破该预算。
- **单票建议上限 15%（卫星仓内）**：Watchlist `positionPct >= 15` 时 Exec 禁止 `ADD`（→ `HOLD`，Why=`SIZE_CAP_BLOCK`）。不自动 TRIM；候选 `BUY` 不因本规则拦截。
- **同板块合计上限 30%（卫星仓内）**：同一东财行业持仓 `positionPct` 之和 ≥ 30% 时，禁止对该行业 `BUY`/`ADD`（Why=`SECTOR_CONC_BLOCK`）。不自动 TRIM；无仓位数字的持仓不计入合计。
- **卫星仓总仓位硬闸**：Watchlist 内有限正数 `positionPct` 合计 ≥ Gate.`positionRangeHint` 上界时，禁止任意 `BUY`/`ADD`（Why=`SLEEVE_CAP_BLOCK`）。不 TRIM；hint 缺失/`—`/无法解析或未传合计则 fail-open。
- **袖子预算可见性**：Watchlist Gate 横幅显示 `Sleeve 合计% / hint上界%`；持仓有成本但缺 `positionPct` 时琥珀色提示（硬闸仍 fail-open，需补全账本后闸才准）。
- **开火仓位建议（Suggest%）**：`BUY`/`ADD` 时建议本次加仓幅度 = `min(5% clip, 单票15%余量, 板块30%余量, 袖子 hint 余量)`；Exec / Attention / Positions / Copy 同口径展示 `+N% (binding)`。

### Execution Gate（下游执行合同）

Dashboard 与「Copy all Markdown」顶部输出 **Execution Gate**，把分散信号收成当日执行模式：

| mode | allowNewEntries | 含义 |
|------|-----------------|------|
| ATTACK | true | Strong + SRV Stable（等）时允许开新仓/加仓 |
| HOLD_ONLY | false | 分化或 SRV Elevated：禁止新开，只管理持仓 |
| DEFEND | false | 广度恐慌 / SRV Extreme_High / Weak / 极端风险：防守 |

Watchlist 每行另有 **Action Card**（Exec / Trigger / Trail）：EXIT、TRIM、HOLD、ADD、BUY、WATCH、**PURGE**。Trigger 列按仓位语义拆分：**空仓**显示 Entry_Trigger（`buyZoneHigh`），**持仓**显示 Exit_Stop（`max(hardStop, trailStop)`）。Dist%：空仓 `(Entry−Current)/Current`，持仓 `(Current−Exit)/Current`。下游 AI 应优先服从 Gate 与 Action，而不是自行重算红绿灯。

### Decision Journal（执行决策闭环）

Dashboard 卡片 **Decision Journal** 把 Gate + Action Card 写成可回放时间线（Postgres：`execution_snapshots` / `execution_decision_changes`）。

- **Exec Attention（卡顶）**：`Sleeve` 预算 → Must act（EXIT→TRIM）→ Fire（BUY/ADD，受 Gate）→ 缺仓提示 → 今日关键变更 Top3。Must act / Fire **优先 live**（Watchlist market + `deriveActionCard`），无行情时回退 Journal snapshot。
- **自动采集**：Sync All、盘中 5 分钟 poll、Watchlist 仓位变更 debounce、收盘后 eod、手动 Snapshot now。Action Card 仍由前端 `deriveActionCard` 计算后 POST。
- **变更流水**：`mode` / `action` / `why` / `trigger` / `entryTrigger` / `exitStop` / `hardStop` / `trailStop` / `positionPct` 变化落库；同决策 content_hash 只心跳更新 `captured_at`。
- **Latest Actions（delta）**：Journal 只列出当日 Action / Trigger / Entry_Trigger / Exit_Stop / HardStop / TrailStop 变更的标的；静默 WATCH→WATCH 不入表。
- **Copy all Markdown（忙人包）**：纯数据 Payload（**无**内嵌 AI instructions）。顺序：`## Since last copy` → Gate → Attention → Cond order（休市带 `[Queue for Next Open]`）→ Journal → 宏观原料 → **`## Combat Positions & Watchlist (Unified)`**（含 Entry_Trigger / Exit_Stop；PURGE 行在报告后物理剔除）。行为合同只在 System Prompt。Dashboard 另有 **Sync & Copy** 一键。
- 迁移：`PYTHONPATH=src alembic upgrade head`（revision `0010_execution_decision_journal`）。

### 下游判断 AI（Copy → Agent）

外部 / 本仓判断 AI 的 system prompt 见 **[downstream-ai-prompt.md](./downstream-ai-prompt.md)（V7.5 · Entry/Exit · PURGE · 休市挂单）**。

要点：硬合同不变；先**操作表**，再**今日焦点≤3条**（写透）+ **战场扫描 7 行短句**防漏报；禁夸张套话与注水。

### 主线绑定（BUY/ADD 硬闸）

Watchlist 是**监控池**（TV Screener → 回撤 + TrendOK 导入），**不等于**买单。

`BUY` / `ADD` 在 Gate=ATTACK 之外，还必须同时满足：

1. **主线**：所属东财行业 ∈ `5D 净流入 Top3` ∪ `Momentum Breakout`（今日净流入≥20亿且排名升≥10）
2. **非防守板块**：排除银行、电力、公用事业、中药、煤炭、高速公路
3. **非见光死**：日内涨幅 ≤6%（`>` 6% 拦截；缺报价/`null` 不拦截）
4. **非弱市高开**：`gapUp`（真跳空：当日低点 > 前日高点）且市场 `Weak`/`Diverging` → 禁止 BUY/ADD；缺 gap / 非弱市 regime 不拦截。与 Alerts `gap_up_weak_market` 同条件；后端 live 时可能已将 `buyAction` 置 avoid，Action 为合同层双保险。
5. **非板块过浓**：该东财行业已持仓合计 `positionPct >= 30%` → 禁止 BUY/ADD，`SECTOR_CONC_BLOCK`；未传暴露度映射或无仓位数字不拦截。
6. **非袖子打满**：卫星仓 `positionPct` 合计 ≥ Gate.`positionRangeHint` 上界 → 禁止 BUY/ADD，`SLEEVE_CAP_BLOCK`；hint 不可解析或未传合计不拦截。

否则 Exec 降为 `WATCH`（候选）或持仓 `HOLD`（不加仓、不 TRIM），Why 为 `NOT_MAINLINE` / `SECTOR_OUTFLOW_BLOCK`（全日板块净流出）/ `DEFENSE_SECTOR_BLOCK` / `MISSING_INDUSTRY` / `INTRADAY_SURGE_BLOCK` / `GAP_UP_WEAK_BLOCK` / `SECTOR_CONC_BLOCK` / `SLEEVE_CAP_BLOCK`。与 Import 过滤正交。

空仓且 `Score < 30` 且 `TrendOK=no` → Action=`PURGE`（Why=`PURGE_GC`）；Copy/报告生成后从 Watchlist 物理删除。盘后三日低分自动化仍保留，且仅 `Pos%==0`（或缺失）才移除。

### 持仓 TRIM（DEFEND + 主线失效）

对**已持仓**，Action 在 EXIT / 动能警告 TRIM 之后，还有两道减仓硬闸：

| 条件 | Action | Why |
|------|--------|-----|
| Gate = DEFEND（含 SRV Extreme_High 电风扇、广度恐慌、Weak 等） | TRIM | `GATE_DEFEND` |
| 主线集合已就绪，且行业不在集合（或缺行业名） | TRIM | `MAINLINE_FADE` / `MISSING_INDUSTRY` |

优先级：`EXIT` > `WARN_REDUCE_HALF` > `GATE_DEFEND` > `MAINLINE_FADE` > ADD/HOLD。

- **HOLD_ONLY**：不因 Gate 强制 TRIM，仍可因主线失效 TRIM。
- 主线数据未就绪时：不因失效误 TRIM（避免空洞数据砍仓）。

### 选股先选板块

Industry Flow 模块帮助识别热点板块，Watchlist 会给热点行业的股票额外加分。

### 趋势为王

TrendOK 系统确保只关注趋势健康的股票，避免接飞刀。

### 顺势而为

Market Sentiment 的红绿灯帮助判断市场状态，控制卫星仓仓位风险。

### 纪律执行

买入建议和止损系统提供明确的交易规则，减少情绪干扰。

---

## 文档说明

- 每个模块文档独立成篇，可单独阅读
- 文档从业务角度描述，不涉及代码实现
- 文档会随功能迭代持续更新

---

## 相关文档

- [下游判断 AI Prompt（V7）](./downstream-ai-prompt.md)

- [优化 Checklist（架构债务 & Agent 任务）](../optimization-checklist.md)
- [项目整体架构](../architecture-and-requirements.md)
- [Screener 模块设计](../design/screener-module.md)