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

**双市场独立仓位（A股 / 港股）**：`executionGate` 顶层字段仍为 A 股闸门（兼容下游），并新增 `cnGate`（=顶层）与 `hkGate`。两市场**各自独立**按本市场红绿灯给出 `positionRangeHint`，不共用仓位预算——总仓位 100% 可在 A 股与港股之间分配。HK 闸门由恒生指数 + 恒生科技指数驱动，叠加全局 riskMode（extreme_caution / no_new_positions 强制 DEFEND）；不含 A 股特有的广度恐慌 / SRV / 盘中溢出覆盖。Dashboard / Copy all 分别展示 `A股闸门` / `港股闸门`。

指数红绿灯覆盖面：**A 股** = 上证指数（featured，占 2 格）+ 创业板指 + 中证500；**港股** = 恒生指数 + 恒生科技指数。Regime 语义：Strong = 本市场全部绿灯，Diverging = 部分绿灯，Weak = 无绿灯；`positionRangeHint` 取本市场更保守（更弱）灯对应的仓位区间。

**ETF 资金流 = 二级确认/过滤因子（V6.4）**：不独立触发买卖。`ETF_WATCHLIST` 按类别归并成 `etfFlowSignal`（broad→国家队方向、sector→板块动量），`verdict=confirm/neutral/contradict`。执行闸在数据完整时：`confirm` 仅追加原因 `ETF_FLOW_CONFIRM`；`contradict` 追加 `ETF_FLOW_CONTRADICT` 并把普通 `ATTACK` 降为 `HOLD_ONLY`。永不升级、不降 `WEAK_ATTACK` 与硬 `DEFEND`，数据不完整时忽略。Dashboard 展示为情绪卡片内一行「资金确认」徽标，完整 ETF 明细表只保留在 Index 页次级面板与 AI copy 输出。

Watchlist 每行另有 **Action Card**（Exec / Trigger / Trail）：EXIT、TRIM、HOLD、ADD、BUY、WATCH、**WATCH_SILENT**、**PURGE**。Trigger 列按仓位语义拆分：**空仓**显示 Entry_Trigger（`buyZoneHigh`），**持仓**显示 Exit_Stop（`max(hardStop, trailStop)`）。Dist%：空仓 `(Entry−Current)/Current`，持仓 `(Current−Exit)/Current`。Unified Copy 表另含 **CostPrice / P&L% / EntryDate / Locked_T1**（A 股 T+1：当日买入禁 EXIT）。下游 AI 应优先服从 Gate 与 Action，而不是自行重算红绿灯。

### Decision Journal（执行决策闭环）

Dashboard 卡片 **Decision Journal** 把 Gate + Action Card 写成可回放时间线（Postgres：`execution_snapshots` / `execution_decision_changes`）。

- **Exec Attention（卡顶）**：`Sleeve` 预算 → Must act（EXIT→TRIM）→ Fire（BUY/ADD，受 Gate）→ 缺仓提示 → 今日关键变更 Top3。Must act / Fire **优先 live**（Watchlist market + `deriveActionCard`），无行情时回退 Journal snapshot。
- **自动采集**：Sync All、盘中 5 分钟 poll、Watchlist 仓位变更 debounce、收盘后 eod、手动 Snapshot now。Action Card 仍由前端 `deriveActionCard` 计算后 POST。
- **变更流水**：`mode` / `action` / `why` / `trigger` / `entryTrigger` / `exitStop` / `hardStop` / `trailStop` / `positionPct` 变化落库；同决策 content_hash 只心跳更新 `captured_at`。
- **Latest Actions（delta）**：Journal 只列出当日 Action / Trigger / Entry_Trigger / Exit_Stop / HardStop / TrailStop 变更的标的；静默 WATCH→WATCH 不入表。
- **Copy all Markdown（忙人包）**：纯数据 Payload（**无**内嵌 AI instructions）。顺序：`## Since last copy` → Gate → Attention → Cond order（休市带 `[Queue for Next Open]`）→ Journal → 宏观原料 → **`## Combat Positions & Watchlist (Unified)`**（含 CostPrice/P&L%/EntryDate/Locked_T1、Entry_Trigger/Exit_Stop；PURGE 行在报告后物理剔除；Alpha S 豁免为 WATCH_SILENT）。行为合同只在 System Prompt。Dashboard 另有 **Sync & Copy** 一键。
- 迁移：`PYTHONPATH=src alembic upgrade head`（revision `0010_execution_decision_journal`）。

### 下游判断 AI（Copy → Agent）

外部 / 本仓判断 AI 的 system prompt 见 **[downstream-ai-prompt.md](./downstream-ai-prompt.md)（V7.6 · CostPrice/P&L · Alpha S · T+1）**。

要点：硬合同不变；先**操作表**，再**今日焦点≤3条**（写透）+ **战场扫描 7 行短句**防漏报；禁夸张套话与注水。

### 主线绑定（BUY/ADD 硬闸）

Watchlist 是**监控池**（TV Screener → 回撤 + TrendOK 导入），**不等于**买单。

`BUY` / `ADD` 在 Gate=`ATTACK` 或 `WEAK_ATTACK` 之外，还必须同时满足：

1. **主线**：所属东财行业 ∈ `5D 净流入 Top3` ∪ `Momentum Breakout`（今日净流入≥20亿且排名升≥10）
2. **非防守板块**：排除银行、电力、公用事业、中药、煤炭、高速公路
3. **非见光死**：日内涨幅 ≤6%（`>` 6% 拦截；缺报价/`null` 不拦截）。**TIP-007 例外**：Gate=`ATTACK` 且行业已过主线，且 `buyMode=B_momentum`、`TrendOK=true`、扣日内 spike 前 Score≥85（Anti-Spike −20 仍反映在展示分上）时，允许日内 ≤9%；Why=`MOMENTUM_SURGE_ALLOW`。>9% 仍拦截。
4. **非弱市高开**：`gapUp`（真跳空：当日低点 > 前日高点）且市场 `Weak`/`Diverging` → 禁止 BUY/ADD；缺 gap / 非弱市 regime 不拦截。与 Alerts `gap_up_weak_market` 同条件；后端 live 时可能已将 `buyAction` 置 avoid，Action 为合同层双保险。
5. **非板块过浓**：该东财行业已持仓合计 `positionPct >= 30%` → 禁止 BUY/ADD，`SECTOR_CONC_BLOCK`；未传暴露度映射或无仓位数字不拦截。
6. **非袖子打满**：卫星仓 `positionPct` 合计 ≥ Gate.`positionRangeHint` 上界 → 禁止 BUY/ADD，`SLEEVE_CAP_BLOCK`；hint 不可解析或未传合计不拦截。
7. **弱市/DEFEND 尾盘时间锁（V6.2）**：`Gate.mode=DEFEND` 或 `marketRegime=Weak` 时，仅上海时间 **14:30–14:50** 允许新开/加仓；更早 → `TIME_LOCK_WEAK_REGIME`，更晚 → `MARKET_CLOSING_LOCK`。豁免：`ATTACK` + `Strong`。`WEAK_ATTACK` 已过 14:30 门槛，仍受 `>14:50` 收盘锁。
8. **防守双轨袖子（V6.2）**：`DEFEND` 下白名单行业（石油石化/公用事业/煤炭/银行/有色金属）且 ∈ 5D 净流入 Top3、Score≥70、TrendOK=ok 时，可豁免全局禁开，Action=`BUY` Why=`DEFENSIVE_SLEEVE_ALLOW`；袖子合计上限 10%、单票 5%。防守持仓豁免 `GATE_DEFEND` TRIM。HardStop 收紧为 `max(EMA10, Current×0.965)`。**Beta&lt;0.8 硬条件本期未接（follow-up）**。
9. **超大单日资金突破豁免（V6.3）**：单板块 1D 净流入 >500 亿 **且** 上涨家数 >4000 **且** ≥14:30 → Gate 升级为 `WEAK_ATTACK`（`allowNewEntries=true`，Suggest% 硬顶 5%，Why 含 `INTRADAY_OVERFLOW_OVERRIDE`）。不覆盖 `BREADTH_PANIC` / `RISK_*`。

否则 Exec 降为 `WATCH`（候选）或持仓 `HOLD`（不加仓、不 TRIM），Why 为 `NOT_MAINLINE` / `SECTOR_OUTFLOW_BLOCK`（全日板块净流出）/ `DEFENSE_SECTOR_BLOCK` / `MISSING_INDUSTRY` / `INTRADAY_SURGE_BLOCK` / `MOMENTUM_SURGE_ALLOW`（合法放行）/ `GAP_UP_WEAK_BLOCK` / `SECTOR_CONC_BLOCK` / `SLEEVE_CAP_BLOCK` / `TIME_LOCK_WEAK_REGIME` / `MARKET_CLOSING_LOCK` / `DEFENSIVE_SLEEVE_ALLOW`（合法放行）/ `TREND_RECOVERING`（V6.3 准买区）。与 Import 过滤正交。

空仓且 `Score < 30` 且 `TrendOK=no` → Action=`PURGE`（Why=`PURGE_GC`）；Copy/报告生成后从 Watchlist 物理删除。**豁免**：Alpha Radar Max Grade=`S` → Action=`WATCH_SILENT`（Why=`ALPHA_S_WATCH`），留池静默观察、不物理删（无视 TrendOK/catalystScore）。**V6.3**：S 级 + 放量（≥2.5×10 日均量）+ 大阳线 → `trendStatus=recovering`、Score≥60，解除静默为 `WATCH`（Why=`TREND_RECOVERING`，非自动 BUY）。盘后三日低分自动化：仅 `Pos%==0`（或缺失）才移除；**仅**催化窗口内仍含 Max Grade=`S` 的票豁免三日 GC（非 S 的 `source=alpha_radar` 与其它来源相同可被清）。

持仓 `entryDate`（上海日历日）等于今日 → `Locked_T1=True`：本应 EXIT/TRIM 时降为 `HOLD`（Why=`T1_LOCK`），Cond 卖单草稿跳过；缺 `entryDate` → `Locked_T1=MISSING` fail-closed（Why=`ENTRY_DATE_MISSING`，禁卖并报警）。首次 `positionPct` 从 0→>0 时自动戳 `entryDate`。**V6.2 Zero-Pos**：`positionPct` 置 0/`null` 时自动清 `costPrice` / `maxPrice` / `entryDate`，避免残留成本被当成持仓而报 `ENTRY_DATE_MISSING`。

空仓 `Entry_Trigger`（buyZoneHigh）≤ `HardStop`（stopLossPrice）→ 禁 BUY（Why=`ENTRY_BELOW_STOP`）；后端 TrendOK 同步将 `buyAction` 置 `avoid`。

硬止损 `TRIGGER_HIT` 的 Cond 草稿：`Order_Price` = 当日跌停价（非 Exit_Stop），避免跳空低开废单。

### 持仓 TRIM（DEFEND + 主线失效）

对**已持仓**，Action 在 EXIT / 动能警告 TRIM 之后，还有两道减仓硬闸：

| 条件 | Action | Why |
|------|--------|-----|
| Gate = DEFEND（含 SRV Extreme_High 电风扇、广度恐慌、Weak 等） | TRIM | `GATE_DEFEND` |
| 主线集合已就绪，且行业不在集合（或缺行业名） | TRIM | `MAINLINE_FADE` / `MISSING_INDUSTRY` |

优先级：`EXIT` > `WARN_REDUCE_HALF` > `GATE_DEFEND` > `MAINLINE_FADE` > ADD/HOLD。

- **HOLD_ONLY**：不因 Gate 强制 TRIM，仍可因主线失效 TRIM。
- **WEAK_ATTACK（V6.3）**：不触发 `GATE_DEFEND` TRIM；允许限量开火。
- 主线数据未就绪时：不因失效误 TRIM（避免空洞数据砍仓）。
- **V6.2**：防守白名单持仓在 DEFEND 下**豁免** `GATE_DEFEND` TRIM（仍可因主线失效 TRIM）。

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
- [docs 总索引](../README.md)
- [设计与未完成概念](../designs/README.md)