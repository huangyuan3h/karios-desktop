# 机会双子星下一阶段：工程 · 业务对齐 · 回测可分析（2026-09-02）

> **状态**：用户已拍板「实盘默认 clip4」。本页是下一阶段工作流，**不是**再开一轮卫星扫参。
> **产品**：机会双子星 v3.1 clip4（4 槽 × 总资产 12.5%）。择强单轨是核心腿 / Settings 对照。
> **落地后**：条目迁 `optimization-checklist.md` OPT-128+；本文件迁 archive 或删。
> **还需要谁拍板**：无（方向已定）。每条 OPT 开独立 Agent，不扩 scope。

---

## 0. 冻结、不要再碰

下一阶段 **不**用新回测去推翻这些（除非三窗 >5pt 全过）：

| 冻结 | 原因 |
|------|------|
| `skip_t1_limit` + `pool_mode=strict` | 涨停不补；replace/fallback 卫星质量更差 |
| 4 × 12.5% NAV | 10×5% 拒收；3×16.5% 尾险上升 |
| body=3，无金字塔折进卫星 | 加仓会撞 15% SIZE_CAP，和低波桶冲突 |
| 回测成交 = T 开盘 | 14:30 买到 ≠ 回测开盘价 |
| body=3 收盘卖，无 −5% | 2026-09-03 三窗：protect5 / trail-after-body 全拒；Live 对齐冻结腿 |
| window-local 空簿 vs 连续簿 | 两套表不许混读 |
| past_year 不当拒收闸 | 闸仍是 OOS2/train/valid |

优化对象是 **把这套已经赢的配方跑成可执行、可核对、可解释的产品**，不是再找一个更猛的卫星。

---

## 1. 工程（live = 配方）

目标：Watchlist 当天做的事，和 Timeline `strategy=twin_star` 同一套规则、同一套数字。

| # | 做什么 | 为什么 | 入口 |
|---|--------|--------|------|
| E1 | 策略模式默认 `twin_star`（本轮已做） | 空 localStorage / 通知 API 无 `mode` 都走 clip4 | `strategy-settings.ts` · `GET /api/notifications` |
| E2 [done] 2026-09-02 | 卫星占用真值 = Watchlist 仓，不是引擎 `openPositions` | 引擎回放可到 15 只旧槽；实盘只有 4 槽 | OPT-131 · `twin_star_daily.py` `liveHoldings` |
| E3 [done] 2026-09-02 | 14:30 名单 / 12:30 快照失败可见 | 默认策略后，东财快照挂了等于当天没卫星 | OPT-133 · `intraday_snapshot_status` · 通知 `lane=system` |
| E4 [done] 2026-09-02 | `GET /api/backtest/twin-star/action` 进 `@karios/shared` Zod | 前后端字段再漂一次就会买错 12.5% | OPT-134 · `TwinStarActionResponseSchema` · `clip4` |
| E5 [done] 2026-09-02 | 卫星 paper 簿（入/出/body 日），不要复用 S-3 paper | C4 对照现在只服务股票篮 | OPT-131 · `paper_trades` source=`twin_star` |
| E6 [done] 2026-09-02 | 核心腿 ETF 日线 + `stock_dailybasic` 新鲜度当双子星健康项 | OPT-057/058 已修过静默腐烂 | OPT-133 · `/api/health/datasources` |

**不做**：把 14:30 成交价写进回测当开盘价；服务端再存一份 strategyMode（单用户桌面，localStorage 够）。

## 2. 业务对齐（Watchlist 就是驾驶舱）

目标：打开 Watchlist，只看到双子星该做的事；S-3 只在 pick=STOCK 时出现。

| # | 做什么 | 为什么 |
|---|--------|--------|
| B1 [done] 2026-09-02 | 归因对照 / 对齐横幅从「单轨 vs 账本」改成「双子星 vs 账本」（核心% + 卫星 4 槽） | OPT-129 |
| B2 [done] 2026-09-02 | pick≠STOCK：CN 持仓全部卫星；到期卖 / −5% 保护止损 / 复制止损单 | OPT-135 · 默认路径，不是 opt-in |
| B3 [done] 2026-09-02 | pick=STOCK：卫星 recipe 与 S-3 篮拆开，禁止金字塔/移动止损贴到卫星名 | OPT-135 · Health + 通知按标的拆账 |
| B4 [done] 2026-09-02 | QuickBuy 默认 12.5% NAV，不预填 S-3 10% | OPT-129 · 表行 `SAT_SLOT_NAV_PCT`；Health 走 trade plan |
| B5 [done] 2026-09-02 | 日流程写进 Watchlist：14:20 提醒 → 14:30 出名单 → 先调核心再买卖卫星 | OPT-135 · Health「今日顺序」 |

**不做**：自动下单 / 券商 API（仍手动）。

## 3. 回测可分析（能回答「今天为什么这样」）

目标：回测页默认双子星，拆得开核心 / 卫星 / 涨停跳过，而不是一根 NAV。

| # | 做什么 | 为什么 |
|---|--------|--------|
| A1 [done] 2026-09-02 | Timeline 叠加：twin / 核心 / 卫星 NAV + `satActive` 阴影 + 开闸占用只数 | OPT-130 |
| A2 [done] 2026-09-02 | 每日表增加：strict 候选数、涨停跳过数、实际成交槽、空槽回核 | OPT-132 |
| A3 [done] 2026-09-02 | 窗口标签：三窗 / 产品过去一年 / trailing / holdout 只读 | OPT-130 · 展示窗不当拒收闸 |
| A4 [done] 2026-09-02 | 卫星成交 blotter：symbol、振幅名次、是否 skip_t1、body 出日、贡献 pt | OPT-132 |
| A5 [done] 2026-09-02 | C4 双子星占用对照 + 停用 S-3 缺 19 只当交易提醒 | OPT-135 · 统计 C4 仍等 S-3 20 笔；双子星用你卫星仓 vs 引擎模拟 |

**不做**：在 Timeline 里再挂 5 档 clip 网格；holdout 未满窗不调参。

## 4. 建议顺序（约 2 周注意力）

```text
本轮已做  E1 默认 twin_star
第 1 刀   B1+B4  Watchlist 文案/仓位与 clip4 对齐（当天能用）[done] 2026-09-02 OPT-129
第 2 刀   A1+A3  Timeline 能拆核心/卫星/窗口（能解释）[done] 2026-09-02 OPT-130
第 3 刀   E2+E5  占用真值 + 卫星 paper（能对照）[done] 2026-09-02 OPT-131
第 4 刀   A2+A4  跳过/成交 blotter（能审计涨停）[done] 2026-09-02 OPT-132
第 5 刀   E3+E6  快照/日线健康（默认策略不能哑火）[done] 2026-09-02 OPT-133
第 6 刀   E4 Zod + clip4 字面量（买错 12.5% 会拒收）[done] 2026-09-02 OPT-134
第 7 刀   B2/B3/B5 日流程 + A5 占用对照 / 停用 S-3 recon 铃 [done] 2026-09-02 OPT-135
下一刀    运营观察（任意交易日三句验收）· 并行不抢 OPT-124 tushare 多 token
```

验收口令：任意交易日能回答这三句——「核心该持什么、卫星 4 槽里有谁、有谁因涨停没买」——Watchlist 与 Timeline 答案一致。
