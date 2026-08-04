# TIP-009 · Alpha 映射质量抽检 + 错映射惩罚（数据驱动 · 用户零操作版本）

> **完成日期**：2026-08-04  
> **关联 todo**：[§2 收益 · P2 Alpha 映射质量抽检](../todo.md) + [§3 业务规则 · TIP-009](../trading-improvement-checklist.md)  
> **用户工作流前提**：日常只做 Dashboard "Sync and Copy" → 把 markdown 喂给外部 AI agent → AI 决策 BUY/ADD → 写回 Watchlist。

## 1. 问题与重做方案

原 TIP-009 计划让用户做"周抽检 / 周一标 ✓/✗/?"，与用户实际工作流（**只点 Sync + Copy**）冲突。重做后**全自动、零人工反馈**。

## 2. 5 类自动 penalty 信号（全部从已有数据算）

| # | 信号 | penalty | 数据源 |
|---|------|---------|--------|
| 1 | **行业不匹配** | **0.6**（最强）| `data/seed/theme_industry_map.json` + `stock_eastmoney_industry` |
| 2 | **历史胜率低** | **0.5** | `paper_trades` 30D 命中/总数（macroTheme 维度）|
| 3 | **名称歧义** | **0.4** | `_lookup_by_name` 候选 top1/top2 字符前缀/子串重合 |
| 4 | **板块资金流背离** | **0.3** | `industry_fund_flow` 5D 净流出 vs trend 暗示方向 |
| 5 | **个股资金流背离** | **0.2** | `fund_flow` 5D 净流出 vs trend 暗示方向 |

penalty = `max(signal1, signal2, ..., signal5)`（不累加，避免过度惩罚）。

## 3. 数据驱动映射（不是硬编码种子词）

`scripts/build_theme_industry_map.py` 从历史 `alpha_radar_trends` 跑一次（季度更新即可）：

- 统计每个 `macro_theme` 映射股票的真实行业分布（join `stock_eastmoney_industry`）
- 阈值 ≥70% cnSymbols 落在某 EM 板块 → 该主题加入 `themes`
- 主题未达阈值或样本不足（<3 映射）→ 进 `unmapped_themes`，penalty=0（无信号）
- 当前 90 天样本 → **11 个主题** 覆盖（光通信 / AI 芯片 / 存储 / 半导体设备等）

**用户说"种子词不关心"** → 不写 LLM 写死的种子词表，让历史数据自己说话。

## 4. 用户工作流（**仍是 2 步**）

```
Sync → Copy (markdown) → 给外部 AI agent
                          ↓
                    AI 决策 BUY/ADD
                          ↓
                  写回 Watchlist
```

用户日常**只**做 Sync + Copy。**0 增量操作**。

## 5. Copy markdown 增强（被动可见）

Copy 末尾多 2 section（API `GET /api/alpha-radar/auto-qa-stats` 返回）：

```markdown
## Alpha Radar · Auto-QA
- sinceDays: 7
- themesCovered: 11

### ⚠ Mapping warnings
| Trend | Symbol | Industry | Expected | Penalty |
| --- | --- | --- | --- | ---: |
| HBM 涨价 | CN:600036 招商银行 | 银行 | 半导体 | 60% |

### Theme historical win-rate (paper-trading)
| Macro Theme | Wins / Total | Win Rate |
| --- | ---: | ---: |
| 某某概念 | 1 / 6 | 17% |
```

**外部 AI agent** 看到错映射警告 + 主题胜率 → 决策 BUY/ADD 时自带上下文。

## 6. 实现位置

| 层 | 文件 | 改动 |
|----|------|------|
| Script | `scripts/build_theme_industry_map.py`（new）| 历史 → JSON 种子 |
| Service（new）| `service/alpha_radar_qa.py` | 5 信号综合 + `get_auto_qa_stats` + `name_search_is_ambiguous` |
| Service | `service/alpha_radar_catalyst.py` | `list_catalyst_stocks` 输出加 `autoQaPenalty` / `adjustedCatalystScore` / `themeHistoricalWinRate` 字段 |
| Service | `service/alpha_radar_symbol_resolve.py` | `_lookup_by_name` 加歧义检测（confidence 0.75 → 0.55；rationale 加 "(ambiguous)"；resolved dict 加 `ambiguous: True`）|
| Service | `service/watchlist_automation.py` | `compute_alpha_additions` 应用 penalty；增加 `auto_qa_penalty` 拒因 |
| API | `api/alpha_radar_routes.py` | `GET /api/alpha-radar/auto-qa-stats?sinceDays=7&limit=20` |
| UI | `apps/desktop-ui/src/lib/alpha-radar-catalyst.ts` | `fetchAutoQaStats` + `buildAutoQaMarkdown` + `formatCatalystStockSummaryLine` 加 `⚠QA -X%` 标 |
| UI | `apps/desktop-ui/src/lib/dashboard-export.ts` | Copy markdown 末尾插入 auto-qa section |
| Docs | `docs/modules/alpha-incubator.md` | 加 §auto-qa-rules 节（机器规则，非模板）|
| Tests | `tests/test_alpha_radar_qa.py`（new）| 12 单测（歧义 / 5 信号 / win rate / stats）|
| Tests | `alpha-radar-catalyst.test.ts` | +5 单测（QA flag + auto_qa markdown）|
| Seed | `data/seed/theme_industry_map.json` | 季度跑脚本更新 |

## 7. 验证

| 项 | 结果 |
|----|------|
| backend pytest | **1274 passed**, 3 skipped（pre-existing） |
| backend 新增单测 | 12 passed（alpha_radar_qa） |
| backend 改动相关单测 | 31 passed（symbol_resolve + catalyst + watchlist_automation） |
| frontend typecheck | clean |
| frontend lint | 0 errors（27 pre-existing warnings）|
| frontend test | **440 passed**, 1 skipped（+5 新增）|
| theme_industry_map 实跑 | 90d 数据 → 11 主题 / 5326 bytes |
| get_auto_qa_stats 端到端 | `themesCovered: 11` 返回正确；当前 7d 趋势不在 seed → 0 penalties（正确：覆盖仅 7.9% 时不误杀）|

## 8. 不做的事（明确）

- ❌ 不加 UI ✓/✗/? 反馈按钮
- ❌ 不加 `alpha_radar_mapping_feedback` 表
- ❌ 不写周抽检 markdown 模板（用户私人流程）
- ❌ 不写 LLM 写死的种子词（数据驱动）
- ❌ 不动 LLM prompt
- ❌ 不引入外部 catalog
- ❌ 不让用户做任何额外操作

## 9. 后续观察项

- **季度跑** `build_theme_industry_map.py` 更新种子（建议 30/90/180 天三次回测，看覆盖率）
- **覆盖率 <30% 是预期**：LLM 主题不停涌现，seed 永远滞后。penalty=0 是 graceful default
- **theme win rate** 待 paper-trading 跑 N 周后才稳定（当前 0 low-win theme 命中是因为样本不足）
- 后续可考虑加板块 + 个股资金流背离信号（信号 4 / 5）—— 当前预留接口未启用