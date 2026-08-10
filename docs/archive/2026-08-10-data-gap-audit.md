# 数据打通作战 · 全量盘点 + P0-1/P1-4 实施 · 归档于 2026-08-10

## 当时的目标（todo 链接）
- docs/todo.md §6「数据打通作战计划」（2026-08-10 立，用户拍板「先把数据打通，让回测更精准可预测」）
- P0-1 港股复权统一 · P1-4 A 股算分 job 修复（本归档覆盖两项；P0-2/P1-3/P2 项仍在 todo）

## 数据盘点基线（2026-08-10 实测 DB + 接口）

| 数据 | A 股 | 港股 |
|------|------|------|
| 个股日线 | 5760 只 · 2023-01 起 · 当日 ✅ | 2803 只 · 1998 起 · 当日 ✅（腾讯链） |
| 复权 | adj_factor 97% 填充 ✅（tushare 单一源） | **混源 ⚠️（本日修复）** |
| 指数 | index_daily 当日 ✅ | HSI/HSTECH 滞后 1 天 ⚠️ |
| 行业/资金流/情绪 | 当日 ✅（主线评分滞后 1 天） | 无（定案接受） |
| score 覆盖 | 当日 700+（修复前仅 204） | 当日 497（修复前仅 200） |

## 实际做了什么

### P0-1 港股复权统一（核心修复）
1. **实锤混源**：daily 表 HK 历史段 = tushare 不复权（首次回填）、近端 = 腾讯 qfq（每日增量）
   ——除权日跳空被 EMA/RS/RSI 误读为崩盘。抽查：01398 差 48%、00388 差 11%。
2. 新增 `scripts/hk_adj_consistency_check.py`：拉腾讯 qfq 全量 vs DB 逐日比对 → 定位混写断层。
3. 新增 `scripts/hk_reseed_qfq.py`：全量重灌 2804 只 2022-06-01 起 qfq（单次 640 根翻页），
   upsert 覆盖（ON CONFLICT DO UPDATE）；结果 **317.7 万行 · 0 失败**；16 只抽样全一致。
4. **重固化 HK 基线**（`run_walk_forward.py --market HK --save-baseline`）：
   OOS2 **+268.0%**/DD29.7/夏普2.21/344笔 · train +26.9%/18.9/1.91/169笔 · valid +60.6%/8.3/6.32/84笔
   （vs 旧基线：OOS2 +86.9→+268.0 即 +181pt；train -19.1pt = 旧基线不复权假信号，新基线为准）
5. 真值表同步：`docs/modules/strategy-params.md` §1b 版本历史 + §5 备忘 #8/#9。

### P1-4 A 股算分 job 修复（两个根因）
1. **一次性事件**：uvicorn 08-10 15:59 重启错失 17:30 cron（misfire 不补跑）→ 手动补算
   `run_watchlist_automation(force=True)` 完成 08-10（runId e47b9ab1/8ae2f824）。
2. **代码 bug**：`compute_trendok_for_symbols` 硬编码 `syms[:200]` 上限 → HK universe 500 只只算 200、
   CN screener 700+ 只算 204。修复：`record_score_snapshots` 改为 200/块分 chunk。
   验证：CN 204→**700** · HK 200→**497**（08-10 当日 score 齐全）。
3. 相关测试 291 passed（watchlist_automation/trendok/paper_s3 域）无回归。

## 验证 / 数据
- 复权校验脚本 16 只抽样：**all consistent**（含 01398/00388 修复前后对比）
- HK score 08-10：497/500 有分（3 只无分=停牌/数据不足，正常）
- `walk_forward_hk_baseline.json` 已重固化

## 后续影响 / 留给谁
- **P0-2**：恒指官网成分接口探索（vol top 500 代理偏差 ~5-10% 仍未解决）
- **P1-3**：主线评分/stock_dailybasic/USDCNH/HSI/HSTECH 滞后 1 天 + staleness 监控未做
- **P2-5**：A 股长历史（>2023）腾讯 fqkline 翻页方案未做（回测扩窗用）
- **P2-6**：HK amount NULL 历史洞未回补
- 观察：未来 3 个交易日 watchlist_automation cron（17:30）自动跑覆盖 ≥700 只验证
- 注意：**HK 日线每日增量必须保持腾讯 qfq 口径**——tushare 降级写入会重新引入混源断层
  （hk_daily.py 链路已把 tencent 置首，tushare 仅 last-resort）
