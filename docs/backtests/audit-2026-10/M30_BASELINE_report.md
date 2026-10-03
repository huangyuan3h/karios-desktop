# M30 基线切换报告（Yuan 2026-10-03 19:44 决策执行，只读研究）

## 1. 改了什么
- `~/Projects/wealth-ideas/STRATEGY_LEDGER.md`：硬标准基线切为母港M30（70%港湾 S-3 picks + 30% B3 5-ETF inverse-vol，月频再平衡；COMPARE同尺子 M30 = long +152.6% / OOS2 +42.4% / valid +35.0% / holdout -8.8%）；新硬规则：候选需在 long/valid/holdout 三窗收益全超 M30 才算找到；星舰B降为观察（重启用需 rolling-40-trade sum转正 AND 月度大盘P&L转正）；已测清单每行加 vs M30 差值列（COMPARE/STARB_FILL/STARB_HISTORY/NEWLEGS数字；历史非同尺子行注明未同尺子、不计）。
- `~/Projects/karios-desktop`（分支 feat/fleet-strategy，未合并未推送）：基线文案 星舰B→M30 —— 回测页 Timeline 策略说明/表注（homeport_m30=当前基线，starship_b=观察+复活条件）、因子冷库规则行与 m30/starship_b 来源标注、舰队 tab 标题/图例（星舰B（观察））/复活闸说明、策略目录旗标（母港=基线·M30，星舰B=观察·待复活+复活条件，舰队 cons 基线口径）、策略模式标签/描述（homeport=基线·M30，starship_b=观察·待复活）。星舰B曲线保留可见（标观察）；随机/沪深300对照线保留（回测页基准对比与 placebo 逻辑未动）。
- 回测页新增基线卡（`M30BaselineCard`，data-testid="m30-baseline-card"，只读不下单）：见 §3。

## 2. 在哪里看（pnpm dev → localhost:3000 → 回测）
- `pnpm dev`（仓库根）→ 浏览器 localhost:3000 → 左侧「回测」→ 默认「策略总览」tab 顶部即基线卡；同一页 Timeline 切「母港」看 M30 防守档曲线（对比页 Timeline 同源）；「因子冷库」看统一体检表（M30=基线标注，星舰B=冷库观察）；「舰队」看四窗表+净值图（星舰B（观察）线+沪深300线保留）。

## 3. M30 下次再平衡目标（120万，只读，不下单）
- 结构：港湾 84万（70%）＋ B3 36万（30%），月首首个交易日再平衡，5bp/边，单腿偏离超5pt才动，留1%现金防费。
- B3 36万（2026-09-01 60d逆波权重，`services/data-sync-service/data/etf/etf_daily.csv` close_adj 实算）：511260十年国债 31.14万（86.5%）· 510300沪深300 1.37万（3.8%）· 518880黄金 1.33万（3.7%）· 513100纳指 1.19万（3.3%）· 510500中证500 0.94万（2.6%）。
- 港湾 84万：S-3股票 0 ＋ 停车 OIL 513350 84万（2026-08-10~09-30 holdout S-3 0 closed、idle100%，H2d §3.5；08-19~09-30重仓OIL，trail8，09-24 REPO一次，回落即回OIL）。
- 上次再平衡：2026-09-01（9月首个交易日，B3权重锚）；下次：2026-10首个交易日（国庆后，T收→T+1开执行）。
- 月5分钟清单（SOLUTION_report式）：1看NAV＋两腿贡献＋超300（并列long/valid/holdout）· 2看S-3 fills/胜率＋停车换仓＋B3漂移 · 3看成本（实现滑点 vs 5bp/边） · 4看数据日期＋ETF快照健康 · 5看星舰复活灯（rolling40 sum转正 AND 月度大盘P&L转正，再叠paper20＋holdout收复＋过滤版valid转正）再决定星舰是否0→10→20%，否则保持0%。

## 4. 测试结果
- 后端 pytest（all）：4454 passed、3 skipped、4 failed——4个全是 alembic 缺 revision（`No such revision 0056_user_trades_mode_starship_b`，本分支 checkout 缺该 migration 文件，未碰 DB/迁移，与本次改动无关，改前即坏）。
- 前端 vitest（desktop-ui 全量）：947 passed、1 skipped、2 failed——2个是 watchlist 旧失败（PortfolioHealthCard/TodayTodoCard，在干净树上同样失败，与本次改动无关）；本次改动涉及的 6 个文件 45/45 通过（含新增 M30 基线卡测试）；StrategyModeBar 标签测试已随基线改名更新（防守→基线）。
- shared 包：97 passed。typecheck（tsc --noEmit）：干净。build（next build）：成功（仅 themeColor viewport 旧警告，与本次无关）。
- 审计 index/README：karios-audit-2026-10 下无 index/README 文件（仅各冻结报告），冻结报告正文未改（KARIOS … DONE 保持原样）；此前报告中的 “Live=港湾 / 基线=星舰B” 语句自 2026-10-03 19:44 起被本决策取代：当前状态为基线 M30、星舰B观察、live 决策由 Yuan 定，特此声明。
- profit-leads：本次未向 `~/Projects/wealth-ideas/profit-leads.md` 追加（本轮无新“扣费后为正＋分位≥95%＋跨窗一致”发现；M30 为已知 PASS 底仓、星舰B为已知观察、DIV_LV1 为条件观察，均按 H2d/NEWLEGS先例不追加；来源本次 M30 基线切换，特此声明无新增）。

KARIOS M30 BASELINE DONE
