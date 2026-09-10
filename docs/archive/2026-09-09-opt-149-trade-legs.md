# opt-149-trade-legs · 归档于 2026-09-09

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文 0 条未完成（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-149：user_trades 腿拆分 sat/s3 + 审计按腿分支（用户报障 2026-09-09）

**状态**：[x] 2026-09-09（报障：回测页 CoreAuditCard 用 S-3 尺子量卫星单——金字塔线无意义 + panic 误报）

**落地**：
- `alembic/versions/0041_user_trades_leg.py`：`leg TEXT NOT NULL DEFAULT 's3'` + CHECK；`upgrade head` 通过。
- `db/user_trades.py`：CREATE_SQL 同步 + `insert_trade(leg)` 校验 + `latest_buy_leg()`（SELL/ADD 省略时继承开仓腿，pre-0041 库回退 s3）。
- `api/user_trades_routes.py`：`leg` 入参（sat|s3，非法 400；SELL/ADD 省略自动继承）。
- `service/core_holding_audit.py`：sat 分支——BUY 对 paper 双子星账（同票同日=跟随信号），SELL 对 body=3 到期日（entry+2 交易日，SSE 日历），ADD 恒 warn；sat holding 藏金字塔线、带 leg。
- `packages/shared`：`UserTradeLegSchema` + request/stored 可选字段 + 单测（79 passed，build 过）。
- UI：`QuickBuyDialog` 账本二选（12.5% 预选卫星）；`confirmBuy` 透传（SELL 省略走继承）；审计卡腿徽（卫星/核心）。
- 回填：24 行 12.5% CN（09-02 进×8、09-04 卖×12、09-07 进×4）标 sat；其余 15 行 s3。

**验证**：后端 27+43 passed（含新单测 11），CN:99 零残留；前端 PortfolioHealthCard 31 + addflow/userTrades 9 passed，tsc 0；审计实跑 09-07 四票全 ok、无 panic 误报（600540 账外单 2 warn 符合设计）。

**不做（记账）**：300413 SELL×6 等重复行去重（疑 UI 双记，另起 OPT）；expectancy board 按腿拆分（候选）。
