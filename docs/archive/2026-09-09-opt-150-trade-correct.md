# opt-150-trade-correct · 归档于 2026-09-09

> 从 `docs/optimization-checklist.md` 原文迁移（只读快照，不回写）。正文 0 条未完成（另有 3 条冬眠：OPT-045/075/127，见正文冬眠区）。

---

### OPT-150：账本修正机制——PATCH 改腿 + 审计卡一键挪链（用户 2026-09-09）

**状态**：[x] 2026-09-09（journal append-only 无修正口；整数倍仓位易记错腿）

**落地**：
- `db/user_trades.py::update_trade`（仅 leg/positionPct/note；side/symbol/date 不可变）+ `latest_buy_leg()`。
- `api/user_trades_routes.py::PATCH /trades/{id}`（400/404；SELL pnl 不重算——pnl 只与价有关）。
- SELL/ADD 省略 leg 自动继承开仓腿；pre-0041 库回退 s3。
- `core_holding_audit.py`：全部 verdict 带 op id（UI 定位行）。
- `packages/shared`：`UserTradePatchSchema` + 单测（81 passed，build 过）。
- UI：`QuickBuyDialog` 账本二选（12.5% 预选卫星；SELL 不选走继承）；
  `confirmBuy` 透传；`CoreAuditCard` 腿徽 + 整条链"挪到卫星/核心"（按 id PATCH + 刷三查询）。
- 实战：用户 09-09 14:30 的 8 行（旧代码落成 s3）已翻 sat；09-07 整批 BUY ok + SELL 到期 ok。

**验证**：后端 30+16 passed，CN:99 零残留；前端全套 880 passed，tsc 0；审计实跑符合预期
（含 600540 账外单 warn、今日票 paper 未落袋前 warn——17:43 后转 ok，属设计）。

**不做（记账）**：重复行去重（300413 SELL×6 等，另起 OPT）；SELL pnl 重算。
