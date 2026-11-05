# Harbor trail / SELL_TO_REPO timely — 2026-11-05

Branch: `feat/watchlist-follow-m30` · unattended run · **no TRAIL_PCT / clock change**
(`close signal → next open execute` unchanged).

Commit: **80aab82f** — `feat(harbor): pending trail overrides 今日操作 + 14:30 今日调整 Bark`
(+ **87d5af80** — `fix(harbor): 14:30 trail warning falls back to the latest intraday print`)

**Status: HARBOUR TRAIL TIMELY DONE** — live API returns the pending trail, the
Watchlist 今日操作 first row is the Must-act sell → 逆回购, Bark「今日调整」covers
homeport/harbor (and appends the harbor leg in satellite modes). No push to the
remote, no TRAIL_PCT / clock change.

## TL;DR

The 08:19 screenshot had **two** independent root causes, both now fixed:

1. **Ghost leg**: `build_multi_asset_sleeve` treated **any registry row** as a held
   parking leg, including `ETF:513110` at **0%** (a watchlist row). The sleeve
   therefore evaluated the 513110 leg (no `entryDate` → no trail) and printed
   `轮动 513110→513350` — the real **63.5% OIL** leg and its trail were never
   looked at.
2. **Morning hole**: `_etf_trail_exit` only read `d == day`. In the morning (no
   today bar) `cur_close` stayed 0 → `None` → HOLD. It now signals from the
   **latest completed close ≤ day** and is **sticky** until the leg leaves the
   holdings.

Both are backend; the Watchlist only renders what the API says. On the live
registry the sleeve now returns exactly the expected state:

```
action=SELL_TO_REPO  symbol=ETF:513350  pendingOpenSell=True
signalDate=2026-11-03  executeHint=待开盘卖出转逆回购（信号已触发，未执行）
message=ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03，待开盘执行）
```

Verified by replaying `daily_adjust.load_live_sleeve("2026-11-05")` against the
live Docker Postgres (read-only) after the fix.

## What changed

### A) Pending / sticky trail (backend · `service/multi_asset_sleeve.py`)

- `_etf_trail_exit`: evaluates the **first breached completed close** since the
  entry reference (session before `entryDate`, engine parity kept) and returns it
  every day until the leg is sold.
  - new fields: `symbol`, `trailExit`, `pendingOpenSell`, `signalDate`,
    `executeHint`.
  - `pendingOpenSell=True` when the signal close predates `day` (morning /
    unexecuted); message then says `待开盘执行`.
  - no look-ahead: series is cut at `day` (`_series(as_of=day)`).
- `build_multi_asset_sleeve`: **held = parking ETF with positionPct > 0 only**
  (fixes the 0% ghost leg). MA200-wipe `SELL_TO_REPO` and `ROTATE`/`BUY` also
  carry `symbol`/`signalDate`/`executeHint`.
- New `parking_trail_levels` / `intraday_trail_watch` / `latest_1430_print` /
  `intraday_trail_watch_for_day` for the 14:30 early warning (see below).

### B) Watchlist 今日操作 / 操作引导 / 停车体检 (UI)

- `lib/m30-follow.ts`
  - `M30PendingParkSell` + `pendingParkSellFromSleeve()` (reads
    `multiAssetSleeve`, no trail math duplicated in TS).
  - `buildM30Targets({ pendingParkSell })`: harbor park target = **0 (逆回购)**
    for the exiting leg, proceeds counted as cash target, **every BUY on that
    symbol suppressed** (including merged B3+park lines — only the park share is
    sold, the B3 share target stays), action `mustAct` sorted **first**.
  - `M30IntradayWarn` + `intradayTrailWarnFromSleeve()` and
    `parkBuyBlockedSymbol` (14:30 warning blocks adds, never creates a sell).
- `PortfolioHealthCard.tsx`
  - `今日操作`: red **Must-act** banner (`⛔ 停车腿止损待执行：卖出 513350 → 逆回购`)
    + first row `卖 513350 → 逆回购`, `data-must-act="true"`, exec badge
    `待开盘卖出转逆回购`; Must-act rows bypass the rebalance cadence gate (a stop
    is not a rebalance) and are excluded from the `今天无需操作` path.
  - `操作引导`: first step is the SELL → repo (`待卖 · 513350 → 逆回购` chip),
    warning branch for the 14:30 state.
  - headline now reads `停车止损 1 笔（先卖）` instead of a buy-only summary.
- `MultiAssetHealthBlock.tsx` (停车体检)
  - red `⛔ 断线止损待执行：卖出 513350 → 逆回购` block, chip `待卖：513350`,
    per-row `待卖 · 止损` / `待卖` badge (no `持有`/`加仓` on that leg).
  - 14:30 warn block `⚠ 14:30 现价已达卖出条件 … 门槛价 …`
- `M30FollowBlock.tsx` / `TodayTodoCard.tsx`: consume the same sleeve exit
  (Must-act row / todo item), so no secondary surface can suggest a buy.

### C) Bark

- `service/daily_adjust.py` (new): `compose_daily_adjust(mode, …)` — one payload
  per selected mode. `harbor`/`homeport` compose from the live sleeve
  (pending exit → `卖出 X → 逆回购`; ROTATE/BUY/HOLD/REPO → their lines;
  nothing → explicit `今日无需调整`). **Satellite modes also append the live
  Harbor parking leg** (`港湾停车腿 · …`) whenever it carries an action — a mode
  switch must never hide the default book's trail (`selected_strategy_mode()`
  is currently `starship_b`, so this path is the one that will fire today).
  Also `emit_trail_if_active(day)`.
- `scheduler/satellite_live_job.py` `_emit_action` no longer returns early for
  non-satellite modes: **every trading day 14:30** (cron `30,33,37 14 * * mon-fri`,
  Asia/Shanghai) it emits `satellite_action` with `dailyAdjust=true`, dedupe
  `satellite_action:{tradeDate}:{mode}`. Runs even when the satellite panel is
  incomplete or the job failed (`insert_record` failure path kept;
  `satellite_live_panel` added to `HIGH_JOB_TYPES`).
- Trail fire same evening: `sleeve_paper_job` (18:20) calls
  `emit_trail_if_active(day)` → event `satellite_action` with `trailAlert=true`,
  dedupe `harbor_trail:{day}:{symbol}:SELL_TO_REPO`.
- `format_bark`: `🛑 {label}止损 · date` (trailAlert) and
  `📋 {label} · 今日调整 {date}` (dailyAdjust, `✅` when no change).
- Bark subscription: **reused `satellite_action`** (sub 280 already includes it)
  → no subscription write needed, no new event type to expand.

### 14:30 intraday early warning (steer)

`build_multi_asset_sleeve` adds `intradayWarn` when today's **14:30 print**
(`bar_5min` `source=live_1430`) is `≤ threshold` (`peak × (1 − 8%)`, raw basis,
same as the daily series basis) or within **1pt above** it (`near`):

```
{active, status: breached|near, px, thresholdPx, peak, drawdownPct, gapPct,
 message: "…现价X ≤ 卖出阈值价Y（峰值… 回撤…）→ 现价已达卖出条件",
 executeHint: "若收盘仍低于阈值价 → 明日开盘卖出转逆回购（不盘中卖）"}
```

It **never changes `action`** (close still decides) and it **blocks adds** on
that symbol in the UI. The 14:30 Bark gets the same line (`mustWatch: true`,
conditional adjustment), so a warned day is never silent and never shows 买油.

Price source order (steer: 当时价 **或** 最新价), DB-only, never network:
1. `live_1430` snapshot row for the day (written by the 14:30 panel job **before**
   it composes/pushes — so the 14:30 push always has it for quoted symbols);
2. fallback: the **latest stored intraday print** of the day (`bar_5min`, any
   slot) when the `live_1430` row is missing for that symbol;
3. none → no warning (never guess). The payload carries `pxSource`
   (`live_1430` / `bar_5min_latest`) so the copy can be audited.

## Tests

Backend (`services/data-sync-service`, `.venv/bin/python -m pytest`, `-o addopts=""`):

- `tests/test_multi_asset_trail.py` — fire on close, sticky pending next morning
  **without today bar**, within band none, no look-ahead, raw fallback, engine
  peak parity, **0% ghost leg regression**, 14:30 warn (breached / near / calm),
  warn does not flip `action`, **latest-intraday-print fallback** (`pxSource`).
- `tests/test_daily_adjust.py` (new) — homeport pending trail → sell→repo;
  HOLD → `今日无需调整`; rotate; satellite exits; missing panel; intraday warn
  is conditional (`mustWatch`, `mustAct=false`).
- `tests/test_webhook_format.py` — `今日调整` / `🛑 港湾停车止损` Bark bodies.
- `tests/test_satellite_live.py` — 14:30 emit for homeport + dedupe.

Broader regression run: **240 passed** (multi_asset / portfolio_health /
satellite / sleeve paper auto+recon / parking / notifications / webhook).
Full check also run earlier: 226 passed.

Frontend (`apps/desktop-ui`):

- `npx tsc --noEmit` clean.
- `npx vitest run` → **114 files / 1003 tests passed, 1 skipped**.
- New cases: pending trail overrides target math (first row `卖 513350`,
  `data-must-act`, no `买 513350`, banner + `待卖`), 14:30 warn banner, B3-member
  park leg keeps its B3 share, `parkBuyBlockedSymbol` suppression,
  `pendingParkSellFromSleeve` / `intradayTrailWarnFromSleeve`.

## Verify (manual, ~1 min)

1. Hard refresh the Watchlist (`Cmd+Shift+R`) on **母港 M30**.
2. 今日操作 first row must be **卖 原油 ETF 513350（港湾停车）** with
   `（港湾停车止损 → 逆回购（…峰值回撤8.5%…））` and the badge
   `待开盘卖出转逆回购`; the red banner reads
   `⛔ 停车腿止损待执行：卖出 513350 → 逆回购 · 信号日 2026-11-03 收盘`.
3. No `买 513350` row anywhere; 停车体检 shows `待卖：513350`, `待卖 · 止损`.
4. API check (already confirmed after the restart):
   `curl -s 'http://127.0.0.1:4330/v1/agent/portfolio-health?markets=CN' | jq '.multiAssetSleeve | {action,symbol,pendingOpenSell,signalDate,executeHint}'`
   → `SELL_TO_REPO / ETF:513350 / true / 2026-11-03 / 待开盘卖出转逆回购…`
5. 14:30 job: `satellite_live_panel` (cron `30,33,37 14 * * mon-fri`);
   `sync_job_record` row per capture, `webhook_events` dedupe
   `satellite_action:{date}:homeport`. On a no-action day the Bark body is
   `今日无需调整` (never silence). Real body dry-run (read-only, prints exactly
   what `format_bark` pushes):
   ```bash
   cd services/data-sync-service && PYTHONPATH=src .venv/bin/python -c \
     "from data_sync_service.service.daily_adjust import compose_daily_adjust; \
      print(compose_daily_adjust('homeport'))"
   ```
   Today it renders: `📋 母港 M30 · 今日调整 2026.11.05` /
   `⚠️ 卖出：ETF:513350 → 逆回购（ETF:513350峰值回撤8.5% ≥8%）· 待开盘卖出转逆回购（信号已触发，未执行）（信号日 2026-11-03）`
6. Optional: `POST /sync/run/satellite_live_panel` (manual trigger) to push today's
   `今日调整` immediately.

## Ops / restart

- data-sync uvicorn was started 2026-11-04 13:19 inside `turbo dev`
  (`pnpm dev` → turbo, PIDs **61461** `uv run uvicorn`, **61483** uvicorn child,
  port 4330). No `--reload` → the new scheduler/service code required one restart.
- **What actually happened**: killing the uvicorn made `turbo dev` cancel its
  sibling tasks (Next :3000 / AI :4310 went down too, ~30 s), so the stack was
  relaunched as a whole (`pnpm dev`, nohup, log
  `.opencode-runs/pnpm-dev-2026-11-05.log`). It was relaunched twice more while
  iterating on the final code (each relaunch = the same cascade).
  - Killed: **61461** `uv run uvicorn` + **61483** uvicorn (started 2026-11-04
    13:19), **5548** (temporary standalone uvicorn, stopped before relaunch),
    **5673** / **7796** (earlier `pnpm dev` wrappers).
  - Intermediate relaunch (the one the steer saw): uv **6231**, uvicorn **6269**
    :4330, Next **6270** :3000, AI **6272** :4310 — replaced by the final code.
  - **Current (final) listeners**, Asia/Shanghai:
    - data-sync :4330 → uvicorn PID **10387**
    - Next :3000 → PID **10393**
    - AI :4310 → PID **10394** (Hono, `/` 404 is normal — no root route)
    - `pnpm dev` wrapper → PID **9795** (background, nohup)
  - Exactly one listener per port; nothing was killed a second time beyond the
    two relaunches above.
- Live verify (pasted, after the final relaunch):
  ```
  $ curl -s 'http://127.0.0.1:4330/v1/agent/portfolio-health?markets=CN'
  multiAssetSleeve = {action: SELL_TO_REPO, label: 卖出转repo(止损),
    symbol: ETF:513350, pendingOpenSell: true, signalDate: 2026-11-03,
    executeHint: 待开盘卖出转逆回购（信号已触发，未执行）, intradayWarn: null,
    idlePct: 0.0, parkPct: 100.0}
  message = ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03，待开盘执行）
  pick    = OIL ETF:513350 mom60 9.94
  multiAssetHoldings = [("ETF:513350", 63.51, action=SELL_TO_REPO, pendingOpenSell=true),
                        ("ETF:511260", 36.66, action=None)]
  ```
  Note the correct route is **`/v1/agent/portfolio-health`**;
  `/api/portfolio/health` 404s.
- The last patch (intraday-print fallback, `87d5af80`) is **not** in the running
  process (no further restart, per the steer). Live impact today: none — the
  14:30 job persists the `live_1430` snapshot **before** it composes/pushes, so
  the primary source is always present for that push. The fallback becomes
  active on the next natural restart.
- 14:30 dry-run with the real settings (`selected_strategy_mode()=starship_b`):
  title `📋 星舰 B · 今日调整 2026.11.05`, body includes
  `港湾停车腿 · 卖出：ETF:513350 → 逆回购（…峰值回撤8.5% ≥8%）· 待开盘卖出转逆回购（信号已触发，未执行）（信号日 2026-11-03）`.
  No push was sent by hand — the 14:30 job (`satellite_live_panel`) will send it
  today, and `sleeve_paper_auto` (18:20) re-emits the trail alert.

## Known limits / follow-ups

- `selected_strategy_mode()` currently returns **`starship_b`** (server-side
  setting; the Watchlist *view* is 母港 M30). The 14:30 push is therefore
  labelled 星舰 B, but the Harbor trail line is appended in every mode, so the
  sell instruction is present. Switching the stored mode back to `homeport` is a
  user decision (not changed here).
- `TRAIL_PCT`, M30 70/30, B3 math, harbor pick rule, intraday discretionary
  exits: **untouched**.
- B3 monthly lines still produce their own buys on a trail day (they are the
  30% leg, not the parking leg); only the exiting symbol is buy-blocked.
- The 14:30 warning needs a 14:30 print (`live_1430`); with no print the sleeve
  stays on the close basis (no false alarm).
- `sleeve_paper_auto` (paper book) now sees the same sticky exit — it is a no-op
  once the leg is closed (idempotent close by trade id).
- The worktree keeps unrelated pre-existing dirt (AccountBar / MobileShell /
  WatchlistPage / em_push2 / factor_vault / zt_pool / audit docs): **not staged,
  not reverted**.

## Post-DONE reload (Asia/Shanghai 2026-11-05 08:53)

To load `87d5af80` (latest-intraday-print fallback) into the live process, data-sync
was restarted once more. Killing only the uvicorn child briefly cascaded turbo
(Next/AI dropped); full `pnpm dev` relaunched and is healthy:

- `pnpm dev` wrapper PID **12304** (log `.opencode-runs/pnpm-dev-2026-11-05-reload87d5.log`)
- data-sync :4330 → PID **13055**
- Next :3000 → PID **13056** (`GET /` 200)
- AI :4310 → PID **13057**

Live re-verify `/v1/agent/portfolio-health?markets=CN`:
`action=SELL_TO_REPO, symbol=ETF:513350, pendingOpenSell=true, signalDate=2026-11-03`.
`intradayWarn=null` this morning (no today live_1430 / no intraday print yet — expected).
Fallback path is loaded (`pxSource` / `latest_intraday_print` in process code).
No push.
