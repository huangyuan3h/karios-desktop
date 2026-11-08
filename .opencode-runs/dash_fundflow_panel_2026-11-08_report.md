# Dashboard「资金流全景」整幅面板 — 交付报告（2026-11-08）

任务授权：Yuan（2026-11-08）。范围：仅本机 OpenCode，无 Cursor CloudAgent。

## 1. 需求与结论

| 需求 | 落地方式 |
|------|----------|
| Dashboard 加一个**独立的**「资金流全景」大面板（不是把 `NationalTeamCard` 那一条 spark 放大） | 新增整幅块 `FundFlowPanoramaCard`，放在两栏卡片区**下方**（全宽，不走 `renderCard` 两栏系统） |
| 三张图与 `https://zhibiao.it-t.xyz/global/national-team/` 对齐：国家队宽基 ETF 份额 / 两融余额 / 北向资金 | 复用回测页 `FundFlowPanel` 的同一套图表代码，只渲染 `etf` / `margin` / `north` 三段 |
| 同数据、大图、可读 | 同一 `useFundFlowSeriesQuery`（`/api/backtest/flow-series`），单图高度 120px → **220px**；保留 6M/1Y/3Y 切换与默认 1Y |
| 保留 `NationalTeamCard` 摘要 | 卡片保留不动，仅把「看全图」按钮从"跳回测页"改为**本页平滑滚动**到新面板 |
| Watchlist 不得出现资金流图 | 未触碰 Watchlist；新增测试断言 `WatchlistPage.tsx` 不含 `FundFlowPanel` / `FundFlowPanoramaCard` |
| 外链只作上下文 | 面板右上角 `参考页：zhibiao · 国家队`（`target=_blank` + `rel=noreferrer noopener`），无交易/权重输入 |
| 优先抽取复用，最小改动面 | 只给 `FundFlowPanel` 加 4 个可选 props，未重写图表逻辑 |

## 2. 改动文件

| 文件 | 类型 | 说明 |
|------|------|------|
| `apps/desktop-ui/src/components/pages/FundFlowPanel.tsx` | 改 | 新增可选 props：`sections`（子集渲染，默认全 5 段）、`chartHeight`（默认 120，保持回测页不变）、`span`（无 Timeline 时默认跨度，默认 `1y`）、`embedded`（去掉外框，供卡片内嵌）。`MiniChart`/`SectionHeader` 增加 `hidden` 早退，渲染顺序仍固定 |
| `apps/desktop-ui/src/components/dashboard/FundFlowPanoramaCard.tsx` | 新 | Dashboard 整幅区块：标题 + 口径副标题 + 只读标记 + 参考页外链 + 内嵌三图 |
| `apps/desktop-ui/src/components/pages/DashboardPage.tsx` | 改 | 引入并在两栏 `})()}` 之后挂 `<FundFlowPanoramaCard className="mt-4" />`；新增 `scrollToFundFlowPanel`（`document.getElementById('dashboard-fund-flow')?.scrollIntoView`）传给 `NationalTeamCard` |
| `apps/desktop-ui/src/components/dashboard/NationalTeamCard.tsx` | 改 | 新增可选 `onOpenPanel`（优先于 `onOpenBacktest`，后者保留以兼容其它调用方）；按钮文案随 handler 切换 |
| `apps/desktop-ui/src/components/dashboard/FundFlowPanoramaCard.test.tsx` | 新 | 5 个用例：三图且只有三图、外链属性、不可用兜底、Dashboard 挂载位置（源码断言：位于两栏 grid 之后）、Watchlist 无资金流图 |
| `apps/desktop-ui/src/components/dashboard/NationalTeamCard.test.tsx` | 改 | 新增 1 个用例：`onOpenPanel` 点击回调 |

未改：后端 / 接口字段 / `flow-series` 查询 / 交易策略 / TRAIL / cooldown / env。

## 3. 行为细节

- **不新增接口**：Dashboard 与回测页共用 `GET /api/backtest/flow-series?start&end`（`staleTime` 5 分钟，同 queryKey 命中后不会重复请求）。
- **口径与回测页完全一致**：国家队 = 4 只宽基份额合计（含红带撤退态 B 闸）、两融 = 全市场余额（仅完整日）、北向 = 2024-08-19 后为成交额口径（标题与 footer 保留原提示，不混算）。
- **仍是显示层**：面板内保留「只读上下文 · 不影响交易」与原有「显示层 · 不影响交易」脚注，B 闸只展示。
- **回测页零回归**：`FundFlowPanel` 默认参数不变（5 段 / 120px / 带外框 / 默认 1Y），`BacktestPage.test.tsx` 23 个用例全绿。
- **移动端**：整幅块在两栏 grid 之后渲染，`lg:hidden` / `hidden lg:flex` 两套布局都会在其下方看到（全宽），不受栏宽限制。

## 4. 验证

```bash
cd apps/desktop-ui
npx tsc -p tsconfig.json --noEmit        # 0 error
npx eslint <5 个改动文件>                 # 0 error（仅 FundFlowPanel 既有 rawRows memo 警告）
npx vitest run src/components/dashboard src/components/pages   # 17 files / 111 tests pass
npx vitest run                                                # 118 files / 1033 tests pass（1 skip 既有）
```

- 前端 dev server：`:3000`（`next dev`，PID 87966）在跑，改动经热更新即可看到；页面 `GET /` 返回 200。若之后改成 `serve out` 的静态包，需要先 `pnpm -C apps/desktop-ui build`。
- `:4330` 未触碰；未重启任何后端进程。
- 本轮无后端/DB/Alembic 改动，无需迁移。

## 5. 约束遵守

- 只暂存本任务 6 个源码/测试文件 + 本报告，**未动**原有 dirty 文件（`AccountBar.tsx`、`MobileShell.tsx`、`WatchlistPage.tsx`、`docs/backtests/audit-2026-10/*`、`services/.../em_push2_http.py` 等）。
- 无策略 / 参数 / TRAIL / cooldown / env 改动；不改 Live 执行面。
- 本地提交（commit message：`feat(dashboard): dedicated 资金流全景 panel (ETF share / margin / north)`），**未 push**。

## 6. 后续可选（未做，避免扩大改动面）

1. 若希望 Dashboard 也能看到「三路叠加合计」与「散户小单」，只需在 `PANORAMA_SECTIONS` 增补（组件已支持子集）。
2. 若要把 `NationalTeamCard` 的 spark 也换成同源迷你图，可后续单独抽 `SparkLine`，本轮不必要。
