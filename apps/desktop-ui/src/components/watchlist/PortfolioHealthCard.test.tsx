import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ComponentProps } from 'react';

import { PortfolioHealthCard } from './PortfolioHealthCard';

const { fetchPortfolioHealth } = vi.hoisted(() => ({
  fetchPortfolioHealth: vi.fn(),
}));
vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth };
});

type QueryStub = { data: unknown; isError: boolean; isFetching: boolean };
const backtestMock = vi.hoisted(() => ({
  b3State: null as unknown,
  useBacktestReconQuery: vi.fn(
    (): QueryStub => ({
      data: { ok: true, items: [] },
      isError: false,
      isFetching: false,
    }),
  ),
  useSleeveReconQuery: vi.fn(
    (): QueryStub => ({
      data: undefined,
      isError: false,
      isFetching: false,
    }),
  ),
  useTimelineQuery: vi.fn(
    (): QueryStub => ({
      data: { rows: [] },
      isError: false,
      isFetching: false,
    }),
  ),
  useSatelliteLivePanelQuery: vi.fn(
    (): QueryStub => ({
      data: { ok: true, panel: null, stale: false },
      isError: false,
      isFetching: false,
    }),
  ),
}));
vi.mock('@/lib/queries/backtest', async () => {
  const actual =
    await vi.importActual<typeof import('@/lib/queries/backtest')>('@/lib/queries/backtest');
  return {
    ...actual,
    useBacktestReconQuery: backtestMock.useBacktestReconQuery,
    useB3StateQuery: () => ({ data: backtestMock.b3State }),
    useSleeveReconQuery: backtestMock.useSleeveReconQuery,
    useTimelineQuery: backtestMock.useTimelineQuery,
    useSatelliteLivePanelQuery: backtestMock.useSatelliteLivePanelQuery,
  };
});

const accountMock = vi.hoisted(() => ({ capital: 1_000_000 as number | null }));
vi.mock('@/lib/account-settings', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/account-settings')>();
  return { ...actual, useAccountSettings: () => ({ capital: accountMock.capital, rate: 0.92 }) };
});

const B3_OK = {
  ok: true,
  asOf: '2026-10-07',
  rebalanceDate: '2026-10-01',
  month: '2026-10',
  universe: [
    { symbol: '510300.SH', name: '沪深300', targetPct: 20, driftPct: 0, deltaPct: 0, px: 4.6 },
    { symbol: '510500.SH', name: '中证500', targetPct: 20, driftPct: 0, deltaPct: 0, px: 7.0 },
    { symbol: '518880.SH', name: '黄金', targetPct: 20, driftPct: 0, deltaPct: 0, px: 6.0 },
    { symbol: '513100.SH', name: '纳指100', targetPct: 20, driftPct: 0, deltaPct: 0, px: 1.6 },
    { symbol: '511260.SH', name: '10年国债', targetPct: 20, driftPct: 0, deltaPct: 0, px: 134.0 },
  ],
  trades: [],
  note: 'B3 腿内部权重',
};

const sentimentMock = vi.hoisted(() => ({
  useDashboardSentimentQuery: vi.fn(() => ({
    data: undefined,
    isError: false,
    dataUpdatedAt: 0,
    isFetching: false,
  })),
}));
vi.mock('@/lib/queries/sentiment', async () => {
  const actual =
    await vi.importActual<typeof import('@/lib/queries/sentiment')>('@/lib/queries/sentiment');
  return { ...actual, useDashboardSentimentQuery: sentimentMock.useDashboardSentimentQuery };
});

const HOLDING = {
  symbol: 'HK:00700',
  name: '腾讯控股',
  positionPct: 6.3,
  pnlPct: 0.6,
  drawdownFromPeakPct: -2.7,
  holdingDays: 11,
  stopLossLine: 452.2,
  trailingLine: 452.8,
  pyramidTriggerLine: 487.9,
  pyramidAdded: false,
  expireDate: '2026-09-27',
  action: 'HOLD',
};

/** Same Shanghai-weekend source of truth as the card: weekends relabel buys to view-only 候选. */
function shanghaiWeekendNow(): boolean {
  const wd = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
  }).format(new Date());
  return wd === '周六' || wd === '周日';
}

function renderCard(props: ComponentProps<typeof PortfolioHealthCard> = {}) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <PortfolioHealthCard {...props} />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  fetchPortfolioHealth.mockReset();
  backtestMock.b3State = B3_OK;
  accountMock.capital = 1_000_000;
  window.localStorage.clear();
  sentimentMock.useDashboardSentimentQuery.mockReturnValue({
    data: undefined,
    isError: false,
    dataUpdatedAt: 0,
    isFetching: false,
  });
  backtestMock.useSleeveReconQuery.mockReturnValue({
    data: undefined,
    isError: false,
    isFetching: false,
  });
  backtestMock.useSatelliteLivePanelQuery.mockReturnValue({
    data: { ok: true, panel: null, stale: false },
    isError: false,
    isFetching: false,
  });
});

describe('PortfolioHealthCard (harbor)', () => {
  it('renders the harbor decision header with market state and holdings', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '核心 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-07',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [HOLDING],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect(screen.queryByText('核心腿状态（港湾配方）')).toBeNull();
    expect(screen.queryByText(/机会双子星/)).toBeNull();
    expect(screen.queryByText(/单轨择优/)).toBeNull();
    expect(await screen.findByText('Weak · 空仓观望')).toBeDefined();
    expect(await screen.findByText('Strong · 进攻')).toBeDefined();
    expect(await screen.findByText('腾讯控股')).toBeDefined();
    expect(await screen.findByText('✅ 持有')).toBeDefined();
    expect(await screen.findByText('2026-08-07')).toBeDefined();
    const exp = screen.queryByText('展开');
    if (exp) fireEvent.click(exp);
    expect(screen.getByText(/今日无开仓候选（regime=Weak/)).toBeDefined();
  });

  it('keeps the recon gap visible (read-only) when the gate is closed', async () => {
    backtestMock.useBacktestReconQuery.mockReturnValue({
      data: {
        ok: true,
        items: [
          {
            reconDate: '2026-09-16',
            market: 'CN',
            expected: 5,
            actual: 3,
            aligned: 3,
            missing: 2,
            extra: 0,
            detail: [
              { type: 'missing', symbol: 'CN:600519', score: 80, positionPct: 0.1 },
            ],
          },
        ],
      },
      isError: false,
      isFetching: false,
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有',
        message: '弱市',
        pick: { key: 'REPO', mom60: 0, symbol: 'REPO' },
      },
      tradeDate: '2026-09-16',
      regime: 'Weak',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    // No expand click: the stock section stays open so gaps/EXITs survive 弱市.
    expect(await screen.findByText(/股票篮对账 · 2026-09-16/)).toBeDefined();
    expect(await screen.findByText(/缺 2/)).toBeDefined();
    // Gate closed: no 提醒买入 buttons anywhere, gaps are view-only.
    expect(screen.queryByRole('button', { name: /提醒买入/ })).toBeNull();
  });

  it('shows stock candidates with the core 10% sizing when pick=STOCK', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'BUY',
        label: '买入股票篮',
        message: '核心 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      panicCooldown: { active: false },
      s3Rules: { suggestedSizePct: 10, envScaleToday: 1 },
      s3Candidates: [
        { symbol: 'CN:600519', name: '贵州茅台', score: 82, rs: 0.93 },
        { symbol: 'CN:000001', name: '平安银行', score: 74, rs: 0.85 },
      ],
      holdings: [],
      hkHealth: { regime: 'Diverging', s3Candidates: [], holdings: [] },
    });
    renderCard();
    // BuyList title (the ops-panel guidance above mentions 股票篮买入 too,
    // so pin the BuyList-specific suffix).
    expect(await screen.findByText(/S-3 核心 · score 前 5/)).toBeDefined();
    expect(await screen.findByText(/每票建议 10%/)).toBeDefined();
    expect(screen.getAllByText('买 10%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('贵州茅台')).toBeDefined();
  });

  it('keeps core stock buys + parking when the idle cash is parked in an ETF', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '港湾停车：持有 513350（mom60 4.98%）',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'harbor',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      panicCooldown: { active: false },
      s3Candidates: [{ symbol: 'CN:600519', name: '贵州茅台', score: 82 }],
      s3Rules: { suggestedSizePct: 10, envScaleToday: 1 },
      holdings: [],
      hkHealth: null,
      multiAssetHoldings: [
        {
          symbol: 'ETF:513350',
          key: 'OIL',
          name: '原油 ETF',
          positionPct: 94,
          marketData: { close: 1.2, ma200: 1.0, above: true },
        },
      ],
    });
    renderCard();
    // Parking guidance: idle cash only, core untouched.
    expect(await screen.findByText(/停车只停闲钱/)).toBeDefined();
    // Harbor: an ETF parking pick must NOT disable the S-3 core buys.
    // (Weekends relabel the list to view-only 候选 — same assertion, both days.)
    if (shanghaiWeekendNow()) {
      expect(await screen.findByText(/周末 · 股票篮候选/)).toBeDefined();
    } else {
      expect(await screen.findByText(/股票篮买入/)).toBeDefined();
    }
    expect(screen.getByText('贵州茅台')).toBeDefined();
    expect(screen.queryByText(/应轮出/)).toBeNull();
    // Parking block must use the canonical key from the API (OPT-206).
    expect(await screen.findByText(/停车 OIL mom60/)).toBeDefined();
  });

  it('renders the core sleeve reconciliation when available', async () => {
    backtestMock.useSleeveReconQuery.mockReturnValue({
      data: {
        ok: true,
        recon: {
          day: '2026-09-01',
          ok: true,
          decisionAvailable: true,
          label: '卖出纳指 ETF 转向股票篮',
          action: 'ROTATE',
          expectedBuys: ['CN:600519'],
          expectedSells: ['ETF:513110'],
          paperBuysToday: ['CN:600519'],
          paperSellsToday: [],
          missedBuys: [],
          missedSells: ['ETF:513110'],
          extraOpens: [],
          userBuys: [],
          userSells: [],
          userAlignment: 'pending',
        },
      },
      isError: false,
      isFetching: false,
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'ROTATE',
        label: '轮动',
        message: '核心 ROTATE',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
      },
      tradeDate: '2026-09-01',
      regime: 'Diverging',
      panicCooldown: { active: false },
      s3Candidates: [{ symbol: 'CN:600519', name: '贵州茅台', score: 82 }],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/核心腿对账 · 2026-09-01/)).toBeDefined();
    expect(screen.getAllByText(/你的账户/).length).toBeGreaterThan(0);
    expect(
      screen.getByText(/待执行 · 下一交易日 开盘买 CN:600519/),
    ).toBeDefined();
    expect(screen.getByText(/镜像盘/)).toBeDefined();
    expect(screen.getByText(/缺卖 513110/)).toBeDefined();
  });

  it('shows a harbor error banner when the health endpoint fails', async () => {
    fetchPortfolioHealth.mockRejectedValue(new Error('boom'));
    renderCard();
    expect(await screen.findByText(/港湾暂无数据（data-sync-service 未响应）/)).toBeDefined();
  });

  it('renders the satellite leg from the starport timeline for starport mode', async () => {
    // B3 unavailable here so the leg block stays hidden, same as before the
    // b3-state mock (real request failed → null).
    backtestMock.b3State = undefined;
    renderCard({ mode: 'starport' });
    expect(await screen.findByTestId('satellite-leg-block')).toBeDefined();
    expect(backtestMock.useTimelineQuery).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      'starport',
      true,
    );
    expect(screen.queryByTestId('b3-leg-block')).toBeNull();
  });

  it('renders starport operation hints from the blended satellite payload', async () => {
    backtestMock.useTimelineQuery.mockReturnValue({
      data: {
        rows: [
          {
            date: '2026-09-14',
            satActive: true,
            satPositions: 4,
            satSlots: 4,
            satCapacity: 4,
            gateOpen: false,
          },
        ],
        summary: { fusedPct: 1 },
        satWeight: 0.2,
        satCapacity: 4,
        openPositions: [
          {
            ts: '300906.SZ',
            entryDate: '2026-09-11',
            entryPrice: 28.12,
            daysLeft: 1,
          },
        ],
      },
      isError: false,
      isFetching: false,
    });
    renderCard({ mode: 'starport' });
    expect(await screen.findByText(/策略参照（引擎 · 研究档）/)).toBeDefined();
    expect(screen.getByText(/星港：母港 0\.8 \+ 卫星 0\.2/)).toBeDefined();
    expect(screen.getByText(/14:30 到期卖出（余 1 日）：/)).toBeDefined();
  });

  it('prefers today’s 14:30 live panel over the replay day', async () => {
    backtestMock.useTimelineQuery.mockReturnValue({
      data: {
        rows: [
          {
            date: '2026-09-16',
            satActive: true,
            satPositions: 4,
            satSlots: 4,
            satCapacity: 4,
            gateOpen: true,
          },
        ],
        satWeight: 1,
        satCapacity: 4,
        openPositions: [],
      },
      isError: false,
      isFetching: false,
    });
    backtestMock.useSatelliteLivePanelQuery.mockReturnValue({
      data: {
        ok: true,
        stale: false,
        panel: {
          tradeDate: '2026-09-17',
          generatedAt: '2026-09-17T14:30:12+08:00',
          decisionAvailable: true,
          gateOpen: false,
          breadth1430: 0.28,
          gapCount: 34,
          bucketSize: 11,
          poolSize: 11,
          ranked: [],
          wouldFill: [],
        },
      },
      isError: false,
      isFetching: false,
    });
    renderCard({ mode: 'starship' });
    expect(await screen.findByTestId('satellite-live-panel')).toBeDefined();
    expect(screen.getByText('闸 关 · 今日只卖不买')).toBeDefined();
    expect(screen.getByText(/今日 2026-09-17 现场/)).toBeDefined();
  });
});

describe('PortfolioHealthCard M30 ops (homeport)', () => {
  const ROTATE_HEALTH = {
    tradeDate: '2026-10-08',
    regime: 'Strong',
    scoreFresh: true,
    circuitBlocked: false,
    panicCooldown: null,
    s3Candidates: [],
    holdings: [],
    hkHealth: { holdings: [] },
    multiAssetHoldings: [
      {
        symbol: 'ETF:511260',
        key: 'BOND10',
        name: '10年国债',
        positionPct: 69.3,
        marketData: { close: 134, ma200: 130, above: true },
      },
      {
        symbol: 'ETF:513100',
        key: 'NASDAQ',
        name: '纳指',
        positionPct: 3.6,
        marketData: { close: 1.6, ma200: 1.5, above: true },
      },
    ],
    multiAssetSleeve: {
      active: true,
      action: 'ROTATE',
      label: '轮动',
      message: '港湾停车轮动：卖出 ETF:513110 → ETF:513350',
      pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.42, mom60: 4.98 },
      idlePct: 26.1,
      parkPct: 70,
    },
  };

  it('shows a one-line verdict plus a sells-first checklist with amounts', async () => {
    fetchPortfolioHealth.mockResolvedValue({ ...ROTATE_HEALTH } as never);
    renderCard({ mode: 'homeport' });
    // 顶部一行结论：卖 1 笔、买 5 笔（B3 四线补足 + 港湾停车 OIL）。
    const conclusion = await screen.findByTestId('ops-conclusion');
    expect(conclusion.textContent).toMatch(/今天：卖 1 笔、买 5 笔，共 6 笔/);
    const rows = await screen.findAllByTestId('m30-ops-row');
    expect(rows).toHaveLength(6);
    expect(rows[0]?.getAttribute('data-side')).toBe('SELL');
    expect(rows[0]?.getAttribute('data-symbol')).toBe('ETF:511260');
    expect(rows.slice(1).every((r) => r.getAttribute('data-side') === 'BUY')).toBe(true);
    // 511260 大额卖出是腿偏离腾资金补港湾停车，不是 B3 月频权重变化。
    expect(rows[0]?.textContent).toMatch(/腿偏离腾资金补港湾停车/);
    expect(rows[0]?.textContent).not.toMatch(/（B3 月频目标）/);
    // B3 纯月频补足行保留月频原因。
    expect(screen.getAllByText('（B3 月频目标）').length).toBeGreaterThanOrEqual(3);
    // 停车相关行注明 T+1 开盘执行（港湾停车买入 + 腾资金补停车的 511260 卖出）。
    expect(
      screen.getAllByText('信号日收盘后、下一交易日开盘执行').length,
    ).toBeGreaterThanOrEqual(2);
    // 每条直接给出份数 + 约金额，不再指向已下线的跟仓卡。
    expect(screen.queryByText(/母港M30跟仓/)).toBeNull();
    const amounts = screen.getAllByText(/份 ≈ ¥/);
    expect(amounts.length).toBeGreaterThanOrEqual(2);
    // 每条一个“记录成交”按钮。
    expect(screen.getAllByTestId('ops-record')).toHaveLength(6);
  });

  it('keeps the rotation message once and states the B3 rule accurately', async () => {
    fetchPortfolioHealth.mockResolvedValue({ ...ROTATE_HEALTH } as never);
    renderCard({ mode: 'homeport' });
    await screen.findByTestId('m30-ops');
    // 同一句话在整页只出现一次（操作引导保留一处，停车体检/对账不再重复）。
    const hits = (document.body.textContent ?? '').split(
      '港湾停车轮动：卖出 ETF:513110 → ETF:513350',
    ).length - 1;
    expect(hits).toBe(1);
    // “B3 腿 ETF 不卖”是错的（B3 按月频目标会产生买卖单）：只保留准确说法一处。
    expect(screen.queryByText(/B3 腿 ETF 不卖/)).toBeNull();
    const b3Hits = (document.body.textContent ?? '').split(
      'B3 腿只按月频目标调，不随停车轮动换仓',
    ).length - 1;
    expect(b3Hits).toBe(1);
  });

  it('does NOT defer parking/B3 buys when the circuit breaker trips (no stock buys)', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      ...ROTATE_HEALTH,
      circuitBlocked: true,
    } as never);
    renderCard({ mode: 'homeport' });
    // 熔断只拦 S-3 股票新开仓：本单 5 笔买入全是停车/B3，照常可执行。
    const conclusion = await screen.findByTestId('ops-conclusion');
    expect(conclusion.textContent).toMatch(/今天：卖 1 笔、买 5 笔，共 6 笔/);
    expect(conclusion.textContent).not.toMatch(/只卖不买/);
    expect(screen.queryByText(/买入先别做/)).toBeNull();
    expect(screen.queryByTestId('ops-risk-note')).toBeNull();
    expect(screen.queryByText('暂缓')).toBeNull();
    const buttons = screen.getAllByTestId('ops-record') as HTMLButtonElement[];
    expect(buttons).toHaveLength(6);
    expect(buttons.filter((b) => b.disabled)).toHaveLength(0);
  });

  it('defers ONLY S-3 stock buys when the circuit breaker trips', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-10-08',
      regime: 'Strong',
      scoreFresh: true,
      circuitBlocked: true,
      panicCooldown: null,
      s3Candidates: [],
      // 3% 实持 → 7% 目标：S-3 股票买入 4%，应被熔断暂缓。
      holdings: [{ symbol: 'CN:600519', name: '贵州茅台', positionPct: 3, action: 'HOLD', lastClose: 10 }],
      hkHealth: { holdings: [] },
      multiAssetHoldings: [],
      multiAssetSleeve: {
        active: true,
        action: 'BUY',
        label: '买入',
        message: '港湾停车轮动',
        pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.42, mom60: 4.98 },
      },
    } as never);
    renderCard({ mode: 'homeport' });
    const conclusion = await screen.findByTestId('ops-conclusion');
    // 标题只暂缓 S-3，不出现无范围的“今天只卖不买”。
    expect(conclusion.textContent).toMatch(/S-3 买入 1 笔熔断暂缓/);
    expect(conclusion.textContent).not.toMatch(/今天只卖不买/);
    expect(await screen.findByText(/回撤熔断中：S-3 股票买入暂缓/)).toBeDefined();
    expect(screen.getByText(/停车\s*ETF \/ B3 买入与卖出照常/)).toBeDefined();
    const stales = await screen.findAllByText('暂缓');
    expect(stales).toHaveLength(1);
    const rows = await screen.findAllByTestId('m30-ops-row');
    const stockRow = rows.find((r) => r.getAttribute('data-symbol') === 'CN:600519');
    expect(stockRow?.textContent).toMatch(/暂缓/);
    const buttons = screen.getAllByTestId('ops-record') as HTMLButtonElement[];
    expect(buttons.filter((b) => b.disabled)).toHaveLength(1);
    expect(buttons.filter((b) => !b.disabled).length).toBeGreaterThan(0);
  });

  it('defers ONLY S-3 stock buys in weak regime and panic cooldown', async () => {
    const base = {
      tradeDate: '2026-10-08',
      scoreFresh: true,
      circuitBlocked: false,
      s3Candidates: [],
      holdings: [{ symbol: 'CN:600519', name: '贵州茅台', positionPct: 3, action: 'HOLD', lastClose: 10 }],
      hkHealth: { holdings: [] },
      multiAssetHoldings: [],
      multiAssetSleeve: {
        active: true,
        action: 'BUY',
        label: '买入',
        message: '港湾停车轮动',
        pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.42, mom60: 4.98 },
      },
    } as Record<string, unknown>;
    for (const health of [
      { ...base, regime: 'Weak', panicCooldown: null },
      { ...base, regime: 'Strong', panicCooldown: { active: true, cooldownEndDate: '2026-10-10' } },
    ]) {
      fetchPortfolioHealth.mockResolvedValue(health as never);
      const { unmount } = renderCard({ mode: 'homeport' });
      expect(await screen.findAllByText('暂缓')).toHaveLength(1);
      const rows = await screen.findAllByTestId('m30-ops-row');
      expect(rows.find((r) => r.getAttribute('data-symbol') === 'CN:600519')?.textContent).toMatch(
        /暂缓/,
      );
      // 停车买入行不受闸影响：可点记录成交。
      const parkBtn = rows
        .find((r) => r.getAttribute('data-symbol') === 'ETF:513350')
        ?.querySelector('[data-testid="ops-record"]') as HTMLButtonElement;
      expect(parkBtn?.disabled).toBe(false);
      unmount();
    }
  });

  it('records a sell from the checklist row', async () => {
    fetchPortfolioHealth.mockResolvedValue({ ...ROTATE_HEALTH } as never);
    renderCard({ mode: 'homeport' });
    const rows = await screen.findAllByTestId('m30-ops-row');
    const sellRow = rows[0] as HTMLElement;
    const btn = sellRow.querySelector('[data-testid="ops-record"]') as HTMLButtonElement;
    fireEvent.click(btn);
    // QuickBuyDialog opens in SELL mode with the M30 pct prefilled.
    expect(await screen.findByText('确认卖出')).toBeDefined();
  });

  it('shows 今天无需操作 when the book tracks M30', async () => {
    const stocks = Array.from({ length: 10 }, (_, i) => ({
      symbol: `CN:6000${i.toString().padStart(2, '0')}`,
      name: `票${i}`,
      positionPct: 7,
      action: 'HOLD',
      lastClose: 10,
    }));
    const etfs = ['510300', '510500', '518880', '513100', '511260'].map((c) => ({
      symbol: `ETF:${c}`,
      name: c,
      positionPct: 6,
    }));
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-10-08',
      regime: 'Strong',
      scoreFresh: true,
      circuitBlocked: false,
      panicCooldown: null,
      s3Candidates: [],
      holdings: stocks,
      hkHealth: { holdings: [] },
      multiAssetHoldings: etfs,
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有',
        message: '持有',
        pick: { key: 'STOCK', symbol: null, close: null },
      },
    } as never);
    renderCard({ mode: 'homeport' });
    expect(await screen.findByTestId('ops-empty')).toBeDefined();
    expect((await screen.findAllByText(/今天无需操作/)).length).toBeGreaterThanOrEqual(1);
    expect(screen.queryAllByTestId('m30-ops-row')).toHaveLength(0);
  });

  it('asks for 总资金 when capital is missing', async () => {
    accountMock.capital = null;
    fetchPortfolioHealth.mockResolvedValue({ ...ROTATE_HEALTH } as never);
    renderCard({ mode: 'homeport' });
    expect(await screen.findByText(/填总资金后列出金额与份数/)).toBeDefined();
  });

  it('a fired trail overrides the target math: first row sells the held park leg → repo', async () => {
    // 2026-11-05 live case: 63.5% parked in OIL, the 2026-11-03 close breached
    // the 8% trail. The pick is still OIL, so target-vs-current used to print
    // 买 513350 — the screenshot bug. Sleeve SELL_TO_REPO must win.
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-11-05',
      regime: 'Strong',
      scoreFresh: true,
      circuitBlocked: false,
      panicCooldown: null,
      s3Candidates: [],
      holdings: [],
      hkHealth: { holdings: [] },
      multiAssetHoldings: [
        {
          symbol: 'ETF:513350',
          key: 'OIL',
          name: '原油 ETF',
          positionPct: 63.5,
          marketData: { close: 1.28, ma200: 1.5, above: false },
        },
      ],
      multiAssetSleeve: {
        active: true,
        action: 'SELL_TO_REPO',
        label: '卖出转repo(止损)',
        message: 'ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03，待开盘执行）',
        symbol: 'ETF:513350',
        pendingOpenSell: true,
        signalDate: '2026-11-03',
        executeHint: '待开盘卖出转逆回购（信号已触发，未执行）',
        pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.28, mom60: 4.98 },
        idlePct: 0,
        parkPct: 63.5,
      },
    } as never);
    renderCard({ mode: 'homeport' });

    const rows = await screen.findAllByTestId('m30-ops-row');
    // First row = the must-act SELL → 逆回购 (before any rebalance math).
    expect(rows[0]?.getAttribute('data-side')).toBe('SELL');
    expect(rows[0]?.getAttribute('data-symbol')).toBe('ETF:513350');
    expect(rows[0]?.getAttribute('data-must-act')).toBe('true');
    expect(rows[0]?.textContent).toMatch(/逆回购/);
    expect(rows[0]?.textContent).toMatch(/待开盘卖出|次日开盘卖出/);
    // Never buy the leg we are exiting.
    expect(
      rows.some(
        (r) => r.getAttribute('data-side') === 'BUY' && r.getAttribute('data-symbol') === 'ETF:513350',
      ),
    ).toBe(false);
    // Must-act banner + 操作引导 first step.
    expect(await screen.findByTestId('ops-must-act')).toBeDefined();
    expect(screen.getByTestId('pick-pending-sell').textContent).toMatch(/待卖 · 513350/);
    // Parking card is not a plain 持有.
    expect(screen.getByTestId('parking-must-act').textContent).toMatch(/卖出 513350 → 逆回购/);
    expect(screen.getAllByText('待卖').length).toBeGreaterThanOrEqual(1);
    // Headline never claims "no action needed".
    expect(screen.queryByText(/今天无需操作/)).toBeNull();
  });

  it('shows the 14:30 现价触线 warning without switching to intraday execution', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-11-05',
      regime: 'Strong',
      scoreFresh: true,
      circuitBlocked: false,
      panicCooldown: null,
      s3Candidates: [],
      holdings: [],
      hkHealth: { holdings: [] },
      multiAssetHoldings: [
        {
          symbol: 'ETF:513350',
          key: 'OIL',
          name: '原油 ETF',
          positionPct: 40,
          marketData: { close: 1.281, ma200: 1.5, above: false },
        },
      ],
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有',
        message: '港湾停车：持有 ETF:513350（mom60 4.98%）',
        pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.281, mom60: 4.98 },
        idlePct: 0,
        parkPct: 70,
        intradayWarn: {
          active: true,
          status: 'breached',
          symbol: 'ETF:513350',
          px: 1.281,
          thresholdPx: 1.289,
          peak: 1.401,
          drawdownPct: 8.6,
          message: 'ETF:513350现价1.281 ≤ 卖出阈值价1.289（峰值1.401 回撤8.6%）→ 现价已达卖出条件',
          executeHint: '若收盘仍低于阈值价 → 明日开盘卖出转逆回购（不盘中卖）',
          tradeDate: '2026-11-05',
        },
      },
    } as never);
    renderCard({ mode: 'homeport' });
    const warn = await screen.findByTestId('ops-intraday-warn');
    expect(warn.textContent).toMatch(/现价已达卖出条件/);
    expect(warn.textContent).toMatch(/门槛价 1.289/);
    expect(warn.textContent).toMatch(/明日开盘卖出转逆回购/);
    // Clock unchanged: no executable SELL row from the warning alone, but the
    // page must not say 「今天无需操作」 nor suggest buying the same leg.
    expect(
      screen.queryAllByTestId('m30-ops-row').some(
        (r) => r.getAttribute('data-side') === 'BUY' && r.getAttribute('data-symbol') === 'ETF:513350',
      ),
    ).toBe(false);
    expect(screen.queryByText(/今天无需操作/)).toBeNull();
  });
});
