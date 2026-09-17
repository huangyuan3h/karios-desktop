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
    useSleeveReconQuery: backtestMock.useSleeveReconQuery,
    useTimelineQuery: backtestMock.useTimelineQuery,
    useSatelliteLivePanelQuery: backtestMock.useSatelliteLivePanelQuery,
  };
});

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
    expect(await screen.findByText(/股票篮买入/)).toBeDefined();
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
    expect(await screen.findByText(/操作提示（研究档 · 不进 Live）/)).toBeDefined();
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
