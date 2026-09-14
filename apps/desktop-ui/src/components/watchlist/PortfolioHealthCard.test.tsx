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
}));
vi.mock('@/lib/queries/backtest', async () => {
  const actual =
    await vi.importActual<typeof import('@/lib/queries/backtest')>('@/lib/queries/backtest');
  return {
    ...actual,
    useBacktestReconQuery: backtestMock.useBacktestReconQuery,
    useSleeveReconQuery: backtestMock.useSleeveReconQuery,
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
    expect(await screen.findByText('核心腿状态（港湾配方）')).toBeDefined();
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
    expect(await screen.findByText(/股票篮买入（核心 pick=STOCK/)).toBeDefined();
    expect(await screen.findByText(/每票建议 10%/)).toBeDefined();
    expect(screen.getAllByText('买 10%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('贵州茅台')).toBeDefined();
  });

  it('does not offer stock buys when the core pick is an ETF', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '核心 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      panicCooldown: { active: false },
      s3Candidates: [{ symbol: 'CN:600519', name: '贵州茅台', score: 82 }],
      holdings: [],
      hkHealth: null,
      multiAssetHoldings: [
        {
          symbol: 'ETF:513350',
          name: '原油 ETF',
          positionPct: 94,
          marketData: { close: 1.2, ma200: 1.0, above: true },
        },
      ],
    });
    renderCard();
    expect(await screen.findByText(/闲置现金停进核心 ETF/)).toBeDefined();
    expect(screen.queryByText(/股票篮买入（核心 pick=STOCK/)).toBeNull();
    fireEvent.click(screen.getByText('展开'));
    expect(await screen.findByText(/核心 pick=/)).toBeDefined();
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
    expect(screen.getByText(/你待执行（次日开盘）/)).toBeDefined();
  });

  it('shows a harbor error banner when the health endpoint fails', async () => {
    fetchPortfolioHealth.mockRejectedValue(new Error('boom'));
    renderCard();
    expect(await screen.findByText(/港湾暂无数据（data-sync-service 未响应）/)).toBeDefined();
  });
});
