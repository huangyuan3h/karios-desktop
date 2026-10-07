import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { TodayTodoCard } from './TodayTodoCard';

const { fetchPortfolioHealth } = vi.hoisted(() => ({
  fetchPortfolioHealth: vi.fn(),
}));
vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth };
});

type QueryStub = { data: unknown; isError: boolean; isFetching: boolean; isPending?: boolean };
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
  useB3StateQuery: vi.fn(
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
    useB3StateQuery: backtestMock.useB3StateQuery,
  };
});

/** Same Shanghai-weekend source of truth as the card: weekend candidates are info-level, not counted. */
function shanghaiWeekendNow(): boolean {
  const wd = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
  }).format(new Date());
  return wd === '周六' || wd === '周日';
}

function renderCard() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <TodayTodoCard />
    </QueryClientProvider>,
  );
}

const HEALTH = {
  tradeDate: '2026-09-16',
  regime: 'Strong',
  scoreFresh: true,
  circuitBlocked: false,
  panicCooldown: null,
  s3Candidates: [{ symbol: 'CN:600519', name: '茅台' }],
  holdings: [{ symbol: 'CN:600519', name: '茅台', action: 'EXIT' }],
};

describe('TodayTodoCard', () => {
  beforeEach(() => {
    vi.mocked(fetchPortfolioHealth).mockReset();
    backtestMock.useBacktestReconQuery.mockReturnValue({
      data: { ok: true, items: [] },
      isError: false,
      isFetching: false,
    });
    backtestMock.useSleeveReconQuery.mockReturnValue({
      data: undefined,
      isError: false,
      isFetching: false,
    });
    backtestMock.useB3StateQuery.mockReturnValue({
      data: undefined,
      isError: false,
      isFetching: false,
    });
    window.localStorage.clear();
  });

  it('lists 缺买/卖出/候选 in order with deep links', async () => {
    vi.mocked(fetchPortfolioHealth).mockResolvedValue(HEALTH as never);
    backtestMock.useSleeveReconQuery.mockReturnValue({
      data: {
        ok: true,
        recon: {
          missedBuys: ['ETF:513350'],
          missedSells: [],
          extraOpens: [],
          userAlignment: 'aligned',
          userBuys: [],
          userSells: [],
        },
      },
      isError: false,
      isFetching: false,
    });
    const onScroll = vi.fn();
    window.addEventListener('karios-scroll-to', onScroll as EventListener);
    renderCard();
    // Weekend: the candidate row drops to info level, so the counted total is 2, not 3.
    const weekend = shanghaiWeekendNow();
    expect(await screen.findByText(new RegExp(`今日待办（${weekend ? 2 : 3}）`))).toBeDefined();
    expect(await screen.findByText(/停车腿偏离：缺买 ETF:513350/)).toBeDefined();
    expect(await screen.findByText(/持仓卖出 1 只：茅台/)).toBeDefined();
    if (weekend) {
      expect(await screen.findByText(/股票篮 1 只候选（周末先看，不下单）/)).toBeDefined();
    } else {
      expect(await screen.findByText(/股票篮 1 只候选，14:30 前决定买哪几只/)).toBeDefined();
    }
    const links = await screen.findAllByText('去处理 →');
    expect(links.length).toBe(3);
    fireEvent.click(links[0]);
    expect(onScroll).toHaveBeenCalledOnce();
    window.removeEventListener('karios-scroll-to', onScroll as EventListener);
  });

  it('links an HK-only recon gap straight to the HK block', async () => {
    vi.mocked(fetchPortfolioHealth).mockResolvedValue({
      ...HEALTH,
      s3Candidates: [],
      holdings: [],
    } as never);
    backtestMock.useBacktestReconQuery.mockReturnValue({
      data: { ok: true, items: [{ market: 'HK', missing: 4, extra: 5 }] },
      isError: false,
      isFetching: false,
    });
    let anchor = '';
    const onScroll = (e: Event) => {
      anchor = (e as CustomEvent<{ anchor: string }>).detail?.anchor ?? '';
    };
    window.addEventListener('karios-scroll-to', onScroll as EventListener);
    renderCard();
    expect(await screen.findByText(/HK 缺4\/多5/)).toBeDefined();
    fireEvent.click(await screen.findByText('去处理 →'));
    expect(anchor).toBe('recon-hk');
    window.removeEventListener('karios-scroll-to', onScroll as EventListener);
  });

  it('shows the all-clear state when nothing needs doing', async () => {
    vi.mocked(fetchPortfolioHealth).mockResolvedValue({
      ...HEALTH,
      s3Candidates: [],
      holdings: [{ symbol: 'CN:600519', name: '茅台', action: 'HOLD' }],
    } as never);
    renderCard();
    expect(await screen.findByText(/今日无待办/)).toBeDefined();
  });

  it('shows a retry button when health data fails', async () => {
    vi.mocked(fetchPortfolioHealth).mockRejectedValue(new Error('down'));
    renderCard();
    expect(await screen.findByText(/待办暂不可用/)).toBeDefined();
    fireEvent.click(await screen.findByText('重试'));
    expect(vi.mocked(fetchPortfolioHealth)).toHaveBeenCalledTimes(2);
  });

  it('adds an M30 rebalance todo for homeport past the drift threshold', async () => {
    vi.mocked(fetchPortfolioHealth).mockResolvedValue({
      tradeDate: '2026-10-07',
      regime: 'Strong',
      scoreFresh: true,
      circuitBlocked: false,
      panicCooldown: null,
      s3Candidates: [],
      holdings: [],
      hkHealth: { holdings: [] },
      multiAssetHoldings: [],
      multiAssetSleeve: { pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.42 } },
    } as never);
    backtestMock.useB3StateQuery.mockReturnValue({
      data: {
        ok: true,
        universe: [
          { symbol: '510300.SH', name: '沪深300', targetPct: 20, driftPct: 20, deltaPct: 0, px: 4.6 },
          { symbol: '510500.SH', name: '中证500', targetPct: 20, driftPct: 20, deltaPct: 0, px: 7 },
          { symbol: '518880.SH', name: '黄金', targetPct: 20, driftPct: 20, deltaPct: 0, px: 6 },
          { symbol: '513100.SH', name: '纳指100', targetPct: 20, driftPct: 20, deltaPct: 0, px: 1.6 },
          { symbol: '511260.SH', name: '10年国债', targetPct: 20, driftPct: 20, deltaPct: 0, px: 134 },
        ],
      },
      isError: false,
      isFetching: false,
    });
    renderCard();
    // Empty book vs 70/30 targets → 70pt drift, off-cycle rebalance fires.
    expect(await screen.findByText(/母港M30需再平衡/)).toBeDefined();
  });
});
