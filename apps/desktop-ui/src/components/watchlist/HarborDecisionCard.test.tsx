import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { HarborDecisionCard } from './HarborDecisionCard';

vi.mock('@/lib/queries/behaviorAudit', () => ({
  useBehaviorAuditQuery: () => ({
    data: [
      {
        market: 'CN',
        missingList: [{ symbol: 'CN:600000', score: 70 }],
        extraList: [{ symbol: 'CN:600519' }],
      },
    ],
  }),
  useRefreshBehaviorAudit: () => ({ mutateAsync: vi.fn(), isPending: false }),
}));

vi.mock('@/lib/queries/portfolioHealth', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/portfolioHealth')>();
  return { ...actual, fetchPortfolioHealth: vi.fn() };
});

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return { ...actual, useTimelineQuery: () => ({ data: { rows: [] } }) };
});

vi.mock('@/lib/queries/userTrades', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/userTrades')>();
  return { ...actual, recordUserTrade: vi.fn(), invalidateUserTradesQueries: vi.fn() };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { recordUserTrade } from '@/lib/queries/userTrades';

const mockHealth = vi.mocked(fetchPortfolioHealth);
const mockRecord = vi.mocked(recordUserTrade);

function renderCard(mode: 'harbor' | 'homeport' | 'starport' | 'starship' = 'harbor') {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <HarborDecisionCard mode={mode} />
    </QueryClientProvider>,
  );
}

describe('HarborDecisionCard', () => {
  beforeEach(() => {
    mockHealth.mockReset();
    mockRecord.mockReset();
    vi.unstubAllGlobals();
  });

  it('renders the single Harbor decision surface', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [],
      multiAssetHoldings: [{ symbol: 'ETF:513350', positionPct: 60 }],
      hkHealth: { holdings: [] },
    } as never);
    renderCard();
    expect(screen.getByText('港湾 · 今日决策')).toBeTruthy();
    expect(
      screen.getByText('S-3 核心 + 闲置现金 ETF 停车场 · 100% 硬切 · 与 Timeline 同源'),
    ).toBeTruthy();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
  });

  it('follows the selected strategy view (星港)', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [],
      multiAssetHoldings: [],
      hkHealth: { holdings: [] },
    } as never);
    renderCard('starport');
    expect(screen.getByText('星港 · 今日决策')).toBeTruthy();
    expect(
      screen.getByText('母港底仓 × 卫星 1/3 曝露 · 核心腿买卖照常 · 与 Timeline 同源'),
    ).toBeTruthy();
    expect(screen.getByText(/卫星有仓日 1\/3 overlay/)).toBeTruthy();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
  });

  it('records an ETF parking buy through the execution log', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'BUY',
        idlePct: 60,
        pick: { key: 'OIL', symbol: 'ETF:513350', mom60: 29.5, close: 1.39 },
      },
      holdings: [],
      multiAssetHoldings: [],
      hkHealth: { holdings: [] },
    } as never);
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, text: async () => '' });
    vi.stubGlobal('fetch', fetchMock);
    renderCard();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());

    fireEvent.change(screen.getByLabelText('成交价'), { target: { value: '1.4' } });
    fireEvent.click(screen.getByRole('button', { name: '记一笔' }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('/commodities/sleeve/execution-log');
    const body = JSON.parse(String(init.body));
    expect(body.symbol).toBe('ETF:513350');
    expect(body.fillPrice).toBe(1.4);
    expect(mockRecord).not.toHaveBeenCalled();
  });

  it('records a stock sell through /trades (user_trades)', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [],
      multiAssetHoldings: [],
      hkHealth: { holdings: [] },
    } as never);
    mockRecord.mockResolvedValue({} as never);
    renderCard();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());

    const itemSelect = screen.getByLabelText('标的') as unknown as HTMLSelectElement;
    fireEvent.change(itemSelect, { target: { value: 'CN:600519' } });
    fireEvent.change(screen.getByLabelText('成交价'), { target: { value: '1500' } });
    fireEvent.click(screen.getByRole('button', { name: '记一笔' }));

    await waitFor(() => expect(mockRecord).toHaveBeenCalled());
    expect(mockRecord.mock.calls[0][0]).toMatchObject({
      symbol: 'CN:600519',
      side: 'SELL',
      price: 1500,
    });
  });
});
