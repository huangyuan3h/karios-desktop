import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PickStrongAlignBanner } from './PickStrongAlignBanner';

vi.mock('@/lib/queries/portfolioHealth', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/portfolioHealth')>();
  return { ...actual, fetchPortfolioHealth: vi.fn() };
});

const b3Mock = vi.hoisted(() => ({ useB3StateQuery: vi.fn() }));
vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@/lib/queries/backtest')>();
  return { ...actual, useB3StateQuery: b3Mock.useB3StateQuery };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';

const mockHealth = vi.mocked(fetchPortfolioHealth);

function renderBanner(mode: 'harbor' | 'homeport' | 'starport' | 'starship' = 'harbor') {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <PickStrongAlignBanner mode={mode} />
    </QueryClientProvider>,
  );
}

describe('PickStrongAlignBanner', () => {
  beforeEach(() => {
    mockHealth.mockReset();
    b3Mock.useB3StateQuery.mockReset();
    b3Mock.useB3StateQuery.mockReturnValue({ data: undefined, isPending: true });
    window.localStorage.clear();
  });

  it('shows a quiet aligned stamp (not silence) when the book is aligned', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [],
      multiAssetHoldings: [{ symbol: 'ETF:513350', positionPct: 100 }],
      hkHealth: { holdings: [] },
    } as never);
    renderBanner();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
    // Aligned is visible-but-quiet so users can tell it apart from "not loaded".
    expect(await screen.findByText('✓ 已对齐')).toBeDefined();
  });

  it('shows the strategy-labeled banner and the unwired-legs note when diverged', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [{ symbol: 'CN:600519', positionPct: 100 }],
      multiAssetHoldings: [],
      hkHealth: { holdings: [] },
    } as never);
    renderBanner('starport');
    expect(await screen.findByText('星港日对齐')).toBeDefined();
    expect(await screen.findByText(/偏离星港/)).toBeDefined();
    expect(await screen.findByText(/卫星 0\.2 overlay.*OPT-186/)).toBeDefined();
  });

  it('renders the M30 follow panel (not the harbor 100% check) for homeport', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        pick: { key: 'OIL', symbol: 'ETF:513350', close: 1.42 },
      },
      holdings: [],
      multiAssetHoldings: [{ symbol: 'ETF:510300', positionPct: 6 }],
      hkHealth: { holdings: [] },
    } as never);
    b3Mock.useB3StateQuery.mockReturnValue({
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
    });
    renderBanner('homeport');
    expect(await screen.findByTestId('m30-follow-block')).toBeDefined();
    expect(await screen.findByText('母港M30跟仓')).toBeDefined();
    // B3 ETFs are M30 targets — the harbor "非今日停车 ETF" warning must not fire.
    expect(screen.queryByText('持有非今日停车 ETF')).toBeNull();
  });
});
