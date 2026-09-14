import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PickStrongAlignBanner } from './PickStrongAlignBanner';

vi.mock('@/lib/queries/portfolioHealth', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/portfolioHealth')>();
  return { ...actual, fetchPortfolioHealth: vi.fn() };
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
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: true, action: 'HOLD', pick: { key: 'OIL', symbol: 'ETF:513350' } },
      holdings: [],
      multiAssetHoldings: [{ symbol: 'ETF:513350', positionPct: 100 }],
      hkHealth: { holdings: [] },
    } as never);
  });

  it('defaults to the harbor align title', async () => {
    renderBanner();
    expect(await screen.findByText('港湾日对齐')).toBeDefined();
  });

  it('follows the selected strategy view and notes the unwired legs', async () => {
    renderBanner('starport');
    expect(await screen.findByText('星港日对齐')).toBeDefined();
    expect(await screen.findByText(/卫星 1\/3 overlay.*OPT-186/)).toBeDefined();
  });
});
