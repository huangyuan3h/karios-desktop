import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ThirdAssetSleeveBanner } from './ThirdAssetSleeveBanner';

vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth: vi.fn() };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';

const mockHealth = vi.mocked(fetchPortfolioHealth);

function renderBanner() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <ThirdAssetSleeveBanner />
    </QueryClientProvider>,
  );
}

describe('ThirdAssetSleeveBanner', () => {
  beforeEach(() => {
    mockHealth.mockReset();
  });

  it('renders nothing when the sleeve is inactive', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: { active: false, action: 'NONE', message: '' },
    } as never);
    const { container } = renderBanner();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
    await waitFor(() => expect(container.firstChild).toBeNull());
  });

  it('ignores legacy thirdAssetSleeve-only payloads (not 择强 SSOT)', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'BUY_513100',
        message: 'legacy nasdaq sleeve',
        etf: 'ETF:513100',
      },
    } as never);
    const { container } = renderBanner();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
    await waitFor(() => expect(container.firstChild).toBeNull());
  });

  it('renders mom_compare multiAssetSleeve buy hint', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'BUY',
        label: '买入 OIL',
        message: '择强 OIL → 买入',
        pick: { key: 'OIL', symbol: 'ETF:513350', mom60: 12.3, close: 1.2, ma200: 1.0 },
        idlePct: 94,
        mode: 'mom_compare',
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/择强单轨/)).toBeInTheDocument();
    expect(screen.getByText(/买入 OIL/)).toBeInTheDocument();
  });

  it('renders rotate when pick switches', async () => {
    mockHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'ROTATE',
        message: '择强轮动：卖出 ETF:518880 → ETF:513350',
        pick: { key: 'OIL', symbol: 'ETF:513350', mom60: 8 },
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/择强轮动/)).toBeInTheDocument();
  });
});
