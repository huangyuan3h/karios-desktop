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
      thirdAssetSleeve: { active: false, action: 'NONE', message: '' },
    } as never);
    const { container } = renderBanner();
    await waitFor(() => expect(mockHealth).toHaveBeenCalled());
    await waitFor(() => expect(container.firstChild).toBeNull());
  });

  it('renders the buy hint when idle cash + above MA200', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'BUY_513100',
        message: '当前闲置资金 94% 且 ETF:513100 在200日线上 → 建议买入',
        etf: 'ETF:513100',
        price: 2.239,
        ma200: 1.983,
        idlePct: 94,
        asOfDate: '2026-08-18',
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/第三资产套筒/)).toBeInTheDocument();
    expect(screen.getByText(/建议买入/)).toBeInTheDocument();
    expect(screen.getByText(/现价 2.239/)).toBeInTheDocument();
  });

  it('renders the sell-to-A-share hint when the S-3 line has a buy point', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'SELL_TO_A_SHARE',
        message: 'A股有买点 → 卖出 ETF:513100，资金换回 A 股',
        etf: 'ETF:513100',
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/A股有买点/)).toBeInTheDocument();
  });

  it('renders the sell-to-repo hint when 513100 breaks the 200d MA', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'SELL_TO_REPO',
        label: '卖出 513100 · 转逆回购',
        message: 'ETF:513100 跌破200日线 → 卖出转逆回购',
        etf: 'ETF:513100',
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/跌破200日线/)).toBeInTheDocument();
  });

  it('renders the dont-buy hint when below the 200d MA and not holding', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'DONT_BUY',
        label: '今日不买 513100',
        message: 'ETF:513100 跌破200日线 → 今天别买，资金留逆回购',
        etf: 'ETF:513100',
      },
    } as never);
    renderBanner();
    expect(await screen.findByText(/今日不买 513100/)).toBeInTheDocument();
    expect(screen.getByText(/别买/)).toBeInTheDocument();
  });

  it('renders the dont-buy hint when fully deployed', async () => {
    mockHealth.mockResolvedValue({
      thirdAssetSleeve: {
        active: true,
        action: 'DONT_BUY',
        label: '今日不买 513100',
        message: '资金已部署（闲置 0%）→ 今日不买',
        etf: 'ETF:513100',
        idlePct: 0,
      },
    } as never);
    renderBanner();
    expect(await screen.findAllByText(/今日不买/)).toHaveLength(2);
    expect(screen.getByText(/资金已部署/)).toBeInTheDocument();
  });
});