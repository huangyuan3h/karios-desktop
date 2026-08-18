import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { MobileShell } from './MobileShell';

vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth: vi.fn() };
});
vi.mock('@/lib/queries/behaviorAudit', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/behaviorAudit')>(
    '@/lib/queries/behaviorAudit',
  );
  return {
    ...actual,
    useBehaviorAuditQuery: vi.fn(() => ({ data: [], isLoading: false })),
  };
});
vi.mock('@/lib/queries/news', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/news')>('@/lib/queries/news');
  return {
    ...actual,
    useNewsItemsQuery: vi.fn(() => ({ data: { items: [] }, isLoading: false })),
  };
});
vi.mock('@/lib/queries/industryFlow', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/industryFlow')>(
    '@/lib/queries/industryFlow',
  );
  return {
    ...actual,
    useIndustryFundFlowQuery: vi.fn(() => ({ data: null, isLoading: false })),
    useIndustryMainlineQuery: vi.fn(() => ({ data: null })),
  };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';

const mockHealth = vi.mocked(fetchPortfolioHealth);

const HEALTH = {
  tradeDate: '2026-08-14',
  regime: 'Diverging',
  strength: 47.9,
  sentiment: 'caution',
  panicCooldown: { active: false },
  circuitBlocked: false,
  s3Candidates: [
    { symbol: 'CN:600801', name: '华新建材', score: 67.4 },
  ],
  holdings: [
    {
      symbol: 'CN:300628',
      name: '亿联网络',
      pnlPct: -1.05,
      holdingDays: 10,
      stopLossLine: 37.905,
      trailingLine: 36.828,
      expireDate: '2026-10-03',
      action: 'HOLD',
    },
  ],
  hkHealth: {
    tradeDate: '2026-08-14',
    regime: 'Weak',
    sentiment: 'caution',
    panicCooldown: { active: true, cooldownEndDate: '2026-08-14' },
    circuitBlocked: false,
    s3Candidates: [],
    holdings: [],
  },
};

function renderShell() {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <MobileShell />
    </QueryClientProvider>,
  );
}

describe('MobileShell (IA v2 — 3 tabs + header 更多)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHealth.mockResolvedValue(HEALTH as never);
    localStorage.clear();
  });

  it('header: logo + more button; default tab is dashboard', async () => {
    renderShell();
    expect(screen.getByText('Karios')).toBeTruthy();
    expect(screen.getByLabelText('更多')).toBeTruthy();
    await waitFor(() => {
      expect(screen.getByText('今日状态')).toBeTruthy();
    });
  });

  it('shows gate badges (CN open / HK closed) in the header', async () => {
    renderShell();
    await waitFor(() => {
      expect(screen.getAllByText('A股').length).toBeGreaterThan(0);
      expect(screen.getAllByText('港股').length).toBeGreaterThan(0);
    });
  });

  it('watchlist tab merges act list, holdings and watchlist', async () => {
    renderShell();
    fireEvent.click(screen.getByText('自选'));
    await waitFor(() => {
      expect(screen.getByText(/下午 2 点买入清单/)).toBeTruthy();
      expect(screen.getByText(/持仓（1）/)).toBeTruthy();
      expect(screen.getByText(/华新建材/)).toBeTruthy();
      expect(screen.getByText(/亿联网络/)).toBeTruthy();
    });
  });

  it('watchlist flags EXIT holdings under 需要卖出', async () => {
    mockHealth.mockResolvedValue({
      ...HEALTH,
      holdings: [{ ...HEALTH.holdings[0], action: 'EXIT', pnlPct: -5.4 }],
    } as never);
    renderShell();
    fireEvent.click(screen.getByText('自选'));
    await waitFor(() => {
      expect(screen.getByText(/需要卖出/)).toBeTruthy();
      expect(screen.getAllByText('退出').length).toBeGreaterThan(0);
    });
  });

  it('agent tab shows quick questions', async () => {
    renderShell();
    fireEvent.click(screen.getByText('Agent'));
    await waitFor(() => {
      expect(screen.getByText(/当前市场怎么看/)).toBeTruthy();
      expect(screen.getByText(/持仓有什么风险/)).toBeTruthy();
    });
  });

  it('更多 panel opens with all remaining features', async () => {
    renderShell();
    fireEvent.click(screen.getByLabelText('更多'));
    await waitFor(() => {
      expect(screen.getByText('行为对账')).toBeTruthy();
      expect(screen.getByText('任务调度')).toBeTruthy();
      expect(screen.getByText('设置')).toBeTruthy();
    });
  });

  it('opens a page from 更多 and returns via back button', async () => {
    renderShell();
    fireEvent.click(screen.getByLabelText('更多'));
    fireEvent.click(screen.getByText('任务调度'));
    await waitFor(() => {
      expect(screen.getByText(/任务调度（/)).toBeTruthy();
    });
    fireEvent.click(screen.getByLabelText('返回'));
    await waitFor(() => {
      expect(screen.getAllByText('首页').length).toBeGreaterThan(0);
    });
  });
});
