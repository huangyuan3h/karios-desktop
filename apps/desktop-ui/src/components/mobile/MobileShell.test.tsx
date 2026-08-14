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
    useBehaviorAuditQuery: vi.fn(() => ({
      data: [],
      isLoading: false,
    })),
  };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';

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

describe('MobileShell (Family Hub Phase 0)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockHealth.mockResolvedValue(HEALTH as never);
  });

  it('shows gate badges (CN open / HK closed) and the buy list', async () => {
    renderShell();
    await waitFor(() => {
      expect(screen.getByText(/A股 · 可买/)).toBeTruthy();
      expect(screen.getByText(/港股 · 不可买/)).toBeTruthy();
      expect(screen.getByText(/华新建材/)).toBeTruthy();
      expect(screen.getByText(/下午 2 点买入清单/)).toBeTruthy();
    });
  });

  it('shows holdings with stop lines on the 持仓 tab', async () => {
    renderShell();
    fireEvent.click(screen.getByText('持仓'));
    await waitFor(() => {
      expect(screen.getByText(/亿联网络/)).toBeTruthy();
    });
    expect(screen.getByText(/止损线/)).toBeTruthy();
    expect(screen.getByText('37.905')).toBeTruthy();
  });

  it('flags EXIT holdings on the 执行 tab', async () => {
    mockHealth.mockResolvedValue({
      ...HEALTH,
      holdings: [
        { ...HEALTH.holdings[0], action: 'EXIT', pnlPct: -5.4 },
      ],
    } as never);
    renderShell();
    await waitFor(() => {
      expect(screen.getByText(/需要卖出/)).toBeTruthy();
      expect(screen.getByText('🚩退出')).toBeTruthy();
    });
  });

  it('shows audit deviations on the 对账 tab', async () => {
    vi.mocked(useBehaviorAuditQuery).mockReturnValue({
      data: [
        {
          auditDate: '2026-08-13',
          market: 'CN',
          expected: 0,
          actual: 1,
          extra: 1,
          missing: 0,
          extraList: [
            { symbol: 'CN:600002', name: '某票', kind: 'exited', costPrice: 10 },
          ],
        },
      ],
      isLoading: false,
    } as never);
    renderShell();
    fireEvent.click(screen.getByText('对账'));
    await waitFor(() => {
      expect(screen.getByText(/该卖没卖/)).toBeTruthy();
    });
  });
});
