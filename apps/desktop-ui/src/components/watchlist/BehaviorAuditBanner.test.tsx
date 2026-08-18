import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BehaviorAuditBanner } from './BehaviorAuditBanner';

vi.mock('@/lib/queries/behaviorAudit', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/behaviorAudit')>(
    '@/lib/queries/behaviorAudit',
  );
  return {
    ...actual,
    useBehaviorAuditQuery: vi.fn(() => ({
      data: [
        {
          auditDate: '2026-08-13',
          market: 'CN',
          expected: 0,
          actual: 1,
          extra: 1,
          missing: 0,
          extraList: [
            {
              symbol: 'CN:300628',
              name: '亿联网络',
              costPrice: 39.9,
              entryDate: '2026-08-04',
              kind: 'never_entered',
            },
            {
              symbol: 'CN:600002',
              name: '该卖没卖',
              costPrice: 10,
              entryDate: '2026-07-01',
              kind: 'exited',
            },
          ],
          missingList: [{ symbol: 'HK:00005', score: 78 }],
        },
        {
          auditDate: '2026-08-13',
          market: 'HK',
          expected: 19,
          actual: 0,
          extra: 0,
          missing: 19,
          missingList: [
            { symbol: 'HK:02343', score: 100 },
            { symbol: 'HK:02359', score: 97 },
          ],
        },
      ],
      isLoading: false,
    })),
    useRefreshBehaviorAudit: () => ({ mutateAsync: vi.fn(), isPending: false }),
  };
});

vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth: vi.fn() };
});

import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import { useBehaviorAuditQuery } from '@/lib/queries/behaviorAudit';

const mockHealth = vi.mocked(fetchPortfolioHealth);

function renderBanner() {
  return render(
    <QueryClientProvider client={new QueryClient()}>
      <BehaviorAuditBanner />
    </QueryClientProvider>,
  );
}

describe('BehaviorAuditBanner (OPT-106)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Default: gates open on both markets (no filtering).
    mockHealth.mockResolvedValue({
      tradeDate: '2026-08-14',
      regime: 'Diverging',
      panicCooldown: { active: false },
      circuitBlocked: false,
      hkHealth: { tradeDate: '2026-08-14', regime: 'Diverging', panicCooldown: { active: false }, circuitBlocked: false },
    });
  });

  it('flags 买了不该买 and 该卖没卖 from extraList', async () => {
    renderBanner();
    await waitFor(() => {
      expect(screen.getAllByText(/买了不该买/).length).toBeGreaterThan(0);
      expect(screen.getAllByText(/该卖没卖/).length).toBeGreaterThan(0);
      expect(screen.getByText(/CN:300628/)).toBeTruthy();
      expect(screen.getByText(/CN:600002/)).toBeTruthy();
    });
  });

  it('flags 该持没买 from missingList when gates are open', async () => {
    renderBanner();
    await waitFor(() => {
      expect(screen.getAllByText(/该持没买：/).length).toBeGreaterThan(0);
      expect(screen.getByText(/HK:00005/)).toBeTruthy();
      expect(screen.getByText(/HK:02343/)).toBeTruthy();
    });
  });
});

describe('BehaviorAuditBanner — gate closed (2026-08-14)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // CN panic cooldown + HK regime Weak → both gates closed today.
    mockHealth.mockResolvedValue({
      tradeDate: '2026-08-14',
      regime: 'Diverging',
      panicCooldown: { active: true, cooldownEndDate: '2026-08-14' },
      circuitBlocked: false,
      hkHealth: { tradeDate: '2026-08-14', regime: 'Weak', panicCooldown: { active: false }, circuitBlocked: false },
    });
  });

  it('keeps exit/held-wrong rows but hides 该持没买 suggestions', async () => {
    renderBanner();
    await waitFor(() => {
      // Actionable rows stay.
      expect(screen.getAllByText(/该卖没卖/).length).toBeGreaterThan(0);
      expect(screen.getAllByText(/买了不该买/).length).toBeGreaterThan(0);
      // Un-actionable buy suggestions gone (「该持没买：」rows only — the
      // 「已隐藏 N 条该持没买」disclosure line legitimately contains the phrase).
      expect(screen.queryByText(/该持没买：/)).toBeNull();
      expect(screen.queryByText(/HK:02343/)).toBeNull();
      expect(screen.queryByText(/HK:00005/)).toBeNull();
      // Count of hidden suggestions disclosed.
      expect(screen.getByText(/已隐藏 3 条该持没买/)).toBeTruthy();
    });
  });

  it('goes quiet when everything flagged is an un-actionable buy', async () => {
    mockHealth.mockResolvedValue({
      tradeDate: '2026-08-14',
      regime: 'Diverging',
      panicCooldown: { active: true, cooldownEndDate: '2026-08-14' },
      circuitBlocked: false,
      hkHealth: { tradeDate: '2026-08-14', regime: 'Weak', panicCooldown: { active: false }, circuitBlocked: false },
    });
    // Override audit data: no extras, only missing rows.
    vi.mocked(useBehaviorAuditQuery).mockReturnValue({
      data: [
        {
          auditDate: '2026-08-13',
          market: 'HK',
          expected: 19,
          actual: 0,
          extra: 0,
          missing: 19,
          missingList: [{ symbol: 'HK:02359', score: 97 }],
        },
      ],
      isLoading: false,
    } as never);
    renderBanner();
    await waitFor(() => {
      expect(screen.getByText(/无待操作提醒/)).toBeTruthy();
      expect(screen.getByText(/已隐藏 1 条该持没买/)).toBeTruthy();
      expect(screen.queryByText(/该持没买：/)).toBeNull();
      expect(screen.queryByText(/⚠ 行为与 S-3 回测不一致/)).toBeNull();
    });
  });
});
