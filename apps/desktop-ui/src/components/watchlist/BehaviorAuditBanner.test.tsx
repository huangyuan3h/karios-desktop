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
    useBehaviorAuditQuery: () => ({
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
      ],
      isLoading: false,
    }),
    useRefreshBehaviorAudit: () => ({ mutateAsync: vi.fn(), isPending: false }),
  };
});

describe('BehaviorAuditBanner (OPT-106)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('flags 买了不该买 and 该卖没卖 from extraList', async () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <BehaviorAuditBanner />
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getAllByText(/买了不该买/).length).toBeGreaterThan(0);
      expect(screen.getAllByText(/该卖没卖/).length).toBeGreaterThan(0);
      expect(screen.getByText(/CN:300628/)).toBeTruthy();
      expect(screen.getByText(/CN:600002/)).toBeTruthy();
    });
  });

  it('flags 该持没买 from missingList', async () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <BehaviorAuditBanner />
      </QueryClientProvider>,
    );
    await waitFor(() => {
      expect(screen.getByText(/该持没买/)).toBeTruthy();
      expect(screen.getByText(/HK:00005/)).toBeTruthy();
    });
  });
});
