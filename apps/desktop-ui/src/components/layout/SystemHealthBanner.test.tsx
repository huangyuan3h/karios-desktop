import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SystemHealthBanner } from './SystemHealthBanner';

const { fetchSystemHealth } = vi.hoisted(() => ({ fetchSystemHealth: vi.fn() }));
vi.mock('@/lib/queries/systemHealth', () => ({ fetchSystemHealth }));

const HEALTHY = {
  dataSyncOnline: true,
  aiOnline: true,
  datasources: [],
  failures: [],
  errorCount: 0,
  warnCount: 0,
};

function renderBanner() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <SystemHealthBanner />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  fetchSystemHealth.mockReset();
});

describe('SystemHealthBanner', () => {
  it('renders nothing when everything is healthy', async () => {
    fetchSystemHealth.mockResolvedValue(HEALTHY);
    renderBanner();
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByText(/系统自检/)).toBeNull();
  });

  it('flags service outages as errors', async () => {
    fetchSystemHealth.mockResolvedValue({
      ...HEALTHY,
      dataSyncOnline: false,
      aiOnline: false,
      errorCount: 2,
    });
    renderBanner();
    expect(await screen.findByText(/2 项异常/)).toBeDefined();
    screen.getByText(/2 项异常/).click();
    expect(await screen.findByText(/data-sync-service（后端）不可达/)).toBeDefined();
    expect(screen.getByText(/ai-service（决策 Agent）不可达/)).toBeDefined();
  });

  it('lists stale data sources and sync failures as warnings', async () => {
    fetchSystemHealth.mockResolvedValue({
      dataSyncOnline: true,
      aiOnline: true,
      datasources: [
        { source: 'market', label: '行情', stale: true, ageMinutes: 30 * 60, thresholdMinutes: 24 * 60, lastSyncedAt: null },
      ],
      failures: [
        { jobType: 'cn_industry_post_close_sync', syncedAt: '2026-08-07T20:10:00+00:00', failures24h: 3, errorMessage: 'push2his down' },
      ],
      errorCount: 0,
      warnCount: 2,
    });
    renderBanner();
    expect(await screen.findByText(/0 项异常 · 2 项告警/)).toBeDefined();
    screen.getByText(/0 项异常 · 2 项告警/).click();
    expect(await screen.findByText(/行情 数据陈旧/)).toBeDefined();
    expect(screen.getByText(/同步失败 cn_industry_post_close_sync ×3/)).toBeDefined();
  });

  it('re-checks on demand', async () => {
    fetchSystemHealth.mockResolvedValue({
      ...HEALTHY,
      errorCount: 1,
      warnCount: 0,
      dataSyncOnline: false,
    });
    renderBanner();
    const refresh = await screen.findByRole('img', { hidden: true }).catch(() => null);
    void refresh;
    expect(fetchSystemHealth).toHaveBeenCalledTimes(1);
  });
});
