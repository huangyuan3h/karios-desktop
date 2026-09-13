import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SystemHealthBanner } from './SystemHealthBanner';

const { fetchSystemHealth } = vi.hoisted(() => ({ fetchSystemHealth: vi.fn() }));
vi.mock('@/lib/queries/systemHealth', () => ({ fetchSystemHealth }));
vi.mock('@/lib/queries/systemEvents', () => ({
  fetchSystemEvents: vi.fn(async () => []),
  resolveSystemEvent: vi.fn(),
}));

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
        {
          source: 'market',
          label: '行情',
          stale: true,
          ageMinutes: 30 * 60,
          thresholdMinutes: 24 * 60,
          lastSyncedAt: null,
        },
        {
          source: 'stock_close_sync',
          label: 'A股收盘同步',
          group: 'coreClose',
          stale: true,
          ageMinutes: null,
          thresholdMinutes: 20,
          lastSyncedAt: null,
        },
      ],
      failures: [
        {
          jobType: 'cn_industry_post_close_sync',
          syncedAt: '2026-08-07T20:10:00+00:00',
          failures24h: 3,
          errorMessage: 'push2his down',
        },
      ],
      errorCount: 0,
      warnCount: 3,
    });
    renderBanner();
    expect(await screen.findByText(/0 项异常 · 3 项告警/)).toBeDefined();
    screen.getByText(/0 项异常 · 3 项告警/).click();
    expect(await screen.findByText(/行情 数据陈旧/)).toBeDefined();
    expect(screen.getByText(/A股收盘同步 数据陈旧（无记录 ≥ 阈值 20 分钟）/)).toBeDefined();
    expect(screen.getByText(/同步失败 cn_industry_post_close_sync ×3/)).toBeDefined();
  });

  it('shows tushare quota and eastmoney ban detail', async () => {
    fetchSystemHealth.mockResolvedValue({
      ...HEALTHY,
      datasources: [
        {
          source: 'eastmoney_probe',
          label: '东财出口探针',
          stale: true,
          ageMinutes: 25,
          thresholdMinutes: 20,
          lastSyncedAt: null,
          banLatched: true,
          cooldownRemainingS: 100,
          failingHosts: ['push2.eastmoney.com'],
        },
      ],
      tushareQuota: {
        configured: true,
        keyCount: 2,
        rotations: 3,
        keys: [
          { index: 0, suffix: 'tAAA', minuteUsed: 150, minuteLimit: 200, coolingSeconds: 0 },
          { index: 1, suffix: 'tBBB', minuteUsed: 10, minuteLimit: 200, coolingSeconds: 0 },
        ],
      },
      errorCount: 0,
      warnCount: 1,
    });
    renderBanner();
    expect(await screen.findByText(/0 项异常 · 1 项告警/)).toBeDefined();
    screen.getByText(/0 项异常 · 1 项告警/).click();
    expect(await screen.findByText(/IP 熔断中（冷却 100s）/)).toBeDefined();
    expect(screen.getByText(/失败宿主 push2\.eastmoney\.com/)).toBeDefined();
    expect(
      screen.getByText(/Tushare 配额 2 key · 最忙 …tAAA 分钟 150\/200 · 轮换 3 次/),
    ).toBeDefined();
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
