import { afterEach, describe, expect, it, vi } from 'vitest';

import { DASHBOARD_POLL_MS, dashboardRefetchIntervalMs } from './intervals';

vi.mock('@/lib/market-hours', () => ({
  isShanghaiTradingTime: vi.fn(),
}));

import { isShanghaiTradingTime } from '@/lib/market-hours';

afterEach(() => {
  vi.mocked(isShanghaiTradingTime).mockReset();
});

describe('dashboardRefetchIntervalMs', () => {
  it('returns poll interval during trading hours', () => {
    vi.mocked(isShanghaiTradingTime).mockReturnValue(true);
    expect(dashboardRefetchIntervalMs()).toBe(DASHBOARD_POLL_MS);
  });

  it('returns false outside trading hours', () => {
    vi.mocked(isShanghaiTradingTime).mockReturnValue(false);
    expect(dashboardRefetchIntervalMs()).toBe(false);
  });
});
