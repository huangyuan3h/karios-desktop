import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS,
  DASHBOARD_AUTO_SYNC_TRADING_MS,
  DASHBOARD_POLL_MS,
  dashboardAutoSyncIntervalMs,
  dashboardRefetchIntervalMs,
} from './intervals';

vi.mock('@/lib/market-hours', () => ({
  isShanghaiSyncWindow: vi.fn(),
  isShanghaiTradingTime: vi.fn(),
}));

import { isShanghaiSyncWindow, isShanghaiTradingTime } from '@/lib/market-hours';

afterEach(() => {
  vi.mocked(isShanghaiTradingTime).mockReset();
  vi.mocked(isShanghaiSyncWindow).mockReset();
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

describe('dashboardAutoSyncIntervalMs', () => {
  it('returns the trading cadence during trading hours', () => {
    vi.mocked(isShanghaiTradingTime).mockReturnValue(true);
    vi.mocked(isShanghaiSyncWindow).mockReturnValue(true);
    expect(dashboardAutoSyncIntervalMs()).toBe(DASHBOARD_AUTO_SYNC_TRADING_MS);
  });

  it('returns the after-hours cadence inside the quote window', () => {
    vi.mocked(isShanghaiTradingTime).mockReturnValue(false);
    vi.mocked(isShanghaiSyncWindow).mockReturnValue(true);
    expect(dashboardAutoSyncIntervalMs()).toBe(DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS);
  });

  it('returns false off-hours', () => {
    vi.mocked(isShanghaiTradingTime).mockReturnValue(false);
    vi.mocked(isShanghaiSyncWindow).mockReturnValue(false);
    expect(dashboardAutoSyncIntervalMs()).toBe(false);
  });
});
