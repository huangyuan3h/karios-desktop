import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useDashboardAutoSync } from '@/hooks/useDashboardAutoSync';
import {
  DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS,
  DASHBOARD_AUTO_SYNC_TRADING_MS,
} from '@/lib/queries/intervals';

vi.mock('@/lib/market-hours', () => ({
  isShanghaiSyncWindow: vi.fn(),
  isShanghaiTradingTime: vi.fn(),
}));

vi.mock('@/lib/queries/intervals', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/intervals')>();
  return {
    ...actual,
    dashboardAutoSyncIntervalMs: vi.fn(),
  };
});

import { isShanghaiSyncWindow } from '@/lib/market-hours';
import { dashboardAutoSyncIntervalMs } from '@/lib/queries/intervals';

describe('useDashboardAutoSync', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.mocked(isShanghaiSyncWindow).mockReturnValue(true);
    vi.mocked(dashboardAutoSyncIntervalMs).mockReturnValue(DASHBOARD_AUTO_SYNC_TRADING_MS);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('catches up shortly after mount inside the window', async () => {
    const onSync = vi.fn().mockResolvedValue({ ok: true });
    const { result } = renderHook(() => useDashboardAutoSync({ onSync, busy: false }));

    expect(onSync).not.toHaveBeenCalled();
    await act(async () => {
      vi.advanceTimersByTime(2000);
    });
    expect(onSync).toHaveBeenCalledTimes(1);
    expect(result.current.lastAutoSyncAt).not.toBeNull();
  });

  it('repeats on the trading interval and never overlaps while busy', async () => {
    const onSync = vi.fn().mockResolvedValue({ ok: true });
    const { rerender } = renderHook(({ busy }) => useDashboardAutoSync({ onSync, busy }), {
      initialProps: { busy: false },
    });

    await act(async () => {
      vi.advanceTimersByTime(2000);
    });
    expect(onSync).toHaveBeenCalledTimes(1);

    // Mark busy: the next tick is skipped.
    rerender({ busy: true });
    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_TRADING_MS);
    });
    expect(onSync).toHaveBeenCalledTimes(1);

    // Idle again: ticks resume.
    rerender({ busy: false });
    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_TRADING_MS);
    });
    expect(onSync).toHaveBeenCalledTimes(2);
  });

  it('does nothing outside the sync window', async () => {
    vi.mocked(isShanghaiSyncWindow).mockReturnValue(false);
    vi.mocked(dashboardAutoSyncIntervalMs).mockReturnValue(false);
    const onSync = vi.fn().mockResolvedValue({ ok: true });
    const { result } = renderHook(() => useDashboardAutoSync({ onSync, busy: false }));

    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_TRADING_MS * 3);
    });
    expect(onSync).not.toHaveBeenCalled();
    expect(result.current.lastAutoSyncAt).toBeNull();
  });

  it('uses the after-hours period when trading is over but the quote window is open', async () => {
    vi.mocked(dashboardAutoSyncIntervalMs).mockReturnValue(DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS);
    const onSync = vi.fn().mockResolvedValue({ ok: true });
    renderHook(() => useDashboardAutoSync({ onSync, busy: false }));

    await act(async () => {
      vi.advanceTimersByTime(2000);
    });
    expect(onSync).toHaveBeenCalledTimes(1);

    // Trading period elapses with no extra sync — only after-hours period ticks.
    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_TRADING_MS);
    });
    expect(onSync).toHaveBeenCalledTimes(1);

    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_AFTER_HOURS_MS - DASHBOARD_AUTO_SYNC_TRADING_MS);
    });
    expect(onSync).toHaveBeenCalledTimes(2);
  });

  it('cleans up timers on unmount', async () => {
    const onSync = vi.fn().mockResolvedValue({ ok: true });
    const { unmount } = renderHook(() => useDashboardAutoSync({ onSync, busy: false }));
    unmount();
    await act(async () => {
      vi.advanceTimersByTime(DASHBOARD_AUTO_SYNC_TRADING_MS * 5);
    });
    expect(onSync).not.toHaveBeenCalled();
  });
});
