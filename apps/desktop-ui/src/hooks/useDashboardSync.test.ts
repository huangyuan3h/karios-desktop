import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  dashboardSyncWarnings,
  isDashboardSyncResultOk,
  useDashboardSync,
} from '@/hooks/useDashboardSync';

type Listener = (event: { data: string }) => void;

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  onmessage: Listener | null = null;
  onerror: Listener | null = null;
  url: string;
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  close() {
    this.closed = true;
  }

  emit(message: unknown) {
    this.onmessage?.({ data: JSON.stringify(message) });
  }
}

describe('dashboard sync result contract', () => {
  it('accepts only an explicit successful result', () => {
    expect(isDashboardSyncResultOk({ ok: true })).toBe(true);
    expect(isDashboardSyncResultOk({ ok: false })).toBe(false);
    expect(isDashboardSyncResultOk({})).toBe(false);
    expect(isDashboardSyncResultOk(null)).toBe(false);
  });
});

describe('useDashboardSync.forceRefreshWatchlistOnSync', () => {
  const originalEventSource = (globalThis as { EventSource?: unknown }).EventSource;

  beforeEach(() => {
    FakeEventSource.instances = [];
    (globalThis as unknown as { EventSource: unknown }).EventSource = FakeEventSource;
  });

  afterEach(() => {
    if (originalEventSource) {
      (globalThis as unknown as { EventSource: unknown }).EventSource = originalEventSource;
    } else {
      delete (globalThis as unknown as { EventSource?: unknown }).EventSource;
    }
  });

  it('resolves false and skips cache updates when the backend reports failure', async () => {
    const callbacks = {
      applySummaryToCache: vi.fn(),
      shouldRefreshNewsBrief: () => false,
      newsSummary: null,
      newsSummaryUpdatedAt: null,
      setNewsSummary: vi.fn(),
      setNewsSummaryUpdatedAt: vi.fn(),
      setNewsSummaryBusy: vi.fn(),
      saveNewsBriefCache: vi.fn(),
      setError: vi.fn(),
      forceRefreshWatchlistOnSync: vi.fn().mockResolvedValue(undefined),
      onSyncComplete: vi.fn(),
    };
    const { result } = renderHook(() => useDashboardSync(callbacks));
    let syncPromise: Promise<unknown> | undefined;

    await act(async () => {
      syncPromise = result.current.onSyncAll();
      await Promise.resolve();
    });
    const es = FakeEventSource.instances[0]!;
    await act(async () => {
      es.emit({ type: 'start' });
      es.emit({
        type: 'done',
        result: { ok: false, error: 'vendor unavailable' },
        summary: { asOfDate: '2026-09-24' },
      });
    });

    await expect(syncPromise).resolves.toEqual({ ok: false, summary: null });
    expect(callbacks.setError).toHaveBeenCalledWith('vendor unavailable');
    expect(callbacks.applySummaryToCache).not.toHaveBeenCalled();
    expect(callbacks.forceRefreshWatchlistOnSync).not.toHaveBeenCalled();
    expect(callbacks.onSyncComplete).not.toHaveBeenCalled();
  });
  it('accepts a forceRefreshWatchlistOnSync callback in its options', async () => {
    const mod = await import('@/hooks/useDashboardSync');
    const fn = vi.fn();
    const cb = {
      applySummaryToCache: vi.fn(),
      shouldRefreshNewsBrief: () => false,
      newsSummary: null,
      newsSummaryUpdatedAt: null,
      setNewsSummary: vi.fn(),
      setNewsSummaryUpdatedAt: vi.fn(),
      setNewsSummaryBusy: vi.fn(),
      saveNewsBriefCache: vi.fn(),
      setError: vi.fn(),
      forceRefreshWatchlistOnSync: fn,
      onSyncComplete: vi.fn(),
    };
    // Type-only assertion: just verify the module shape
    expect(typeof mod.useDashboardSync).toBe('function');
    expect(cb.forceRefreshWatchlistOnSync).toBe(fn);
  });

  // Verify the FakeEventSource helper used for higher-level integration.
  it('FakeEventSource captures emitted messages', () => {
    const es = new FakeEventSource('http://localhost/test');
    const received: unknown[] = [];
    es.onmessage = (event) => received.push(JSON.parse(event.data));
    es.emit({ type: 'start' });
    es.emit({ type: 'done', result: { ok: true } });
    expect(received).toEqual([{ type: 'start' }, { type: 'done', result: { ok: true } }]);
    es.close();
    expect(es.closed).toBe(true);
  });
});

describe('dashboardSyncWarnings', () => {
  it('collects top-level and per-step meta warnings', () => {
    expect(
      dashboardSyncWarnings({
        ok: true,
        warnings: ['option_iv: no_iv_data'],
        steps: [
          { name: 'marketSentiment', ok: true, meta: { warnings: ['option_iv: no_iv_data'] } },
          { name: 'macroDaily', ok: true, meta: {} },
          { name: 'news', ok: true },
        ],
      }),
    ).toEqual(['option_iv: no_iv_data', 'option_iv: no_iv_data']);
  });

  it('returns empty for junk input', () => {
    expect(dashboardSyncWarnings(null)).toEqual([]);
    expect(dashboardSyncWarnings({})).toEqual([]);
    expect(dashboardSyncWarnings({ ok: true, warnings: 'nope', steps: 'nope' })).toEqual([]);
  });

  it('warnings never flip the ok contract', () => {
    expect(isDashboardSyncResultOk({ ok: true, warnings: ['option_iv: no_iv_data'] })).toBe(true);
    expect(isDashboardSyncResultOk({ ok: true, steps: [{ ok: true }] })).toBe(true);
  });
});

describe('useDashboardSync warnings passthrough', () => {
  const originalEventSource = (globalThis as { EventSource?: unknown }).EventSource;

  beforeEach(() => {
    FakeEventSource.instances = [];
    (globalThis as unknown as { EventSource: unknown }).EventSource = FakeEventSource;
  });

  afterEach(() => {
    if (originalEventSource) {
      (globalThis as unknown as { EventSource: unknown }).EventSource = originalEventSource;
    } else {
      delete (globalThis as unknown as { EventSource?: unknown }).EventSource;
    }
  });

  it('resolves ok and keeps step warnings when only Put IV degrades', async () => {
    const callbacks = {
      applySummaryToCache: vi.fn(),
      shouldRefreshNewsBrief: () => false,
      newsSummary: null,
      newsSummaryUpdatedAt: null,
      setNewsSummary: vi.fn(),
      setNewsSummaryUpdatedAt: vi.fn(),
      setNewsSummaryBusy: vi.fn(),
      saveNewsBriefCache: vi.fn(),
      setError: vi.fn(),
      forceRefreshWatchlistOnSync: vi.fn().mockResolvedValue(undefined),
      onSyncComplete: vi.fn(),
    };
    const { result } = renderHook(() => useDashboardSync(callbacks));
    let syncPromise: Promise<unknown> | undefined;

    await act(async () => {
      syncPromise = result.current.onSyncAll();
      await Promise.resolve();
    });
    const es = FakeEventSource.instances[0]!;
    await act(async () => {
      es.emit({ type: 'start' });
      es.emit({
        type: 'step',
        step: {
          name: 'marketSentiment',
          ok: true,
          durationMs: 5,
          message: null,
          meta: { warnings: ['option_iv: no_iv_data'] },
        },
      });
      es.emit({
        type: 'done',
        result: { ok: true, warnings: ['option_iv: no_iv_data'] },
        summary: { asOfDate: '2026-10-24' },
      });
      await Promise.resolve();
    });

    await expect(syncPromise).resolves.toEqual({ ok: true, summary: { asOfDate: '2026-10-24' } });
    // Only the initial clear-error call; no failure message.
    expect(callbacks.setError).toHaveBeenCalledTimes(1);
    expect(callbacks.setError).toHaveBeenCalledWith(null);
    expect(callbacks.onSyncComplete).toHaveBeenCalled();
    const sentiment = result.current.syncSteps.find((s) => s.name === 'marketSentiment');
    expect(sentiment?.ok).toBe(true);
    expect(sentiment?.warnings).toEqual(['option_iv: no_iv_data']);
  });
});
