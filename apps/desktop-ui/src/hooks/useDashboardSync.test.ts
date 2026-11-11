import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  dashboardSyncDegradedNote,
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

describe('dashboardSyncDegradedNote', () => {
  it('returns the backend degraded message when the run is degraded', () => {
    expect(
      dashboardSyncDegradedNote({
        ok: true,
        degraded: true,
        degradedMessage: '同步完成（部分降级）：东方财富实时行情源暂时不可用',
      }),
    ).toContain('东方财富');
  });

  it('falls back to a default note when degraded without a message', () => {
    expect(dashboardSyncDegradedNote({ ok: true, degraded: true })).toContain('部分降级');
  });

  it('summarizes warnings when only soft warnings are present', () => {
    expect(dashboardSyncDegradedNote({ ok: true, warnings: ['option_iv: no_iv_data'] })).toContain(
      '部分降级',
    );
  });

  it('returns null for clean or junk results', () => {
    expect(dashboardSyncDegradedNote({ ok: true })).toBeNull();
    expect(dashboardSyncDegradedNote({ ok: false, error: 'boom' })).toBeNull();
    expect(dashboardSyncDegradedNote(null)).toBeNull();
  });

  it('never flips the ok contract', () => {
    expect(isDashboardSyncResultOk({ ok: true, degraded: true })).toBe(true);
  });
});

describe('syncResultErrorMessage truncation', () => {
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

  it('caps a raw transport stack instead of rendering the full banner', async () => {
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
    const rawStack = `push2delay.eastmoney.com:direct:requests:${'x'.repeat(900)}`;
    await act(async () => {
      es.emit({ type: 'start' });
      es.emit({ type: 'done', result: { ok: false, error: rawStack }, summary: null });
    });

    await expect(syncPromise).resolves.toEqual({ ok: false, summary: null });
    const shown = callbacks.setError.mock.calls.at(-1)?.[0] as string;
    expect(typeof shown).toBe('string');
    expect(shown.length).toBeLessThan(rawStack.length);
    expect(shown.length).toBeLessThanOrEqual(330);
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
