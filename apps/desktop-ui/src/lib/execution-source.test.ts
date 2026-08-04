import { describe, expect, it, vi } from 'vitest';

import {
  buildSourceContext,
  formatSourceAttributionMarkdown,
  inferSource,
  SOURCE_DISPLAY_ORDER,
  withSymbol,
  type SourceStatsResponse,
} from './execution-source';

describe('inferSource', () => {
  it('returns TV when symbol is in TV screener set', () => {
    const ctx = buildSourceContext({ tvSymbols: ['CN:600000'], alphaSymbols: ['CN:600519'] });
    expect(inferSource('CN:600000', ctx)).toBe('TV');
  });

  it('returns ALPHA when symbol is only in alpha catalyst set', () => {
    const ctx = buildSourceContext({ tvSymbols: ['CN:600000'], alphaSymbols: ['CN:600519'] });
    expect(inferSource('CN:600519', ctx)).toBe('ALPHA');
  });

  it('returns MANUAL when symbol is in neither set', () => {
    const ctx = buildSourceContext({ tvSymbols: ['CN:600000'], alphaSymbols: [] });
    expect(inferSource('CN:601398', ctx)).toBe('MANUAL');
  });

  it('is case-insensitive on symbol and sets', () => {
    const ctx = buildSourceContext({ tvSymbols: ['cn:600000'], alphaSymbols: ['CN:600519'] });
    expect(inferSource('CN:600000', ctx)).toBe('TV');
    expect(inferSource('cn:600519', ctx)).toBe('ALPHA');
  });

  it('returns MANUAL for empty symbol', () => {
    expect(inferSource('', buildSourceContext({ tvSymbols: ['X'], alphaSymbols: [] }))).toBe(
      'MANUAL',
    );
  });
});

describe('buildSourceContext / withSymbol', () => {
  it('normalizes symbols to uppercase in sets', () => {
    const ctx = buildSourceContext({ tvSymbols: ['cn:600000'], alphaSymbols: ['cn:600519'] });
    expect(ctx.tvSymbols.has('CN:600000')).toBe(true);
    expect(ctx.alphaSymbols.has('CN:600519')).toBe(true);
  });

  it('withSymbol adds TV symbol without mutating original', () => {
    const ctx = buildSourceContext({ tvSymbols: ['CN:600000'], alphaSymbols: [] });
    const next = withSymbol(ctx, 'cn:600519', 'TV');
    expect(next.tvSymbols.has('CN:600519')).toBe(true);
    expect(ctx.tvSymbols.has('CN:600519')).toBe(false);
  });

  it('withSymbol adds ALPHA symbol', () => {
    const ctx = buildSourceContext({ tvSymbols: [], alphaSymbols: [] });
    const next = withSymbol(ctx, 'CN:600519', 'ALPHA');
    expect(next.alphaSymbols.has('CN:600519')).toBe(true);
    expect(next.tvSymbols.size).toBe(0);
  });

  it('withSymbol ignores MANUAL', () => {
    const ctx = buildSourceContext({ tvSymbols: [], alphaSymbols: [] });
    const next = withSymbol(ctx, 'CN:600519', 'MANUAL');
    expect(next.tvSymbols.size).toBe(0);
    expect(next.alphaSymbols.size).toBe(0);
  });
});

describe('formatSourceAttributionMarkdown', () => {
  const stats: SourceStatsResponse = {
    sinceDays: 30,
    lookbackDays: 30,
    generatedAt: '2026-08-05T00:00:00Z',
    bySource: {
      TV: { buySignals: 12, closed: 8, wins: 5, losses: 3, winRate: 0.625 },
      ALPHA: { buySignals: 4, closed: 2, wins: 2, losses: 0, winRate: 1 },
      MANUAL: { buySignals: 3, closed: 1, wins: 0, losses: 1, winRate: 0 },
    },
    openTradesBySource: { TV: 4, ALPHA: 1 },
  };

  it('renders table with win-rate percentages', () => {
    const md = formatSourceAttributionMarkdown(stats);
    expect(md).toContain('## Execution · Source attribution (30d)');
    expect(md).toContain('| TV | 12 | 8 | 5 | 3 | 62.5% | 4 |');
    expect(md).toContain('| ALPHA | 4 | 2 | 2 | 0 | 100.0% | 1 |');
    expect(md).toContain('| MANUAL | 3 | 1 | 0 | 1 | 0.0% | 0 |');
  });

  it('emits note when no buckets exist', () => {
    const empty: SourceStatsResponse = {
      sinceDays: 30,
      lookbackDays: 30,
      generatedAt: '2026-08-05T00:00:00Z',
      bySource: {},
      openTradesBySource: {},
    };
    const md = formatSourceAttributionMarkdown(empty);
    expect(md).toContain('- note: no BUY signals / closed trades in window yet');
  });

  it('includes UNKNOWN bucket when present', () => {
    const withUnknown: SourceStatsResponse = {
      ...stats,
      bySource: {
        ...stats.bySource,
        UNKNOWN: { buySignals: 9, closed: 6, wins: 3, losses: 3, winRate: 0.5 },
      },
    };
    const md = formatSourceAttributionMarkdown(withUnknown);
    expect(md).toContain('| UNKNOWN | 9 | 6 | 3 | 3 | 50.0% | 0 |');
  });

  it('honors heading option', () => {
    const md = formatSourceAttributionMarkdown(stats, { heading: '###' });
    expect(md).toContain('### Execution · Source attribution (30d)');
  });

  it('SOURCE_DISPLAY_ORDER is stable', () => {
    expect(SOURCE_DISPLAY_ORDER).toEqual(['TV', 'ALPHA', 'MANUAL', 'UNKNOWN']);
  });
});

describe('fetchSourceStats', () => {
  it('fetches from /v1/execution/source-stats with sinceDays', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        sinceDays: 30,
        lookbackDays: 30,
        generatedAt: 'x',
        bySource: {},
        openTradesBySource: {},
      }),
    });
    vi.stubGlobal('fetch', fetchMock);
    const { fetchSourceStats } = await import('./execution-source');
    const resp = await fetchSourceStats('http://test', 30);
    expect(fetchMock).toHaveBeenCalledWith(
      'http://test/v1/execution/source-stats?sinceDays=30',
      expect.objectContaining({ cache: 'no-store' }),
    );
    expect(resp.sinceDays).toBe(30);
    vi.unstubAllGlobals();
  });

  it('throws on non-ok response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500, text: async () => 'boom' }));
    const { fetchSourceStats } = await import('./execution-source');
    await expect(fetchSourceStats('http://test', 30)).rejects.toThrow(/500/);
    vi.unstubAllGlobals();
  });
});
