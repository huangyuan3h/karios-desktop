import { describe, expect, it } from 'vitest';

import {
  WATCHLIST_COLUMN_HELP,
  buildWatchlistColumnTooltipBody,
  getWatchlistColumnHelp,
} from './watchlist-column-help';

describe('WATCHLIST_COLUMN_HELP registry', () => {
  it('covers every header rendered in WatchlistTable', () => {
    // Mirror the column order in WatchlistTable.tsx (2026-08-01 audit).
    const expectedColumns = [
      'color',
      'symbol',
      'name',
      'industry',
      'positionPct',
      'costPrice',
      'currentPrice',
      'stopLoss',
      'execAction',
      'trigger',
      'trail',
      'buy',
      'hotTop3',
      'rs',
      'vwap',
      'intradayPct',
      'volumeRatio',
      'instFlow',
      'gap',
      'alerts',
      'pnl',
      'score',
      'trendOk',
      'action',
    ];
    const missing = expectedColumns.filter((c) => !WATCHLIST_COLUMN_HELP[c]);
    expect(missing, `Missing help entries: ${missing.join(', ')}`).toEqual([]);
  });

  it('every entry has non-empty label, short, detail', () => {
    for (const [id, h] of Object.entries(WATCHLIST_COLUMN_HELP)) {
      expect(h.id, id).toBe(id);
      expect(h.label.length, id).toBeGreaterThan(0);
      expect(h.short.length, id).toBeGreaterThan(10);
      // detail is ReactNode; either string content or rendered tree.
      const isNonEmpty = typeof h.detail === 'string' ? h.detail.length > 5 : h.detail != null;
      expect(isNonEmpty, id).toBe(true);
    }
  });

  it('every entry has a unit OR explains qualitative value', () => {
    // Some columns are categorical (color, execAction, action) and have no unit.
    const categoricalIds = new Set([
      'color',
      'execAction',
      'trigger',
      'buy',
      'alerts',
      'industry',
      'name',
      'symbol',
      'gap',
      'hotTop3',
      'trendOk',
    ]);
    for (const [id, h] of Object.entries(WATCHLIST_COLUMN_HELP)) {
      if (categoricalIds.has(id)) continue;
      // We don't strictly require a unit, but short must mention unit/format hint.
      const blob = h.short + String(h.detail ?? '');
      const hasUnit = /(元|¥|%|x|股|金额|价格|主力|实时|浮|位|级|0-100|score|积分|评分)/i.test(blob);
      expect(hasUnit, `${id} should mention a unit or format`).toBe(true);
    }
  });
});

describe('getWatchlistColumnHelp', () => {
  it('returns known entry', () => {
    expect(getWatchlistColumnHelp('rs').id).toBe('rs');
    expect(getWatchlistColumnHelp('rs').label).toMatch(/RS|相对/);
  });

  it('returns fallback for unknown id (does not throw)', () => {
    const out = getWatchlistColumnHelp('does_not_exist');
    expect(out.id).toBe('does_not_exist');
    expect(out.short).toBe('');
  });
});

describe('buildWatchlistColumnTooltipBody', () => {
  it('renders label + sub + short + detail into ReactNode', () => {
    const h = getWatchlistColumnHelp('rs');
    const node = buildWatchlistColumnTooltipBody(h);
    expect(node).toBeTruthy();
  });

  it('respects optional hint', () => {
    const h = getWatchlistColumnHelp('stopLoss');
    const node = buildWatchlistColumnTooltipBody(h, { hint: 'extra tip' });
    expect(node).toBeTruthy();
  });
});
