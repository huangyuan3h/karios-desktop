import { describe, expect, it } from 'vitest';

import { shouldHideForAuditFilter, sortWatchlistItems } from './WatchlistTable';
import type { WatchlistItem } from '@karios/shared';
import type { TrendOkResult } from '@/lib/api/types';

function item(symbol: string, positionPct?: number | null): WatchlistItem {
  return {
    symbol,
    name: symbol,
    addedAt: '2026-08-01',
    positionPct: positionPct ?? null,
  } as WatchlistItem;
}

function trendWith(scores: Record<string, number>): Record<string, TrendOkResult> {
  return Object.fromEntries(
    Object.entries(scores).map(([symbol, score]) => [
      symbol,
      { symbol, score } as unknown as TrendOkResult,
    ]),
  );
}

describe('sortWatchlistItems', () => {
  it('puts held names (positionPct > 0) first, then score desc, then RS desc', () => {
    const items = [
      item('CN:000001', null), // score 70, rs 0.5
      item('CN:000002', 5), // held, score 60
      item('CN:000003', null), // score 90, rs 0.4
      item('CN:000004', null), // score 90, rs 0.8 (tie with 000003 → rs wins)
      item('CN:000005', 0), // position 0 → not held
    ];
    const trend = trendWith({ 'CN:000001': 70, 'CN:000002': 60, 'CN:000003': 90, 'CN:000004': 90, 'CN:000005': 80 });
    const rsRanks = { 'CN:000001': 0.5, 'CN:000002': 0.9, 'CN:000003': 0.4, 'CN:000004': 0.8, 'CN:000005': 0.3 };

    const out = sortWatchlistItems(items, trend, true, 'desc', rsRanks).map((i) => i.symbol);
    expect(out[0]).toBe('CN:000002'); // held first even with lower score
    expect(out.slice(1)).toEqual(['CN:000004', 'CN:000003', 'CN:000005', 'CN:000001']);
  });

  it('keeps holdings first when score sort is disabled', () => {
    const items = [item('CN:000001'), item('CN:000002', 5), item('CN:000003')];
    const out = sortWatchlistItems(items, {}, false, 'desc').map((i) => i.symbol);
    expect(out[0]).toBe('CN:000002');
    expect(new Set(out)).toEqual(new Set(['CN:000001', 'CN:000002', 'CN:000003']));
  });

  it('respects asc direction within the held group', () => {
    const items = [item('CN:000001'), item('CN:000002', 5), item('CN:000003', 8)];
    const trend = trendWith({ 'CN:000001': 70, 'CN:000002': 60, 'CN:000003': 80 });
    const out = sortWatchlistItems(items, trend, true, 'asc').map((i) => i.symbol);
    expect(out.slice(0, 2)).toEqual(['CN:000002', 'CN:000003']); // held asc by score
  });

  it('does not mutate the input array', () => {
    const items = [item('CN:000001'), item('CN:000002', 5)];
    const before = [...items];
    sortWatchlistItems(items, {}, true, 'desc');
    expect(items).toEqual(before);
  });
});

describe('shouldHideForAuditFilter (OPT-106)', () => {
  const extra = new Set(['CN:300628', 'HK:00005']);

  it('hides only flagged symbols when the filter is on', () => {
    expect(shouldHideForAuditFilter('CN:300628', extra, true)).toBe(true);
    expect(shouldHideForAuditFilter('CN:600000', extra, true)).toBe(false);
  });

  it('never hides when the filter is off or the set is empty', () => {
    expect(shouldHideForAuditFilter('CN:300628', extra, false)).toBe(false);
    expect(shouldHideForAuditFilter('CN:300628', undefined, true)).toBe(false);
    expect(shouldHideForAuditFilter('CN:300628', new Set<string>(), true)).toBe(false);
  });
});
