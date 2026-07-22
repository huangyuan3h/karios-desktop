import { describe, expect, it } from 'vitest';
import {
  WatchlistItemSchema,
  WatchlistRegistryItemSchema,
  WatchlistRegistryResponseSchema,
} from './watchlist';

describe('WatchlistRegistryItemSchema', () => {
  it('validates registry item from API', () => {
    const item = WatchlistRegistryItemSchema.parse({
      symbol: 'CN:000001',
      name: 'Ping An Bank',
      addedAt: '2026-06-18T08:00:00.000Z',
      source: 'manual',
      color: '#ffffff',
      positionPct: 10,
      costPrice: 10.5,
      maxPrice: 12.0,
      entryDate: '2026-06-18',
    });
    expect(item.symbol).toBe('CN:000001');
    expect(item.source).toBe('manual');
    expect(item.entryDate).toBe('2026-06-18');
  });

  it('accepts screener_fallback source', () => {
    const item = WatchlistRegistryItemSchema.parse({
      symbol: 'CN:000001',
      source: 'screener_fallback',
    });
    expect(item.source).toBe('screener_fallback');
  });
});

describe('WatchlistItemSchema', () => {
  it('accepts client nameStatus enrichment', () => {
    const item = WatchlistItemSchema.parse({
      symbol: 'CN:000001',
      addedAt: '2026-06-18T08:00:00.000Z',
      nameStatus: 'resolved',
    });
    expect(item.nameStatus).toBe('resolved');
  });
});

describe('WatchlistRegistryResponseSchema', () => {
  it('validates GET /watchlist/registry response', () => {
    const res = WatchlistRegistryResponseSchema.parse({
      ok: true,
      count: 1,
      items: [{ symbol: 'CN:000001', addedAt: '2026-06-18T08:00:00.000Z' }],
    });
    expect(res.count).toBe(1);
    expect(res.items[0]?.symbol).toBe('CN:000001');
  });
});
