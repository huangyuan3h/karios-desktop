import { describe, expect, it } from 'vitest';

import { B3StateSchema } from './b3State';

describe('B3StateSchema', () => {
  it('parses a full state payload', () => {
    const out = B3StateSchema.parse({
      ok: true,
      asOf: '2026-09-11',
      rebalanceDate: '2026-09-01',
      month: '2026-09',
      universe: [
        { symbol: '510300.SH', name: '沪深300', targetPct: 24.1, driftPct: 25.3, deltaPct: -1.2 },
      ],
      trades: [{ symbol: '510300.SH', name: '沪深300', side: 'SELL', deltaPct: -1.2 }],
      note: '月频',
    });
    expect(out.universe?.[0]?.name).toBe('沪深300');
    expect(out.trades?.[0]?.side).toBe('SELL');
  });

  it('parses the fail-open payload', () => {
    const out = B3StateSchema.parse({ ok: false, error: 'panel missing' });
    expect(out.ok).toBe(false);
  });

  it('rejects an unknown trade side', () => {
    expect(() =>
      B3StateSchema.parse({
        ok: true,
        trades: [{ symbol: 'x', name: 'x', side: 'HOLD', deltaPct: 1 }],
      }),
    ).toThrow();
  });
});
