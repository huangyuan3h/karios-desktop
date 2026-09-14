import { describe, expect, it } from 'vitest';

import { StrategyCatalogSchema } from './strategyCatalog';

const entry = {
  key: 'harbor',
  name: '港湾',
  structure: 'S-3 + 停车场',
  status: 'live',
  statusLabel: 'Live',
  timelineStrategy: 'harbor',
  doc: 'docs/backtests/stable/etf-parking-baseline-2026-09-13.md',
  tag: 'harbor-p1-20260913',
  updated: '2026-09-14',
  windows: {
    OOS2: { total: 55.2, cagr: 58.0, mdd: -14.3, sharpe: 1.73 },
    train: { total: 52.2, cagr: 138.2, mdd: -8.0, sharpe: 3.01 },
    valid: { total: 50.3, cagr: 156.6, mdd: -21.8, sharpe: 2.14 },
    long: { total: 201.5, cagr: 25.7, mdd: -22.8, sharpe: 1.0 },
  },
  pros: ['四窗全正'],
  cons: ['回撤深'],
};

describe('StrategyCatalogSchema', () => {
  it('parses a catalog payload', () => {
    const out = StrategyCatalogSchema.parse({ ok: true, strategies: [entry] });
    expect(out.strategies[0]?.name).toBe('港湾');
  });

  it('accepts a null timelineStrategy for legacy rows', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [{ ...entry, key: 'twin_star', status: 'rejected', timelineStrategy: null }],
    });
    expect(out.strategies[0]?.timelineStrategy).toBeNull();
  });

  it('accepts the parallel twin_star entry with its timeline', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [
        { ...entry, key: 'twin_star', status: 'parallel_candidate', timelineStrategy: 'twin_star' },
      ],
    });
    expect(out.strategies[0]?.timelineStrategy).toBe('twin_star');
    expect(out.strategies[0]?.status).toBe('parallel_candidate');
  });

  it('rejects unknown statuses and missing windows', () => {
    expect(() =>
      StrategyCatalogSchema.parse({ ok: true, strategies: [{ ...entry, status: 'maybe' }] }),
    ).toThrow();
    const { windows: _windows, ...rest } = entry;
    expect(() => StrategyCatalogSchema.parse({ ok: true, strategies: [rest] })).toThrow();
  });
});
