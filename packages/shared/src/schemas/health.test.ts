import { describe, expect, it } from 'vitest';
import { DatasourcesResponseSchema } from './health';

const BASE_SOURCE = {
  source: 'market',
  label: '行情',
  lastSyncedAt: '2026-09-06T09:00:00+00:00',
  ageMinutes: 10,
  thresholdMinutes: 1440,
  stale: false,
};

describe('DatasourcesResponseSchema', () => {
  it('accepts a plain source row without quota', () => {
    const out = DatasourcesResponseSchema.parse({
      ok: true,
      generatedAt: '2026-09-06T09:10:00+00:00',
      sources: [BASE_SOURCE],
    });
    expect(out.sources).toHaveLength(1);
    expect(out.tushare_quota).toBeUndefined();
  });

  it('accepts eastmoney_probe extras and tushare_quota', () => {
    const out = DatasourcesResponseSchema.parse({
      ok: true,
      generatedAt: '2026-09-06T09:10:00+00:00',
      sources: [
        {
          ...BASE_SOURCE,
          source: 'eastmoney_probe',
          label: '东财出口探针',
          stale: true,
          banLatched: true,
          cooldownRemainingS: 100,
          failingHosts: ['push2.eastmoney.com'],
        },
      ],
      tushare_quota: {
        configured: true,
        keyCount: 2,
        rotations: 1,
        keys: [{ index: 0, suffix: 'tAAA', minuteUsed: 5, minuteLimit: 200, coolingSeconds: 0 }],
        daily: { index_global: { limit: 100, usedTotal: 7 } },
      },
    });
    expect(out.sources[0].banLatched).toBe(true);
    expect(out.tushare_quota?.keys?.[0].suffix).toBe('tAAA');
  });

  it('accepts unconfigured quota', () => {
    const out = DatasourcesResponseSchema.parse({
      ok: true,
      generatedAt: '2026-09-06T09:10:00+00:00',
      sources: [],
      tushare_quota: { configured: false },
    });
    expect(out.tushare_quota?.configured).toBe(false);
  });

  it('rejects a source missing required staleness fields', () => {
    expect(() =>
      DatasourcesResponseSchema.parse({
        ok: true,
        generatedAt: 'x',
        sources: [{ source: 'market', label: '行情' }],
      }),
    ).toThrow();
  });
});
