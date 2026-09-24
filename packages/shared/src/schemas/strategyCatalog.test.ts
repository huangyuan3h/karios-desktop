import { describe, expect, it } from 'vitest';

import { StrategyCatalogSchema } from './strategyCatalog';

const entry = {
  key: 'harbor',
  name: '港湾',
  structure: 'S-3 + 停车场',
  status: 'live',
  statusLabel: 'Live · 日落',
  role: 'live',
  roleLabel: 'Live 底座',
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

const REGIME = {
  fit: ['高波动 + 指数弱于 MA200'],
  unfit: ['停车资产单日 ±10% 抽搐'],
  evidence: [{ label: '2025', value: 'v1 +32.6 / v2 +66.0 / Δ+33.4' }],
  note: '只描述，不作闸门',
};

describe('StrategyCatalogSchema', () => {
  it('parses a catalog payload', () => {
    const out = StrategyCatalogSchema.parse({ ok: true, strategies: [entry] });
    expect(out.strategies[0]?.name).toBe('港湾');
  });

  it('accepts a null timelineStrategy for legacy rows', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [{ ...entry, key: 'starport', status: 'rejected', timelineStrategy: null }],
    });
    expect(out.strategies[0]?.timelineStrategy).toBeNull();
  });

  it('accepts the defensive homeport row pointing at the M30 timeline', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [
        {
          ...entry,
          key: 'homeport',
          status: 'product_candidate',
          role: 'defense',
          roleLabel: '防守',
          timelineStrategy: 'homeport_m30',
        },
      ],
    });
    expect(out.strategies[0]?.timelineStrategy).toBe('homeport_m30');
    expect(out.strategies[0]?.role).toBe('defense');
  });

  it('accepts the parallel twin_star comparison entry', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [
        {
          ...entry,
          key: 'twin_star',
          name: '双子星',
          status: 'parallel_candidate',
          statusLabel: '并行对照',
          role: 'balanced',
          roleLabel: '对照',
          timelineStrategy: 'twin_star',
        },
      ],
    });
    expect(out.strategies[0]?.status).toBe('parallel_candidate');
    expect(out.strategies[0]?.timelineStrategy).toBe('twin_star');
  });

  it('accepts the canonical H2-a25 variant metadata', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [
        {
          ...entry,
          key: 'starship_robust',
          canonical: true,
          variant: {
            sleeveMode: 'h2',
            hystBand: 0.02,
            sleeveWeight: 0.25,
            b3Weight: 0.75,
          },
          risk: 'K3 failed: long MDD delta -1.6pt',
        },
      ],
    });
    expect(out.strategies[0]?.canonical).toBe(true);
    expect(out.strategies[0]?.variant?.hystBand).toBe(0.02);
    expect(out.strategies[0]?.risk).toContain('K3');
  });

  it('accepts an optional regime map and omits it for rows without one', () => {
    const out = StrategyCatalogSchema.parse({
      ok: true,
      strategies: [{ ...entry, key: 'starship', regime: REGIME }],
    });
    expect(out.strategies[0]?.regime?.fit[0]).toContain('高波动');
    expect(out.strategies[0]?.regime?.evidence[0]?.label).toBe('2025');
    const noRegime = StrategyCatalogSchema.parse({ ok: true, strategies: [entry] });
    expect(noRegime.strategies[0]?.regime).toBeUndefined();
  });

  it('rejects unknown statuses and missing windows', () => {
    expect(() =>
      StrategyCatalogSchema.parse({ ok: true, strategies: [{ ...entry, status: 'maybe' }] }),
    ).toThrow();
    const { windows, ...rest } = entry;
    expect(windows.OOS2.total).toBe(55.2);
    expect(() => StrategyCatalogSchema.parse({ ok: true, strategies: [rest] })).toThrow();
  });
});
