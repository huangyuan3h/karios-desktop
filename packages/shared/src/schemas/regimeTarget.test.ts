import { describe, expect, it } from 'vitest';

import { RegimeTargetSchema } from './regimeTarget';

describe('RegimeTargetSchema', () => {
  it('parses a bull H2-a25 target', () => {
    const out = RegimeTargetSchema.parse({
      ok: true,
      asOf: '2026-11-13',
      confirmedAt: '2026-10-30',
      close: 4100.1,
      ma200: 3900.2,
      distancePct: 5.12,
      state: 'bull',
      stateLabel: '牛市 / risk-on',
      target: 'starship_robust',
      targetLabel: '稳健星舰 H2-a25',
      sgapExposure: 'high',
      nextReview: '2026-11-30',
      rule: '月末确认：000300 > MA200 → 稳健星舰 H2-a25；否则 → 星港 0.2',
    });
    expect(out.target).toBe('starship_robust');
    expect(out.sgapExposure).toBe('high');
  });

  it('parses a bear starport target', () => {
    const out = RegimeTargetSchema.parse({
      ok: true,
      state: 'bear',
      target: 'starport',
      sgapExposure: 'low',
    });
    expect(out.target).toBe('starport');
  });

  it('parses the fail-open payload', () => {
    const out = RegimeTargetSchema.parse({ ok: false, error: '000300 history 0 bars < 200' });
    expect(out.ok).toBe(false);
  });

  it('rejects an unknown target', () => {
    expect(() => RegimeTargetSchema.parse({ ok: true, target: 'm30' })).toThrow();
  });
});
