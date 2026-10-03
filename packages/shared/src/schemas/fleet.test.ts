import { describe, expect, it } from 'vitest';

import { FleetSchema } from './fleet';

describe('FleetSchema', () => {
  it('accepts the frozen fleet payload', () => {
    const parsed = FleetSchema.parse({
      ok: true,
      fleet: {
        meta: { source: 'frozen' },
        windows: {
          fleet: { long: { total: 163.6, mdd: -13.3, sharpe: 1.3 } },
          base: { long: { total: 136.2 } },
        },
        defense: { days: 1252, L1_days: 0, L2_days: 69, L3_days: 20, L4_days: 0 },
        equity: { dates: ['2021-08-02'], fleet: [1.0], base: [1.0], starship_b: [1.0], hs300: [1.0] },
      },
    });
    expect(parsed.ok).toBe(true);
    expect(parsed.fleet.windows?.fleet?.['long']?.total).toBe(163.6);
  });

  it('rejects a missing fleet payload', () => {
    expect(() => FleetSchema.parse({ ok: true })).toThrow();
  });
});
