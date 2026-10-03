import { describe, expect, it } from 'vitest';

import { FactorVaultSchema } from './factorVault';

const ROW = {
  id: 'starship_b',
  name: '星舰B默认',
  family: '星舰B族',
  source: 'S-gap',
  doc: 'FINAL',
  unit: '点',
  status: 'cold',
  badge: '冷库',
  percentile: 10.9,
  net: -19.8,
  win_rate: 0.288,
  trades: 40,
  trades_raw: 52,
  rolling_window: 'holdout',
  spark: [99.9, 100, 50.2, 10.9],
  history: [
    { window: 'OOS2', date: '2025-08-07', percentile: 99.9, net: 221.9, trades: 293, revived: true },
    { window: 'holdout', date: '2026-09-30', percentile: 10.9, net: -19.8, trades: 52, revived: false },
  ],
  position: 0,
  confirmations: 0,
  days_since_change: 0,
  updated: '2026-09-30',
};

describe('FactorVaultSchema', () => {
  it('parses a minimal vault payload', () => {
    const out = FactorVaultSchema.parse({
      ok: true,
      vault: {
        meta: {
          source: 'frozen',
          n_factors: 1,
          revival: { rule: 'H2k-K2', N: 40, X: 75, off_below: 50, steps: [0, 10, 20], min_trades: 20, step_gap: 20, note: 'rule' },
          cost: '32.28bp',
        },
        summary: { n_factors: 1, n_cold: 1, n_watch: 0, n_revived: 0 },
        factors: [ROW],
      },
    });
    expect(out.vault.factors[0]?.badge).toBe('冷库');
    expect(out.vault.summary.n_cold).toBe(1);
  });

  it('rejects a bad status', () => {
    expect(() =>
      FactorVaultSchema.parse({
        ok: true,
        vault: {
          meta: {
            source: 'x',
            n_factors: 1,
            revival: { rule: 'r', N: 40, X: 75, off_below: 50, steps: [0], min_trades: 20, step_gap: 20, note: 'n' },
            cost: 'c',
          },
          summary: { n_factors: 1, n_cold: 0, n_watch: 1, n_revived: 0 },
          factors: [{ ...ROW, status: 'hot' }],
        },
      }),
    ).toThrow();
  });
});
