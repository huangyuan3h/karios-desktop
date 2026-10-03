import { describe, expect, it } from 'vitest';

import { SgapDecaySchema } from './sgapDecay';

describe('SgapDecaySchema', () => {
  it('parses a minimal decay payload', () => {
    const out = SgapDecaySchema.parse({
      ok: true,
      decay: {
        meta: {
          source: 'h2f_trades.json',
          n_trades: 2,
          windows: {
            OOS2: { start: '2024-08-01', end: '2025-08-07' },
            train: { start: '2025-08-08', end: '2026-02-27' },
            valid: { start: '2026-03-02', end: '2026-08-07' },
            holdout: { start: '2026-08-10', end: '2026-09-30' },
          },
        },
        summary: {
          n_trades: 2,
          date_start: '2021-08-09',
          date_end: '2026-09-23',
          cum_total: 1.5,
          max_drawdown: -0.5,
          latest_r40_mean: null,
          latest_r40_win_rate: null,
          latest_r40_percentile: null,
          latest_r40_t: null,
          revival_pos: 0,
        },
        rolling: [],
        monthly: [{ month: '2026-09', count: 20, sum: -11.39 }],
        equity: [{ i: 0, date: '2021-08-09', cum: 2.47, drawdown: 0 }],
      },
    });
    expect(out.decay.summary.n_trades).toBe(2);
    expect(out.decay.monthly[0]?.month).toBe('2026-09');
  });

  it('rejects a missing summary', () => {
    expect(() =>
      SgapDecaySchema.parse({
        ok: true,
        decay: { meta: {}, rolling: [], monthly: [], equity: [] },
      }),
    ).toThrow();
  });
});
