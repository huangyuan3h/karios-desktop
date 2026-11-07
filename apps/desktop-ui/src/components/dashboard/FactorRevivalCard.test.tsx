import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FactorRevivalCard } from './FactorRevivalCard';

const mock = vi.hoisted(() => ({
  vault: null as unknown,
  sgap: null as unknown,
}));

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useFactorVaultQuery: () => ({
      data: mock.vault,
      isPending: false,
      isError: false,
      refetch: vi.fn(),
    }),
    useSgapDecayQuery: () => ({
      data: mock.sgap,
      isPending: false,
      isError: false,
      refetch: vi.fn(),
    }),
  };
});

const VAULT = {
  ok: true,
  vault: {
    meta: { source: 'test', n_factors: 2, revival: {}, cost: '5bp' },
    summary: { n_factors: 2, n_cold: 1, n_watch: 0, n_revived: 1 },
    factors: [
      {
        id: 'harbor',
        name: '港湾 (参考)',
        family: 'harbor',
        source: 'test',
        doc: 'doc',
        unit: '%',
        status: 'revived',
        badge: '复活',
        percentile: 99.9,
        net: 42.7,
        win_rate: 0.6,
        trades: 40,
        trades_raw: 40,
        rolling_window: '40',
        spark: [],
        history: [{ window: 'valid', date: '2026-10-01', percentile: 99.9, net: 42.7, trades: 40, revived: true }],
        position: 0,
        confirmations: 2,
        days_since_change: 3,
        updated: '2026-11-06',
      },
      {
        id: 'cold',
        name: '冷因子',
        family: 'x',
        source: 'test',
        doc: 'doc',
        unit: '%',
        status: 'watch',
        badge: '观察',
        percentile: 55,
        net: 1,
        win_rate: 0.5,
        trades: 20,
        trades_raw: 20,
        rolling_window: '40',
        spark: [],
        history: [],
        position: 0,
        confirmations: 0,
        days_since_change: 10,
        updated: '2026-11-06',
      },
    ],
  },
};

const SGAP = {
  ok: true,
  decay: {
    meta: { source: 'test', n_trades: 100, windows: {}, generated_at: '' },
    summary: {
      n_trades: 100,
      date_start: '2025-01-01',
      date_end: '2026-11-06',
      cum_total: 10,
      max_drawdown: -5,
      latest_r40_mean: 0.4,
      latest_r40_win_rate: 0.55,
      latest_r40_percentile: 80,
      latest_r40_t: 2,
      revival_pos: 10,
    },
    rolling: [],
    monthly: [],
    equity: [],
  },
};

beforeEach(() => {
  mock.vault = VAULT;
  mock.sgap = SGAP;
});

describe('FactorRevivalCard', () => {
  it('makes revived factors and the S-gap light obvious on Dashboard', () => {
    render(<FactorRevivalCard />);
    expect(screen.getByTestId('factor-revival-card')).toBeDefined();
    expect(screen.getByTestId('factor-revival-count').textContent).toMatch(/1 个复活/);
    expect(screen.getByTestId('sgap-revival').textContent).toMatch(/S-gap 复活灯 10%/);
    expect(screen.getByTestId('sgap-revival').getAttribute('data-active')).toBe('true');
    expect(screen.getByTestId('revived-factor').textContent).toMatch(/港湾 \(参考\)/);
    expect(screen.getByTestId('revived-factor').textContent).toMatch(/99.9%/);
  });

  it('stays honest when nothing is revived', () => {
    mock.vault = {
      ...VAULT,
      vault: {
        ...VAULT.vault,
        summary: { n_factors: 2, n_cold: 1, n_watch: 1, n_revived: 0 },
        factors: VAULT.vault.factors.map((f) => ({ ...f, status: 'watch', badge: '观察' })),
      },
    };
    mock.sgap = {
      ...SGAP,
      decay: { ...SGAP.decay, summary: { ...SGAP.decay.summary, revival_pos: 0 } },
    };
    render(<FactorRevivalCard />);
    expect(screen.getByText(/暂无复活因子/)).toBeDefined();
    expect(screen.getByTestId('sgap-revival').getAttribute('data-active')).toBe('false');
  });
});
