import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SgapHealthCard } from './SgapHealthCard';

const mock = vi.hoisted(() => ({ decay: null as unknown }));

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useSgapDecayQuery: () => ({
      data: mock.decay,
      isError: false,
      isLoading: false,
      refetch: vi.fn(),
    }),
  };
});

const DECAY = {
  ok: true,
  decay: {
    meta: { source: 'h2f_trades.json', n_trades: 1130, windows: {} },
    summary: {
      n_trades: 1130,
      date_start: '2021-08-09',
      date_end: '2026-09-23',
      cum_total: 491.09,
      max_drawdown: -24.88,
      latest_r40_mean: -0.37,
      latest_r40_win_rate: 0.3,
      latest_r40_percentile: 2.2,
      latest_r40_t: -2.45,
      revival_pos: 0,
    },
    rolling: [
      {
        i: 40,
        date: '2026-09-23',
        r40: { mean: -0.37, sum: -14.8, win_rate: 0.3, t: -2.45, percentile: 2.2 },
        r60: null,
        crowd_amt_w: 42344,
        crowd_circ: 1151806,
        large_edge: 0.02,
        revival_pos: 0,
      },
    ],
    monthly: [],
    equity: [],
  },
};

describe('SgapHealthCard', () => {
  beforeEach(() => {
    mock.decay = DECAY;
  });

  it('shows the decay and crowding numbers next to the auto target', () => {
    render(<SgapHealthCard />);
    const card = screen.getByTestId('sgap-health-card');
    expect(card.textContent).toContain('衰减');
    expect(card.textContent).toContain('-0.37');
    expect(card.textContent).toContain('2.2%');
    expect(card.textContent).toContain('4.23 亿');
    expect(card.textContent).toContain('115.2 亿');
    expect(card.textContent).toContain('2026H2');
    expect(card.textContent).toContain('6.3×');
  });

  it('fails open when the S-gap report is missing', () => {
    mock.decay = null;
    render(<SgapHealthCard />);
    expect(screen.getByText(/S-gap 数据不可用/)).toBeDefined();
  });
});
