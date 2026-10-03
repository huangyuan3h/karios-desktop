import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BacktestPage } from './BacktestPage';
import { SgapDecayPanel } from './SgapDecayPanel';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

const DECAY = {
  ok: true,
  decay: {
    meta: {
      source: 'h2f_trades.json',
      n_trades: 1130,
      windows: {
        OOS2: { start: '2024-08-01', end: '2025-08-07' },
        train: { start: '2025-08-08', end: '2026-02-27' },
        valid: { start: '2026-03-02', end: '2026-08-07' },
        holdout: { start: '2026-08-10', end: '2026-09-30' },
      },
    },
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
        i: 39,
        date: '2021-10-01',
        r40: { mean: 0.4, sum: 16, win_rate: 0.55, t: 1.8, percentile: 80 },
        r60: null,
        crowd_amt_w: 6500,
        crowd_circ: 340000,
        large_edge: 5,
        revival_pos: 10,
      },
      {
        i: 40,
        date: '2021-10-02',
        r40: { mean: -0.37, sum: -14.8, win_rate: 0.3, t: -2.45, percentile: 2.2 },
        r60: null,
        crowd_amt_w: 42344,
        crowd_circ: 1151806,
        large_edge: 0.02,
        revival_pos: 0,
      },
    ],
    monthly: [
      { month: '2021-08', count: 35, sum: 34.74 },
      { month: '2026-09', count: 20, sum: -11.39 },
    ],
    equity: [
      { i: 39, date: '2021-10-01', cum: 515.97, drawdown: 0 },
      { i: 40, date: '2021-10-02', cum: 491.09, drawdown: -24.88 },
    ],
  },
};

function renderPanel() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <SgapDecayPanel />
    </QueryClientProvider>,
  );
}

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <BacktestPage />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('harbor'));
  apiGetJson.mockReset();
  apiGetJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/api/backtest/sgap-decay')) return DECAY;
    if (String(path).includes('/api/backtest/strategy-catalog'))
      return { ok: true, strategies: [] };
    if (String(path).includes('/api/backtest/timeline'))
      return { ok: true, strategy: '港湾', rows: [], summary: {} };
    return { ok: true };
  });
});

describe('SgapDecayPanel', () => {
  it('renders the computed summary line (not hardcoded copy)', async () => {
    renderPanel();
    const el = await screen.findByTestId('sgap-summary');
    expect(el.textContent).toContain('1130');
    expect(el.textContent).toContain('491.1');
    expect(el.textContent).toContain('30.0%');
    expect(el.textContent).toContain('2.2%');
    expect(el.textContent).toContain('0%');
  });

  it('renders all seven time-axis charts', async () => {
    renderPanel();
    for (const id of [
      'sgap-chart-rollret',
      'sgap-chart-winrate',
      'sgap-chart-monthly',
      'sgap-chart-percentile',
      'sgap-chart-crowd',
      'sgap-chart-edge',
      'sgap-chart-equity',
    ]) {
      expect(await screen.findByTestId(id)).toBeDefined();
    }
  });

  it('shows the backend-missing hint on 404', async () => {
    apiGetJson.mockImplementation(async (path: string) => {
      if (String(path).includes('/api/backtest/sgap-decay')) throw new Error('404');
      return { ok: true };
    });
    renderPanel();
    expect(await screen.findByText(/generate_sgap_decay/)).toBeDefined();
  });
});

describe('BacktestPage sgap tab', () => {
  it('opens the S-gap 失效趋势 tab from the backtest page', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('S-gap 失效趋势'));
    expect(await screen.findByTestId('sgap-summary')).toBeDefined();
    expect(await screen.findByTestId('sgap-chart-equity')).toBeDefined();
  });
});
