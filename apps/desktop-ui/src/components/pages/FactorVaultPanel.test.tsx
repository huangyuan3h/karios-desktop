import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BacktestPage } from './BacktestPage';
import { FactorVaultPanel } from './FactorVaultPanel';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

const VAULT = {
  ok: true,
  vault: {
    meta: {
      source: 'frozen',
      n_factors: 3,
      revival: { rule: 'H2k-K2', N: 40, X: 75, off_below: 50, steps: [0, 10, 20], min_trades: 20, step_gap: 20, note: 'rule' },
      cost: '32.28bp',
    },
    summary: { n_factors: 3, n_cold: 1, n_watch: 1, n_revived: 1 },
    factors: [
      {
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
          { window: 'train', date: '2026-02-27', percentile: 100, net: 71.4, trades: 124, revived: true },
          { window: 'valid', date: '2026-08-07', percentile: 50.2, net: 5.9, trades: 52, revived: false },
          { window: 'holdout', date: '2026-09-30', percentile: 10.9, net: -19.8, trades: 52, revived: false },
        ],
        position: 0,
        confirmations: 0,
        days_since_change: 2,
        updated: '2026-09-30',
      },
      {
        id: 'harbor',
        name: '港湾 (参考)',
        family: 'E1',
        source: 'S-3',
        doc: 'H2d',
        unit: '点',
        status: 'watch',
        badge: '观察',
        percentile: null,
        net: 37.5,
        win_rate: null,
        trades: 16,
        trades_raw: 16,
        rolling_window: 'valid',
        spark: [48.6, 99.6, 99.8, null],
        history: [
          { window: 'OOS2', date: '2025-08-07', percentile: 48.6, net: 31.7, trades: 83, revived: false },
          { window: 'holdout', date: '2026-09-30', percentile: null, net: 0, trades: 0, revived: false },
          { window: 'valid', date: '2026-08-07', percentile: 99.8, net: 37.5, trades: 16, revived: false },
          { window: 'train', date: '2026-02-27', percentile: 99.6, net: 40.9, trades: 52, revived: true },
        ],
        position: 0,
        confirmations: 0,
        days_since_change: 0,
        updated: '2026-08-07',
      },
      {
        id: 'h01',
        name: 'H01 5连阴hold5',
        family: '超卖族',
        source: '五连阴',
        doc: 'H2',
        unit: '%/笔',
        status: 'revived',
        badge: '复活',
        percentile: 98.0,
        net: 2.16,
        win_rate: 0.609,
        trades: 40,
        trades_raw: 3016,
        rolling_window: 'holdout',
        spark: [96.4, 90, 0.0, 98.0],
        history: [
          { window: 'OOS2', date: '2025-08-07', percentile: 96.4, net: 0.47, trades: 18688, revived: true },
          { window: 'train', date: '2026-02-27', percentile: 90, net: 0.54, trades: 13051, revived: true },
          { window: 'valid', date: '2026-08-07', percentile: 0.0, net: -1.21, trades: 15809, revived: false },
          { window: 'holdout', date: '2026-09-30', percentile: 98.0, net: 2.16, trades: 3016, revived: true },
        ],
        position: 0,
        confirmations: 1,
        days_since_change: 0,
        updated: '2026-09-30',
      },
    ],
  },
  history: [],
};

function renderPanel() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <FactorVaultPanel />
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
    if (String(path).includes('/api/backtest/factor-vault')) return VAULT;
    if (String(path).includes('/api/backtest/sgap-decay'))
      return { ok: true, decay: { summary: {}, rolling: [], monthly: [], equity: [], meta: { windows: {} } } };
    if (String(path).includes('/api/backtest/strategy-catalog'))
      return { ok: true, strategies: [] };
    if (String(path).includes('/api/backtest/timeline'))
      return { ok: true, strategy: '港湾', rows: [], summary: {} };
    return { ok: true };
  });
});

describe('FactorVaultPanel', () => {
  it('renders the computed summary line (not hardcoded copy)', async () => {
    renderPanel();
    const el = await screen.findByTestId('vault-summary');
    expect(el.textContent).toContain('3');
    expect(el.textContent).toContain('冷库 1');
    expect(el.textContent).toContain('观察 1');
    expect(el.textContent).toContain('复活 1');
  });

  it('renders one table row per factor with badges and sparklines', async () => {
    renderPanel();
    expect(await screen.findByTestId('vault-row-starship_b')).toBeDefined();
    expect(await screen.findByTestId('vault-row-harbor')).toBeDefined();
    expect(screen.getAllByText('冷库').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('观察').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('复活').length).toBeGreaterThanOrEqual(1);
    expect(document.querySelectorAll('[data-testid="vault-spark"]').length).toBe(3);
  });

  it('expands a time-axis percentile/return chart on row click', async () => {
    renderPanel();
    fireEvent.click(await screen.findByTestId('vault-row-starship_b'));
    expect(await screen.findByTestId('vault-detail-starship_b')).toBeDefined();
    expect(await screen.findByTestId('vault-chart-starship_b')).toBeDefined();
  });

  it('shows the backend-missing hint on 404', async () => {
    apiGetJson.mockImplementation(async (path: string) => {
      if (String(path).includes('/api/backtest/factor-vault')) throw new Error('404');
      return { ok: true };
    });
    renderPanel();
    expect(await screen.findByText(/generate_factor_vault/)).toBeDefined();
  });
});

describe('BacktestPage vault tab', () => {
  it('opens the 因子冷库 tab from the backtest page', async () => {
    renderPage();
    fireEvent.click(await screen.findByText('因子冷库'));
    expect(await screen.findByTestId('vault-summary')).toBeDefined();
    expect(await screen.findByTestId('vault-table')).toBeDefined();
  });
});
