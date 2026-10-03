import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';

import { FleetPanel } from './FleetPanel';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

const FLEET = {
  ok: true,
  fleet: {
    meta: { source: 'frozen', note: 'research only' },
    windows: {
      fleet: {
        OOS2: { total: 50.3, mdd: -7.7, sharpe: 2.24 },
        valid: { total: 10.0, mdd: -13.3, sharpe: 1.06 },
        holdout: { total: -8.4, mdd: -9.5, sharpe: -3.55 },
        long: { total: 163.6, mdd: -13.3, sharpe: 1.3 },
      },
      base: {
        OOS2: { total: 45.0 },
        valid: { total: 9.6 },
        holdout: { total: -8.4 },
        long: { total: 136.2 },
      },
    },
    defense: { days: 1252, L1_days: 0, L2_days: 69, L3_days: 20, L4_days: 0 },
    equity: {
      dates: ['2021-08-02', '2021-08-03', '2026-09-30'],
      fleet: [1.0, 1.01, 2.636],
      base: [1.0, 1.009, 2.362],
      starship_b: [1.0, 1.02, 8.313],
      hs300: [1.0, 0.99, 0.951],
    },
  },
};

function renderPanel() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <FleetPanel />
    </QueryClientProvider>,
  );
}

describe('FleetPanel', () => {
  it('shows the 4-window metrics table with defense states', async () => {
    vi.mocked(apiGetJson).mockResolvedValue(FLEET);
    renderPanel();
    expect(await screen.findByTestId('fleet-panel')).toBeTruthy();
    expect(screen.getByText('舰队总回报')).toBeTruthy();
    expect(screen.getByTestId('fleet-defense')).toBeTruthy();
  });

  it('shows a loading state before the payload arrives', () => {
    vi.mocked(apiGetJson).mockImplementation(() => new Promise(() => {}));
    renderPanel();
    expect(screen.getByText(/加载中/)).toBeTruthy();
  });
});
