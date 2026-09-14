import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { StrategyStatusCard } from './StrategyStatusCard';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

const WINDOWS = {
  OOS2: { total: 89.4, cagr: 94.5, mdd: -5.0, sharpe: 4.45 },
  train: { total: 40.7, cagr: 102.3, mdd: -3.9, sharpe: 4.54 },
  valid: { total: 20.7, cagr: 54.6, mdd: -11.4, sharpe: 1.93 },
  long: { total: 198.1, cagr: 25.4, mdd: -11.2, sharpe: 1.84 },
};

function catalog() {
  return {
    ok: true,
    strategies: [
      {
        key: 'starport',
        name: '星港',
        structure: '母港 × 卫星 1/3 曝露',
        status: 'product_candidate_increment',
        statusLabel: '产品候选增量',
        timelineStrategy: 'starport',
        doc: 'docs/backtests/stable/harbor-b3-sat-2026-09-14.md',
        tag: 'h-b3-sat-20260914',
        updated: '2026-09-14',
        windows: WINDOWS,
        pros: ['换基座吸收 2/3 valid 拖累'],
        cons: ['valid 窗相对母港 −4.9pt、踩线'],
      },
      {
        key: 'starship',
        name: '星舰',
        structure: '卫星 standalone',
        status: 'aggressive_unaudited',
        statusLabel: '激进 · 未审计',
        timelineStrategy: 'starship',
        doc: 'docs/backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md',
        tag: 'starship',
        updated: '2026-09-14',
        windows: WINDOWS,
        pros: ['long +463.6%'],
        cons: ['执行审计未过'],
      },
    ],
  };
}

function renderCard(mode: 'harbor' | 'homeport' | 'starport' | 'starship') {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <StrategyStatusCard mode={mode} />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiGetJson.mockResolvedValue(catalog());
});

describe('StrategyStatusCard', () => {
  it('shows the starport profile and wiring notes', async () => {
    renderCard('starport');
    expect(await screen.findByText('星港')).toBeDefined();
    expect(screen.getByText('母港 × 卫星 1/3 曝露')).toBeDefined();
    expect(screen.getByText(/长窗 \+198\.1% \/ SR 1\.84 \/ MDD -11\.2%/)).toBeDefined();
    expect(screen.getByText(/换基座吸收 2\/3 valid 拖累/)).toBeDefined();
    expect(screen.getByText(/OPT-186/)).toBeDefined();
  });

  it('shows the starship unaudited warning', async () => {
    renderCard('starship');
    expect(await screen.findByText(/未过执行审计，不进 Live/)).toBeDefined();
  });

  it('fails open (renders nothing) when the catalog has no entry', () => {
    apiGetJson.mockResolvedValue({ ok: true, strategies: [] });
    const { container } = renderCard('homeport');
    expect(container.firstChild).toBeNull();
  });
});
