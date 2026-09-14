import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { StrategyCatalogPanel } from './StrategyCatalogPanel';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

const WINDOWS = {
  OOS2: { total: 1, cagr: 1, mdd: -1, sharpe: 1 },
  train: { total: 1, cagr: 1, mdd: -1, sharpe: 1 },
  valid: { total: 1, cagr: 1, mdd: -1, sharpe: 1 },
  long: { total: 1, cagr: 1, mdd: -1, sharpe: 1 },
};

const CATALOG = {
  ok: true,
  strategies: [
    {
      key: 'harbor',
      name: '港湾',
      structure: 'S-3 择强核心 + 闲置现金 ETF 停车场',
      status: 'live',
      statusLabel: 'Live',
      timelineStrategy: 'harbor',
      doc: 'docs/backtests/stable/etf-parking-baseline-2026-09-13.md',
      tag: 'harbor-p1-20260913',
      updated: '2026-09-14',
      windows: {
        ...WINDOWS,
        OOS2: { total: 55.2, cagr: 58.0, mdd: -14.3, sharpe: 1.73 },
        long: { total: 201.5, cagr: 25.7, mdd: -22.8, sharpe: 1.0 },
      },
      pros: ['四窗全正、绝对收益最强（long +201.5%）'],
      cons: ['long Sharpe 仅 1.00、回撤深（−22.8%）'],
    },
    {
      key: 'homeport',
      name: '母港',
      structure: '港湾 × B3 风险预算 50/50',
      status: 'product_candidate',
      statusLabel: '产品候选',
      timelineStrategy: 'homeport',
      doc: 'docs/backtests/stable/harbor-riskbudget-2026-09-13.md',
      tag: 'h-mix-20260913',
      updated: '2026-09-14',
      windows: WINDOWS,
      pros: ['回撤近腰斩'],
      cons: ['收益近半'],
    },
    {
      key: 'starport',
      name: '星港',
      structure: '母港 × 卫星 1/3 曝露（H-B3-SAT chosen=1/3）',
      status: 'product_candidate_increment',
      statusLabel: '产品候选增量',
      timelineStrategy: 'starport',
      doc: 'docs/backtests/stable/harbor-b3-sat-2026-09-14.md',
      tag: 'h-b3-sat-20260914',
      updated: '2026-09-14',
      windows: {
        ...WINDOWS,
        OOS2: { total: 89.4, cagr: 94.5, mdd: -5.0, sharpe: 4.45 },
        long: { total: 198.1, cagr: 25.4, mdd: -11.2, sharpe: 1.84 },
      },
      pros: ['换基座吸收 2/3 valid 拖累'],
      cons: ['valid 窗相对母港 −4.9pt、踩线'],
    },
    {
      key: 'starship',
      name: '星舰',
      structure: '习惯 S-gap 卫星 standalone',
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
    {
      key: 'twin_star',
      name: '双子星',
      structure: '港湾核心 × 卫星 50/50（并行对照档）',
      status: 'parallel_candidate',
      statusLabel: '并行对照',
      timelineStrategy: 'twin_star',
      doc: 'docs/backtests/stable/twin-star-parking-refit-2026-09-13.md',
      tag: 'b12',
      updated: '2026-09-14',
      windows: WINDOWS,
      pros: ['（历史）OOS2 +149.7'],
      cons: ['valid 窗相对港湾核心 Δ−21.2（未过 K1/K2）'],
    },
  ],
};

function renderPanel() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <StrategyCatalogPanel />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiGetJson.mockResolvedValue(CATALOG);
});

describe('StrategyCatalogPanel', () => {
  it('renders the five family tabs and the selected strategy windows', async () => {
    renderPanel();
    expect(await screen.findByText('策略族总览')).toBeDefined();
    for (const name of ['港湾', '母港', '星港', '星舰', '双子星']) {
      expect(screen.getByRole('button', { name: new RegExp(name) })).toBeDefined();
    }
    expect(screen.getByText('+55.2%')).toBeDefined();
    expect(screen.getByText('1.73')).toBeDefined();
    expect(screen.getByText('四窗全正、绝对收益最强（long +201.5%）')).toBeDefined();
    expect(screen.getByText(/Live 恒为港湾/)).toBeDefined();
  });

  it('switches to 星港 details on click', async () => {
    renderPanel();
    fireEvent.click(await screen.findByRole('button', { name: /星港/ }));
    expect(await screen.findByText(/母港 × 卫星 1\/3 曝露/)).toBeDefined();
    expect(screen.getByText('+89.4%')).toBeDefined();
    expect(screen.getByText('valid 窗相对母港 −4.9pt、踩线')).toBeDefined();
  });

  it('shows the twin_star parallel entry with its timeline hint', async () => {
    renderPanel();
    fireEvent.click(await screen.findByRole('button', { name: /双子星/ }));
    expect((await screen.findAllByText(/并行对照/)).length).toBeGreaterThan(0);
    expect(
      await screen.findByText(/可在「对比」页将 Timeline 切到「双子星」查看逐日曲线/),
    ).toBeDefined();
  });
});
