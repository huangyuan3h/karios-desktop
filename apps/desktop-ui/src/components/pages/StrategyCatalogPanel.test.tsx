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
      key: 'starship',
      name: '星舰',
      structure: '习惯 S-gap 卫星 standalone',
      status: 'aggressive_pending',
      statusLabel: '激进 · 前置未满',
      role: 'offense',
      roleLabel: '进攻',
      timelineStrategy: 'starship',
      doc: 'docs/backtests/stable/sgap-habit-satellite-standalone-2026-09-14.md',
      tag: 'starship',
      updated: '2026-09-14',
      windows: WINDOWS,
      pros: ['long +463.6%'],
      cons: ['前置未满：paper 3/20 + 用户授权'],
      regime: {
        fit: ['大盘弱 + 波动大时最赚'],
        unfit: ['停车资产抽搐时'],
        evidence: [{ label: '2025', value: '+66.0' }],
        note: '只描述、不作闸门',
      },
    },
    {
      key: 'starship_robust',
      name: '稳健星舰 H2-a25',
      structure: '卫星 + cashShare(T−1) × (25% H2 ETF + 75% B3)',
      status: 'product_candidate',
      statusLabel: '现行 canonical · K3 风险',
      role: 'balanced',
      roleLabel: '稳健',
      timelineStrategy: 'starship_robust',
      doc: 'docs/backtests/stable/sat-h2-a25-2026-09-24.md',
      tag: 'sat-h2-a25-v1-20260924',
      updated: '2026-09-24',
      canonical: true,
      variant: {
        sleeveMode: 'h2',
        hystBand: 0.02,
        sleeveWeight: 0.25,
        b3Weight: 0.75,
      },
      risk: 'K3 failed: long MDD delta -1.6pt',
      windows: WINDOWS,
      pros: ['现行稳健研究/展示口径'],
      cons: ['K3 风险未过'],
    },
    {
      key: 'starport',
      name: '星港',
      structure: '母港 × 卫星 0.2 曝露（H-B3-SAT chosen=0.2）',
      status: 'product_candidate_increment',
      statusLabel: '产品候选增量',
      role: 'balanced',
      roleLabel: '均衡',
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
      key: 'homeport',
      name: '母港',
      structure: '港湾 70% × B3 风险预算 30%（M30 防守档）',
      status: 'product_candidate',
      statusLabel: '产品候选',
      role: 'defense',
      roleLabel: '防守',
      timelineStrategy: 'homeport_m30',
      doc: 'docs/backtests/stable/homeport-weight-tune-2026-09-16.md',
      tag: 'h-mix-m30-20260916',
      updated: '2026-09-16',
      windows: WINDOWS,
      pros: ['回撤近腰斩'],
      cons: ['收益近半'],
    },
    {
      key: 'harbor',
      name: '港湾',
      structure: 'S-3 择强核心 + 闲置现金 ETF 停车场',
      status: 'live',
      statusLabel: 'Live · 日落',
      role: 'live',
      roleLabel: 'Live 底座',
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
      regime: {
        fit: ['大盘在 200 日均线上方时：每天约 +0.17%，约一年 +42%'],
        unfit: ['大盘横着磨、波动不大不小：每天 −0.03%'],
        evidence: [{ label: '2025', value: '+51.6（10.9 / 1.90）' }],
        note: '只描述、不作闸门',
      },
    },
    {
      key: 'twin_star',
      name: '双子星',
      structure: '港湾核心和卫星打法各放一半钱（并行对照）',
      status: 'parallel_candidate',
      statusLabel: '并行对照',
      role: 'balanced',
      roleLabel: '对照',
      timelineStrategy: 'twin_star',
      doc: 'docs/backtests/stable/twin-star-parking-refit-2026-09-13.md',
      tag: 'b12',
      updated: '2026-09-17',
      windows: WINDOWS,
      pros: ['行情好的年份冲得最猛'],
      cons: ['卫星状态差的年份很平庸'],
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
  it('renders the regime fit map and evidence for the selected strategy', async () => {
    renderPanel();
    fireEvent.click(await screen.findByRole('button', { name: /港湾/ }));
    expect(await screen.findByText('什么行情下好用（只是记录，不是买卖开关）')).toBeDefined();
    expect(screen.getByText(/大盘在 200 日均线上方时/)).toBeDefined();
    expect(screen.getByText(/大盘横着磨/)).toBeDefined();
    expect(screen.getByText('+51.6（10.9 / 1.90）')).toBeDefined();
  });

  it('renders the three tiers plus sunset baseline and the parallel twin-star entry', async () => {
    renderPanel();
    expect(await screen.findByText('策略族总览')).toBeDefined();
    for (const name of ['星舰', '稳健星舰 H2-a25', '星港', '母港', '港湾', '双子星']) {
      expect(screen.getByRole('button', { name: new RegExp(`^${name}`) })).toBeDefined();
    }
    // Role badges: one per tier tab, plus one in the selected-strategy header
    // (default selection = starship, so 进攻 appears twice).
    expect(screen.getAllByText('进攻')).toHaveLength(2);
    expect(screen.getByText('均衡')).toBeDefined();
    expect(screen.getByText('防守')).toBeDefined();
    expect(screen.getByText('Live 底座')).toBeDefined();
    expect(screen.getByText('long +463.6%')).toBeDefined();
    // Parallel entry carries its own badge + a working Timeline hint.
    fireEvent.click(screen.getByRole('button', { name: /双子星/ }));
    expect((await screen.findAllByText('并行对照')).length).toBeGreaterThan(0);
    expect(
      await screen.findByText(/可在「对比」页将 Timeline 切到「双子星」查看逐日曲线/),
    ).toBeDefined();
    // Sunset baseline still reachable with its Live badge.
    fireEvent.click(screen.getByRole('button', { name: /港湾/ }));
    expect(await screen.findByText('+55.2%')).toBeDefined();
    expect(screen.getByText('1.73')).toBeDefined();
    expect(screen.getByText('四窗全正、绝对收益最强（long +201.5%）')).toBeDefined();
    expect(screen.getByText(/Live 恒为港湾/)).toBeDefined();
  });

  it('switches to 星港 details on click', async () => {
    renderPanel();
    fireEvent.click(await screen.findByRole('button', { name: /星港/ }));
    expect(await screen.findByText(/母港 × 卫星 0\.2 曝露/)).toBeDefined();
    expect(screen.getByText('+89.4%')).toBeDefined();
    expect(screen.getByText('valid 窗相对母港 −4.9pt、踩线')).toBeDefined();
  });

  it('shows the defensive homeport row pointing at the M30 timeline', async () => {
    renderPanel();
    fireEvent.click(await screen.findByRole('button', { name: /防守/ }));
    expect(await screen.findByText(/港湾 70% × B3 风险预算 30%/)).toBeDefined();
    expect(
      await screen.findByText(/可在「对比」页将 Timeline 切到「母港」查看逐日曲线/),
    ).toBeDefined();
  });
});
