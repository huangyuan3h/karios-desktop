import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PortfolioHealthCard } from './PortfolioHealthCard';

const { fetchPortfolioHealth } = vi.hoisted(() => ({
  fetchPortfolioHealth: vi.fn(),
}));
vi.mock('@/lib/queries/portfolioHealth', () => ({ fetchPortfolioHealth }));

const HOLDING = {
  symbol: 'HK:00700',
  name: '腾讯控股',
  positionPct: 6.3,
  pnlPct: 0.6,
  drawdownFromPeakPct: -2.7,
  holdingDays: 11,
  stopLossLine: 452.2,
  trailingLine: 452.8,
  pyramidTriggerLine: 487.9,
  pyramidAdded: false,
  expireDate: '2026-09-27',
  action: 'HOLD',
};

function renderCard() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <PortfolioHealthCard />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  fetchPortfolioHealth.mockReset();
});

describe('PortfolioHealthCard', () => {
  it('renders market state + holdings from the health endpoint', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-07',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [HOLDING],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect(await screen.findByText(/S-3 持仓体检 · A 股 \/ 港股并行/)).toBeDefined();
    expect(await screen.findByText("Weak · 空仓观望")).toBeDefined();
    expect(await screen.findByText("Strong · 进攻")).toBeDefined();
    expect(await screen.findByText("腾讯控股")).toBeDefined();
    expect(await screen.findByText("✅ 持有")).toBeDefined();
    expect(await screen.findByText("2026-08-07")).toBeDefined();
    expect(screen.getByText(/今日无开仓候选（regime=Weak/)).toBeDefined();
    expect(fetchPortfolioHealth).toHaveBeenCalledTimes(1);
  });

  it('flags EXIT holdings with the trigger reason', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-07',
      regime: 'Strong',
      sentiment: 'normal',
      s3Candidates: [],
      holdings: [{ ...HOLDING, action: 'EXIT', reason: 'trailing_stop（峰值回撤8.5% >= 8% 阈值）' }],
    });
    renderCard();
    expect(await screen.findByText('🔴 建议退出')).toBeDefined();
    expect(screen.getByText(/trailing_stop/)).toBeDefined();
  });

  it('shows weak-regime no-candidate note and candidate chips when present', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-07',
      regime: 'Strong',
      sentiment: 'normal',
      s3Candidates: [{ symbol: 'CN:600111', name: '北方稀土', score: 71.0 }],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText('北方稀土')).toBeDefined();
    expect(await screen.findByText(/score=71/)).toBeDefined();
    expect(screen.getAllByText(/当前无持仓/).length).toBeGreaterThan(0);
  });

  it('shows a fallback note when the endpoint is unreachable', async () => {
    fetchPortfolioHealth.mockRejectedValue(new Error('fetch failed'));
    renderCard();
    expect(await screen.findByText(/持仓体检暂不可用/)).toBeDefined();
  });

  it('opens the stock page when a holding row is clicked', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-07',
      regime: 'Weak',
      sentiment: 'normal',
      s3Candidates: [],
      holdings: [HOLDING],
    });
    const onOpenStock = vi.fn();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <PortfolioHealthCard onOpenStock={onOpenStock} />
      </QueryClientProvider>,
    );
    const row = await screen.findByText('腾讯控股');
    row.click();
    expect(onOpenStock).toHaveBeenCalledWith('HK:00700');
  });
});
