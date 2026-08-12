import { fireEvent, render, screen, waitFor } from '@testing-library/react';
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

  it('distinguishes stale scores from a real no-candidate decision', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-11',
      regime: 'Strong',
      s3Candidates: [],
      scoreDataAsOfDate: '2026-08-10',
      scoreFresh: false,
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/分数未更新（截至 2026-08-10）/)).toBeDefined();
    expect(screen.getByText(/分数截至 2026-08-10/)).toBeDefined();
  });

  it('collapses candidates to the top-5 buy list with backtest size + expand toggle', async () => {
    const mk = (n: string, s: number) => ({ symbol: `HK:${s}`, name: n, score: s, rs: 0.8 });
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [mk('A', 100), mk('B', 99), mk('C', 98), mk('D', 97), mk('E', 96), mk('F', 95)],
      s3CandidateTotal: 19,
      s3Rules: { suggestedSizePct: 10 },
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/明日买入清单/)).toBeDefined();
    expect(screen.getByText(/候选池 19 只/)).toBeDefined();
    expect(screen.getByText(/每票建议 10%/)).toBeDefined();
    expect(screen.getAllByText('买 10%').length).toBe(5);
    expect(screen.queryByText('F')).toBeNull();
    fireEvent.click(screen.getByText(/展开全部 6 只/));
    expect(screen.getByText('F')).toBeDefined();
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

  it('remind-buy button opens the dialog and adds the stock to watchlist + local reminder', async () => {
    localStorage.clear();
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [
        { symbol: 'HK:02099', name: '中国黄金国际', score: 88.0 },
        { symbol: 'CN:600111', name: '北方稀土', score: 71.0 },
      ],
      s3Rules: { suggestedSizePct: 10 },
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/明日买入清单/)).toBeDefined();

    const remindButtons = screen.getAllByText('提醒买入');
    expect(remindButtons.length).toBe(2);
    fireEvent.click(remindButtons[0]);

    expect(await screen.findByText(/目标买入价/)).toBeDefined();
    expect(screen.getAllByText(/中国黄金国际/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/S-3 建议仓位 10%/)).toBeDefined();

    fireEvent.change(screen.getByPlaceholderText('0.000'), { target: { value: '88.5' } });
    fireEvent.change(screen.getByPlaceholderText('为什么买 / 买多少 / 什么条件下放弃…'), {
      target: { value: 'HSI 站上 MA20 后买入' },
    });
    fireEvent.click(screen.getByText('确认加入自选'));

    await screen.findByText(/买入提醒（1）/);
    expect(screen.getByText(/目标价 88.5/)).toBeDefined();

    const registryBody = fetchMock.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes('/watchlist/registry'),
    )?.[1] as { method?: string; body?: string };
    expect(registryBody).toBeDefined();
    expect(registryBody.method).toBe('POST');
    const posted = JSON.parse(String(registryBody.body));
    expect(posted.items[0].symbol).toBe('HK:02099');
    expect(posted.items[0].source).toBe('research');

    const stored = JSON.parse(localStorage.getItem('karios_buy_reminders') ?? '[]');
    expect(stored[0]).toMatchObject({
      symbol: 'HK:02099',
      targetPrice: 88.5,
      note: 'HSI 站上 MA20 后买入',
    });
    expect(screen.getByText('已提醒')).toBeDefined();
    vi.unstubAllGlobals();
  });

  it('removes a reminder from the reminder bar without touching watchlist rows', async () => {
    localStorage.setItem(
      'karios_buy_reminders',
      JSON.stringify([
        { symbol: 'HK:02099', name: '中国黄金国际', targetPrice: 88.5, note: '等回踩', createdAt: '2026-08-12T00:00:00.000Z' },
      ]),
    );
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/买入提醒（1）/)).toBeDefined();
    fireEvent.click(screen.getByText('移除提醒'));
    await waitFor(() => expect(screen.queryByText(/买入提醒（1）/)).toBeNull());
    expect(JSON.parse(localStorage.getItem('karios_buy_reminders') ?? '[]')).toHaveLength(0);
  });

  it('quick-buy button opens the modal and records a paper trade without touching watchlist', async () => {
    localStorage.clear();
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [
        { symbol: 'HK:02099', name: '中国黄金国际', score: 88.0, rs: 0.85 },
        { symbol: 'CN:600111', name: '北方稀土', score: 71.0, rs: 0.7 },
      ],
      s3Rules: { suggestedSizePct: 10 },
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/明日买入清单/)).toBeDefined();

    const buyButtons = screen.getAllByText('买入');
    expect(buyButtons.length).toBe(2);
    fireEvent.click(buyButtons[0]);

    expect(await screen.findByText(/记入模拟盘（paper trade）/)).toBeDefined();
    expect(screen.getByText(/仓位 %/)).toBeDefined();

    const priceInput = await screen.findByPlaceholderText('0.000');
    fireEvent.change(priceInput, { target: { value: '88.5' } });
    fireEvent.change(screen.getByDisplayValue('10'), { target: { value: '15' } });
    fireEvent.click(screen.getByText('确认买入'));

    await waitFor(() => expect(screen.queryByText('确认买入')).toBeNull());
    expect(screen.getByText('✓ 已买入')).toBeDefined();

    const tradeBody = fetchMock.mock.calls.find(
      (c: unknown[]) => String(c[0]).includes('/trades'),
    )?.[1] as { method?: string; body?: string } | undefined;
    expect(tradeBody).toBeDefined();
    expect(tradeBody!.method).toBe('POST');
    const posted = JSON.parse(String(tradeBody!.body));
    expect(posted).toMatchObject({
      symbol: 'HK:02099',
      side: 'BUY',
      price: 88.5,
      positionPct: 15,
      source: 'RESEARCH',
      market: 'HK',
    });

    const registryCalls = fetchMock.mock.calls.filter((c: unknown[]) =>
      String(c[0]).includes('/watchlist/registry'),
    );
    expect(registryCalls).toHaveLength(0);
    expect(localStorage.getItem('karios_buy_reminders')).toBeNull();
    vi.unstubAllGlobals();
  });

  it('renders backtest recon inside the market panel with expandable missing list', async () => {
    localStorage.clear();
    const fetchMock = vi.fn(async (url: string) => {
      if (String(url).includes('/api/backtest/recon/latest')) {
        return new Response(
          JSON.stringify({
            ok: true,
            items: [
              {
                reconDate: '2026-08-07',
                market: 'HK',
                window: 'valid',
                expected: 19,
                actual: 0,
                aligned: 0,
                missing: 19,
                extra: 0,
                alignedReturnDiffPct: null,
                detail: [
                  { type: 'missing', symbol: 'HK:02099', entry: '2026-08-05', score: 88.0, positionPct: 0.1 },
                  { type: 'missing', symbol: 'HK:00081', entry: '2026-08-05', score: 79.0, positionPct: 0.1 },
                ],
              },
              {
                reconDate: '2026-08-07',
                market: 'CN',
                window: 'valid',
                expected: 0,
                actual: 0,
                aligned: 0,
                missing: 0,
                extra: 0,
                alignedReturnDiffPct: 0.1,
                detail: [],
              },
            ],
          }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    });
    vi.stubGlobal('fetch', fetchMock);
    fetchPortfolioHealth.mockResolvedValue({
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [],
      holdings: [],
      hkHealth: { regime: 'Diverging', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect((await screen.findAllByText(/回测口径 · 2026-08-07 对账/)).length).toBe(2);
    expect(screen.getByText(/回测应持 19 · 实持 0 · 缺 19 · 多 0/)).toBeDefined();
    expect(screen.getByText(/看缺票（19）/)).toBeDefined();

    fireEvent.click(screen.getByText(/看缺票（19）/));
    expect(screen.getAllByText('缺票').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('HK:02099').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/入场 score 88.0/)).toBeDefined();
    expect(screen.getAllByText(/建议 10%/).length).toBeGreaterThanOrEqual(1);

    fireEvent.click(screen.getAllByText('提醒买入')[0]);
    expect(await screen.findByText(/目标买入价/)).toBeDefined();
    vi.unstubAllGlobals();
  });
});
