import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { PortfolioHealthCard } from './PortfolioHealthCard';
import * as watchlistStorage from '@/lib/watchlist-storage';
import * as userTrades from '@/lib/queries/userTrades';

const { fetchPortfolioHealth } = vi.hoisted(() => ({
  fetchPortfolioHealth: vi.fn(),
}));
vi.mock('@/lib/queries/portfolioHealth', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/portfolioHealth')>(
    '@/lib/queries/portfolioHealth',
  );
  return { ...actual, fetchPortfolioHealth };
});

const marketHoursMock = vi.hoisted(() => ({
  getShanghaiMinutes: vi.fn(() => 15 * 60),
  satNamesVisible: vi.fn(() => true),
}));
vi.mock('@/lib/market-hours', async () => {
  const actual = await vi.importActual<typeof import('@/lib/market-hours')>('@/lib/market-hours');
  return {
    ...actual,
    getShanghaiMinutes: marketHoursMock.getShanghaiMinutes,
    satNamesVisible: marketHoursMock.satNamesVisible,
  };
});

const SAT_OPEN = {
  data: {
    sat: {
      asOf: '2026-08-28',
      gateOpen: true,
      breadth: 0.588,
      gapCount: 111,
      candidates: [{ ts: '000712.SZ', name: '锦江投资', amp: 1, gapPct: 5, close: 10 }],
      note: null,
      coreTargetPct: 50,
      satTargetPct: 50,
      book: { asOf: '2026-08-28', holdings: [], exitsDue: [], body: 3 },
    },
  },
  isError: false,
  dataUpdatedAt: Date.now(),
  isFetching: false,
};

const twinStarMock = vi.hoisted(() => ({
  useTwinStarActionQuery: vi.fn(() => SAT_OPEN),
}));
type TwinStarQuery = ReturnType<typeof import('@/lib/queries/backtest').useTwinStarActionQuery>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const useTwinStarActionQueryMock = twinStarMock.useTwinStarActionQuery as any;
vi.mock('@/lib/queries/backtest', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/backtest')>(
    '@/lib/queries/backtest',
  );
  return { ...actual, useTwinStarActionQuery: twinStarMock.useTwinStarActionQuery };
});

vi.mock('@/lib/watchlist-market', () => ({
  fetchWatchlistMarketSnapshot: vi.fn(async (symbols: string[]) => ({
    trend: {},
    quotes: Object.fromEntries(
      symbols.map((s) => [s.toUpperCase(), { tsCode: s, price: 10.5, tradeTime: null, amount: null, volume: null, preClose: null, pctChg: null }]),
    ),
  })),
}));

const sentimentMock = vi.hoisted(() => ({
  useDashboardSentimentQuery: vi.fn(() => ({
    data: undefined,
    isError: false,
    dataUpdatedAt: 0,
    isFetching: false,
  })),
}));
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const useDashboardSentimentQueryMock = sentimentMock.useDashboardSentimentQuery as any;
vi.mock('@/lib/queries/sentiment', async () => {
  const actual = await vi.importActual<typeof import('@/lib/queries/sentiment')>(
    '@/lib/queries/sentiment',
  );
  return { ...actual, useDashboardSentimentQuery: sentimentMock.useDashboardSentimentQuery };
});

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

function setStrategyMode(mode: 'twin_star' | 'single_track') {
  window.localStorage.setItem('karios.strategyMode', JSON.stringify(mode));
}

beforeEach(() => {
  fetchPortfolioHealth.mockReset();
  marketHoursMock.getShanghaiMinutes.mockReturnValue(15 * 60);
  marketHoursMock.satNamesVisible.mockReturnValue(true);
  useTwinStarActionQueryMock.mockReturnValue(SAT_OPEN);
  useDashboardSentimentQueryMock.mockReturnValue({
    data: undefined,
    isError: false,
    dataUpdatedAt: 0,
    isFetching: false,
  });
  window.localStorage.removeItem('karios.strategyMode');
  setStrategyMode('single_track');
});

describe('PortfolioHealthCard', () => {
  it('defaults to twin-star copy when strategyMode is unset', async () => {
    window.localStorage.removeItem('karios.strategyMode');
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      sentiment: 'normal',
      multiAssetHoldings: [],
      holdings: [],
      hkHealth: null,
      markets: { CN: { regime: 'Diverging' }, HK: { regime: 'Weak' } },
    });
    renderCard();
    expect(await screen.findByText(/机会双子星 · 今日决策/)).toBeDefined();
    expect(screen.queryByText(/单轨择优 · 今日复刻/)).toBeNull();
  });

  it('renders market state + holdings from the health endpoint', async () => {
    setStrategyMode('single_track');
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-07',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [HOLDING],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect(await screen.findByText(/单轨择优 · 今日复刻/)).toBeDefined();
    expect(await screen.findByText("Weak · 空仓观望")).toBeDefined();
    expect(await screen.findByText("Strong · 进攻")).toBeDefined();
    expect(await screen.findByText("腾讯控股")).toBeDefined();
    expect(await screen.findByText("✅ 持有")).toBeDefined();
    expect(await screen.findByText("2026-08-07")).toBeDefined();
    const exp = screen.queryByText('展开');
    if (exp) fireEvent.click(exp);
    expect(screen.getByText(/今日无开仓候选（regime=Weak/)).toBeDefined();
  });

  it('shows twin-star copy (core-leg wording, no 100% hard-switch)', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      sentiment: 'normal',
      multiAssetHoldings: [],
      holdings: [],
      hkHealth: null,
      markets: { CN: { regime: 'Diverging' }, HK: { regime: 'Weak' } },
    });
    renderCard();
    expect(await screen.findByText(/机会双子星 · 今日决策/)).toBeDefined();
    expect(screen.queryByText(/100% 硬切/)).toBeNull();
    expect(screen.queryByText(/单轨择优/)).toBeNull();
    expect(await screen.findByText(/机会口径 · 核心 50%/)).toBeDefined();
    expect((await screen.findAllByText(/买入 锦江投资/)).length).toBeGreaterThan(0);
    expect(screen.getByText('锦江投资')).toBeDefined();
    expect(screen.getAllByText('000712.SZ').length).toBeGreaterThan(0);
    expect(await screen.findByText(/卫星闸 · R-wide 开闸 breadth 0\.588/)).toBeDefined();
    expect(await screen.findByText(/引擎模拟仓空/)).toBeDefined();
  });

  it('flags satellite data failure with a retry badge', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      sentiment: 'normal',
      multiAssetHoldings: [],
      holdings: [],
      hkHealth: null,
      markets: { CN: { regime: 'Diverging' }, HK: { regime: 'Weak' } },
    });
    useTwinStarActionQueryMock.mockReturnValue({
      data: undefined,
      isError: true,
      dataUpdatedAt: 0,
      isFetching: false,
    });
    renderCard();
    expect(await screen.findByText(/⚠ 数据失败 · 重试中/)).toBeDefined();
  });

  it('renders alpha events + industry fund flow info layers', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-12',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [
        {
          ...HOLDING,
          symbol: 'CN:300628',
          name: '亿联网络',
          alphaEvents: [
            {
              trend: '通信设备景气',
              grade: 'B',
              confidence: 0.85,
              daysAgo: 1,
              riskStatus: 'ok',
              focus: '5G 资本开支超预期',
            },
          ],
          industryFlow: { industry: '通信', netInflow5d: -47.69, rank5d: 26, total: 31 },
        },
      ],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect(await screen.findByText(/通信设备景气/)).toBeDefined();
    expect(screen.getByText(/催化B/)).toBeDefined();
    expect(screen.getByText(/1天前/)).toBeDefined();
    expect(screen.getByText(/通信 5日 -47.69亿（第26\/31）/)).toBeDefined();
  });

  it('flags EXIT holdings with the trigger reason', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-07',
      regime: 'Strong',
      sentiment: 'normal',
      s3Candidates: [],
      holdings: [{ ...HOLDING, action: 'EXIT', reason: 'trailing_stop（峰值回撤8.5% >= 8% 阈值）' }],
    });
    renderCard();
    expect(await screen.findByText('🔴 卖出')).toBeDefined();
    expect(screen.getByText(/trailing_stop/)).toBeDefined();
  });

  it('shows weak-regime no-candidate note and candidate chips when present', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
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
    setStrategyMode('single_track');
    fetchPortfolioHealth.mockRejectedValue(new Error('fetch failed'));
    renderCard();
    expect(await screen.findByText(/单轨择优暂不可用/)).toBeDefined();
  });

  it('distinguishes stale scores from a real no-candidate decision', async () => {
    setStrategyMode('single_track');
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-11',
      regime: 'Strong',
      s3Candidates: [],
      scoreDataAsOfDate: '2026-08-10',
      scoreFresh: false,
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    const exp2 = await screen.findByText(/单轨择优 · 今日复刻/);
    expect(exp2).toBeDefined();
    const expandBtn = screen.queryByText('展开');
    if (expandBtn) fireEvent.click(expandBtn);
    expect(await screen.findByText(/分数未更新（截至 2026-08-10）/)).toBeDefined();
    expect(screen.getByText(/分数截至 2026-08-10/)).toBeDefined();
  });

  it('collapses candidates to the top-5 buy list with backtest size + expand toggle', async () => {
    const mk = (n: string, s: number) => ({ symbol: `HK:${s}`, name: n, score: s, rs: 0.8 });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [mk('A', 100), mk('B', 99), mk('C', 98), mk('D', 97), mk('E', 96), mk('F', 95)],
      s3CandidateTotal: 19,
      s3Rules: { suggestedSizePct: 10 },
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/下午 2 点 · 股票篮买入/)).toBeDefined();
    // expand stock basket if collapsed
    const expand = screen.queryByText('展开');
    if (expand) fireEvent.click(expand);
    expect(screen.getByText(/候选池 19 只/)).toBeDefined();
    expect(screen.getByText(/每票建议 10%/)).toBeDefined();
    expect(screen.getAllByText('买 10%').length).toBe(5);
    expect(screen.queryByText('F')).toBeNull();
    fireEvent.click(screen.getByText(/展开全部 6 只/));
    expect(screen.getByText('F')).toBeDefined();
  });

  it('uses env-scaled sleeve when envScaleToday present (D3)', async () => {
    const mk = (n: string, s: number) => ({ symbol: `CN:${s}`, name: n, score: s, rs: 0.8 });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-15',
      regime: 'Strong',
      s3Candidates: [mk('A', 100), mk('B', 99)],
      s3CandidateTotal: 2,
      s3Rules: { suggestedSizePct: 12.5, envScaleToday: 1.25 },
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/每票建议 12.5%（10% × 今日环境×1.25 · 已含 D3 环境仓位）/)).toBeDefined();
    expect(screen.getAllByText('买 12.5%').length).toBe(2);
  });

  it('opens the stock page when a holding row is clicked', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
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
    setStrategyMode('single_track');
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
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
    expect(await screen.findByText(/下午 2 点 · 股票篮买入/)).toBeDefined();
    // expand stock basket if collapsed
    const expand = screen.queryByText('展开');
    if (expand) fireEvent.click(expand);

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
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
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

  it('quick-buy button opens the modal, writes watchlist, and records a paper trade', async () => {
    localStorage.clear();
    setStrategyMode('single_track');
    const fetchMock = vi.fn();
    fetchMock.mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal('fetch', fetchMock);
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
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
    expect(await screen.findByText(/下午 2 点 · 股票篮买入/)).toBeDefined();
    // expand stock basket if collapsed
    const expand = screen.queryByText('展开');
    if (expand) fireEvent.click(expand);

    const buyButtons = screen.getAllByText('买入');
    expect(buyButtons.length).toBe(2);
    fireEvent.click(buyButtons[0]);

    expect(await screen.findByText(/写入 Watchlist 自选并记入模拟盘/)).toBeDefined();
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
    expect(registryCalls.length).toBeGreaterThan(0);
    expect(localStorage.getItem('karios_buy_reminders')).toBeNull();
    vi.unstubAllGlobals();
  });

  it('renders backtest recon inside the market panel with expandable missing list', async () => {
    localStorage.clear();
    setStrategyMode('single_track');
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
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-10',
      regime: 'Strong',
      s3Candidates: [],
      holdings: [],
      hkHealth: { regime: 'Diverging', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect((await screen.findAllByText(/股票篮对账 · 2026-08-07/)).length).toBe(2);
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

  it('renders signal summary line + candidate info layers', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-12',
      regime: 'Strong',
      sentiment: 'hot',
      panicCooldown: { active: false },
      infoSummary: { holdingsCount: 1, eventHoldings: 1, industryOutflow: 1, industryInflow: 0 },
      s3Candidates: [
        {
          symbol: 'CN:600111',
          name: '北方稀土',
          ts_code: '600111.SH',
          score: 71,
          rs: 0.62,
          alphaEvents: [
            { trend: '稀土催化', grade: 'A', daysAgo: 1, riskStatus: 'ok', confidence: 0.9 },
          ],
          industryFlow: { industry: '有色金属', netInflow5d: 8.2, rank5d: 2, total: 31 },
        },
      ],
      holdings: [HOLDING],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect(await screen.findByText(/信号 · 1 持仓/)).toBeDefined();
    expect(screen.getByText(/1 只有 α 事件/)).toBeDefined();
    expect(screen.getByText(/1 只行业资金流出/)).toBeDefined();
    expect(screen.getByText(/下午 2 点 · 股票篮买入/)).toBeDefined();
    expect(screen.getByText(/稀土催化/)).toBeDefined();
    expect(screen.getByText(/有色金属 5日\+8.2亿（第2\/31）/)).toBeDefined();
  });

  it('marks gate-closed states boldly (Weak / panic / circuit)', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-12',
      regime: 'Weak',
      sentiment: 'hot',
      panicCooldown: { active: true, cooldownEndDate: '2026-08-12' },
      circuitBlocked: true,
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: { regime: 'Strong', s3Candidates: [], holdings: [] },
    });
    renderCard();
    await screen.findByText(/闸门关闭 · 今日不买/);
    // HK line (Strong) stays unmarked — only the CN panel carries the mark.
    expect(
      screen
        .getAllByText(/闸门关闭 · 今日不买/)
        .filter((el) => el.tagName === 'SPAN' && el.className.includes('font-bold')),
    ).toHaveLength(1);
  });

  it('does not mark the gate when open (Strong / Diverging)', async () => {
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 12, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-12',
      regime: 'Diverging',
      sentiment: 'hot',
      panicCooldown: { active: false },
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    await screen.findByText(/Diverging · 满仓进攻/);
    expect(screen.queryByText(/闸门关闭/)).toBeNull();
  });

  it('keeps satellite buys on the R-wide gate only — S-3 Execution Gate DEFEND does not block twin-star satellite candidates', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-01',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: true, cooldownEndDate: '2026-09-01' },
      circuitBlocked: false,
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    useDashboardSentimentQueryMock.mockReturnValue({
      data: {
        marketSentiment: {
          executionGate: {
            mode: 'DEFEND',
            allowNewEntries: false,
            marketRegime: 'Weak',
            indexLight: '红',
            srvLevel: null,
            reasons: ['regime_weak'],
          },
        },
      },
      isError: false,
      dataUpdatedAt: Date.now(),
      isFetching: false,
    });
    renderCard();
    expect((await screen.findAllByText(/买入 锦江投资/)).length).toBeGreaterThan(0);
    expect(screen.queryByText(/暂不买入/)).toBeNull();
    expect(screen.queryByText(/闸门关闭/)).toBeNull();
  });

  it('hides satellite buy names before 14:30 even when a same-day snapshot exists', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    marketHoursMock.getShanghaiMinutes.mockReturnValue(10 * 60);
    marketHoursMock.satNamesVisible.mockReturnValue(false);
    useTwinStarActionQueryMock.mockReturnValue({
      ...SAT_OPEN,
      data: {
        sat: {
          ...SAT_OPEN.data.sat,
          approx: true,
          snapshotAt: '2026-09-02T11:17:41+08:00',
        },
      },
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-01',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect((await screen.findAllByText(/候选 14:30 后公布/)).length).toBeGreaterThan(0);
    expect(screen.queryByText(/卫星缺口买入/)).toBeNull();
    expect(screen.queryByText(/000712\.SZ/)).toBeNull();
    expect(await screen.findByText(/卫星闸 · R-wide 开闸 breadth 0\.588/)).toBeDefined();
  });

  it('hides satellite buys and warns when the 12:30 snapshot failed', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    useTwinStarActionQueryMock.mockReturnValue({
      ...SAT_OPEN,
      data: {
        sat: {
          ...SAT_OPEN.data.sat,
          snapshotMissing: true,
          snapshotStale: false,
          snapshotReason: 'no_session_snapshot',
        },
      },
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-08-28',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/今日盘中快照失败 → 卫星名单不可用/)).toBeDefined();
    expect((await screen.findAllByText(/持有原油 ETF/)).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/今日盘中快照失败，卫星名单不可用/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/卫星缺口买入/)).toBeNull();
    expect(screen.queryByText(/买入 锦江投资/)).toBeNull();
  });

  it('does not paint the S-3 execution gate on twin-star even when pick=STOCK and DEFEND', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有股票篮',
        message: '择强 STOCK',
        pick: { key: 'STOCK', mom60: 6.88, symbol: 'STOCK' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-01',
      regime: 'Weak',
      sentiment: 'normal',
      panicCooldown: { active: true, cooldownEndDate: '2026-09-01' },
      circuitBlocked: false,
      infoSummary: { holdingsCount: 0, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [],
      hkHealth: null,
    });
    useDashboardSentimentQueryMock.mockReturnValue({
      data: {
        marketSentiment: {
          executionGate: {
            mode: 'DEFEND',
            allowNewEntries: false,
            marketRegime: 'Weak',
            indexLight: '红',
            srvLevel: null,
            reasons: ['regime_weak'],
          },
        },
      },
      isError: false,
      dataUpdatedAt: Date.now(),
      isFetching: false,
    });
    renderCard();
    expect(await screen.findByText(/机会双子星 · 今日决策/)).toBeDefined();
    expect(screen.queryByText(/S-3 闸门关闭/)).toBeNull();
    expect((await screen.findAllByText(/买入 锦江投资/)).length).toBeGreaterThan(0);
  });

  it('lists today\'s gap buys when recipe replay is 4/4 but live satellite is empty', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    useTwinStarActionQueryMock.mockReturnValue({
      data: {
        sat: {
          asOf: '2026-09-01',
          gateOpen: true,
          breadth: 0.626,
          gapCount: 35,
          candidates: [
            { ts: '600352.SH', amp: 1, gapPct: 2, close: 10 },
            { ts: '603339.SH', amp: 1, gapPct: 2, close: 10 },
            { ts: '601168.SH', amp: 1, gapPct: 2, close: 10 },
          ],
          note: '盘中近似（12:30 快照）',
          coreTargetPct: 50,
          satTargetPct: 50,
          book: {
            asOf: '2026-09-01',
            holdings: Array.from({ length: 4 }, (_, i) => ({
              ts: `60020${String(i).padStart(1, '0')}.SH`,
              daysLeft: 2,
            })),
            exitsDue: [],
            body: 3,
          },
        },
      },
      isError: false,
      dataUpdatedAt: Date.now(),
      isFetching: false,
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-01',
      regime: 'Diverging',
      sentiment: 'normal',
      multiAssetHoldings: [],
      holdings: [],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText(/^今日$/)).toBeDefined();
    expect(screen.getByText(/对照，不是券商仓/)).toBeDefined();
    expect(screen.getAllByText(/600352\.SH/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/持仓簿满 4\/4/)).toBeNull();
    expect(screen.getByText(/刷新行情/)).toBeDefined();
    expect(screen.getByText(/引擎模拟 4 只/)).toBeDefined();
  });

  it('keeps ETFs and lists buy size as % of NAV when STOCK has no executable names', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    useTwinStarActionQueryMock.mockReturnValue({
      data: {
        sat: {
          asOf: '2026-09-01',
          gateOpen: true,
          breadth: 0.626,
          gapCount: 35,
          candidates: [{ ts: '600352.SH', amp: 1, gapPct: 2, close: 10 }],
          coreTargetPct: 50,
          satTargetPct: 50,
          book: {
            asOf: '2026-09-01',
            holdings: Array.from({ length: 4 }, (_, i) => ({ ts: `60100${i}.SH`, daysLeft: 2 })),
            exitsDue: [],
            body: 3,
          },
        },
      },
      isError: false,
      dataUpdatedAt: Date.now(),
      isFetching: false,
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '择强→股票',
        message: '择强 STOCK（mom60 6.88%）> ETF → 卖出 ETF:513350 回股票篮',
        pick: {
          key: 'STOCK',
          mom60: 6.88,
          symbol: 'STOCK',
          all_mom: { STOCK: 6.88, NASDAQ: 5.16, OIL: 4.98, GOLD: 1, BOND10: 0.4 },
        },
        etfPick: { key: 'NASDAQ', mom60: 5.16, symbol: 'ETF:513110' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-01',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [],
      multiAssetHoldings: [
        { symbol: 'ETF:513110', positionPct: 48.6, name: '纳指' },
        { symbol: 'ETF:513350', positionPct: 42, name: '原油' },
      ],
      hkHealth: { regime: 'Weak', s3Candidates: [], holdings: [] },
    });
    renderCard();
    expect((await screen.findAllByText(/不要为 STOCK 清空 ETF/)).length).toBeGreaterThan(0);
    expect(await screen.findByText(/^今日$/)).toBeDefined();
    expect(screen.queryByText(/今日无买卖/)).toBeNull();
    expect(screen.getAllByText(/600352\.SH/).length).toBeGreaterThan(0);
    expect(screen.getAllByText('持有').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/减仓 12\.5/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/减仓 2/)).toBeNull();
    expect(screen.queryByText(/资金调向 STOCK/)).toBeNull();
  });

  it('does not prompt 卖出 on a 0% leftover NASDAQ ETF after the user sold it', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 7.89, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-02',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [],
      multiAssetHoldings: [
        { symbol: 'ETF:513110', positionPct: 0, name: '纳指' },
        { symbol: 'ETF:513350', positionPct: 51.5, name: '原油' },
      ],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText('ETF:513350')).toBeDefined();
    expect(screen.queryByText('ETF:513110')).toBeNull();
    expect(screen.queryByText('卖出')).toBeNull();
    expect(screen.getAllByText('持有').length).toBeGreaterThan(0);
  });

  it('writes a satellite gap buy onto the watchlist with cost and size', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    const save = vi.spyOn(watchlistStorage, 'saveWatchlist').mockResolvedValue({ ok: true, synced: true });
    vi.spyOn(watchlistStorage, 'loadWatchlist').mockReturnValue([]);
    vi.spyOn(userTrades, 'recordUserTrade').mockResolvedValue({ id: 't1' } as never);
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 4.98, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-02',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      s3Candidates: [],
      holdings: [],
      multiAssetHoldings: [{ symbol: 'ETF:513350', positionPct: 50, name: '原油' }],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText('卫星缺口买入')).toBeDefined();
    fireEvent.click(screen.getAllByRole('button', { name: '买入' })[0]!);
    expect(await screen.findByText(/写入 Watchlist 自选并记入模拟盘/)).toBeDefined();
    await waitFor(() => {
      expect(screen.getByPlaceholderText('0.000')).not.toBeDisabled();
    });
    fireEvent.click(screen.getByRole('button', { name: '确认买入' }));
    await waitFor(() => expect(save).toHaveBeenCalled());
    const written = save.mock.calls[0]?.[0] as Array<{ symbol: string; positionPct?: number; costPrice?: number; name?: string | null }>;
    expect(written[0]?.symbol).toBe('CN:000712');
    expect(written[0]?.positionPct).toBe(12.5);
    expect(written[0]?.costPrice).toBe(10.5);
    expect(written[0]?.name).toBe('锦江投资');
    expect(userTrades.recordUserTrade).toHaveBeenCalledWith(
      expect.objectContaining({ symbol: 'CN:000712', side: 'BUY', positionPct: 12.5 }),
    );
    save.mockRestore();
  });

  it('puts live CN holdings in 卫星仓 instead of 股票篮应轮出 when pick=OIL', async () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    useTwinStarActionQueryMock.mockReturnValue({
      data: {
        sat: {
          asOf: '2026-09-02',
          gateOpen: true,
          breadth: 0.526,
          gapCount: 32,
          candidates: [{ ts: '603221.SH', name: '爱玛科技', amp: 1, gapPct: 2, close: 10 }],
          note: '盘中近似（15:00 快照）',
          coreTargetPct: 50,
          satTargetPct: 50,
          frozen: true,
          book: { asOf: '2026-09-02', holdings: [], exitsDue: [], body: 3 },
        },
      },
      isError: false,
      dataUpdatedAt: Date.now(),
      isFetching: false,
    });
    fetchPortfolioHealth.mockResolvedValue({
      multiAssetSleeve: {
        active: true,
        action: 'HOLD',
        label: '持有原油 ETF',
        message: '择强 OIL',
        pick: { key: 'OIL', mom60: 7.89, symbol: 'ETF:513350' },
        mode: 'mom_compare',
      },
      tradeDate: '2026-09-02',
      regime: 'Diverging',
      sentiment: 'normal',
      panicCooldown: { active: false },
      infoSummary: { holdingsCount: 4, eventHoldings: 0, industryOutflow: 0, industryInflow: 0 },
      s3Candidates: [],
      holdings: [
        { ...HOLDING, symbol: 'CN:300413', name: '芒果超媒', positionPct: 12.5, action: 'HOLD', costPrice: 20, entryDate: '2026-09-02', lastClose: 21, pnlPct: 5 },
        { ...HOLDING, symbol: 'CN:603318', name: '水发燃气', positionPct: 12.5, action: 'HOLD', costPrice: 20, entryDate: '2026-09-02', lastClose: 21, pnlPct: 5 },
        { ...HOLDING, symbol: 'CN:600540', name: '新赛股份', positionPct: 12.5, action: 'HOLD', costPrice: 20, entryDate: '2026-09-02', lastClose: 21, pnlPct: 5 },
        { ...HOLDING, symbol: 'CN:301012', name: '扬电科技', positionPct: 12.5, action: 'HOLD', costPrice: 20, entryDate: '2026-09-02', lastClose: 21, pnlPct: 5 },
      ],
      multiAssetHoldings: [{ symbol: 'ETF:513350', positionPct: 51.5, name: '原油' }],
      hkHealth: null,
    });
    renderCard();
    expect(await screen.findByText('卫星仓')).toBeDefined();
    expect(screen.getByText(/你卫星仓 4\/4/)).toBeDefined();
    expect(screen.getByText('芒果超媒')).toBeDefined();
    expect(screen.getByText('水发燃气')).toBeDefined();
    expect(screen.getByText('新赛股份')).toBeDefined();
    expect(screen.getByText('扬电科技')).toBeDefined();
    expect(screen.getByText(/股票篮未启用 · 核心是 OIL · 卫星见上方/)).toBeDefined();
    expect(screen.queryByText(/股票篮应轮出/)).toBeNull();
    expect(screen.queryByText(/核心腿非 STOCK · 应轮出/)).toBeNull();
    expect(screen.queryByText('卫星缺口买入')).toBeNull();
    expect(screen.queryByText('603221.SH')).toBeNull();
    expect(screen.queryByText('🔴 卖出')).toBeNull();
    expect(screen.getByText('复制止损单')).toBeDefined();
    expect(screen.getAllByText(/已持 1\/3/).length).toBe(4);
    expect(screen.getAllByText(/到期 2026-09-04/).length).toBe(4);
    expect(screen.getAllByText(/止损 19/).length).toBe(4);
    expect(screen.queryByText(/补录入场日/)).toBeNull();
  });
});
