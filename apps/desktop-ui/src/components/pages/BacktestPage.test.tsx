import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { BacktestPage } from './BacktestPage';

const { apiGetJson } = vi.hoisted(() => ({ apiGetJson: vi.fn() }));
vi.mock('@/lib/api/client', () => ({ apiGetJson }));

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <BacktestPage />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  apiGetJson.mockReset();
  apiGetJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/api/backtest/overview')) {
      return {
        ok: true,
        cnBaseline: {
          generatedAt: '2026-08-12T02:25:19Z',
          windows: {
            OOS2: { totalNetPnlPct: 112.654, winRate: 0.48, sharpe: 5.22, trades: null, maxDrawdownPct: 23.346 },
            train: { totalNetPnlPct: 76.734, winRate: 0.435, sharpe: 3.31, trades: null, maxDrawdownPct: 16.637 },
            valid: { totalNetPnlPct: 88.212, winRate: 0.613, sharpe: 8.8, trades: null, maxDrawdownPct: 11.778 },
          },
        },
        hkBaseline: {
          generatedAt: '2026-08-10T13:35:04Z',
          windows: {
            OOS2: { totalNetPnlPct: 267.987, winRate: 0.39, sharpe: 2.21, trades: null, maxDrawdownPct: 29.719 },
            train: { totalNetPnlPct: 26.855, winRate: 0.414, sharpe: 1.91, trades: null, maxDrawdownPct: 18.863 },
            valid: { totalNetPnlPct: 60.647, winRate: 0.417, sharpe: 6.32, trades: null, maxDrawdownPct: 8.329 },
          },
        },
        rollingOos: {
          windowStart: '2026-05-13',
          windowEnd: '2026-08-11',
          warning: true,
          warnings: ['HK: -8.5% dd=19.5% sharpe=-3.2 trades=55'],
          markets: {
            CN: { closed: 1, winRate: 0.0, totalNetPnlPct: -0.633, maxDrawdownPct: 0.633, sharpe: null },
            HK: { closed: 55, winRate: 0.255, totalNetPnlPct: -8.451, maxDrawdownPct: 19.497, sharpe: -3.2 },
          },
        },
        longWindowCN: {
          window: '2021-08-01 ~ 2026-08-11',
          totalNetPnlPct: 250.8,
          maxDrawdownPct: 40.9,
          sharpe: 2.65,
          trades: 1401,
          byYear: { 2021: 341, 2022: 93, 2023: -263, 2024: 606, 2025: 956, 2026: 1325 },
        },
      };
    }
    if (String(path).includes('/api/backtest/recon/latest')) {
      return {
        ok: true,
        items: [
          { reconDate: '2026-08-07', market: 'HK', window: 'valid', expected: 19, actual: 0, aligned: 0, missing: 19, extra: 0 },
          { reconDate: '2026-08-07', market: 'CN', window: 'valid', expected: 0, actual: 0, aligned: 0, missing: 0, extra: 0 },
        ],
      };
    }
    if (String(path).includes('/api/backtest/core-audit')) {
      return {
        ok: true,
        day: '2026-09-01',
        gate: { regime: '—', panicActive: false, gateOpen: false },
        holdings: [],
        counts: { ok: 0, warn: 0, violation: 0 },
      };
    }
    if (String(path).includes('/api/backtest/paper-vs-backtest')) {
      return {
        ok: true,
        report: {
          generatedAt: '2026-08-12',
          sampleCount: 2,
          verdict: '样本 <20 笔：结论待积累（C4 未定案）',
          rows: [
            {
              symbol: 'HK:00622',
              market: 'HK',
              entryDate: '2026-08-10',
              paper: { pnlPct: 2.12, closeReason: 'trailing_stop' },
              backtest: { pnlPct: 2.02, closeReason: 'end_of_window' },
              diff: { entryPriceDiffPct: 0 },
              note: '存在差异',
            },
          ],
          summary: {
            paper: { closed: 2, winRate: 0.5, avgPnlPct: -1.0 },
            backtestMatched: { closed: 1, winRate: 1.0, avgPnlPct: 2.02 },
          },
        },
      };
    }
    if (String(path).includes('/api/backtest/sleeve-nav')) return {};
    if (String(path).includes('/api/backtest/timeline')) {
      return {
        ok: true,
        strategy: 'twin_star',
        mode: 'opportunity_twin_star',
        opportunity: true,
        summary: { fusedPct: 12.5, corePct: 8.1, basePct: 3.2, maxDdFusedPct: 9.4 },
        rows: [
          {
            date: '2026-08-01',
            deployedPct: 100,
            idlePct: 0,
            positions: 0,
            cnPositions: 0,
            hkPositions: 0,
            stockMarket: '',
            stockSymbols: [],
            stockMom: null,
            pick: 'GOLD',
            pickTs: '518880.SH',
            navBase: 1.01,
            navSleeve: null,
            navSingle: 1.05,
            navMulti: 1.05,
            navBaseReturnPct: 1,
            navSingleReturnPct: 5,
            navMultiReturnPct: 5,
            satNav: 1.02,
            satNavReturnPct: 2,
            coreNav: 1.08,
            coreNavReturnPct: 8,
            satPositions: 1,
            satSlots: 1,
            satActive: true,
            gapCount: 3,
            strictCount: 1,
            skipT1Count: 1,
            filledToday: 1,
            idleSlots: 3,
            gateOpen: true,
            exits: [],
          },
        ],
        blotter: [
          {
            kind: 'skip_t1',
            date: '2026-08-01',
            ts: '000001.SZ',
            amp: 1.2,
            ampRank: 1,
            skipT1: true,
            contribPct: 0,
            closeReason: 'skip_t1_limit',
          },
          {
            kind: 'fill',
            date: '2026-08-04',
            ts: '000002.SZ',
            amp: 0.8,
            ampRank: 2,
            skipT1: false,
            entryDate: '2026-08-01',
            exitDate: '2026-08-04',
            exitDue: '2026-08-04',
            pnlPct: -1.5,
            contribPct: -0.38,
            closeReason: 'body_exit',
            heldDays: 3,
          },
        ],
      };
    }
    if (String(path).includes('/api/backtest/return-attribution')) {
      return { ok: true, rows: [], summary: {} };
    }
    if (String(path).includes('/api/backtest/twin-star')) {
      return { ok: true, core: {}, sat: {} };
    }
    if (String(path).includes('/api/backtest/exit-attribution')) {
      return {
        ok: true,
        days: 5,
        closedCount: 0,
        withForwardCount: 0,
        excluded: 0,
        insufficient: true,
        hint: null,
        overall: {
          count: 0,
          avgFwdPct: null,
          earlyCount: 0,
          wellCount: 0,
          neutralCount: 0,
          earlyRate: null,
          wellRate: null,
        },
        byReason: {},
        exposure: { maxSimultaneous: 0, singleStockWeightFloorPct: null, note: '' },
      };
    }
    if (String(path).includes('/api/backtest/correlation-status')) {
      return {
        ok: true,
        capPct: 30,
        clusters: {},
        overLimit: [],
        blockedSymbols: [],
        topPairs: [],
        empiricalNote: null,
      };
    }
    // Keep other compare-tab endpoints from throwing and unmounting the page.
    if (String(path).includes('/api/backtest/')) return { ok: true };
    return { ok: true };
  });
});

describe('BacktestPage', () => {
  it('shows opportunity twin-star habit timeline on compare tab', async () => {
    renderPage();
    expect(await screen.findByText(/机会双子星 · 习惯C1\+14:30卖（Live）/)).toBeDefined();
    expect(await screen.findByText(/核心目标%/)).toBeDefined();
    expect(screen.getByText(/择强单轨累计/)).toBeDefined();
    expect(screen.getAllByText(/滚动过去一年/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/产品过去一年/)).toBeDefined();
    expect(screen.getByText(/三窗 · OOS2/)).toBeDefined();
    expect(screen.getByText(/NAV 叠加/)).toBeDefined();
    expect(screen.getByText(/开闸占用/)).toBeDefined();
    expect(screen.getByText('核心NAV%')).toBeDefined();
    expect(screen.getByText('空槽回核')).toBeDefined();
    expect(screen.getByText('卫星 blotter')).toBeDefined();
    expect(screen.getAllByText(/涨停跳过/).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('000001.SZ')).toBeDefined();
    expect(screen.getByText('000002.SZ')).toBeDefined();
  });

  it('switches timeline query to the OOS2 gate window', async () => {
    renderPage();
    expect(await screen.findByText(/机会双子星 · 习惯C1\+14:30卖（Live）/)).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: /三窗 · OOS2/ }));
    expect(
      apiGetJson.mock.calls.some((c: unknown[]) => String(c[0]).includes('start=2024-08-01')),
    ).toBe(true);
  });
  it('shows the S-3 conclusion board with baselines, long window and params', async () => {
    renderPage();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/S-3 股票腿三窗/)).toBeDefined();
    expect(await screen.findByText('112.7%')).toBeDefined();
    expect(screen.getByText('88.2%')).toBeDefined();
    expect(screen.getByText('+250.8%')).toBeDefined();
    expect(screen.getByText('1401 笔')).toBeDefined();
    expect(screen.getByText('2023')).toBeDefined();
    expect(screen.getByText('-263')).toBeDefined();
    expect(screen.getAllByText('score 65').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('RS 前 50%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText('RS 前 40%').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('移动 -12%')).toBeDefined();
  });

  it('shows rolling OOS warning and recon strip', async () => {
    renderPage();
    expect(await screen.findByText(/回测 vs Paper 对账/)).toBeDefined();
    expect(screen.getByText('缺 19 · 多 0')).toBeDefined();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/滚动 OOS（最近 90 天/)).toBeDefined();
    expect(screen.getByText(/HK: -8.5% dd=19.5% sharpe=-3.2 trades=55/)).toBeDefined();
    expect(screen.getAllByText('港股').length).toBeGreaterThanOrEqual(1);
  });

  it('shows the C4 paper-vs-backtest comparison with verdict banner', async () => {
    renderPage();
    expect(await screen.findByText(/C4 · paper vs 回测逐笔对照/)).toBeDefined();
    expect(await screen.findByText(/样本 <20 笔：结论待积累（C4 未定案）/)).toBeDefined();
    expect(screen.getByText('50.0%')).toBeDefined();
    expect(screen.getByText('-1.0%')).toBeDefined();
    expect(screen.getByText('100.0%')).toBeDefined();
    expect(screen.getByText('HK:00622')).toBeDefined();
    expect(screen.getByText('trailing_stop')).toBeDefined();
    expect(screen.getByText('存在差异')).toBeDefined();
  });

  it('collapses the advanced parameter tools behind a toggle', async () => {
    renderPage();
    fireEvent.click(screen.getByText('回测基线'));
    expect(await screen.findByText(/高级：参数敏感度工具/)).toBeDefined();
    expect(screen.queryByText('运行回测')).toBeNull();
    fireEvent.click(screen.getByText(/高级：参数敏感度工具/));
    expect(screen.getByText('运行回测')).toBeDefined();
    expect(screen.getByText('敏感度网格 (36)')).toBeDefined();
  });
});
