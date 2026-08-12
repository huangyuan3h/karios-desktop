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
    throw new Error(`unexpected call: ${path}`);
  });
});

describe('BacktestPage', () => {
  it('shows the S-3 conclusion board with baselines, long window and params', async () => {
    renderPage();
    expect(await screen.findByText(/S-3 回测结论（定案口径/)).toBeDefined();
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
    expect(await screen.findByText(/滚动 OOS（最近 90 天/)).toBeDefined();
    expect(screen.getByText(/HK: -8.5% dd=19.5% sharpe=-3.2 trades=55/)).toBeDefined();
    expect(screen.getAllByText('港股').length).toBeGreaterThanOrEqual(1);
    expect(await screen.findByText(/回测 vs Paper 对账/)).toBeDefined();
    expect(screen.getByText('缺 19 · 多 0')).toBeDefined();
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
    expect(await screen.findByText(/高级：参数敏感度工具/)).toBeDefined();
    expect(screen.queryByText('运行回测')).toBeNull();
    fireEvent.click(screen.getByText(/高级：参数敏感度工具/));
    expect(screen.getByText('运行回测')).toBeDefined();
    expect(screen.getByText('敏感度网格 (36)')).toBeDefined();
  });
});
