import fs from 'node:fs';
import path from 'node:path';

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FundFlowPanoramaCard, FUND_FLOW_REFERENCE_URL } from './FundFlowPanoramaCard';

const flowMock = vi.hoisted(() => ({ rows: [] as unknown[], isError: false }));

vi.mock('@/lib/queries/backtest', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/queries/backtest')>();
  return {
    ...actual,
    useFundFlowSeriesQuery: () => ({
      data: { ok: true, start: '', end: '', rows: flowMock.rows },
      isError: flowMock.isError,
      isFetching: false,
      refetch: vi.fn(),
    }),
  };
});

function row(date: string, share: number): Record<string, unknown> {
  return {
    date,
    etfShareYi: share,
    etfShareD20Pct: 1.2,
    gateOn: false,
    marginTrillion: 2.2,
    marginD20Pct: 0.5,
    northDailyYi: 10,
    northD20Yi: 120,
    smNetPct: 1,
    natDailyYi: 2,
    natD20Yi: 30,
    marginDailyYi: 1,
    marginD20Yi: 10,
  };
}

const ROWS = [row('2026-11-04', 980), row('2026-11-05', 1000), row('2026-11-06', 1022)];

beforeEach(() => {
  flowMock.rows = ROWS;
  flowMock.isError = false;
});

describe('FundFlowPanoramaCard (Dashboard)', () => {
  it('renders the three context charts at Dashboard scale', () => {
    render(<FundFlowPanoramaCard />);
    expect(screen.getByTestId('dashboard-fund-flow-panel')).toBeDefined();
    expect(screen.getAllByText(/资金流全景/).length).toBeGreaterThan(0);
    expect(screen.getByText(/国家队宽基 ETF 份额 · 两融余额 · 北向资金/)).toBeDefined();
    expect(screen.getByText(/只读上下文 · 不影响交易/)).toBeDefined();

    // 国家队份额 / 两融 / 北向 — and only those three.
    expect(screen.getByTestId('ff-etf')).toBeDefined();
    expect(screen.getByTestId('ff-margin')).toBeDefined();
    expect(screen.getByTestId('ff-north')).toBeDefined();
    expect(screen.queryByTestId('ff-total')).toBeNull();
    expect(screen.queryByTestId('ff-sm')).toBeNull();
  });

  it('links to the public reference page without leaving trading surfaces', () => {
    render(<FundFlowPanoramaCard />);
    const link = screen.getByRole('link', { name: /参考页：zhibiao/ });
    expect(link.getAttribute('href')).toBe(FUND_FLOW_REFERENCE_URL);
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toContain('noopener');
  });

  it('surfaces the unavailable state instead of empty charts', () => {
    flowMock.isError = true;
    flowMock.rows = [];
    render(<FundFlowPanoramaCard />);
    expect(screen.getByText(/资金流数据不可用/)).toBeDefined();
    expect(screen.queryByTestId('ff-etf')).toBeNull();
  });
});

describe('Dashboard placement', () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'pages', 'DashboardPage.tsx'), 'utf-8');

  it('mounts the panorama as a standalone full-width block, not inside the two columns', () => {
    expect(source).toContain("from '@/components/dashboard/FundFlowPanoramaCard'");
    expect(source).toContain('<FundFlowPanoramaCard');
    // Rendered after the two-column card grid closes (full width, not in `renderCard`).
    const gridEnd = source.indexOf('})()}');
    expect(gridEnd).toBeGreaterThan(0);
    expect(source.indexOf('<FundFlowPanoramaCard')).toBeGreaterThan(gridEnd);
  });

  it('keeps fund-flow charts off the Watchlist surface', () => {
    const watchlist = fs.readFileSync(
      path.join(__dirname, '..', 'pages', 'WatchlistPage.tsx'),
      'utf-8',
    );
    expect(watchlist).not.toContain('FundFlowPanel');
    expect(watchlist).not.toContain('FundFlowPanoramaCard');
  });

  it('no longer mounts the compact national-team card on Dashboard', () => {
    expect(source).not.toContain('NationalTeamCard');
    expect(source).not.toContain('nationalTeam');
    expect(source).not.toContain('国家队 / 资金确认');
    // The panorama is the only 资金流 surface left on this page.
    expect(source.match(/<FundFlowPanoramaCard/g)?.length).toBe(1);
  });
});
