import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { WatchlistImportDebug } from './WatchlistImportDebug';

const ROWS = [
  {
    symbol: 'CN:600519',
    name: '贵州茅台',
    trendOk: true,
    score: 85,
    intradayChgPct: 3.2,
    gapUp: true,
    riskAlerts: [{ code: 'R1', message: '高位风险', severity: 'block' }],
    stopLossPrice: 1500.5,
    buyWhy: '趋势良好',
    marketRegime: 'Strong',
  },
  {
    symbol: 'CN:000858',
    name: '五粮液',
    trendOk: false,
    score: 40,
    intradayChgPct: -1.2,
    gapUp: false,
    missingData: ['bars', 'daily'],
    stopLossPrice: null,
    buyWhy: null,
  },
];

const baseProps = (over: Record<string, unknown> = {}) => ({
  importDebug: { rows: ROWS as never, updatedAt: '2026-08-07T08:00:00+08:00', scanned: 2, funnel: { total: 2, kept: 1, rejected: 1 }, trendOkCount: 1 } as never,
  importDebugOpen: true,
  setImportDebugOpen: vi.fn(),
  importDebugFilter: '',
  setImportDebugFilter: vi.fn(),
  importDebugScoreSortDir: 'desc' as const,
  setImportDebugScoreSortDir: vi.fn(),
  watchlistSet: new Set<string>(),
  addSymbolToWatchlist: vi.fn(),
  setCode: vi.fn(),
  setError: vi.fn(),
  ...over,
});

describe('WatchlistImportDebug', () => {
  it('renders rows with trend icons, alerts, and notes', () => {
    render(<WatchlistImportDebug {...baseProps()} />);
    expect(screen.getByText('CN:600519')).toBeInTheDocument();
    expect(screen.getByText('✅')).toBeInTheDocument();
    expect(screen.getByText('❌')).toBeInTheDocument();
    expect(screen.getByText('高位风险')).toBeInTheDocument();
    expect(screen.getByText('bars, daily')).toBeInTheDocument();
  });

  it('shows Add button for missing symbols and In watchlist for existing', () => {
    render(<WatchlistImportDebug {...baseProps({ watchlistSet: new Set(['CN:600519']) })} />);
    expect(screen.getAllByText('In watchlist').length).toBe(1);
    expect(screen.getByText('Add')).toBeInTheDocument();
  });

  it('adds symbol via Add button', () => {
    const addSymbolToWatchlist = vi.fn();
    render(<WatchlistImportDebug {...baseProps({ addSymbolToWatchlist })} />);
    fireEvent.click(screen.getAllByText('Add')[0]);
    expect(addSymbolToWatchlist).toHaveBeenCalledWith('CN:600519');
  });

  it('fills code and clears error on symbol click', () => {
    const setCode = vi.fn();
    const setError = vi.fn();
    render(<WatchlistImportDebug {...baseProps({ setCode, setError })} />);
    fireEvent.click(screen.getByText('CN:600519'));
    expect(setCode).toHaveBeenCalledWith('CN:600519');
    expect(setError).toHaveBeenCalledWith(null);
  });

  it('filters rows by symbol substring', () => {
    render(<WatchlistImportDebug {...baseProps({ importDebugFilter: '000858' })} />);
    expect(screen.queryByText('CN:600519')).not.toBeInTheDocument();
    expect(screen.getByText('CN:000858')).toBeInTheDocument();
  });

  it('sorts by score desc and asc (nulls last)', () => {
    const rows = [...ROWS, { symbol: 'CN:999999', name: 'x', score: null, trendOk: null, missingData: [], riskAlerts: [] }];
    const { rerender } = render(
      <WatchlistImportDebug {...baseProps({ importDebug: { rows: rows as never, updatedAt: null, scanned: 3, trendOkCount: 1 } as never })} />,
    );
    const symsDesc = screen
      .getAllByRole('row')
      .slice(1)
      .map((r) => r.querySelector('td button')?.textContent ?? '');
    expect(symsDesc).toEqual(['CN:600519', 'CN:000858', 'CN:999999']);
    rerender(
      <WatchlistImportDebug
        {...baseProps({
          importDebug: { rows: rows as never, updatedAt: null, scanned: 3, trendOkCount: 1 } as never,
          importDebugScoreSortDir: 'asc',
        })}
      />,
    );
    const symsAsc = screen
      .getAllByRole('row')
      .slice(1)
      .map((r) => r.querySelector('td button')?.textContent ?? '');
    expect(symsAsc).toEqual(['CN:000858', 'CN:600519', 'CN:999999']);
  });

  it('toggles sort direction and clears filter', () => {
    const setImportDebugScoreSortDir = vi.fn();
    const setImportDebugFilter = vi.fn();
    render(
      <WatchlistImportDebug
        {...baseProps({
          importDebugFilter: 'x',
          setImportDebugScoreSortDir,
          setImportDebugFilter,
        })}
      />,
    );
    fireEvent.click(screen.getByLabelText('Sort by score'));
    expect(setImportDebugScoreSortDir).toHaveBeenCalledWith(expect.any(Function));
    fireEvent.click(screen.getByText('Clear'));
    expect(setImportDebugFilter).toHaveBeenCalledWith('');
  });

  it('shows empty state', () => {
    render(<WatchlistImportDebug {...baseProps({ importDebug: { rows: [], updatedAt: null, scanned: 0, trendOkCount: 0 } as never })} />);
    expect(screen.getByText('No import results yet.')).toBeInTheDocument();
  });
});
