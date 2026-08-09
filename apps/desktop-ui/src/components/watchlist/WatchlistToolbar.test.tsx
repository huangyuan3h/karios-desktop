import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { WatchlistToolbar, type WatchlistToolbarProps } from './WatchlistToolbar';

const base = (over: Partial<WatchlistToolbarProps> = {}): WatchlistToolbarProps => ({
  trendUpdatedAt: '2026-08-07T08:00:00+08:00',
  latestAutomation: null,
  syncBusy: false,
  syncStage: null,
  syncProgress: null,
  syncLogs: [],
  automationBusy: false,
  automationStage: null,
  automationLogs: [],
  automationMsg: null,
  automationSkipRun: null,
  syncMsg: null,
  copyMdStatus: null,
  error: null,
  trendBusy: false,
  itemsCount: 10,
  sortedItemsCount: 10,
  copyMdBusy: false,
  onManualRefreshTrend: vi.fn(),
  onReferenceTable: vi.fn(),
  onCopyMarkdown: vi.fn(),
  onSyncFromScreener: vi.fn(),
  onRunAutomation: vi.fn(),
  onForceAutomationFromSkip: vi.fn(),
  ...over,
});

describe('WatchlistToolbar', () => {
  it('shows scores update time from trendUpdatedAt', () => {
    render(<WatchlistToolbar {...base()} />);
    expect(screen.getByText(/Scores updated at/)).toBeInTheDocument();
  });

  it('shows "Scores not loaded yet." when trendUpdatedAt is null', () => {
    render(<WatchlistToolbar {...base({ trendUpdatedAt: null })} />);
    expect(screen.getByText('Scores not loaded yet.')).toBeInTheDocument();
  });

  it('disables Refresh when trendBusy or no items', () => {
    render(<WatchlistToolbar {...base({ trendBusy: true })} />);
    expect(screen.getByLabelText('Refresh watchlist scores')).toBeDisabled();
  });

  it('fires manual refresh on Refresh click', () => {
    const onManualRefreshTrend = vi.fn();
    render(<WatchlistToolbar {...base({ onManualRefreshTrend })} />);
    fireEvent.click(screen.getByLabelText('Refresh watchlist scores'));
    expect(onManualRefreshTrend).toHaveBeenCalled();
  });

  it('renders sync progress with progress bar and last-4 logs', () => {
    render(
      <WatchlistToolbar
        {...base({
          syncBusy: true,
          syncStage: 'Fetching rows',
          syncProgress: { cur: 3, total: 10 },
          syncLogs: ['a', 'b', 'c', 'd', 'e'],
        })}
      />,
    );
    expect(screen.getByText('3/10')).toBeInTheDocument();
    expect(screen.getByText('Fetching rows')).toBeInTheDocument();
    expect(screen.getByText('e')).toBeInTheDocument();
    expect(screen.queryByText('a')).not.toBeInTheDocument();
  });

  it('shows automation skip reason with Force run button', () => {
    const onForceAutomationFromSkip = vi.fn();
    render(
      <WatchlistToolbar
        {...base({
          automationSkipRun: { runId: 'r', tradeDate: '2026-08-07', skipReason: 'too_soon' } as never,
          onForceAutomationFromSkip,
        })}
      />,
    );
    expect(screen.getByText(/Automation skipped \(too_soon\)/)).toBeInTheDocument();
    fireEvent.click(screen.getByText('Force run'));
    expect(onForceAutomationFromSkip).toHaveBeenCalled();
  });

  it('renders copy status in ok/error colors', () => {
    const { rerender } = render(
      <WatchlistToolbar {...base({ copyMdStatus: { ok: true, text: 'Copied!' } })} />,
    );
    expect(screen.getByText('Copied!')).toHaveClass('text-emerald-600');
    rerender(<WatchlistToolbar {...base({ copyMdStatus: { ok: false, text: 'Failed' } })} />);
    expect(screen.getByText('Failed')).toHaveClass('text-red-600');
  });

  it('shows error message in red', () => {
    render(<WatchlistToolbar {...base({ error: 'boom' })} />);
    expect(screen.getByText('boom')).toHaveClass('text-red-600');
  });

  it('disables Import when sync or automation busy', () => {
    render(<WatchlistToolbar {...base({ syncBusy: true })} />);
    expect(screen.getByText('Import from screener')).toBeDisabled();
  });

  it('disables Run automation while automation busy', () => {
    render(<WatchlistToolbar {...base({ automationBusy: true })} />);
    expect(screen.getByText('Run automation')).toBeDisabled();
  });

  it('shows Copying… while copy busy', () => {
    render(<WatchlistToolbar {...base({ copyMdBusy: true })} />);
    expect(screen.getByText('Copying…')).toBeDisabled();
  });
});
