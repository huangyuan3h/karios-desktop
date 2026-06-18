'use client';

import * as React from 'react';
import { ExternalLink, Play, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { formatAutomationSummary, type AutomationRun } from '@/lib/watchlist-automation';

export type WatchlistToolbarProps = {
  trendUpdatedAt: string | null;
  latestAutomation: AutomationRun | null;
  syncBusy: boolean;
  syncStage: string | null;
  syncProgress: { cur: number; total: number } | null;
  syncLogs: string[];
  automationBusy: boolean;
  automationStage: string | null;
  automationLogs: string[];
  automationMsg: string | null;
  automationSkipRun: AutomationRun | null;
  syncMsg: string | null;
  copyMdStatus: { ok: boolean; text: string } | null;
  error: string | null;
  trendBusy: boolean;
  itemsCount: number;
  sortedItemsCount: number;
  copyMdBusy: boolean;
  onManualRefreshTrend: () => void;
  onReferenceTable: () => void;
  onCopyMarkdown: () => void;
  onSyncFromScreener: () => void;
  onRunAutomation: () => void;
  onForceAutomationFromSkip: () => void;
};

export function WatchlistToolbar({
  trendUpdatedAt,
  latestAutomation,
  syncBusy,
  syncStage,
  syncProgress,
  syncLogs,
  automationBusy,
  automationStage,
  automationLogs,
  automationMsg,
  automationSkipRun,
  syncMsg,
  copyMdStatus,
  error,
  trendBusy,
  itemsCount,
  sortedItemsCount,
  copyMdBusy,
  onManualRefreshTrend,
  onReferenceTable,
  onCopyMarkdown,
  onSyncFromScreener,
  onRunAutomation,
  onForceAutomationFromSkip,
}: WatchlistToolbarProps) {
  return (
    <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0 flex-1">
        <div className="text-lg font-semibold">Watchlist</div>
        <div className="mt-1 text-sm text-[var(--k-muted)]">
          Manage the stocks you are watching.
        </div>
        <div className="mt-1 text-xs text-[var(--k-muted)]">
          Names are resolved from Market cache. If names are missing, go to Market and click Sync
          once.
        </div>
        <div className="mt-1 text-xs text-[var(--k-muted)]">
          {trendUpdatedAt
            ? `Scores updated at ${new Date(trendUpdatedAt).toLocaleString()} (auto refresh: 10 min)`
            : 'Scores not loaded yet.'}
        </div>
        <div className="mt-1 text-xs text-[var(--k-muted)]">
          {formatAutomationSummary(latestAutomation) ?? 'Last automation: —'}
          {' · '}
          Next scheduled: weekdays 17:30 (Asia/Shanghai)
        </div>
        {syncBusy && syncStage ? (
          <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
            <div className="flex items-center justify-between gap-2">
              <div className="font-medium">Import from screener</div>
              <div className="text-[var(--k-muted)]">
                {syncProgress ? `${syncProgress.cur}/${syncProgress.total}` : '…'}
              </div>
            </div>
            <div className="mt-1 text-[var(--k-muted)]">{syncStage}</div>
            {syncProgress && syncProgress.total > 0 ? (
              <div className="mt-2 h-2 w-full overflow-hidden rounded bg-[var(--k-surface-2)]">
                <div
                  className="h-full bg-[var(--k-accent)]"
                  style={{
                    width: `${Math.max(
                      0,
                      Math.min(100, (syncProgress.cur / Math.max(1, syncProgress.total)) * 100),
                    ).toFixed(1)}%`,
                  }}
                />
              </div>
            ) : null}
            {syncLogs.length ? (
              <div className="mt-2 space-y-0.5 text-[var(--k-muted)]">
                {syncLogs.slice(-4).map((l, i) => (
                  <div key={i} className="truncate">
                    {l}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        {automationBusy && automationStage ? (
          <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
            <div className="flex items-center justify-between gap-2">
              <div className="font-medium">Run automation</div>
              <div className="text-[var(--k-muted)]">…</div>
            </div>
            <div className="mt-1 text-[var(--k-muted)]">{automationStage}</div>
            {automationLogs.length ? (
              <div className="mt-2 space-y-0.5 text-[var(--k-muted)]">
                {automationLogs.slice(-4).map((l, i) => (
                  <div key={i} className="truncate">
                    {l}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        {automationMsg ? (
          <div className="mt-2 text-xs text-[var(--k-muted)]">{automationMsg}</div>
        ) : null}
        {automationSkipRun ? (
          <div className="mt-2 flex items-center gap-2 text-xs">
            <span className="text-[var(--k-muted)]">
              Automation skipped ({automationSkipRun.skipReason || 'unknown'}).
            </span>
            <Button size="sm" variant="secondary" onClick={onForceAutomationFromSkip}>
              Force run
            </Button>
          </div>
        ) : null}

        {syncMsg ? <div className="mt-2 text-xs text-[var(--k-muted)]">{syncMsg}</div> : null}
        {copyMdStatus ? (
          <div className="mt-2 text-xs">
            <span className={copyMdStatus.ok ? 'text-emerald-600' : 'text-red-600'}>
              {copyMdStatus.text}
            </span>
          </div>
        ) : null}
        {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
      </div>
      <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void onManualRefreshTrend()}
          disabled={trendBusy || !itemsCount}
          className="gap-2"
          aria-label="Refresh watchlist scores"
          title="Fetch latest daily bars from network and recompute"
        >
          <RefreshCw className={trendBusy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
          Refresh
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={onReferenceTable}
          disabled={!sortedItemsCount}
          className="gap-2"
        >
          <ExternalLink className="h-4 w-4" />
          Reference table
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void onCopyMarkdown()}
          disabled={!sortedItemsCount || copyMdBusy}
        >
          {copyMdBusy ? 'Copying…' : 'Copy Markdown'}
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void onSyncFromScreener()}
          disabled={syncBusy || automationBusy}
          className="gap-2"
        >
          <RefreshCw className={syncBusy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
          Import from screener
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void onRunAutomation()}
          disabled={automationBusy || syncBusy}
          className="gap-2"
          title="Run watchlist automation (remove weak, screener import, Alpha Radar S append)"
        >
          <Play className={automationBusy ? 'h-4 w-4 animate-pulse' : 'h-4 w-4'} />
          Run automation
        </Button>
      </div>
    </div>
  );
}
