'use client';

import * as React from 'react';
import type { ExecutionGate, ExecutionSnapshot } from '@karios/shared';

import { Button } from '@/components/ui/button';
import { executionGateBadgeClass, fmtDateTime } from '@/lib/dashboard-format';
import {
  buildExecAttentionQueue,
  formatAttentionFireLine,
  formatDecisionChangeLine,
  resolveAttentionCards,
  type ExecAttentionLine,
} from '@/lib/exec-attention';
import {
  buildExecutionSnapshotPayload,
  filterLatestActionCards,
} from '@/lib/execution-journal';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { useWatchlistMarketQuery } from '@/lib/queries/watchlist';
import {
  useExecutionChangesQuery,
  useExecutionRecentSnapshotsQuery,
  useExecutionSnapshotsQuery,
} from '@/lib/queries/execution-journal';
import type { WatchlistItem } from '@/lib/watchlist-storage';

function AttentionLines({
  lines,
  empty,
  fireStyle = false,
}: {
  lines: ExecAttentionLine[];
  empty: string;
  fireStyle?: boolean;
}) {
  if (!lines.length) {
    return <div className="text-[11px] text-[var(--k-muted)]">{empty}</div>;
  }
  return (
    <ul className="space-y-0.5 font-mono text-[11px] leading-snug">
      {lines.map((x) => (
        <li key={`${x.action}-${x.symbol}`}>
          {fireStyle ? (
            formatAttentionFireLine(x)
          ) : (
            <>
              <span className="font-semibold">{x.symbol}</span>
              <span className="mx-1.5">{x.action}</span>
              <span className="text-[var(--k-muted)]">{x.why ?? '—'}</span>
            </>
          )}
        </li>
      ))}
    </ul>
  );
}

export function DecisionJournalCard(props: {
  gate: ExecutionGate | null;
  watchlistItems?: WatchlistItem[];
  mainlineAllow?: MainlineAllowSet | null;
  captureBusy?: boolean;
  onSnapshotNow?: () => void;
  onNavigate?: (pageId: string) => void;
}) {
  const {
    gate,
    watchlistItems = [],
    mainlineAllow = null,
    captureBusy,
    onSnapshotNow,
    onNavigate,
  } = props;
  const [showHistory, setShowHistory] = React.useState(false);
  const [expandedId, setExpandedId] = React.useState<string | null>(null);

  const symbols = React.useMemo(
    () => watchlistItems.map((i) => i.symbol).filter(Boolean),
    [watchlistItems],
  );
  const marketQ = useWatchlistMarketQuery(symbols);

  const changesQ = useExecutionChangesQuery();
  const snapsQ = useExecutionSnapshotsQuery();
  const recentQ = useExecutionRecentSnapshotsQuery(30);

  const changes = changesQ.data?.items ?? [];
  const todaySnaps = snapsQ.data?.items ?? [];
  const latest = todaySnaps[0] as ExecutionSnapshot | undefined;
  const latestGateMode =
    latest && typeof latest.gate === 'object' && latest.gate && 'mode' in latest.gate
      ? String((latest.gate as { mode?: string }).mode ?? '')
      : '';
  const liveMode = gate?.mode ?? null;
  const modeChanged = Boolean(liveMode && latestGateMode && liveMode !== latestGateMode);

  const snapshotCards = Array.isArray(latest?.cards) ? latest!.cards : [];

  const liveCards = React.useMemo(() => {
    if (!gate) return null;
    if (!watchlistItems.length) return [];
    if (!marketQ.data) return null;
    const payload = buildExecutionSnapshotPayload({
      items: watchlistItems,
      trend: marketQ.data.trend,
      quotes: marketQ.data.quotes,
      gate,
      mainlineAllow,
      source: 'poll',
    });
    return payload?.cards ?? null;
  }, [gate, watchlistItems, marketQ.data, mainlineAllow]);

  const { cards: actionCards, source: cardsSource } = React.useMemo(
    () =>
      resolveAttentionCards({
        liveCards,
        snapshotCards,
      }),
    [liveCards, snapshotCards],
  );

  const attention = React.useMemo(
    () =>
      buildExecAttentionQueue({
        gate,
        watchlistItems,
        cards: actionCards,
        changes,
      }),
    [gate, watchlistItems, actionCards, changes],
  );

  const mustAct = [...attention.exits, ...attention.trims];

  const latestActionCards = React.useMemo(
    () => filterLatestActionCards(snapshotCards, changes),
    [snapshotCards, changes],
  );

  return (
    <div className="space-y-3 text-sm">
      <div className="rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)]/40 px-3 py-2">
        <div className="mb-1 flex flex-wrap items-center gap-2">
          <div className="text-xs font-medium text-[var(--k-muted)]">Exec Attention</div>
          <span className="text-[10px] text-[var(--k-muted)]">
            {cardsSource === 'live'
              ? 'live'
              : cardsSource === 'snapshot'
                ? 'from snapshot'
                : marketQ.isLoading || marketQ.isFetching
                  ? 'loading…'
                  : '—'}
          </span>
          {onNavigate ? (
            <Button
              size="sm"
              variant="ghost"
              className="ml-auto h-6 px-2 text-[11px]"
              onClick={() => onNavigate('watchlist')}
            >
              Open Watchlist
            </Button>
          ) : null}
        </div>
        <div className="text-xs">{attention.sleeveLabel}</div>
        {attention.missingSize > 0 ? (
          <div className="mt-0.5 text-xs font-medium text-amber-700 dark:text-amber-300">
            {attention.missingSize} held missing size
          </div>
        ) : null}
        <div className="mt-2">
          <div className="mb-0.5 text-[11px] font-medium text-[var(--k-muted)]">Must act</div>
          {cardsSource === 'none' && (marketQ.isLoading || marketQ.isFetching) ? (
            <div className="text-[11px] text-[var(--k-muted)]">Loading market…</div>
          ) : (
            <AttentionLines lines={mustAct} empty="None" />
          )}
        </div>
        <div className="mt-2">
          <div className="mb-0.5 text-[11px] font-medium text-[var(--k-muted)]">Fire</div>
          {cardsSource === 'none' && (marketQ.isLoading || marketQ.isFetching) ? (
            <div className="text-[11px] text-[var(--k-muted)]">Loading market…</div>
          ) : attention.fireBlockedByGate ? (
            <div className="text-[11px] text-[var(--k-muted)]">Gate blocks new entries</div>
          ) : (
            <AttentionLines lines={attention.fires} empty="None" fireStyle />
          )}
        </div>
        {attention.keyChanges.length > 0 ? (
          <div className="mt-2">
            <div className="mb-0.5 text-[11px] font-medium text-[var(--k-muted)]">Key changes</div>
            <ul className="space-y-0.5 font-mono text-[11px] leading-snug text-amber-800 dark:text-amber-200">
              {attention.keyChanges.map((c) => (
                <li key={c.id}>{c.line}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {cardsSource === 'snapshot' ? (
          <div className="mt-2 text-[11px] text-[var(--k-muted)]">
            Actions from last journal snapshot (live market unavailable).
          </div>
        ) : null}
        {cardsSource === 'none' && !marketQ.isLoading && !marketQ.isFetching ? (
          <div className="mt-2 text-[11px] text-[var(--k-muted)]">
            No live market or snapshot yet — open Watchlist / Snapshot now.
          </div>
        ) : null}
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {gate ? (
          <span
            className={`inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-semibold ${executionGateBadgeClass(gate.mode)}`}
          >
            Live Gate: {gate.mode}
          </span>
        ) : (
          <span className="text-xs text-[var(--k-muted)]">Gate unavailable</span>
        )}
        {modeChanged ? (
          <span className="text-xs font-medium text-amber-700">
            vs snapshot {latestGateMode || '—'}
          </span>
        ) : null}
        <div className="ml-auto flex items-center gap-1">
          <Button
            size="sm"
            variant="secondary"
            className="h-7 px-2 text-xs"
            disabled={captureBusy || !gate}
            onClick={() => onSnapshotNow?.()}
          >
            {captureBusy ? 'Saving…' : 'Snapshot now'}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-7 px-2 text-xs"
            onClick={() => setShowHistory((v) => !v)}
          >
            {showHistory ? 'Hide history' : 'Recent 5d'}
          </Button>
        </div>
      </div>

      {latest ? (
        <div className="text-xs text-[var(--k-muted)]">
          Latest snapshot: {fmtDateTime(latest.capturedAt ?? null)} · source={latest.source} ·{' '}
          {snapshotCards.length} cards
        </div>
      ) : (
        <div className="text-xs text-[var(--k-muted)]">
          No snapshot yet today. Sync All or Snapshot now to start the journal.
        </div>
      )}

      <div>
        <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">Today&apos;s changes</div>
        {changesQ.isLoading ? (
          <div className="text-xs text-[var(--k-muted)]">Loading…</div>
        ) : changes.length === 0 ? (
          <div className="text-xs text-[var(--k-muted)]">No decision changes recorded today.</div>
        ) : (
          <ul className="max-h-48 space-y-1 overflow-auto font-mono text-[11px] leading-snug">
            {changes.slice(0, 40).map((c) => (
              <li
                key={c.id}
                className={
                  c.field === 'action' || c.field === 'mode'
                    ? 'text-amber-800 dark:text-amber-200'
                    : 'text-[var(--k-fg)]'
                }
              >
                {formatDecisionChangeLine(c)}
              </li>
            ))}
          </ul>
        )}
      </div>

      {latest ? (
        <div>
          <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">
            Latest actions (delta)
          </div>
          {latestActionCards.length === 0 ? (
            <div className="text-xs text-[var(--k-muted)]">
              No Action / Trigger / Stop deltas today (silent WATCH omitted).
            </div>
          ) : (
            <div className="max-h-40 overflow-auto rounded border border-[var(--k-border)]">
              <table className="w-full text-left text-[11px]">
                <thead className="sticky top-0 bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                  <tr>
                    <th className="px-2 py-1">Symbol</th>
                    <th className="px-2 py-1">Action</th>
                    <th className="px-2 py-1">Why</th>
                    <th className="px-2 py-1">Pos%</th>
                  </tr>
                </thead>
                <tbody>
                  {latestActionCards.map((c) => (
                    <tr key={c.symbol} className="border-t border-[var(--k-border)] font-mono">
                      <td className="px-2 py-1">{c.symbol}</td>
                      <td className="px-2 py-1 font-semibold">{c.action}</td>
                      <td className="px-2 py-1 text-[var(--k-muted)]">{c.why ?? '—'}</td>
                      <td className="px-2 py-1">
                        {typeof c.positionPct === 'number' ? c.positionPct.toFixed(1) : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      ) : null}

      {showHistory ? (
        <div>
          <div className="mb-1 text-xs font-medium text-[var(--k-muted)]">Recent snapshots</div>
          <ul className="max-h-40 space-y-1 overflow-auto text-[11px]">
            {(recentQ.data?.items ?? []).slice(0, 15).map((s) => {
              const n = Array.isArray(s.cards) ? s.cards.length : 0;
              const mode =
                s.gate && typeof s.gate === 'object' && 'mode' in s.gate
                  ? String((s.gate as { mode?: string }).mode ?? '—')
                  : '—';
              const open = expandedId === s.id;
              return (
                <li key={s.id} className="rounded border border-[var(--k-border)] px-2 py-1">
                  <button
                    type="button"
                    className="flex w-full items-center justify-between gap-2 text-left font-mono"
                    onClick={() => setExpandedId(open ? null : s.id)}
                  >
                    <span>
                      {s.tradeDate} {fmtDateTime(s.capturedAt ?? null)} · {s.source} · {mode} ·{' '}
                      {n} cards
                    </span>
                    <span className="text-[var(--k-muted)]">{open ? '▾' : '▸'}</span>
                  </button>
                  {open && Array.isArray(s.cards) ? (
                    <div className="mt-1 max-h-28 overflow-auto text-[10px] text-[var(--k-muted)]">
                      {s.cards.map((c) => (
                        <div key={`${s.id}-${c.symbol}`}>
                          {c.symbol} {c.action} {c.why}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </li>
              );
            })}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
