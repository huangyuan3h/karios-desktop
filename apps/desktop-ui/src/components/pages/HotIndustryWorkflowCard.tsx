'use client';

import * as React from 'react';

import { Button } from '@/components/ui/button';

export type HotIndustryPick = {
  industryName: string;
  dailyRank: number | null;
  fiveDayRank: number | null;
  netInflow?: number | null;
  sum5d?: number | null;
};

function rankText(rank: number | null): string {
  if (typeof rank !== 'number' || !Number.isFinite(rank) || rank <= 0) return '—';
  return `#${Math.round(rank)}`;
}

function amountText(x: number | null | undefined): string {
  const n = typeof x === 'number' && Number.isFinite(x) ? x : null;
  if (n == null) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e8) return `${(n / 1e8).toFixed(2)}亿`;
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}万`;
  return `${n.toFixed(0)}`;
}

export function HotIndustryWorkflowCard({
  picks,
  asOfDate,
  onOpenScreener,
  onOpenWatchlist,
  compact = false,
}: {
  picks: HotIndustryPick[];
  asOfDate?: string | null;
  onOpenScreener?: () => void;
  onOpenWatchlist?: () => void;
  compact?: boolean;
}) {
  const [copyState, setCopyState] = React.useState<'idle' | 'ok' | 'err'>('idle');
  const timerRef = React.useRef<number | null>(null);

  React.useEffect(
    () => () => {
      if (timerRef.current != null) window.clearTimeout(timerRef.current);
    },
    [],
  );

  async function copyNames() {
    try {
      const names = picks
        .map((x) => String(x.industryName ?? '').trim())
        .filter(Boolean)
        .slice(0, 3);
      if (!names.length) throw new Error('No hotspot industries');
      await navigator.clipboard.writeText(names.join('\n'));
      setCopyState('ok');
    } catch {
      setCopyState('err');
    } finally {
      if (timerRef.current != null) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(() => setCopyState('idle'), 1800);
    }
  }

  return (
    <div className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-medium">Hot industries → TV Screener → Watchlist</div>
        <div className="text-xs text-[var(--k-muted)]">asOf: {asOfDate || '—'}</div>
      </div>
      <div className="mb-3 text-xs text-[var(--k-muted)]">
        1) Read Industry fund flow (intraday/EOD). 2) Keep today&apos;s top inflow sectors with strong 5D rank.
        3) Cross-filter those sectors in TV Screener, then add only technical-qualified stocks to Watchlist.
      </div>

      <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
        <table className="w-full border-collapse text-xs">
          <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
            <tr className="text-left">
              <th className="px-2 py-2">#</th>
              <th className="px-2 py-2">Industry</th>
              <th className="px-2 py-2">1D rank</th>
              <th className="px-2 py-2">5D rank</th>
              {!compact ? <th className="px-2 py-2">1D net</th> : null}
              {!compact ? <th className="px-2 py-2">5D sum</th> : null}
            </tr>
          </thead>
          <tbody>
            {picks.length ? (
              picks.slice(0, 3).map((p, idx) => (
                <tr key={`${p.industryName}-${idx}`} className="border-t border-[var(--k-border)]">
                  <td className="px-2 py-2 font-mono">{idx + 1}</td>
                  <td className="px-2 py-2">{p.industryName || '—'}</td>
                  <td className="px-2 py-2 font-mono">{rankText(p.dailyRank)}</td>
                  <td className="px-2 py-2 font-mono">{rankText(p.fiveDayRank)}</td>
                  {!compact ? <td className="px-2 py-2 font-mono">{amountText(p.netInflow)}</td> : null}
                  {!compact ? <td className="px-2 py-2 font-mono">{amountText(p.sum5d)}</td> : null}
                </tr>
              ))
            ) : (
              <tr>
                <td className="px-2 py-4 text-center text-[var(--k-muted)]" colSpan={compact ? 4 : 6}>
                  No hotspot industry candidates yet
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button size="sm" variant="secondary" onClick={() => void copyNames()}>
          {copyState === 'idle' ? 'Copy top3 names' : copyState === 'ok' ? 'Copied' : 'Copy failed'}
        </Button>
        {onOpenScreener ? (
          <Button size="sm" variant="secondary" onClick={onOpenScreener}>
            Open TV Screener
          </Button>
        ) : null}
        {onOpenWatchlist ? (
          <Button size="sm" variant="secondary" onClick={onOpenWatchlist}>
            Open Watchlist
          </Button>
        ) : null}
      </div>
    </div>
  );
}
