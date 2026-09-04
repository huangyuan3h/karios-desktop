import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, type PortfolioHealthResponse } from '@/lib/queries/portfolioHealth';

/**
 * T6 (2026-08-19) — NASDAQ-100 ETF (513100) idle-cash sleeve hint.
 *
 * Three-window pre-study (docs/designs/third-asset-sleeve.md) picked the
 * conditional rule: while the S-3 CN line has idle cash and 513100 stays above
 * its 200-day MA, park the idle capital in the ETF; break the MA line -> back
 * to GC001; A-share buy point -> sell 513100 and switch back to A-shares.
 *
 * Renders only when the backend `thirdAssetSleeve.active` says there is a
 * hint worth surfacing (buy / sell-to-A-share / sell-to-repo).
 */
export function ThirdAssetSleeveBanner() {
  const q = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const multi = React.useMemo(
    () => (q.data as PortfolioHealthResponse | undefined)?.multiAssetSleeve ?? null,
    [q.data],
  );
  // Prefer multi-asset 择强单轨 only — do not fall back to T6 thirdAsset as「择强」.
  const sleeve = multi?.active && multi.action !== 'NONE' ? multi : null;

  if (!sleeve?.active || !sleeve.action || sleeve.action === 'NONE') {
    return null;
  }

  const pick = (sleeve as unknown as { pick?: { close?: number; ma200?: number; symbol?: string; key?: string; mom60?: number } }).pick;
  const details = [
    (sleeve as unknown as { price?: number }).price != null
      ? `现价 ${(sleeve as unknown as { price?: number }).price}`
      : pick?.close != null
        ? `现价 ${pick.close}`
        : null,
    (sleeve as unknown as { ma200?: number }).ma200 != null
      ? `MA200 ${(sleeve as unknown as { ma200?: number }).ma200}`
      : pick?.ma200 != null
        ? `MA200 ${pick.ma200}`
        : null,
    sleeve.idlePct != null ? `闲置 ${sleeve.idlePct}%` : null,
    (sleeve as unknown as { asOfDate?: string }).asOfDate ? `asOf ${(sleeve as unknown as { asOfDate?: string }).asOfDate}` : null,
    pick?.mom60 != null ? `mom60 ${pick.mom60}%` : null,
  ]
    .filter(Boolean)
    .join(' · ');

  const styles: Record<string, string> = {
    BUY_513100:
      'border-sky-500/40 bg-sky-500/10 text-sky-800 dark:text-sky-200',
    BUY: 'border-sky-500/40 bg-sky-500/10 text-sky-800 dark:text-sky-200',
    ROTATE: 'border-purple-500/40 bg-purple-500/10 text-purple-800 dark:text-purple-200',
    HOLD: 'border-emerald-500/40 bg-emerald-500/10 text-emerald-800 dark:text-emerald-200',
    SELL_TO_A_SHARE:
      'border-amber-500/40 bg-amber-500/10 text-amber-800 dark:text-amber-200',
    SELL_TO_REPO:
      'border-red-500/40 bg-red-500/10 text-red-800 dark:text-red-200',
    DONT_BUY:
      'border-[var(--k-border)] bg-[var(--k-surface-2)]/60 text-[var(--k-muted)]',
  };
  const icons: Record<string, string> = {
    BUY_513100: '💼',
    BUY: '💼',
    ROTATE: '🔄',
    HOLD: '✅',
    SELL_TO_A_SHARE: '🔔',
    SELL_TO_REPO: '⚠️',
    DONT_BUY: '⏸',
  };

  const etfLabel = pick?.symbol ?? (sleeve as unknown as { etf?: string }).etf ?? '择强';
  const titlePrefix = '择强单轨';

  return (
    <div className={`mb-4 rounded-lg border px-4 py-3 text-sm ${styles[sleeve.action] ?? styles.DONT_BUY}`}>
      <div className="font-medium">
        {icons[sleeve.action] ?? '💼'} {titlePrefix}（{etfLabel}）· {sleeve.label ?? sleeve.action}
      </div>
      <div className="mt-1 text-xs opacity-90">{sleeve.message}</div>
      {details ? <div className="mt-1 text-[11px] opacity-70">{details}</div> : null}
    </div>
  );
}