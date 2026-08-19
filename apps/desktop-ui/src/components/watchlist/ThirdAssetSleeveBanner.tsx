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

  const sleeve = React.useMemo(
    () => (q.data as PortfolioHealthResponse | undefined)?.thirdAssetSleeve ?? null,
    [q.data],
  );

  if (!sleeve?.active || !sleeve.action || sleeve.action === 'NONE') {
    return null;
  }

  const details = [
    sleeve.price != null ? `现价 ${sleeve.price}` : null,
    sleeve.ma200 != null ? `MA200 ${sleeve.ma200}` : null,
    sleeve.idlePct != null ? `闲置 ${sleeve.idlePct}%` : null,
    sleeve.asOfDate ? `asOf ${sleeve.asOfDate}` : null,
  ]
    .filter(Boolean)
    .join(' · ');

  const styles: Record<string, string> = {
    BUY_513100:
      'border-sky-500/40 bg-sky-500/10 text-sky-800 dark:text-sky-200',
    SELL_TO_A_SHARE:
      'border-amber-500/40 bg-amber-500/10 text-amber-800 dark:text-amber-200',
    SELL_TO_REPO:
      'border-red-500/40 bg-red-500/10 text-red-800 dark:text-red-200',
    DONT_BUY:
      'border-[var(--k-border)] bg-[var(--k-surface-2)]/60 text-[var(--k-muted)]',
  };
  const icons: Record<string, string> = {
    BUY_513100: '💼',
    SELL_TO_A_SHARE: '🔔',
    SELL_TO_REPO: '⚠️',
    DONT_BUY: '⏸',
  };

  return (
    <div className={`mb-4 rounded-lg border px-4 py-3 text-sm ${styles[sleeve.action] ?? styles.DONT_BUY}`}>
      <div className="font-medium">
        {icons[sleeve.action] ?? '💼'} 第三资产套筒（{sleeve.etf ?? sleeve.tsCode ?? '513100'}）· {sleeve.label ?? sleeve.action}
      </div>
      <div className="mt-1 text-xs opacity-90">{sleeve.message}</div>
      {details ? <div className="mt-1 text-[11px] opacity-70">{details}</div> : null}
    </div>
  );
}