import type { ExecutionActionCard, ExecutionGate } from '@karios/shared';

import { mdPrice, mdTable } from '@/lib/dashboard-format';
import { deriveActionCard } from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import type { TrendOkResult } from '@/lib/api/types';
import { buildWatchlistRowMetrics } from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';

export function buildPositionsExecutionMarkdown(
  items: WatchlistItem[],
  trend: Record<string, TrendOkResult | undefined>,
  quotes: Record<string, { price?: number | null; preClose?: number | null; pctChg?: number | null; tradeTime?: string | null; amount?: number | null; volume?: number | null } | undefined>,
  gate: ExecutionGate | null,
  heading = '##',
  mainlineAllow: MainlineAllowSet | null = null,
  tradingTime = false,
  todaySh = '',
): string {
  const lines: string[] = [];
  lines.push(`${heading} Positions (execution)`);
  if (!gate?.allowNewEntries) {
    lines.push('- note: BUY/ADD only valid when Execution Gate allowNewEntries=true');
  }
  lines.push('- note: BUY/ADD also require mainline bind (5D Top3 or Momentum) and non-defense sector');
  lines.push('- note: BUY/ADD also blocked when intraday >6% (INTRADAY_SURGE_BLOCK)');
  lines.push('- note: BUY/ADD also blocked on gap-up in Weak/Diverging (GAP_UP_WEAK_BLOCK)');
  lines.push(
    '- note: ADD blocked when positionPct >= 15% (SIZE_CAP_BLOCK); single-name satellite cap',
  );
  lines.push('');
  const headers = [
    'Symbol',
    'Action',
    'Trigger',
    'TrailArmed',
    'Peak',
    'HardStop',
    'TrailStop',
    'Dist%',
    'Mainline',
    'Why',
  ];
  const rows: unknown[][] = [];
  for (const it of items) {
    const t = trend[it.symbol];
    const q = quotes[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: q,
      tradingTime,
      todaySh,
    });
    const card: ExecutionActionCard = deriveActionCard({
      symbol: it.symbol,
      gate,
      trendok: t ?? null,
      position: it,
      currentPrice: rowMetrics.current,
      mainlineAllow,
      intradayChgPct: rowMetrics.intradayChgPct,
      gapUp: typeof t?.gapUp === 'boolean' ? t.gapUp : null,
      marketRegime: t?.marketRegime ?? null,
    });
    const dist =
      typeof card.distPct === 'number' && Number.isFinite(card.distPct)
        ? card.distPct.toFixed(1)
        : '—';
    const mainlineCell = card.mainlineOk
      ? card.mainlineTag || 'ok'
      : 'no';
    rows.push([
      it.symbol,
      card.action,
      mdPrice(card.trigger ?? null),
      card.trailArmed ? 'yes' : 'no',
      mdPrice(card.peak ?? null),
      mdPrice(card.hardStop ?? null),
      mdPrice(card.trailStop ?? null),
      dist,
      mainlineCell,
      card.why ?? '—',
    ]);
  }
  if (!rows.length) {
    lines.push('- No watchlist items.');
    lines.push('');
    return lines.join('\n');
  }
  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n');
}
