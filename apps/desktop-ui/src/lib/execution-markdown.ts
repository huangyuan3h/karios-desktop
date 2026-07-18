import type { ExecutionActionCard, ExecutionGate } from '@karios/shared';

import { mdPrice, mdTable } from '@/lib/dashboard-format';
import { deriveActionCard } from '@/lib/execution-action';
import type { TrendOkResult } from '@/lib/api/types';
import type { WatchlistItem } from '@/lib/watchlist-storage';

export function buildPositionsExecutionMarkdown(
  items: WatchlistItem[],
  trend: Record<string, TrendOkResult | undefined>,
  quotes: Record<string, { price?: number | null } | undefined>,
  gate: ExecutionGate | null,
  heading = '##',
): string {
  const lines: string[] = [];
  lines.push(`${heading} Positions (execution)`);
  if (!gate?.allowNewEntries) {
    lines.push('- note: BUY/ADD only valid when Execution Gate allowNewEntries=true');
  }
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
    'Why',
  ];
  const rows: unknown[][] = [];
  for (const it of items) {
    const t = trend[it.symbol];
    const q = quotes[it.symbol];
    const currentPrice =
      typeof q?.price === 'number' && Number.isFinite(q.price) ? q.price : null;
    const card: ExecutionActionCard = deriveActionCard({
      symbol: it.symbol,
      gate,
      trendok: t ?? null,
      position: it,
      currentPrice,
    });
    const dist =
      typeof card.distPct === 'number' && Number.isFinite(card.distPct)
        ? card.distPct.toFixed(1)
        : '—';
    rows.push([
      it.symbol,
      card.action,
      mdPrice(card.trigger ?? null),
      card.trailArmed ? 'yes' : 'no',
      mdPrice(card.peak ?? null),
      mdPrice(card.hardStop ?? null),
      mdPrice(card.trailStop ?? null),
      dist,
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
