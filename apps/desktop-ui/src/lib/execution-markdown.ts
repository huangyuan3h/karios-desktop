import type { ExecutionActionCard, ExecutionGate } from '@karios/shared';

import { mdPrice, mdScore, mdTable } from '@/lib/dashboard-format';
import {
  buildSectorExposureFromWatchlist,
  buildSleeveExposurePct,
  computePnLPct,
  countHeldMissingPositionPct,
  deriveActionCard,
  formatSleeveBudgetLabel,
  isLockedT1,
  type CatalystPurgeHint,
} from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import type { TrendOkResult } from '@/lib/api/types';
import { formatPnLPct, formatRs, buildWatchlistRowMetrics } from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';

function formatTrendOkCell(t: TrendOkResult | undefined): string {
  if (!t) return '—';
  if (t.trendOk === true) return 'ok';
  if (t.trendOk === false) return 'no';
  return '—';
}

function formatPosPct(v: number | null | undefined): string {
  return typeof v === 'number' && Number.isFinite(v) ? v.toFixed(1) : '—';
}

function formatEntryDate(v: string | null | undefined): string {
  const s = String(v || '').trim();
  return s || '—';
}

function formatLockedT1(entryDate: string | null | undefined, todaySh: string): string {
  if (!String(entryDate || '').trim()) return '—';
  return isLockedT1(entryDate, todaySh) ? 'True' : 'False';
}

export type PositionsExecutionMarkdownResult = {
  markdown: string;
  /** Flat symbols with Action=PURGE this round (remove after report is built). */
  purgeSymbols: string[];
};

/**
 * Unified combat table for Copy all / Watchlist copy.
 * Merges quant factors (RS/Score/TrendOK/Current) with execution contract
 * (Action/Suggest%/Entry_Trigger/Exit_Stop) into one LLM payload table.
 */
export function buildPositionsExecutionMarkdown(
  items: WatchlistItem[],
  trend: Record<string, TrendOkResult | undefined>,
  quotes: Record<
    string,
    | {
        price?: number | null;
        preClose?: number | null;
        pctChg?: number | null;
        tradeTime?: string | null;
        amount?: number | null;
        volume?: number | null;
      }
    | undefined
  >,
  gate: ExecutionGate | null,
  heading = '##',
  mainlineAllow: MainlineAllowSet | null = null,
  tradingTime = false,
  todaySh = '',
  sectorOutflowBlock = false,
  catalystBySymbol: Map<string, CatalystPurgeHint> | null = null,
): PositionsExecutionMarkdownResult {
  const lines: string[] = [];
  lines.push(`${heading} Combat Positions & Watchlist (Unified)`);
  if (!gate?.allowNewEntries) {
    lines.push('- note: BUY/ADD only valid when Execution Gate allowNewEntries=true');
  }
  lines.push('- note: BUY/ADD also require mainline bind (5D Top3 or Momentum) and non-defense sector');
  lines.push('- note: BUY/ADD also blocked when intraday >6% (INTRADAY_SURGE_BLOCK)');
  lines.push('- note: BUY/ADD also blocked on gap-up in Weak/Diverging (GAP_UP_WEAK_BLOCK)');
  lines.push(
    '- note: ADD blocked when positionPct >= 15% (SIZE_CAP_BLOCK); single-name satellite cap',
  );
  lines.push(
    '- note: BUY/ADD blocked when sector positionPct sum >= 30% (SECTOR_CONC_BLOCK)',
  );
  lines.push(
    '- note: BUY/ADD blocked when sleeve positionPct sum >= Gate positionRangeHint max (SLEEVE_CAP_BLOCK)',
  );
  lines.push(
    '- note: Suggest% = min(5% clip, single 15% room, sector 30% room, sleeve hint room)',
  );
  lines.push(
    '- note: Dist% flat = (Entry_Trigger-Current)/Current; held = (Current-Exit_Stop)/Current',
  );
  lines.push(
    '- note: PURGE = Pos%=0 & Score<30 & TrendOK=no (removed after report); Alpha S + score>80 → WATCH_SILENT (kept)',
  );
  lines.push(
    '- note: Locked_T1=True (entryDate=today) → EXIT/TRIM blocked (Why=T1_LOCK); missing entryDate fail-open',
  );
  if (sectorOutflowBlock) {
    lines.push(
      '- note: Mainline=no + SECTOR_OUTFLOW_BLOCK when all sectors net outflow',
    );
  }
  const sleeveExposurePct = buildSleeveExposurePct(items);
  lines.push(
    `- sleeve: ${formatSleeveBudgetLabel(sleeveExposurePct, gate?.positionRangeHint)}`,
  );
  const missingSize = countHeldMissingPositionPct(items);
  if (missingSize > 0) {
    lines.push(
      `- note: ${missingSize} held missing positionPct (sector/sleeve caps fail-open)`,
    );
  }
  lines.push('');
  const sectorExposureByIndustry = buildSectorExposureFromWatchlist(items, trend);
  const headers = [
    'Symbol',
    'Name',
    'RS',
    'Score',
    'TrendOK',
    'Current',
    'Pos%',
    'CostPrice',
    'P&L%',
    'EntryDate',
    'Locked_T1',
    'Action',
    'Suggest%',
    'Entry_Trigger',
    'Exit_Stop',
    'HardStop',
    'TrailStop',
    'Dist%',
    'Mainline',
    'Why',
  ];
  const rows: unknown[][] = [];
  const purgeSymbols: string[] = [];
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
    const catalyst = catalystBySymbol?.get(it.symbol) ?? null;
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
      sectorExposureByIndustry,
      sleeveExposurePct,
      sectorOutflowBlock,
      catalyst,
      todaySh,
    });
    if (card.action === 'PURGE') {
      purgeSymbols.push(it.symbol);
    }
    const dist =
      typeof card.distPct === 'number' && Number.isFinite(card.distPct)
        ? card.distPct.toFixed(1)
        : '—';
    const mainlineCell = card.mainlineOk ? card.mainlineTag || 'ok' : 'no';
    const suggest =
      typeof card.suggestAddPct === 'number' && Number.isFinite(card.suggestAddPct)
        ? `+${card.suggestAddPct.toFixed(1)}${card.suggestSizeNote ? ` (${card.suggestSizeNote})` : ''}`
        : '—';
    const pnl = computePnLPct(
      typeof it.costPrice === 'number' ? it.costPrice : null,
      rowMetrics.current,
    );
    rows.push([
      it.symbol,
      it.name ?? t?.name ?? '—',
      formatRs(t),
      mdScore(t?.score ?? null),
      formatTrendOkCell(t),
      mdPrice(rowMetrics.current),
      formatPosPct(it.positionPct),
      mdPrice(it.costPrice),
      formatPnLPct(pnl),
      formatEntryDate(it.entryDate),
      formatLockedT1(it.entryDate, todaySh),
      card.action,
      suggest,
      mdPrice(card.entryTrigger ?? null),
      mdPrice(card.exitStop ?? null),
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
    return { markdown: lines.join('\n'), purgeSymbols: [] };
  }
  lines.push(mdTable(headers, rows));
  if (purgeSymbols.length) {
    lines.push(
      `- note: Purged ${purgeSymbols.length} symbols (Score<30 & TrendOK=no & flat, not Alpha S) — removed after this report`,
    );
  }
  lines.push('');
  return { markdown: lines.join('\n'), purgeSymbols };
}
