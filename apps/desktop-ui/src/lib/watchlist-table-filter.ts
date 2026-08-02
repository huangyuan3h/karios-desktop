/**
 * Watchlist table visibility filter (2026-08-01 · wife feedback).
 *
 * Drop silent "dead" rows that occupy board space without trading value:
 *   Pos% > 0 → real positions (always show)
 *   OR Score >= 60 → strong candidate
 *   OR TrendOK in ['ok', 'recovering'] → right-side potential
 *   OR Action != WATCH_SILENT → any active (non-silent) watch
 *
 * PURGE rows are also removed (caller still applies post-report GC).
 * Alpha S (WATCH_SILENT) is also dropped from table per user spec;
 * it remains in DB for re-derivation history but is invisible.
 */
import type { TrendOkResult } from '@/lib/api/types';

export type WatchlistVisibilityTrend = Pick<
  TrendOkResult,
  'trendOk' | 'trendStatus' | 'score'
>;

export type WatchlistVisibilityItem = {
  symbol: string;
  positionPct?: number | null;
};

export const WATCHLIST_TABLE_VISIBILITY_NOTE = [
  '- note: hidden rows are silent dead stock (Pos%=— & Score<60 & TrendOK≠ok/recovering & Action=WATCH_SILENT); kept in DB',
  '- note: shown if Pos%>0 (held) OR Score>=60 OR TrendOK=ok/recovering OR Action≠WATCH_SILENT',
];

function isTrendOkPositive(t: WatchlistVisibilityTrend | undefined | null): boolean {
  if (!t) return false;
  const status = String(t.trendStatus ?? '')
    .trim()
    .toLowerCase();
  if (status === 'ok' || status === 'recovering') return true;
  if (status === 'no') return false;
  if (t.trendOk === true) return true;
  return false;
}

function isHighScore(t: WatchlistVisibilityTrend | undefined | null): boolean {
  if (!t) return false;
  const s = t.score;
  return typeof s === 'number' && Number.isFinite(s) && s >= 60;
}

function isHeld(item: WatchlistVisibilityItem): boolean {
  const p = item.positionPct;
  return typeof p === 'number' && Number.isFinite(p) && p > 0;
}

/**
 * Pure helper for testability: returns true if the row should remain visible
 * in the Combat Positions & Watchlist table.
 *
 * Used by:
 *   - execution-markdown.ts (Copy all Markdown)
 *   - WatchlistTable.tsx (on-screen rendering)
 */
export function shouldShowInWatchlistTable(
  item: WatchlistVisibilityItem,
  trend: WatchlistVisibilityTrend | undefined | null,
  action: string | null | undefined,
): boolean {
  if (isHeld(item)) return true;
  if (isHighScore(trend)) return true;
  if (isTrendOkPositive(trend)) return true;
  if (action && action !== 'WATCH_SILENT') return true;
  return false;
}

/**
 * Filter helper that returns a new array preserving order of inputs.
 */
export function filterWatchlistForTable<T extends WatchlistVisibilityItem>(
  items: T[],
  trendMap: Record<string, WatchlistVisibilityTrend | undefined>,
  actionBySymbol: Record<string, string | null | undefined>,
): T[] {
  return items.filter((it) =>
    shouldShowInWatchlistTable(it, trendMap?.[it.symbol], actionBySymbol?.[it.symbol] ?? null),
  );
}

/**
 * Count hidden rows for note diagnostics. Always >= 0; safe to call.
 */
export function countHiddenWatchlistRows(
  total: number,
  visible: number,
): number {
  if (total < visible) return 0;
  return total - visible;
}
