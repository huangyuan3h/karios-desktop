import { loadWatchlist, saveWatchlist, type WatchlistItem } from '@/lib/watchlist-storage';

/**
 * Physically remove PURGE symbols from the watchlist after a report was generated.
 * Returns count removed.
 */
export async function applyWatchlistPurgeAfterReport(
  purgeSymbols: string[],
): Promise<number> {
  if (!purgeSymbols.length) return 0;
  const removeSet = new Set(
    purgeSymbols.map((s) => String(s || '').trim().toUpperCase()).filter(Boolean),
  );
  if (!removeSet.size) return 0;
  const items: WatchlistItem[] = loadWatchlist();
  const next = items.filter((x) => !removeSet.has(String(x.symbol || '').toUpperCase()));
  const removed = items.length - next.length;
  if (removed <= 0) return 0;
  await saveWatchlist(next);
  return removed;
}
