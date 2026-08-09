import type { WatchlistItem } from '@/lib/watchlist-storage';

/**
 * Pure helpers for recording real trades from the watchlist UI:
 * - blended weighted-average cost when adding to an open position
 * - PnL % / holding days when selling
 */

export type PositionEconomics = {
  /** Weighted average cost price after the add. */
  blendedCost: number;
  /** New total position pct after the add. */
  newPositionPct: number;
  /** Pct added in this leg. */
  addPct: number;
};

/**
 * Compute the weighted-average cost after adding `addPct` at `addPrice`
 * to an existing position held at `oldCost` with `oldPct`.
 *
 * blended = (oldCost*oldPct + addPrice*addPct) / (oldPct + addPct)
 */
export function blendAddCost(
  oldCost: number,
  oldPct: number,
  addPrice: number,
  addPct: number,
): PositionEconomics {
  const pct = oldPct + addPct;
  const cost = pct > 0 ? (oldCost * oldPct + addPrice * addPct) / pct : addPrice;
  return {
    blendedCost: Math.round(cost * 1000) / 1000,
    newPositionPct: Math.round(pct * 100) / 100,
    addPct,
  };
}

/** True when the item is a held position (pct > 0 and cost known). */
export function isOpenPosition(item: WatchlistItem): boolean {
  return (
    typeof item.positionPct === 'number' &&
    Number.isFinite(item.positionPct) &&
    item.positionPct > 0
  );
}

/** Gross PnL % for a sell at `exitPrice` against `costBasis`. */
export function sellPnLPct(exitPrice: number, costBasis: number): number {
  if (!Number.isFinite(costBasis) || costBasis <= 0) return 0;
  return Math.round(((exitPrice - costBasis) / costBasis) * 10000) / 100;
}

/** Calendar-day holding period between entryDate and sellDate (YYYY-MM-DD). */
export function holdingDays(entryDate: string, sellDate: string): number {
  const d0 = new Date(`${entryDate}T00:00:00`);
  const d1 = new Date(`${sellDate}T00:00:00`);
  const ms = d1.getTime() - d0.getTime();
  if (!Number.isFinite(ms)) return 0;
  return Math.max(0, Math.round(ms / 86_400_000));
}

/** Detect an ADD: item already has a cost + pct, and a new buy price differs. */
export function isAddOnOpenPosition(
  item: WatchlistItem,
  newPrice: number | null,
): boolean {
  if (newPrice == null || !isOpenPosition(item)) return false;
  const oldCost = item.costPrice;
  if (typeof oldCost !== 'number' || !Number.isFinite(oldCost)) return false;
  return Math.abs(oldCost - newPrice) > 1e-9;
}

/**
 * Map a watchlist item source to the TIP-011 fire-attribution enum
 * (TV / ALPHA / MANUAL) so the expectancy board aligns with paper_trades.
 */
export function tradeSourceForItem(item: WatchlistItem): string {
  switch (item.source) {
    case 'screener':
    case 'screener_fallback':
      return 'TV';
    case 'alpha_radar':
      return 'ALPHA';
    case 'research':
      return 'RESEARCH';
    case 'manual':
      return 'MANUAL';
    default:
      return 'MANUAL';
  }
}

/** Market from symbol prefix (CN: / HK: / ETF: → CN / HK / ETF). */
export function tradeMarketForSymbol(symbol: string): string {
  const prefix = symbol.split(':')[0]?.toUpperCase();
  return prefix === 'HK' || prefix === 'ETF' ? prefix : 'CN';
}
