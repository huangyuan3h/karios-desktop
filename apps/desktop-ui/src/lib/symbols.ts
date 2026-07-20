/** CN A-share symbol helpers shared by watchlist market fetch paths. */

export function toTsCodeFromSymbol(symbol: string): string | null {
  const s = symbol.trim().toUpperCase();
  if (!s.startsWith('CN:')) return null;
  const ticker = s.slice('CN:'.length).trim();
  if (!/^[0-9]{6}$/.test(ticker)) return null;
  const suffix = ticker.startsWith('6') ? 'SH' : 'SZ';
  return `${ticker}.${suffix}`;
}

export function isCnWatchlistSymbol(symbol: string): boolean {
  return toTsCodeFromSymbol(symbol) != null;
}
