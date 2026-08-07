/** Watchlist symbol helpers: CN A-share + HK ticker + ETF → ts_code. */

/** Strip a market suffix (CN:002064.SZ → CN:002064) to the canonical form. */
export function normalizeWatchlistSymbol(symbol: string): string {
  const s = symbol.trim().toUpperCase();
  if (!s.includes(':')) return s;
  const [market, ticker] = s.split(':');
  return `${market}:${(ticker || '').split('.')[0]}`;
}

export function toTsCodeFromSymbol(symbol: string): string | null {
  const s = normalizeWatchlistSymbol(symbol);
  if (!s) return null;
  if (s.startsWith('CN:')) {
    const ticker = s.slice('CN:'.length).trim();
    if (!/^[0-9]{6}$/.test(ticker)) return null;
    const suffix = ticker.startsWith('6') ? 'SH' : 'SZ';
    return `${ticker}.${suffix}`;
  }
  if (s.startsWith('HK:')) {
    const ticker = s.slice('HK:'.length).trim();
    if (!/^[0-9]{1,5}$/.test(ticker)) return null;
    return `${ticker.padStart(5, '0')}.HK`;
  }
  if (s.startsWith('ETF:')) {
    const ticker = s.slice('ETF:'.length).trim();
    if (!/^[0-9]{6}$/.test(ticker)) return null;
    // SH ETFs: 5xxxxx (broad/sector), 6xxxxx, 9xxxxx (cross-border).
    // SZ ETFs: 1xxxxx, 0xxxxx.
    const suffix = ['5', '6', '9'].includes(ticker[0]) ? 'SH' : 'SZ';
    return `${ticker}.${suffix}`;
  }
  return null;
}

export function isCnWatchlistSymbol(symbol: string): boolean {
  const s = symbol.trim().toUpperCase();
  return s.startsWith('CN:');
}

export function isHkWatchlistSymbol(symbol: string): boolean {
  const s = symbol.trim().toUpperCase();
  return s.startsWith('HK:');
}

export function isEtfWatchlistSymbol(symbol: string): boolean {
  const s = symbol.trim().toUpperCase();
  return s.startsWith('ETF:');
}
