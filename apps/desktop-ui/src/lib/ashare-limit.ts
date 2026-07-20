/**
 * A-share daily limit-up / limit-down helpers (mirror backend `_limit_pct_for`).
 * Used for hard-stop Cond Order_Price so sells can fill on gap-downs.
 */

/** Limit band percent for a CN symbol (10 / 20 / 5 ST / 30 BJ). */
export function limitPctForAshare(symbol: string, name?: string | null): number {
  const n = String(name || '').toUpperCase();
  if (n.includes('ST')) return 5;
  const raw = String(symbol || '')
    .trim()
    .toUpperCase()
    .replace(/^CN:/, '');
  const code = raw.split('.', 1)[0] || '';
  if (raw.endsWith('.BJ') || /^(4|8|9)\d{5}$/.test(code)) return 30;
  if (code.startsWith('300') || code.startsWith('301') || code.startsWith('688')) return 20;
  return 10;
}

/** Round to 2 decimals (A-share tick for most boards). */
export function roundAsharePrice(price: number): number {
  return Math.round(price * 100) / 100;
}

/** Day limit-down price from preClose, or null if inputs invalid. */
export function limitDownPrice(
  symbol: string,
  preClose: number | null | undefined,
  name?: string | null,
): number | null {
  if (typeof preClose !== 'number' || !Number.isFinite(preClose) || preClose <= 0) {
    return null;
  }
  const pct = limitPctForAshare(symbol, name);
  return roundAsharePrice(preClose * (1 - pct / 100));
}
