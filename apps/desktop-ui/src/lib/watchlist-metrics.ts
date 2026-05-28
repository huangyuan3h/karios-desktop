export const WATCHLIST_MD_HEADERS = [
  'Symbol',
  'Name',
  'Industry',
  'HotTop3',
  'Position%',
  'CostPrice',
  'Current',
  'VWAP',
  'P&L%',
  'Score',
  'TrendOK',
  'Buy',
  'StopLoss',
  'AsOfDate',
] as const;

export function computePnLPct(
  costPrice: number | null | undefined,
  current: number | null | undefined,
): number | null {
  if (
    typeof costPrice !== 'number' ||
    !Number.isFinite(costPrice) ||
    costPrice <= 0 ||
    typeof current !== 'number' ||
    !Number.isFinite(current)
  ) {
    return null;
  }
  return ((current - costPrice) / costPrice) * 100;
}

export function computeVwap(
  amount: number | null | undefined,
  volume: number | null | undefined,
  source: 'realtime' | 'daily' = 'realtime',
): number | null {
  if (
    typeof amount !== 'number' ||
    !Number.isFinite(amount) ||
    typeof volume !== 'number' ||
    !Number.isFinite(volume) ||
    volume <= 0
  ) {
    return null;
  }
  if (source === 'daily') {
    // Tushare daily: amount in 千元, volume in 手 (100 shares).
    return (amount * 1000) / (volume * 100);
  }
  // Realtime quote: amount in CNY, volume in 手.
  return amount / (volume * 100);
}

export function tradeDateFromTradeTime(tradeTime: string | null | undefined): string | null {
  const s = String(tradeTime ?? '').trim();
  if (!s) return null;
  const m1 = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m1) return m1[1];
  const m2 = s.match(/^(\d{8})$/);
  if (m2) return `${m2[1].slice(0, 4)}-${m2[1].slice(4, 6)}-${m2[1].slice(6, 8)}`;
  return null;
}

/** CN A-share realtime quote is required only when session is open and trend bar is for today. */
export function shouldRequireRealtimeQuote(opts: {
  tradingTime: boolean;
  symbol: string;
  trendAsOfDate: string | null | undefined;
  todaySh: string;
}): boolean {
  if (!opts.tradingTime) return false;
  if (!opts.symbol.toUpperCase().startsWith('CN:')) return false;
  const trendDate = String(opts.trendAsOfDate ?? '').trim();
  return trendDate === opts.todaySh;
}

/**
 * Pick the best "current" price for watchlist display / markdown export.
 * Prefer today's realtime quote during an active CN session; otherwise use latest daily close.
 */
export function resolveWatchlistCurrentPrice(opts: {
  tradingTime: boolean;
  todaySh: string;
  symbol: string;
  trendAsOfDate: string | null | undefined;
  quotePrice: number | null | undefined;
  quoteTradeTime: string | null | undefined;
  trendClose: number | null | undefined;
}): number | null {
  const close =
    typeof opts.trendClose === 'number' && Number.isFinite(opts.trendClose) ? opts.trendClose : null;
  const qPrice =
    typeof opts.quotePrice === 'number' && Number.isFinite(opts.quotePrice) ? opts.quotePrice : null;
  const qDate = tradeDateFromTradeTime(opts.quoteTradeTime ?? null);

  if (
    shouldRequireRealtimeQuote({
      tradingTime: opts.tradingTime,
      symbol: opts.symbol,
      trendAsOfDate: opts.trendAsOfDate,
      todaySh: opts.todaySh,
    }) &&
    qPrice != null &&
    qDate === opts.todaySh
  ) {
    return qPrice;
  }

  if (close != null) return close;

  if (qPrice != null && qDate === opts.todaySh) return qPrice;

  return qPrice ?? close;
}

export function parseQuoteNumber(raw: string | null | undefined): number | null {
  if (raw == null || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

export function formatPnLPct(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}%`;
}

export function formatVwap(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  return value.toFixed(2);
}

export function industryDisplayName(values: Record<string, unknown> | undefined | null): string {
  const em = values?.emIndustry;
  if (typeof em === 'string' && em.trim()) return em.trim();
  const ts = values?.industry;
  if (typeof ts === 'string' && ts.trim()) return ts.trim();
  return '—';
}

export function isHotTop3Industry(t: { values?: Record<string, unknown> | null } | undefined | null): boolean {
  const reasonsRaw = t?.values?.industryFlowReasons;
  const reasons = Array.isArray(reasonsRaw) ? reasonsRaw.map((x) => String(x ?? '')) : [];
  return reasons.includes('hotspots_today_top3');
}

export function formatHotTop3(t: { values?: Record<string, unknown> | null } | undefined | null): string {
  return isHotTop3Industry(t) ? '✓' : '—';
}

export function tushareIndustryTooltip(values: Record<string, unknown> | undefined | null): string | null {
  const em = values?.emIndustry;
  const ts = values?.industry;
  if (typeof em === 'string' && em.trim() && typeof ts === 'string' && ts.trim() && em.trim() !== ts.trim()) {
    return `Tushare: ${ts.trim()}`;
  }
  if (typeof em === 'string' && em.trim()) return null;
  if (typeof ts === 'string' && ts.trim()) return 'Tushare classification; may differ from East Money hotspot boards';
  return null;
}
