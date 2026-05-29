export const WATCHLIST_MD_HEADERS = [
  'Symbol',
  'Name',
  'Industry',
  'HotTop3',
  'Position%',
  'CostPrice',
  'Current',
  'VWAP',
  'Intraday%',
  'GapUp',
  'Alerts',
  'P&L%',
  'Score',
  'TrendOK',
  'Buy',
  'StopLoss',
  'AsOfDate',
] as const;

export const INTRADAY_SURGE_THRESHOLD_PCT = 6.0;
export const VWAP_PREMIUM_MULTIPLIER = 1.05;

const GAP_UP_WEAK_REGIMES = new Set(['Weak', 'Diverging']);

export type WatchlistRiskAlert = {
  code: string;
  severity: 'block' | 'warn';
  message: string;
};

export function isIntradaySurge(intradayChgPct: number | null | undefined): boolean {
  return (
    typeof intradayChgPct === 'number' &&
    Number.isFinite(intradayChgPct) &&
    intradayChgPct > INTRADAY_SURGE_THRESHOLD_PCT
  );
}

export function isAboveVwapPremium(
  current: number | null | undefined,
  vwap: number | null | undefined,
  multiplier: number = VWAP_PREMIUM_MULTIPLIER,
): boolean {
  if (
    typeof current !== 'number' ||
    !Number.isFinite(current) ||
    typeof vwap !== 'number' ||
    !Number.isFinite(vwap) ||
    vwap <= 0
  ) {
    return false;
  }
  return current > vwap * multiplier;
}

export function collectWatchlistRiskAlerts(opts: {
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  marketRegime?: string | null;
  current?: number | null;
  vwap?: number | null;
  riskMetricsLive?: boolean | null;
  serverAlerts?: WatchlistRiskAlert[] | null;
}): WatchlistRiskAlert[] {
  const out: WatchlistRiskAlert[] = [];
  const seen = new Set<string>();

  const push = (alert: WatchlistRiskAlert) => {
    if (seen.has(alert.code)) return;
    seen.add(alert.code);
    out.push(alert);
  };

  for (const alert of opts.serverAlerts ?? []) {
    if (alert?.code && alert.message) push(alert);
  }

  if (isIntradaySurge(opts.intradayChgPct) && opts.riskMetricsLive !== false) {
    push({
      code: 'intraday_surge',
      severity: 'block',
      message: `Intraday change ${opts.intradayChgPct!.toFixed(1)}% exceeds 6.0%; no new positions`,
    });
  }

  const regime = String(opts.marketRegime ?? '').trim();
  if (opts.gapUp === true && GAP_UP_WEAK_REGIMES.has(regime)) {
    push({
      code: 'gap_up_weak_market',
      severity: 'block',
      message: `Gap-up with ${regime} market; do not chase highs`,
    });
  }

  if (isAboveVwapPremium(opts.current, opts.vwap)) {
    push({
      code: 'above_vwap_premium',
      severity: 'warn',
      message: `Price above VWAP x${VWAP_PREMIUM_MULTIPLIER}; extended from average`,
    });
  }

  return out;
}

export function formatIntradayChgPct(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(1)}%`;
}

export function formatGapUp(value: boolean | null | undefined): string {
  if (value == null) return '—';
  return value ? '✓' : 'No';
}

export function resolveIntradayChgPct(opts: {
  fromTrend?: number | null;
  quotePrice?: number | null;
  quotePreClose?: number | null;
  quotePctChg?: number | null;
  quoteTradeDate?: string | null;
  asOfDate?: string | null;
}): number | null {
  if (typeof opts.fromTrend === 'number' && Number.isFinite(opts.fromTrend)) {
    return opts.fromTrend;
  }
  const asOf = String(opts.asOfDate ?? '').trim();
  const qDate = opts.quoteTradeDate ?? null;
  if (!asOf || !qDate || qDate !== asOf) return null;

  const pct = opts.quotePctChg;
  if (typeof pct === 'number' && Number.isFinite(pct)) return pct;

  const price = opts.quotePrice;
  const preClose = opts.quotePreClose;
  if (
    typeof price === 'number' &&
    Number.isFinite(price) &&
    typeof preClose === 'number' &&
    Number.isFinite(preClose) &&
    preClose > 0
  ) {
    return ((price - preClose) / preClose) * 100;
  }
  return null;
}

export function resolveWatchlistVwap(opts: {
  tradingTime: boolean;
  todaySh: string;
  symbol: string;
  trendAsOfDate: string | null | undefined;
  quoteAmount?: number | null;
  quoteVolume?: number | null;
  quoteTradeTime?: string | null;
}): number | null {
  const asOf = String(opts.trendAsOfDate ?? '').trim();
  const qDate = tradeDateFromTradeTime(opts.quoteTradeTime ?? null);

  if (opts.symbol.toUpperCase().startsWith('CN:') && asOf && qDate === asOf) {
    const vwap = computeVwap(opts.quoteAmount ?? null, opts.quoteVolume ?? null, 'realtime');
    if (vwap != null) return vwap;
  }

  if (
    shouldRequireRealtimeQuote({
      tradingTime: opts.tradingTime,
      symbol: opts.symbol,
      trendAsOfDate: opts.trendAsOfDate,
      todaySh: opts.todaySh,
    })
  ) {
    return computeVwap(opts.quoteAmount ?? null, opts.quoteVolume ?? null, 'realtime');
  }

  return null;
}

export function formatRiskAlerts(alerts: WatchlistRiskAlert[]): string {
  if (!alerts.length) return '—';
  return alerts.map((a) => a.message).join('; ');
}

export function hasBlockingWatchlistRisk(alerts: WatchlistRiskAlert[]): boolean {
  return alerts.some((a) => a.severity === 'block');
}

export type WatchlistQuoteSlice = {
  price: number | null;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose?: number | null;
  pctChg?: number | null;
};

export type WatchlistTrendRiskSlice = {
  asOfDate?: string | null;
  values?: { close?: number | null } | null;
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  marketRegime?: string | null;
  riskMetricsLive?: boolean | null;
  riskAlerts?: WatchlistRiskAlert[] | null;
};

export function buildWatchlistRowMetrics(opts: {
  symbol: string;
  trend: WatchlistTrendRiskSlice | null | undefined;
  quote?: WatchlistQuoteSlice | null;
  tradingTime: boolean;
  todaySh: string;
}): {
  current: number | null;
  vwap: number | null;
  intradayChgPct: number | null;
  gapUp: boolean | null;
  alerts: WatchlistRiskAlert[];
} {
  const t = opts.trend;
  const q = opts.quote;
  const close0 = t?.values?.close;
  const trendClose = typeof close0 === 'number' && Number.isFinite(close0) ? close0 : null;
  const asOfDate = t?.asOfDate ?? null;
  const quoteTradeDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
  const current = resolveWatchlistCurrentPrice({
    tradingTime: opts.tradingTime,
    todaySh: opts.todaySh,
    symbol: opts.symbol,
    trendAsOfDate: asOfDate,
    quotePrice: q?.price ?? null,
    quoteTradeTime: q?.tradeTime ?? null,
    trendClose,
  });
  const vwap = resolveWatchlistVwap({
    tradingTime: opts.tradingTime,
    todaySh: opts.todaySh,
    symbol: opts.symbol,
    trendAsOfDate: asOfDate,
    quoteAmount: q?.amount ?? null,
    quoteVolume: q?.volume ?? null,
    quoteTradeTime: q?.tradeTime ?? null,
  });
  const intradayChgPct = resolveIntradayChgPct({
    fromTrend: t?.intradayChgPct,
    quotePrice: q?.price ?? current,
    quotePreClose: q?.preClose ?? null,
    quotePctChg: q?.pctChg ?? null,
    quoteTradeDate,
    asOfDate,
  });
  const gapUp = typeof t?.gapUp === 'boolean' ? t.gapUp : null;
  const alerts = collectWatchlistRiskAlerts({
    intradayChgPct,
    gapUp,
    marketRegime: t?.marketRegime,
    current,
    vwap,
    riskMetricsLive: t?.riskMetricsLive,
    serverAlerts: t?.riskAlerts,
  });
  return { current, vwap, intradayChgPct, gapUp, alerts };
}

export function rowHasWatchlistRiskHighlight(alerts: WatchlistRiskAlert[]): boolean {
  return alerts.length > 0;
}

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
