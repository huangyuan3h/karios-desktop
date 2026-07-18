export const WATCHLIST_MD_HEADERS = [
  'Symbol',
  'Name',
  'Industry',
  'HotTop3',
  'RS',
  'Position%',
  'CostPrice',
  'Current',
  'VWAP',
  'Intraday%',
  'VR',
  'Inst_Flow',
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

/** Market regimes where a true gap-up blocks new entries (Alerts + Action Card). */
export const GAP_UP_WEAK_REGIMES = new Set(['Weak', 'Diverging']);

import type { WatchlistRiskAlert, InstFlow } from '@karios/shared';

export type { WatchlistRiskAlert, InstFlow };

export function isIntradaySurge(intradayChgPct: number | null | undefined): boolean {
  return (
    typeof intradayChgPct === 'number' &&
    Number.isFinite(intradayChgPct) &&
    intradayChgPct > INTRADAY_SURGE_THRESHOLD_PCT
  );
}

/** True gap-up (latest low > prev high) in Weak/Diverging — do not chase. */
export function isGapUpWeakMarket(
  gapUp: boolean | null | undefined,
  marketRegime: string | null | undefined,
): boolean {
  if (gapUp !== true) return false;
  const regime = String(marketRegime ?? '').trim();
  return GAP_UP_WEAK_REGIMES.has(regime);
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

  if (isGapUpWeakMarket(opts.gapUp, opts.marketRegime)) {
    const regime = String(opts.marketRegime ?? '').trim();
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

function finiteNumber(value: unknown): number | null {
  if (typeof value !== 'number' || !Number.isFinite(value)) return null;
  return value;
}

export function resolveVolumeRatio(
  values: Record<string, unknown> | null | undefined,
): number | null {
  const direct = finiteNumber(values?.volumeRatio);
  if (direct != null) return direct;
  const avg5 = finiteNumber(values?.avgVol5);
  const avg30 = finiteNumber(values?.avgVol30);
  if (avg5 == null || avg30 == null) return null;
  if (avg30 > 0) return avg5 / avg30;
  return avg5 > 0 ? 1.0 : 0.0;
}

export function formatVolumeRatio(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  return `${value.toFixed(2)}x`;
}

export type VolumeRatioTone = 'strong' | 'weak' | 'neutral';

export function volumeRatioTone(value: number | null | undefined): VolumeRatioTone {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 'neutral';
  if (value >= 1.5) return 'strong';
  if (value < 1.0) return 'weak';
  return 'neutral';
}

export function volumeRatioClassName(value: number | null | undefined): string {
  const tone = volumeRatioTone(value);
  if (tone === 'strong') return 'font-semibold text-emerald-600';
  if (tone === 'weak') return 'font-semibold text-red-600';
  return '';
}

export function resolveIntradayChgPct(opts: {
  fromTrend?: number | null;
  quotePrice?: number | null;
  quotePreClose?: number | null;
  quotePctChg?: number | null;
  quoteTradeDate?: string | null;
  asOfDate?: string | null;
}): number | null {
  const asOf = String(opts.asOfDate ?? '').trim();
  const qDate = opts.quoteTradeDate ?? null;

  const pctFromQuote = (): number | null => {
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
  };

  // Trend bar stale but closing quote is for today (typical 15:00–17:10 gap).
  if (qDate && asOf && qDate !== asOf) {
    const fromQuote = pctFromQuote();
    if (fromQuote != null) return fromQuote;
  }

  if (typeof opts.fromTrend === 'number' && Number.isFinite(opts.fromTrend)) {
    return opts.fromTrend;
  }

  if (!asOf || !qDate || qDate !== asOf) return null;

  return pctFromQuote();
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
  const isCn = opts.symbol.toUpperCase().startsWith('CN:');

  if (isCn && asOf && qDate === asOf) {
    const vwap = computeVwap(opts.quoteAmount ?? null, opts.quoteVolume ?? null, 'realtime');
    if (vwap != null) return vwap;
  }

  if (isCn && qDate === opts.todaySh && (opts.tradingTime || asOf !== opts.todaySh)) {
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
  values?: (Record<string, unknown> & { close?: number | null }) | null;
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  marketRegime?: string | null;
  riskMetricsLive?: boolean | null;
  riskAlerts?: WatchlistRiskAlert[] | null;
  instFlow?: InstFlow | null;
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
  volumeRatio: number | null;
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
  const volumeRatio = resolveVolumeRatio((t?.values ?? null) as Record<string, unknown> | null);
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
  return { current, vwap, intradayChgPct, volumeRatio, gapUp, alerts };
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
  // Realtime quote: amount in CNY, volume in shares (tushare realtime_quote vol unit).
  return amount / volume;
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

/** CN A-share realtime quote is required during the session for display and risk metrics. */
export function shouldRequireRealtimeQuote(opts: {
  tradingTime: boolean;
  symbol: string;
  trendAsOfDate: string | null | undefined;
  todaySh: string;
}): boolean {
  void opts.trendAsOfDate;
  void opts.todaySh;
  if (!opts.tradingTime) return false;
  return opts.symbol.toUpperCase().startsWith('CN:');
}

/**
 * Pick the best "current" price for watchlist display / markdown export.
 * Prefer today's realtime quote during session; after close prefer synced daily bar,
 * but fall back to today's closing quote while daily sync is still pending.
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
  const trendDate = String(opts.trendAsOfDate ?? '').trim();
  const isCn = opts.symbol.toUpperCase().startsWith('CN:');
  const hasTodayQuote = qPrice != null && qDate === opts.todaySh;

  if (opts.tradingTime && isCn && hasTodayQuote) {
    return qPrice;
  }

  if (trendDate === opts.todaySh && close != null) {
    return close;
  }

  if (hasTodayQuote) {
    return qPrice;
  }

  return close ?? qPrice ?? null;
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

export function formatRs(t: { rs?: number | null; values?: Record<string, unknown> | null } | undefined | null): string {
  const rs = t?.rs ?? (t?.values?.rsValue as number | undefined);
  if (typeof rs !== 'number' || !Number.isFinite(rs)) return '—';
  return `${rs > 0 ? '+' : ''}${rs.toFixed(1)}%`;
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

export function formatInstFlow(instFlow: InstFlow | null | undefined): string {
  const display = instFlow?.display?.trim();
  return display || '—';
}

export function formatInstFlowTooltip(instFlow: InstFlow | null | undefined): string | undefined {
  if (!instFlow?.display) return undefined;
  const seats = instFlow.topBuySeats;
  if (!Array.isArray(seats) || !seats.length) return instFlow.display;
  const seatLines = seats.map((s) => {
    const tag = s.isLhasa ? '拉萨' : s.isInst ? '机构' : '席位';
    const amt =
      typeof s.buyAmt === 'number' && Number.isFinite(s.buyAmt)
        ? `${(s.buyAmt / 1e8).toFixed(2)}亿`
        : '';
    return `${tag}: ${s.name}${amt ? ` (${amt})` : ''}`;
  });
  return `${instFlow.display}\n${seatLines.join('\n')}`;
}

export function isInstFlowRisk(instFlow: InstFlow | null | undefined): boolean {
  if (!instFlow?.onBoard) return false;
  const yi = instFlow.instNetBuyYi;
  if (typeof yi === 'number' && Number.isFinite(yi) && yi < 0) return true;
  return Boolean(instFlow.lhasaDominant);
}
