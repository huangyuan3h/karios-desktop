import { apiGetJson } from '@/lib/api/client';
import { fetchTrendOkMap } from '@/lib/api/trendok';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { chunk } from '@/lib/chunk';
import { mapWithConcurrency } from '@/lib/concurrency';
import { parseQuoteNumber } from '@/lib/watchlist-metrics';
import { isCnWatchlistSymbol, toTsCodeFromSymbol } from '@/lib/symbols';

const DEFAULT_FORCE_BARS_CONCURRENCY = 4;
const DEFAULT_FORCE_BARS_DAYS = 60;
const QUOTE_CHUNK_SIZE = 50;

type QuoteResp = {
  items?: Array<{
    ts_code: string;
    price?: number | null;
    trade_time?: string | null;
    amount?: number | null;
    volume?: number | null;
    pre_close?: number | null;
    pct_chg?: number | null;
  }>;
};

export type WatchlistBarSyncResult = {
  failures: number;
  total: number;
};

export type WatchlistMarketSnapshot = {
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  barSync?: WatchlistBarSyncResult;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimitError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err ?? '');
  return msg.includes('429');
}

async function forceRefreshOneBar(symbol: string, days: number): Promise<boolean> {
  const enc = encodeURIComponent(symbol);
  const path = `/market/stocks/${enc}/bars?days=${days}&force=true`;
  try {
    await apiGetJson(path);
    return true;
  } catch (err) {
    if (!isRateLimitError(err)) return false;
    await sleep(200);
    try {
      await apiGetJson(path);
      return true;
    } catch (retryErr) {
      if (isRateLimitError(retryErr)) await sleep(400);
      return false;
    }
  }
}

export async function forceRefreshWatchlistBars(
  symbols: string[],
  options: { concurrency?: number; days?: number } = {},
): Promise<WatchlistBarSyncResult> {
  const cnSymbols = symbols.filter((s) => s && isCnWatchlistSymbol(s));
  if (!cnSymbols.length) return { failures: 0, total: 0 };

  const concurrency = options.concurrency ?? DEFAULT_FORCE_BARS_CONCURRENCY;
  const days = options.days ?? DEFAULT_FORCE_BARS_DAYS;
  const outcomes = await mapWithConcurrency(cnSymbols, concurrency, (sym) =>
    forceRefreshOneBar(sym, days),
  );
  const failures = outcomes.filter((ok) => !ok).length;
  return { failures, total: cnSymbols.length };
}

function mapQuoteItem(
  it: NonNullable<QuoteResp['items']>[number],
): WatchlistQuote {
  const p = it.price != null ? Number(it.price) : NaN;
  const pre = it.pre_close != null ? Number(it.pre_close) : NaN;
  const pct = it.pct_chg != null ? Number(it.pct_chg) : NaN;
  return {
    tsCode: it.ts_code,
    price: Number.isFinite(p) ? p : null,
    tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
    amount: parseQuoteNumber(it.amount),
    volume: parseQuoteNumber(it.volume),
    preClose: Number.isFinite(pre) ? pre : null,
    pctChg: Number.isFinite(pct) ? pct : null,
  };
}

export async function fetchWatchlistQuotes(
  symbols: string[],
): Promise<Record<string, WatchlistQuote>> {
  const syms = symbols.filter(Boolean);
  const quotes: Record<string, WatchlistQuote> = {};
  if (!syms.length) return quotes;

  const byTsCode = new Map<string, string>();
  const tsCodes = syms
    .map((s) => {
      const t = toTsCodeFromSymbol(s);
      if (t) byTsCode.set(t, s);
      return t;
    })
    .filter(Boolean) as string[];

  const quoteParts = await Promise.all(
    chunk(tsCodes, QUOTE_CHUNK_SIZE).map((part) =>
      apiGetJson<QuoteResp>(`/quote?ts_codes=${encodeURIComponent(part.join(','))}`).catch(
        () => null,
      ),
    ),
  );

  for (const r of quoteParts) {
    for (const it of r?.items ?? []) {
      const sym = byTsCode.get(it.ts_code);
      if (!sym) continue;
      quotes[sym] = mapQuoteItem(it);
    }
  }
  return quotes;
}

export async function fetchWatchlistMarketSnapshot(
  symbols: string[],
  options: { forceMarket?: boolean; realtime: boolean },
): Promise<WatchlistMarketSnapshot> {
  const syms = symbols.filter(Boolean);
  const trend: Record<string, TrendOkResult> = {};
  const quotes: Record<string, WatchlistQuote> = {};
  if (!syms.length) return { trend, quotes };

  let barSync: WatchlistBarSyncResult | undefined;
  if (options.forceMarket) {
    barSync = await forceRefreshWatchlistBars(syms);
  }

  const [trendMap, quotesMap] = await Promise.all([
    fetchTrendOkMap(syms, { realtime: options.realtime }),
    fetchWatchlistQuotes(syms),
  ]);

  for (const row of trendMap.values()) {
    if (row?.symbol) trend[String(row.symbol)] = row;
  }
  for (const [sym, q] of Object.entries(quotesMap)) {
    quotes[sym] = q;
  }

  return { trend, quotes, barSync };
}
