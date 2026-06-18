'use client';

import { useQuery, type QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import { toTsCodeFromSymbol } from '@/lib/symbols';

export type BarsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  bars: Array<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    amount: string;
  }>;
};

export type ChipsResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    profitRatio: string;
    avgCost: string;
    cost90Low: string;
    cost90High: string;
    cost90Conc: string;
    cost70Low: string;
    cost70High: string;
    cost70Conc: string;
  }>;
};

export type FundFlowResp = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    close: string;
    changePct: string;
    mainNetAmount: string;
    mainNetRatio: string;
    superNetAmount: string;
    superNetRatio: string;
    largeNetAmount: string;
    largeNetRatio: string;
    mediumNetAmount: string;
    mediumNetRatio: string;
    smallNetAmount: string;
    smallNetRatio: string;
  }>;
};

type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price: string | null;
    open: string | null;
    high: string | null;
    low: string | null;
    pre_close: string | null;
    change: string | null;
    pct_chg: string | null;
    volume: string | null;
    amount: string | null;
    trade_time: string | null;
  }>;
};

export type StockDetail = {
  bars: BarsResp;
  chips: ChipsResp | null;
  fundFlow: FundFlowResp | null;
};

export const STOCK_DETAIL_STALE_MS = 10 * 60_000;

export function normalizeSymbol(symbol: string): string {
  const s = symbol.trim();
  if (s.startsWith('主板:') || s.startsWith('中小板:') || s.startsWith('创业板:') || s.startsWith('科创板:')) {
    const parts = s.split(':', 2);
    if (parts.length >= 2) {
      return `CN:${parts[1].trim()}`;
    }
  }
  return s;
}

function shanghaiTodayIso(): string {
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Shanghai' });
}

function getShanghaiTimeParts(): { weekday: string; hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(new Date());
  const map = new Map(parts.map((p) => [p.type, p.value]));
  return {
    weekday: map.get('weekday') ?? '',
    hour: Number(map.get('hour') ?? 0),
    minute: Number(map.get('minute') ?? 0),
  };
}

export function isShanghaiTradingTime(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  const inMorning = minutes >= 9 * 60 + 30 && minutes <= 11 * 60 + 30;
  const inAfternoon = minutes >= 13 * 60 && minutes <= 15 * 60;
  return inMorning || inAfternoon;
}

function mergeQuoteIntoBars(d: BarsResp, q: QuoteResp['items'][number]): BarsResp {
  const price = q.price ?? '';
  if (!price) return d;
  const date = (q.trade_time && q.trade_time.slice(0, 10)) || shanghaiTodayIso();
  const nextBar = {
    date,
    open: q.open ?? price,
    high: q.high ?? price,
    low: q.low ?? price,
    close: price,
    volume: q.volume ?? '',
    amount: q.amount ?? '',
  };
  const bars = [...(d.bars ?? [])];
  const last = bars[bars.length - 1];
  if (last && last.date === date) {
    bars[bars.length - 1] = nextBar;
  } else {
    bars.push(nextBar);
  }
  return { ...d, bars };
}

export function getLastDetailSyncMs(symbol: string): number {
  if (typeof window === 'undefined') return 0;
  try {
    const v = window.localStorage.getItem(`karios.market.stockDetailLastSync:${symbol}`);
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  } catch {
    return 0;
  }
}

export function setLastDetailSyncMs(symbol: string, ms: number) {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(`karios.market.stockDetailLastSync:${symbol}`, String(ms));
  } catch {
    // ignore
  }
}

export function stockDetailQueryKey(symbol: string) {
  return ['stock', normalizeSymbol(symbol), 'detail'] as const;
}

export async function fetchStockDetail(
  symbol: string,
  options: { force?: boolean; quote?: boolean } = {},
): Promise<StockDetail> {
  const normalizedSymbol = normalizeSymbol(symbol);
  const age = Date.now() - getLastDetailSyncMs(symbol);
  const shouldForce = options.force ?? age > STOCK_DETAIL_STALE_MS;
  const shouldQuote = options.quote ?? isShanghaiTradingTime();
  const forceParam = shouldForce ? '&force=true' : '';

  const [bars, chips] = await Promise.all([
    apiGetJson<BarsResp>(
      `/market/stocks/${encodeURIComponent(normalizedSymbol)}/bars?days=60${forceParam}`,
    ),
    apiGetJson<ChipsResp>(
      `/market/stocks/${encodeURIComponent(normalizedSymbol)}/chips?days=30${forceParam}`,
    ).catch(() => null),
  ]);

  const tsCode = toTsCodeFromSymbol(normalizedSymbol);
  const [fundFlow, quoteResp] = await Promise.all([
    apiGetJson<FundFlowResp>(
      `/market/stocks/${encodeURIComponent(normalizedSymbol)}/fund-flow?days=30${forceParam}`,
    ).catch(() => null),
    shouldQuote && tsCode
      ? apiGetJson<QuoteResp>(`/quote?ts_code=${encodeURIComponent(tsCode)}`).catch(() => null)
      : Promise.resolve(null),
  ]);

  let barsOut = bars;
  const quoteItem = quoteResp?.items?.[0];
  if (quoteItem) {
    barsOut = mergeQuoteIntoBars(barsOut, quoteItem);
  }

  if (shouldForce) {
    setLastDetailSyncMs(symbol, Date.now());
  }

  return { bars: barsOut, chips, fundFlow };
}

export function stockDetailQueryOptions(symbol: string) {
  return {
    queryKey: stockDetailQueryKey(symbol),
    queryFn: () => fetchStockDetail(symbol),
    staleTime: STOCK_DETAIL_STALE_MS,
  };
}

export function useStockDetailQuery(symbol: string) {
  return useQuery({
    ...stockDetailQueryOptions(symbol),
    enabled: Boolean(symbol?.trim()),
  });
}

export async function refetchStockDetail(
  queryClient: QueryClient,
  symbol: string,
  options: { force?: boolean; quote?: boolean } = {},
): Promise<StockDetail> {
  const key = stockDetailQueryKey(symbol);
  const detail = await queryClient.fetchQuery({
    queryKey: key,
    queryFn: () => fetchStockDetail(symbol, options),
  });
  queryClient.setQueryData(key, detail);
  return detail;
}
