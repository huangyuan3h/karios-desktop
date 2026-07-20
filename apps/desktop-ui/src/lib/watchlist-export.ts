import type { QueryClient } from '@tanstack/react-query';

import {
  buildCatalystPurgeMap,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchCatalystStocks,
} from '@/lib/alpha-radar-catalyst';
import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { buildPositionsExecutionMarkdown } from '@/lib/execution-markdown';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import { refetchWatchlistMarket } from '@/lib/queries/watchlist';
import {
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import { applyWatchlistPurgeAfterReport } from '@/lib/watchlist-purge';
import type { WatchlistItem } from '@/lib/watchlist-storage';
import type { ExecutionGate } from '@karios/shared';
import type { CatalystPurgeHint } from '@/lib/execution-action';

async function loadCatalystPurgeMap(): Promise<Map<string, CatalystPurgeHint> | null> {
  try {
    const resp = await fetchCatalystStocks(
      DATA_SYNC_BASE_URL,
      50,
      DEFAULT_CATALYST_MAX_AGE_DAYS,
    );
    return buildCatalystPurgeMap(resp);
  } catch {
    return null;
  }
}

const COPY_BLOCKING_MISSING_DATA = new Set([
  'no_bars',
  'bars_lt_60',
  'insufficient_indicators',
  'unsupported_market',
  'no_result',
]);

export function copyBlockingMissingData(missingData: string[] | undefined | null): string[] {
  const md = Array.isArray(missingData) ? missingData.filter(Boolean) : [];
  return md.filter((reason) => COPY_BLOCKING_MISSING_DATA.has(reason));
}

export type WatchlistCopyValidationError = {
  ok: false;
  message: string;
};

export type WatchlistCopyBuildResult = {
  ok: true;
  markdown: string;
};

export type WatchlistCopyResult = WatchlistCopyValidationError | WatchlistCopyBuildResult;

export function validateWatchlistCopyData(options: {
  sortedItems: WatchlistItem[];
  trendSnap: Record<string, TrendOkResult>;
  quotesSnap: Record<string, WatchlistQuote>;
  tradingTime: boolean;
  todaySh: string;
}): WatchlistCopyValidationError | null {
  const { sortedItems, trendSnap, quotesSnap, tradingTime, todaySh } = options;
  const missingRealtime: string[] = [];
  const missingTrend: string[] = [];
  const missingHistory: string[] = [];

  for (const it of sortedItems) {
    const sym = it.symbol;
    const t = trendSnap[sym];
    if (!t) {
      missingTrend.push(sym);
      continue;
    }
    const blockingMd = copyBlockingMissingData(t.missingData);
    if (blockingMd.length) {
      missingHistory.push(`${sym} (${blockingMd.join(', ')})`);
    }
    if (
      shouldRequireRealtimeQuote({
        tradingTime,
        symbol: sym,
        trendAsOfDate: t?.asOfDate ?? null,
        todaySh,
      })
    ) {
      const q = quotesSnap[sym];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
        missingRealtime.push(sym);
      }
    }
  }

  if (!missingTrend.length && !missingHistory.length && !missingRealtime.length) {
    return null;
  }

  const parts: string[] = [];
  if (missingRealtime.length) {
    parts.push(
      `missing realtime quote (today): ${missingRealtime.slice(0, 6).join(', ')}${
        missingRealtime.length > 6 ? '…' : ''
      }`,
    );
  }
  if (missingHistory.length) {
    parts.push(
      `missing history/indicators: ${missingHistory.slice(0, 6).join(', ')}${
        missingHistory.length > 6 ? '…' : ''
      }`,
    );
  }
  if (missingTrend.length) {
    parts.push(
      `missing TrendOK result: ${missingTrend.slice(0, 6).join(', ')}${
        missingTrend.length > 6 ? '…' : ''
      }`,
    );
  }
  return { ok: false, message: `Copy aborted: ${parts.join(' | ')}` };
}

export function buildWatchlistMarkdown(options: {
  sortedItems: WatchlistItem[];
  trendSnap: Record<string, TrendOkResult>;
  quotesSnap: Record<string, WatchlistQuote>;
  trendUpdatedAt: string | null;
  tradingTime: boolean;
  todaySh: string;
  executionGate?: ExecutionGate | null;
  mainlineAllow?: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
}): string {
  const {
    sortedItems,
    trendSnap,
    quotesSnap,
    tradingTime,
    todaySh,
    executionGate = null,
    mainlineAllow = null,
    sectorOutflowBlock = false,
  } = options;
  // Same unified combat table as Dashboard Copy all (no separate fat Watchlist dump).
  // Sync helper: no catalyst fetch (PURGE exemption only on Copy / Sync&Copy paths).
  const { markdown } = buildPositionsExecutionMarkdown(
    sortedItems,
    trendSnap,
    quotesSnap,
    executionGate ?? null,
    '##',
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    null,
  );
  return markdown.trim() + '\n';
}

export async function copyWatchlistMarkdown(options: {
  queryClient: QueryClient;
  sortedItems: WatchlistItem[];
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  trendUpdatedAt: string | null;
  executionGate?: ExecutionGate | null;
  mainlineAllow?: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
}): Promise<WatchlistCopyResult> {
  const {
    queryClient,
    sortedItems,
    trend,
    quotes,
    trendUpdatedAt,
    executionGate = null,
    mainlineAllow = null,
    sectorOutflowBlock = false,
  } = options;
  if (!sortedItems.length) {
    return { ok: false, message: 'No items to copy.' };
  }

  const tradingTime = isShanghaiTradingTime();
  const todaySh = getShanghaiTodayIso();
  const syms = sortedItems.map((x) => x.symbol);
  let trendSnap: Record<string, TrendOkResult>;
  let quotesSnap: Record<string, WatchlistQuote>;

  try {
    const fresh = await refetchWatchlistMarket(queryClient, syms, { forceMarket: false });
    trendSnap = fresh.trend;
    quotesSnap = fresh.quotes;
  } catch (e) {
    console.warn('Watchlist copy refresh failed, using cached data:', e);
    trendSnap = trend;
    quotesSnap = quotes;
  }

  const validationError = validateWatchlistCopyData({
    sortedItems,
    trendSnap,
    quotesSnap,
    tradingTime,
    todaySh,
  });
  if (validationError) return validationError;

  const catalystBySymbol = await loadCatalystPurgeMap();
  const { markdown, purgeSymbols } = buildPositionsExecutionMarkdown(
    sortedItems,
    trendSnap,
    quotesSnap,
    executionGate ?? null,
    '##',
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    catalystBySymbol,
  );
  if (purgeSymbols.length) {
    await applyWatchlistPurgeAfterReport(purgeSymbols).catch(() => 0);
  }
  return { ok: true, markdown: markdown.trim() + '\n' };
}
