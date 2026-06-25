import type { QueryClient } from '@tanstack/react-query';

import type { TrendOkResult, WatchlistQuote } from '@/lib/api/types';
import { escapeMarkdownCell, mdLines, mdPrice, mdScore } from '@/lib/dashboard-format';
import { getShanghaiTodayIso, isShanghaiTradingTime } from '@/lib/market-hours';
import { refetchWatchlistMarket } from '@/lib/queries/watchlist';
import { scoreExplainZhLines, trendOkRuleLines, trendOkSummary } from '@/lib/trendok-display';
import { fmtBuyCell } from '@/lib/watchlist-table-cells';
import {
  WATCHLIST_MD_HEADERS,
  buildWatchlistRowMetrics,
  computePnLPct,
  formatGapUp,
  formatHotTop3,
  formatIntradayChgPct,
  formatPnLPct,
  formatRs,
  formatRiskAlerts,
  formatVwap,
  formatInstFlow,
  formatVolumeRatio,
  industryDisplayName,
  isIntradaySurge,
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';

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
}): string {
  const { sortedItems, trendSnap, quotesSnap, trendUpdatedAt, tradingTime, todaySh } = options;
  const generatedAt = new Date().toISOString();
  const lines: string[] = [];
  lines.push('## Watchlist');
  lines.push(`- generatedAt: ${generatedAt}`);
  lines.push(`- items: ${sortedItems.length}`);
  lines.push(
    `- scoresUpdatedAt: ${trendUpdatedAt ? new Date(trendUpdatedAt).toLocaleString() : '—'}`,
  );
  lines.push(`- shanghaiToday: ${todaySh}`);
  lines.push(`- tradingTime: ${tradingTime ? 'true' : 'false'}`);
  lines.push('');

  lines.push('### TrendOK rules');
  lines.push(mdLines(trendOkRuleLines()));
  lines.push('');
  lines.push('### Score（0–100）计分说明');
  lines.push(
    mdLines(scoreExplainZhLines().map((line) => (line.startsWith('-') ? line : `- ${line}`))),
  );
  lines.push('');

  const headers = [...WATCHLIST_MD_HEADERS];
  const blockAlerts: string[] = [];
  lines.push(`| ${headers.join(' | ')} |`);
  lines.push(`| ${headers.map(() => '---').join(' | ')} |`);

  for (const it of sortedItems) {
    const t = trendSnap[it.symbol];
    const buy = fmtBuyCell(t).text;
    const q = quotesSnap[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: q,
      tradingTime,
      todaySh,
    });
    const pnl = computePnLPct(it.costPrice ?? null, rowMetrics.current);
    const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
    const asOf = qDate === todaySh ? qDate : String(t?.asOfDate ?? '');
    const values = (t?.values ?? {}) as Record<string, unknown>;
    const intradayCell = isIntradaySurge(rowMetrics.intradayChgPct)
      ? `⚠️ ${formatIntradayChgPct(rowMetrics.intradayChgPct)}`
      : formatIntradayChgPct(rowMetrics.intradayChgPct);
    const gapCell =
      rowMetrics.gapUp === true ? `⚠️ ${formatGapUp(true)}` : formatGapUp(rowMetrics.gapUp);
    const alertsCell = formatRiskAlerts(rowMetrics.alerts);
    for (const alert of rowMetrics.alerts) {
      if (alert.severity === 'block') {
        blockAlerts.push(`${it.symbol}: ${alert.message}`);
      }
    }
    const row = [
      escapeMarkdownCell(it.symbol),
      escapeMarkdownCell(it.name || '—'),
      escapeMarkdownCell(industryDisplayName(values)),
      escapeMarkdownCell(formatHotTop3(t)),
      escapeMarkdownCell(formatRs(t)),
      escapeMarkdownCell(
        typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
          ? it.positionPct.toFixed(1)
          : '—',
      ),
      escapeMarkdownCell(mdPrice(it.costPrice ?? null)),
      escapeMarkdownCell(mdPrice(rowMetrics.current)),
      escapeMarkdownCell(formatVwap(rowMetrics.vwap)),
      escapeMarkdownCell(intradayCell),
      escapeMarkdownCell(formatVolumeRatio(rowMetrics.volumeRatio)),
      escapeMarkdownCell(formatInstFlow(t?.instFlow)),
      escapeMarkdownCell(gapCell),
      escapeMarkdownCell(alertsCell),
      escapeMarkdownCell(formatPnLPct(pnl)),
      escapeMarkdownCell(mdScore(t?.score ?? null)),
      escapeMarkdownCell(trendOkSummary(t)),
      escapeMarkdownCell(buy),
      escapeMarkdownCell(mdPrice(t?.stopLossPrice ?? null)),
      escapeMarkdownCell(asOf),
    ];
    lines.push(`| ${row.join(' | ')} |`);
  }

  lines.push('');
  if (blockAlerts.length) {
    lines.push('### Risk alerts');
    lines.push(mdLines(blockAlerts.map((line) => `- ${line}`)));
    lines.push('');
  }

  return lines.join('\n').trim() + '\n';
}

export async function copyWatchlistMarkdown(options: {
  queryClient: QueryClient;
  sortedItems: WatchlistItem[];
  trend: Record<string, TrendOkResult>;
  quotes: Record<string, WatchlistQuote>;
  trendUpdatedAt: string | null;
}): Promise<WatchlistCopyResult> {
  const { queryClient, sortedItems, trend, quotes, trendUpdatedAt } = options;
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

  const markdown = buildWatchlistMarkdown({
    sortedItems,
    trendSnap,
    quotesSnap,
    trendUpdatedAt,
    tradingTime,
    todaySh,
  });
  return { ok: true, markdown };
}
