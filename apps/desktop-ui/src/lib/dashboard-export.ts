/* eslint-disable @typescript-eslint/no-explicit-any */
import type { QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import {
  buildCatalystPurgeMap,
  buildCatalystStocksMarkdown,
  buildAlphaRadarTrendsMarkdown,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchAlphaRadarTrendsForCopy,
  fetchCatalystStocks,
  normalizeCatalystSymbol,
  type CatalystCopyContext,
  type CatalystStocksResponse,
} from '@/lib/alpha-radar-catalyst';
import { chunk } from '@/lib/chunk';
import {
  buildTopByDateMap,
  dedupeShownDates,
  fmtAmountCn,
  fmtSignedAmountCn,
  formatSrvIndexLine,
  formatExecutionGateMarkdown,
  mdTable,
  escapeMarkdownCell,
  buildHotIndustriesMarkdown,
} from '@/lib/dashboard-format';
import {
  formatCondOrderDraftMarkdown,
  formatSinceLastCopyMarkdown,
  readLastCopyAt,
  type CondOrderCard,
  type CondOrderQuoteHint,
} from '@/lib/copy-ai-brief';
import {
  buildExecAttentionQueue,
  formatExecAttentionMarkdown,
  resolveAttentionCards,
} from '@/lib/exec-attention';
import { parseExecutionGate } from '@/lib/execution-action';
import {
  buildExecutionSnapshotPayload,
  fetchExecutionJournalMarkdown,
} from '@/lib/execution-journal';
import { buildPositionsExecutionMarkdown } from '@/lib/execution-markdown';
import {
  buildMainlineAllowSet,
  isSectorOutflowBlock,
  type MainlineAllowSet,
} from '@/lib/hot-industry-picks';
import { applyWatchlistPurgeAfterReport } from '@/lib/watchlist-purge';
import type {
  ExecutionChangeListResponse,
  ExecutionGate,
  ExecutionSnapshotListResponse,
} from '@karios/shared';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import {
  getShanghaiTodayIso,
  isShanghaiQuoteWindow,
  isShanghaiSyncWindow,
  isShanghaiTradingTime,
} from '@/lib/market-hours';
import {
  SCREENER_MARKDOWN_HEADERS,
  buildScreenerMarkdownRows,
  countMissingScores,
  extractSymbolsFromSnapshotRows,
  fetchTodayScreenerSymbolsByTitle,
  fetchTrendOkMap,
  screenerMarkdownRowsToTable,
} from '@/lib/screenerExport';
import { toTsCodeFromSymbol } from '@/lib/symbols';
import { screenerSnapshotsQueryOptions } from '@/lib/queries/screener';
import { fetchDashboardSummaryPartial } from '@/lib/queries/dashboard';
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';
import { fetchWatchlistMarketSnapshot, type WatchlistMarketSnapshot } from '@/lib/watchlist-market';
import {
  parseQuoteNumber,
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import { copyBlockingMissingData } from '@/lib/watchlist-export';
import { loadWatchlist, ensureWatchlistHydrated, type WatchlistItem } from '@/lib/watchlist-storage';

type DashboardSummary = any;

/** Copy-all screener filtering (2026-08-01 · wife feedback). */
export const SCREENER_COPY_TOP_N = 10;
export const SCREENER_COPY_MIN_SCORE = 60;

type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price: string | null;
    pre_close: string | null;
    pct_chg: string | null;
    amount: string | null;
    volume: string | null;
    trade_time: string | null;
  }>;
};

function parseDashboardQuoteItem(it: QuoteResp['items'][number]): {
  price: number | null;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose: number | null;
  pctChg: number | null;
} {
  const p = it.price != null ? Number(it.price) : NaN;
  const pre = it.pre_close != null ? Number(it.pre_close) : NaN;
  const pct = it.pct_chg != null ? Number(it.pct_chg) : NaN;
  return {
    price: Number.isFinite(p) ? p : null,
    tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
    amount: parseQuoteNumber(it.amount),
    volume: parseQuoteNumber(it.volume),
    preClose: Number.isFinite(pre) ? pre : null,
    pctChg: Number.isFinite(pct) ? pct : null,
  };
}

function loadWatchlistSymbols(): Set<string> {
  const itemsRaw = loadWatchlist();
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => String(x.symbol).trim().toUpperCase());
  return new Set(items);
}

function symbolsWithMissingTrendInputs(
  symbols: string[],
  trend: Record<string, TrendOkResult>,
): string[] {
  const missing: string[] = [];
  for (const sym of symbols) {
    const row = trend[sym];
    const blocking = copyBlockingMissingData(row?.missingData);
    if (blocking.length) missing.push(sym);
  }
  return missing;
}

async function fetchWatchlistSnapshotForCopy(
  symbols: string[],
  queryClient?: QueryClient,
): Promise<WatchlistMarketSnapshot> {
  if (!queryClient) {
    return fetchWatchlistMarketSnapshot(symbols, {
      forceMarket: false,
      realtime: isShanghaiQuoteWindow(),
    });
  }

  const options = watchlistMarketQueryOptions(symbols);
  const snapshot = await queryClient.fetchQuery(options);
  const missingInputs = symbolsWithMissingTrendInputs(symbols, snapshot.trend);
  if (!missingInputs.length) return snapshot;

  const refreshed = await fetchWatchlistMarketSnapshot(symbols, {
    forceMarket: true,
    realtime: isShanghaiQuoteWindow(),
  });
  queryClient.setQueryData(options.queryKey, refreshed);
  return refreshed;
}

export function buildIndustryMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const ind: any = summary2?.industryFundFlow ?? {};
  const asOfDate = String(ind?.asOfDate ?? summary2?.asOfDate ?? '').trim();

  const datesAll: string[] = Array.isArray(ind?.dates) ? ind.dates : [];
  const rawShownDates = datesAll.slice(-5);
  const byDate = buildTopByDateMap(summary2);
  const { dedupedDates } = dedupeShownDates(rawShownDates, byDate);

  const lines: string[] = [];
  lines.push(`${heading} Industry fund flow`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  lines.push('');

  if (dedupedDates.length) {
    const headers1 = ['#', ...dedupedDates.map((d) => String(d).slice(5))];
    const rows1: unknown[][] = Array.from({ length: 5 }).map((_, i) => [
      i + 1,
      ...dedupedDates.map((d) => String((byDate[d] || [])[i] ?? '')),
    ]);
    lines.push(`${heading}# Top5×Date hotspots (names only)`);
    lines.push('');
    lines.push(mdTable(headers1, rows1));
    lines.push('');
  }

  const buildFlow = (block: any, title: string) => {
    const topRows: any[] = Array.isArray(block?.top) ? block.top : [];
    if (!topRows.length || !dedupedDates.length) return;
    const cols = dedupedDates;
    const headers = ['Industry', 'Sum(5D)', ...cols.map((d) => String(d).slice(5))];
    const rows: unknown[][] = topRows.slice(0, 10).map((r: any) => {
      const seriesArr: any[] = Array.isArray(r?.series) ? r.series : [];
      const m2: Record<string, number> = {};
      for (const p of seriesArr) {
        const dd = String(p?.date ?? '');
        const nv = Number(p?.netInflow ?? 0);
        if (dd) m2[dd] = Number.isFinite(nv) ? nv : 0;
      }
      return [
        String(r?.industryName ?? ''),
        fmtAmountCn(r?.sum5d),
        ...cols.map((d) => fmtAmountCn(m2[d] ?? 0)),
      ];
    });
    lines.push(`${heading}# ${title}`);
    lines.push('');
    lines.push(mdTable(headers, rows));
    lines.push('');
  };

  buildFlow(ind?.flow5d ?? null, '5D net inflow (Top by 5D sum)');
  buildFlow(ind?.flow5dOut ?? null, '5D net outflow (Top by 5D sum)');

  return lines.join('\n').trim() + '\n';
}

export function buildMarketAndMacroMarkdown(
  s: DashboardSummary | null,
  heading = '##',
): string {
  const summary2: any = s ?? {};
  const ms: any = summary2?.marketSentiment ?? {};
  const indexSignals: any[] = Array.isArray(ms?.indexSignals) ? ms.indexSignals : [];
  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];

  if (!indexSignals.length && !macroItems.length) return '';

  const lines: string[] = [];
  lines.push(`${heading} Market & Macro overview`);
  lines.push('');
  lines.push(
    '- note: 指数红绿灯 + 宏观商品/汇率 + 300ETF Put IV 一张表；避免散落重复',
  );

  const headers = ['Name', 'Kind', 'Signal', 'Chg%', 'Close', 'MA5', 'MA20', 'AsOfDate', 'Source'];
  const rows: unknown[][] = [];

  for (const it of indexSignals) {
    const pc = it?.pctChg;
    const chg =
      typeof pc === 'number' && Number.isFinite(pc)
        ? `${pc >= 0 ? '+' : ''}${pc.toFixed(2)}%`
        : '—';
    rows.push([
      String(it?.name ?? it?.tsCode ?? ''),
      'Index',
      String(it?.signal ?? ''),
      chg,
      Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—',
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      String(it?.asOfDate ?? ''),
      String(it?.source ?? '—'),
    ]);
  }

  for (const it of macroItems) {
    const pct = it?.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    const isVol = it?.category === 'volatility';
    const closeStr = Number.isFinite(it?.close)
      ? isVol
        ? `${Number(it.close).toFixed(1)}%`
        : Number(it.close).toFixed(2)
      : '—';
    const signalStr = it?.signalLabel
      ? it.signalLabel
      : it?.signal
        ? String(it.signal)
        : '—';
    const kind = isVol ? 'Vol (IV)' : 'Macro';
    rows.push([
      String(it?.name ?? it?.seriesId ?? ''),
      kind,
      signalStr,
      chg,
      closeStr,
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      String(it?.asOfDate ?? ''),
      String(it?.source ?? ''),
    ]);
  }

  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

export function buildSentimentMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const ms: any = summary2?.marketSentiment ?? {};
  const items: any[] = Array.isArray(ms?.items) ? ms.items : [];
  const latest = items.length ? items[items.length - 1] : null;
  const asOfDate = String(ms?.asOfDate ?? summary2?.asOfDate ?? '').trim();
  const indexSignals: any[] = Array.isArray(ms?.indexSignals) ? ms.indexSignals : [];

  const lines: string[] = [];
  const envZh = String(summary2?.marketEnvironmentZh ?? '').trim();
  if (envZh) {
    lines.push(`${heading} 市场环境摘要`);
    lines.push('');
    lines.push(envZh);
    lines.push('');
  }
  lines.push(`${heading} Market sentiment`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  if (latest) {
    const risk = String(latest?.riskMode ?? '');
    if (risk) lines.push(`- risk: ${risk}`);
    if (risk === 'confirmed_uptrend') {
      lines.push('- ftd: triggered (右侧确立，死锁解除)');
    }
    const rulesArr: string[] = Array.isArray(latest?.rules)
      ? latest.rules.map((x: any) => String(x)).filter(Boolean)
      : [];
    if (
      risk === 'extreme_caution' ||
      rulesArr.some((r) => r.includes('macro_override_lock') || r.includes('breadth_panic'))
    ) {
      lines.push('- macroLock: active');
    }
    lines.push(`- ${formatSrvIndexLine(ms?.srvIndex)}`);
    const up = Number(latest?.upCount ?? 0);
    const down = Number(latest?.downCount ?? 0);
    if (up > 0 || down > 0) {
      lines.push(
        `- Market Breadth: ${up.toLocaleString()} Up / ${down.toLocaleString()} Down`,
      );
    }
    const total = fmtAmountCn(latest?.marketTurnoverCny);
    if (total && total !== '—') lines.push(`- totalTurnover: ${total}`);
    const rules = rulesArr;
    if (rules.length)
      lines.push(`- rules: ${rules.slice(0, 6).join(' • ')}${rules.length > 6 ? '…' : ''}`);
  }
  lines.push('');

  if (indexSignals.length) {
    // 2026-08-01: Index traffic lights moved into the unified Market & Macro
    // overview table (see buildMarketAndMacroMarkdown). Skip duplicate here.
  }

  const last5 = (items || []).slice(-5);
  const headers = ['date', 'up', 'down', 'flat', 'ratio', 'turnover', 'premium%', 'failed%', 'risk'];
  const rows: unknown[][] = last5.map((it: any) => [
    String(it?.date ?? ''),
    Number(it?.upCount ?? 0).toLocaleString(),
    Number(it?.downCount ?? 0).toLocaleString(),
    Number(it?.flatCount ?? 0).toLocaleString(),
    Number.isFinite(it?.upDownRatio) ? Number(it.upDownRatio).toFixed(2) : '—',
    fmtAmountCn(it?.marketTurnoverCny),
    Number.isFinite(it?.yesterdayLimitUpPremium)
      ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
      : '—',
    Number.isFinite(it?.failedLimitUpRate) ? `${Number(it.failedLimitUpRate).toFixed(1)}%` : '—',
    String(it?.riskMode ?? ''),
  ]);
  lines.push(mdTable(headers, rows));
  lines.push('');

  const etfFlow: any = ms?.etfFundFlow ?? {};
  const etfItems: any[] = Array.isArray(etfFlow?.items) ? etfFlow.items : [];
  if (etfItems.length) {
    if (etfFlow?.shareLag) {
      lines.push(
        `- ETF realtime flow incomplete; missing rows are excluded from intraday signals (intradaySafe: ${String(etfFlow?.intradaySafe ?? false)})`,
      );
    }
    const etfHeaders = [
      'ETF Name',
      'Symbol',
      'Main Flow',
      'Super Large',
      'Large',
      '3D Net Flow',
      'Realtime AsOf',
      'Source',
      'Status',
      'Signal',
    ];
    const etfRows: unknown[][] = etfItems.map((it: any) => {
      const status = String(it?.flowStatus ?? (it?.live === true ? 'Live' : '—'));
      const live = it?.live === true || status === 'Live';
      const isMarketClosed = status === 'MarketClosed';
      const flow1dStale =
        !live &&
        !isMarketClosed &&
        it?.netFlow1d == null &&
        (it?.flowAsOfDate != null || it?.netFlow1dLagged != null);
      const flow1d = flow1dStale ? '— (stale)' : fmtSignedAmountCn(it?.netFlow1d);
      return [
        String(it?.name ?? ''),
        String(it?.symbol ?? ''),
        flow1d,
        fmtSignedAmountCn(it?.superLargeNetInflow),
        fmtSignedAmountCn(it?.largeNetInflow),
        fmtSignedAmountCn(it?.netFlow3d),
        String(it?.tradeTime ?? it?.flowAsOfDate ?? etfFlow?.asOfDate ?? '—'),
        String(it?.source ?? '—'),
        isMarketClosed ? 'Market Closed' : status,
        String(it?.signalDisplay ?? it?.signal ?? '—'),
      ];
    });
    lines.push(`${heading} ETF Fund Flow (Top Watchlist)`);
    lines.push('');
    lines.push(mdTable(etfHeaders, etfRows));
    lines.push('');
  }

  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];
  void macroItems;
  // 2026-08-01: 300ETF Put IV moved into the unified Market & Macro overview table
  // (see buildMarketAndMacroMarkdown). Skip duplicate here.

  return lines.join('\n').trim() + '\n';
}

export function buildMacroMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const macroSnapshot: any = summary2?.macroSnapshot ?? {};
  const macroItems: any[] = Array.isArray(macroSnapshot?.macro) ? macroSnapshot.macro : [];

  if (!macroItems.length) return '';

  const lines: string[] = [];
  lines.push(`${heading} Macro indices`);
  lines.push('');

  const headers = ['Name', 'Close', 'Chg%', 'Signal', 'MA5', 'MA20', 'AsOfDate', 'Source'];
  const rows: unknown[][] = macroItems.map((it: any) => {
    const pct = it?.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    const isVol = it?.category === 'volatility';
    const closeStr = Number.isFinite(it?.close)
      ? isVol
        ? `${Number(it.close).toFixed(1)}%`
        : Number(it.close).toFixed(2)
      : '—';
    const signalStr = it?.signalLabel
      ? it.signalLabel
      : it?.signal
        ? String(it.signal)
        : '—';
    return [
      String(it?.name ?? it?.seriesId ?? ''),
      closeStr,
      chg,
      signalStr,
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      String(it?.asOfDate ?? ''),
      String(it?.source ?? ''),
    ];
  });

  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

export async function buildScreenersMarkdown(
  s: DashboardSummary | null,
  heading = '##',
  queryClient?: QueryClient,
  options?: { mode?: 'full' | 'compact' },
): Promise<string> {
  const summary2: any = s ?? {};
  const rows: any[] = Array.isArray(summary2?.screeners) ? summary2.screeners : [];
  const compact = options?.mode === 'compact';
  const lines: string[] = [];
  lines.push(`${heading} Screener sync`);
  lines.push('');
  lines.push(
    `- note: per screener Top ${SCREENER_COPY_TOP_N} by Score (>= ${SCREENER_COPY_MIN_SCORE}); lower scores trimmed`,
  );
  const headers = ['Name', 'capturedAt', 'rows', 'filters', 'kept'];
  const rows2: unknown[][] = rows.map((r: any) => [
    String(r?.name ?? r?.id ?? ''),
    String(r?.capturedAt ?? ''),
    String(r?.rowCount ?? 0),
    String(r?.filtersCount ?? 0),
    `${String(r?.rowCount ?? 0)} raw`,
  ]);
  lines.push(mdTable(headers, rows2));
  lines.push('');

  if (compact) {
    // Compact: skip per-screener row tables (only keep header summary).
    lines.push('- note: compact mode — per-screener row tables omitted (open Screener page for details)');
    lines.push('');
    return lines.join('\n').trim() + '\n';
  }

  const screenerIds = rows
    .map((sc: any) => String(sc?.id ?? '').trim())
    .filter((sid: string) => sid);

  let resolvedScreenerResults: Array<
    | { sid: string; error: string }
    | {
        sid: string;
        snap: {
          id: string;
          screenerId: string;
          capturedAt: string;
          rowCount: number;
          screenTitle: string | null;
          filters: string[];
          url: string;
          headers: string[];
          rows: Array<Record<string, string>>;
        };
        sc: any;
      }
  >;

  if (queryClient && screenerIds.length) {
    const snapMap = await queryClient.fetchQuery(screenerSnapshotsQueryOptions(screenerIds));
    resolvedScreenerResults = screenerIds.map((sid) => {
      const snap = snapMap[sid];
      if (!snap) return { sid, error: 'No snapshot found' };
      return { sid, snap, sc: rows.find((r: any) => String(r?.id ?? '').trim() === sid) };
    });
  } else {
    resolvedScreenerResults = await Promise.all(
      screenerIds.map(async (sid) => {
        try {
          const list = await apiGetJson<{
            items: Array<{ id: string; capturedAt?: string; rowCount?: number }>;
          }>(`/integrations/tradingview/screeners/${encodeURIComponent(sid)}/snapshots?limit=1`);
          const snapId = String(list?.items?.[0]?.id ?? '').trim();
          if (!snapId) return { sid, error: 'No snapshot found' };
          const snap = await apiGetJson<{
            id: string;
            screenerId: string;
            capturedAt: string;
            rowCount: number;
            screenTitle: string | null;
            filters: string[];
            url: string;
            headers: string[];
            rows: Array<Record<string, string>>;
          }>(`/integrations/tradingview/snapshots/${encodeURIComponent(snapId)}`);
          return { sid, snap, sc: rows.find((r: any) => String(r?.id ?? '').trim() === sid) };
        } catch (e) {
          return { sid, error: e instanceof Error ? e.message : String(e) };
        }
      }),
    );
  }

  for (const result of resolvedScreenerResults) {
    if ('error' in result) {
      const sc = rows.find((r: any) => String(r?.id ?? '').trim() === result.sid);
      lines.push(`${heading}# ${escapeMarkdownCell(String(sc?.name ?? result.sid))}`);
      lines.push(`- error: ${escapeMarkdownCell(result.error)}`);
      lines.push('');
      continue;
    }
    const { sid, snap, sc } = result;
    const title = String(snap?.screenTitle ?? sc?.name ?? sid).trim() || sid;
    const capturedAt = String(snap?.capturedAt ?? '').trim();
    const headersTv: string[] = Array.isArray(snap?.headers)
      ? snap.headers.map((h) => String(h ?? ''))
      : [];
    const rowsTv: Array<Record<string, string>> = Array.isArray(snap?.rows) ? snap.rows : [];
    const rawRowCount = rowsTv.length;
    const limit = 200;
    const truncated = rowsTv.length > limit;
    const rowsSlice = rowsTv.slice(0, limit);

    lines.push(`${heading}# ${escapeMarkdownCell(title)}`);
    if (capturedAt) lines.push(`- capturedAt: ${capturedAt}`);
    lines.push(`- rows: ${String(snap?.rowCount ?? rowsTv.length ?? 0)}`);
    if (Array.isArray(snap?.filters) && snap.filters.length) {
      lines.push(
        `- filters: ${snap.filters
          .slice(0, 8)
          .map((x) => escapeMarkdownCell(String(x)))
          .join(' • ')}${snap.filters.length > 8 ? '…' : ''}`,
      );
    }
    if (truncated) lines.push(`- note: scanning first ${limit} raw rows (truncated)`);
    lines.push(
      '- scoreSource: TrendOK (same as Watchlist); Score>90 = candidate for forced research',
    );

    if (headersTv.length && rowsSlice.length) {
      const symbols = extractSymbolsFromSnapshotRows(rowsSlice, headersTv);
      const trendMap = await fetchTrendOkMap(symbols, {
        realtime: isShanghaiTradingTime(),
      });
      const enrichedAll = buildScreenerMarkdownRows(rowsSlice, headersTv, trendMap);
      const eligible = enrichedAll.filter(
        (r) => typeof r.score === 'number' && Number.isFinite(r.score) && r.score >= SCREENER_COPY_MIN_SCORE,
      );
      const topN = eligible.slice(0, SCREENER_COPY_TOP_N);
      const trimmed = enrichedAll.length - topN.length;
      if (trimmed > 0) {
        lines.push(
          `- kept: ${topN.length} of ${enrichedAll.length} (Score>=${SCREENER_COPY_MIN_SCORE} & Top ${SCREENER_COPY_TOP_N})`,
        );
      } else {
        lines.push(`- kept: ${topN.length} of ${enrichedAll.length}`);
      }
      const missingScore = countMissingScores(enrichedAll);
      if (missingScore > 0) lines.push(`- missingScore: ${missingScore}`);
      lines.push('');
      if (topN.length) {
        lines.push(
          mdTable([...SCREENER_MARKDOWN_HEADERS], screenerMarkdownRowsToTable(topN)),
        );
      } else {
        lines.push(`_No rows match Score>=${SCREENER_COPY_MIN_SCORE}._`);
      }
    } else {
      lines.push('');
      lines.push('_No rows._');
    }
    lines.push('');
    void rawRowCount;
  }

  return lines.join('\n').trim() + '\n';
}

export async function buildWatchlistMarkdown(
  queryClient?: QueryClient,
  gate?: ExecutionGate | null,
  mainlineAllow?: MainlineAllowSet | null,
  sectorOutflowBlock = false,
): Promise<string> {
  const itemsRaw = loadWatchlist();
  const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

  const heading = '##';
  if (!items.length) {
    return `${heading} Combat Positions & Watchlist (Unified)\n\n- No watchlist items.\n`;
  }

  const syms = items.map((x) => x.symbol);
  const tradingTime = isShanghaiTradingTime();
  const quoteWindow = isShanghaiSyncWindow();
  const todaySh = getShanghaiTodayIso();

  let trend: Record<string, TrendOkResult>;
  let quotes: Record<
    string,
    {
      price: number | null;
      tradeTime: string | null;
      amount: number | null;
      volume: number | null;
      preClose: number | null;
      pctChg: number | null;
    }
  >;

  if (queryClient) {
    const snapshot = await fetchWatchlistSnapshotForCopy(syms, queryClient);
    trend = snapshot.trend;
    quotes = snapshot.quotes;
  } else {
    const byTsCode = new Map<string, string>();
    const tsCodes = syms
      .map((s) => {
        const t = toTsCodeFromSymbol(s);
        if (t) byTsCode.set(t, s);
        return t;
      })
      .filter(Boolean) as string[];
    const tsCodesChunks = chunk(tsCodes, 50);

    const [trendMap, quoteResults] = await Promise.all([
      fetchTrendOkMap(syms, { realtime: quoteWindow }),
      Promise.all(
        tsCodesChunks.map(async (part) => {
          return apiGetJson<QuoteResp>(
            `/quote?ts_codes=${encodeURIComponent(part.join(','))}`,
          ).catch(() => null);
        }),
      ),
    ]);

    trend = Object.fromEntries(trendMap);
    quotes = {};
    for (const r of quoteResults) {
      for (const it of r?.items ?? []) {
        const sym = byTsCode.get(it.ts_code);
        if (!sym) continue;
        quotes[sym] = parseDashboardQuoteItem(it);
      }
    }
  }

  const sorted = [...items];
  sorted.sort((a, b) => {
    const sa = trend[a.symbol]?.score;
    const sb = trend[b.symbol]?.score;
    const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
    const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return vb - va;
  });

  const missingRealtime: string[] = [];
  const missingTrend: string[] = [];
  const missingHistory: string[] = [];
  for (const it of sorted) {
    const sym = it.symbol;
    const t = trend[sym];
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
      const q = quotes[sym];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
        missingRealtime.push(sym);
      }
    }
  }
  if (missingTrend.length || missingHistory.length || missingRealtime.length) {
    const parts: string[] = [];
    if (missingRealtime.length)
      parts.push(
        `missing realtime quote (today): ${missingRealtime.slice(0, 6).join(', ')}${missingRealtime.length > 6 ? '…' : ''}`,
      );
    if (missingHistory.length)
      parts.push(
        `missing history/indicators: ${missingHistory.slice(0, 6).join(', ')}${missingHistory.length > 6 ? '…' : ''}`,
      );
    if (missingTrend.length)
      parts.push(
        `missing TrendOK result: ${missingTrend.slice(0, 6).join(', ')}${missingTrend.length > 6 ? '…' : ''}`,
      );
    throw new Error(`Copy aborted: ${parts.join(' | ')}`);
  }

  // Unified combat table only — fat Watchlist dump removed for LLM SNR.
  const catalystBySymbol = await fetchCatalystStocks(
    DATA_SYNC_BASE_URL,
    50,
    DEFAULT_CATALYST_MAX_AGE_DAYS,
  )
    .then((resp) => buildCatalystPurgeMap(resp))
    .catch(() => null);
  const { markdown, purgeSymbols } = buildPositionsExecutionMarkdown(
    sorted,
    trend,
    quotes,
    gate ?? null,
    heading,
    mainlineAllow ?? null,
    tradingTime,
    todaySh,
    sectorOutflowBlock,
    catalystBySymbol,
  );
  // Report still lists PURGE rows; remove them from storage for the next copy.
  if (purgeSymbols.length) {
    await applyWatchlistPurgeAfterReport(purgeSymbols).catch(() => 0);
  }
  return markdown.trim() + '\n';
}

export async function buildAlphaRadarCopyContext(
  s: DashboardSummary,
  catalystResp: CatalystStocksResponse,
): Promise<CatalystCopyContext> {
  const watchlistSymbols = loadWatchlistSymbols();
  const screeners: any[] = Array.isArray((s as any)?.screeners) ? (s as any).screeners : [];
  const todayScreenerSymbols = await fetchTodayScreenerSymbolsByTitle(screeners, {
    apiGetJson,
  });

  const catalystSymbols = catalystResp.items.map((row) => normalizeCatalystSymbol(row.symbol));
  const allSymbols = [
    ...new Set<string>([...watchlistSymbols, ...todayScreenerSymbols, ...catalystSymbols]),
  ];

  const trendMapRaw = await fetchTrendOkMap(allSymbols, {
    realtime: isShanghaiTradingTime(),
  });

  const trendMap: CatalystCopyContext['trendMap'] = new Map();
  for (const [sym, trend] of trendMapRaw) {
    trendMap.set(sym, {
      symbol: sym,
      trendOk: trend.trendOk,
      score: trend.score ?? null,
    });
  }

  const watchlistScores = new Map<string, number>();
  for (const sym of watchlistSymbols) {
    const score = trendMap.get(sym)?.score;
    if (typeof score === 'number' && Number.isFinite(score)) {
      watchlistScores.set(sym, score);
    }
  }

  const screenerTrendOkSymbols = new Set<string>();
  for (const sym of todayScreenerSymbols) {
    if (trendMap.get(sym)?.trendOk === true) screenerTrendOkSymbols.add(sym);
  }

  return {
    watchlistSymbols,
    watchlistScores,
    screenerTrendOkSymbols,
    trendMap,
  };
}

export async function buildCompactCatalystMarkdown(s: DashboardSummary): Promise<string> {
  try {
    const resp = await fetchCatalystStocks(DATA_SYNC_BASE_URL, 10, DEFAULT_CATALYST_MAX_AGE_DAYS);
    const ctx = await buildAlphaRadarCopyContext(s, resp);
    return buildCatalystStocksMarkdown(resp, {
      headingLevel: '##',
      mode: 'compact',
      context: ctx,
    });
  } catch {
    return '## Alpha Radar · Top Catalyst Stocks\n\n- Alpha Radar: unavailable\n';
  }
}

export type DashboardCopyAllOptions = {
  summary: DashboardSummary | null;
  newsSummary?: string | null;
  newsSummaryUpdatedAt?: string | null;
  newsFallback?: string | null;
  queryClient?: QueryClient;
  /** Copy mode (2026-08-01): 'compact' trims ~80% of secondary sections for fast scan. */
  mode?: 'full' | 'compact';
};

type ExecutionCopyBundle = {
  attentionMd: string;
  cards: CondOrderCard[];
  quotes: Record<string, CondOrderQuoteHint>;
};

async function buildExecutionCopyBundle(opts: {
  gate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  sectorOutflowBlock?: boolean;
  queryClient?: QueryClient;
}): Promise<ExecutionCopyBundle> {
  const { gate, mainlineAllow, sectorOutflowBlock = false, queryClient } = opts;
  const itemsRaw = loadWatchlist();
  const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

  let liveCards: CondOrderCard[] | null = null;
  let quotes: Record<string, CondOrderQuoteHint> = {};
  if (gate && items.length) {
    try {
      const symbols = items.map((i) => i.symbol);
      const market = await fetchWatchlistSnapshotForCopy(symbols, queryClient);
      const nameBySym = new Map(items.map((i) => [i.symbol, i.name ?? null]));
      quotes = {};
      for (const sym of symbols) {
        const q = market.quotes[sym];
        quotes[sym] = {
          preClose: q?.preClose ?? null,
          name: nameBySym.get(sym) ?? null,
        };
      }
      const catalystBySymbol = await fetchCatalystStocks(
        DATA_SYNC_BASE_URL,
        50,
        DEFAULT_CATALYST_MAX_AGE_DAYS,
      )
        .then((resp) => buildCatalystPurgeMap(resp))
        .catch(() => null);
      const payload = buildExecutionSnapshotPayload({
        items,
        trend: market.trend,
        quotes: market.quotes,
        gate,
        mainlineAllow,
        sectorOutflowBlock,
        catalystBySymbol,
        source: 'poll',
      });
      liveCards = (payload?.cards as CondOrderCard[] | undefined) ?? null;
    } catch {
      liveCards = null;
      quotes = {};
    }
  } else if (gate && !items.length) {
    liveCards = [];
  }

  let snapshotCards: CondOrderCard[] = [];
  try {
    const td = getShanghaiTodayIso();
    const snaps = await apiGetJson<ExecutionSnapshotListResponse>(
      `/execution/snapshots?trade_date=${encodeURIComponent(td)}&limit=1`,
    );
    const latest = snaps?.items?.[0];
    if (latest && Array.isArray(latest.cards)) {
      snapshotCards = latest.cards as CondOrderCard[];
    }
  } catch {
    // ignore
  }

  let changes: ExecutionChangeListResponse['items'] = [];
  try {
    const td = getShanghaiTodayIso();
    const ch = await apiGetJson<ExecutionChangeListResponse>(
      `/execution/changes?trade_date=${encodeURIComponent(td)}&limit=100`,
    );
    changes = Array.isArray(ch?.items) ? ch.items : [];
  } catch {
    changes = [];
  }

  const { cards, source } = resolveAttentionCards({ liveCards, snapshotCards });
  const queue = buildExecAttentionQueue({
    gate,
    watchlistItems: items,
    cards,
    changes,
  });
  return {
    attentionMd: formatExecAttentionMarkdown(queue, { heading: '##', source }),
    cards: cards as CondOrderCard[],
    quotes,
  };
}

async function buildSinceLastCopyMarkdown(): Promise<string> {
  const lastAt = readLastCopyAt();
  const td = getShanghaiTodayIso();
  const params = new URLSearchParams();
  params.set('trade_date', td);
  params.set('limit', '100');
  if (lastAt) params.set('since', lastAt);
  let changes: ExecutionChangeListResponse['items'] = [];
  try {
    const ch = await apiGetJson<ExecutionChangeListResponse>(
      `/execution/changes?${params.toString()}`,
    );
    changes = Array.isArray(ch?.items) ? ch.items : [];
  } catch {
    changes = [];
  }
  return formatSinceLastCopyMarkdown(changes, { lastAt, heading: '##' });
}

export async function buildDashboardCopyAllMarkdown(
  options: DashboardCopyAllOptions,
): Promise<string> {
  await ensureWatchlistHydrated();
  let { summary: s } = options;
  const { newsSummary, newsSummaryUpdatedAt, newsFallback, queryClient, mode = 'full' } = options;
  const compact = mode === 'compact';
  if (!s) {
    throw new Error('No data available. Please refresh first.');
  }
  const macroItems = (s as any)?.macroSnapshot?.macro;
  if (!Array.isArray(macroItems) || macroItems.length === 0) {
    try {
      const macroPartial = await fetchDashboardSummaryPartial({
        includeMacro: true,
        includeSentiment: false,
        includeNews: false,
        includeIndustry: false,
        includeScreeners: false,
      });
      if (macroPartial?.macroSnapshot) {
        s = { ...s, macroSnapshot: macroPartial.macroSnapshot };
      }
    } catch {
      // keep existing summary
    }
  }
  const generatedAt = new Date().toLocaleString('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hour12: false,
  });
  const executionGate = parseExecutionGate((s as any)?.marketSentiment?.executionGate);
  const mainlineAllow = buildMainlineAllowSet(s);
  const sectorOutflowBlock = isSectorOutflowBlock(s);
  const tradingTime = isShanghaiTradingTime();
  const [screenersMd, watchlistMd, catalystMd, alphaTrendsMd, execBundle, sinceLastMd] =
    await Promise.all([
      buildScreenersMarkdown(s, '##', queryClient, { mode }),
      buildWatchlistMarkdown(queryClient, executionGate, mainlineAllow, sectorOutflowBlock),
      buildCompactCatalystMarkdown(s),
      fetchAlphaRadarTrendsForCopy(DATA_SYNC_BASE_URL, 20, DEFAULT_CATALYST_MAX_AGE_DAYS)
        .then(({ items, scope }) =>
          buildAlphaRadarTrendsMarkdown(items, {
            headingLevel: '##',
            mode: compact ? 'compact' : 'compact',
            scopeNote:
              scope === 'recent'
                ? `recent ${DEFAULT_CATALYST_MAX_AGE_DAYS}d (latest batch empty)`
                : 'latest batch',
          }),
        )
        .catch(() => '## Alpha Radar · Structured Trends\n\n- Alpha Radar trends: unavailable\n'),
      buildExecutionCopyBundle({
        gate: executionGate,
        mainlineAllow,
        sectorOutflowBlock,
        queryClient,
      }).catch(
        (): ExecutionCopyBundle => ({
          attentionMd: formatExecAttentionMarkdown(
            buildExecAttentionQueue({
              gate: executionGate,
              watchlistItems: [],
              cards: [],
              changes: [],
            }),
            { source: 'none' },
          ),
          cards: [],
          quotes: {},
        }),
      ),
      buildSinceLastCopyMarkdown().catch(() =>
        formatSinceLastCopyMarkdown([], { lastAt: readLastCopyAt() }),
      ),
    ]);
  const attentionMd = execBundle.attentionMd;
  const condOrderMd = formatCondOrderDraftMarkdown(execBundle.cards, {
    heading: '##',
    allowNewEntries: executionGate?.allowNewEntries === true,
    tradingTime,
    phase: tradingTime ? 'Open' : 'Closed',
    quotes: execBundle.quotes,
  });
  const lines: string[] = [];
  lines.push(`# Copy all (Dashboard)`);
  lines.push(`- generatedAt: ${generatedAt}`);
  lines.push(`- asOfDate: ${String((s as any)?.asOfDate ?? '')}`);
  lines.push('');
  // AI behavior lives in System Prompt (docs/modules/downstream-ai-prompt.md); payload stays data-only.
  lines.push(sinceLastMd.trim());
  lines.push('');
  lines.push(
    formatExecutionGateMarkdown(
      parseExecutionGate((s as any)?.marketSentiment?.executionGate) ??
        (s as any)?.marketSentiment?.executionGate,
      '##',
    ).trim(),
  );
  lines.push('');
  lines.push(attentionMd.trim());
  lines.push('');
  lines.push(condOrderMd.trim());
  lines.push('');
  try {
    const journalMd = await fetchExecutionJournalMarkdown({
      tradeDate: getShanghaiTodayIso(),
      days: 5,
    });
    if (journalMd.trim()) {
      lines.push(journalMd.trim());
      lines.push('');
    }
  } catch {
    lines.push('## Decision Journal');
    lines.push('- note: unavailable (capture snapshots via Dashboard Sync All or Snapshot now)');
    lines.push('');
  }
  if (!compact) {
    lines.push(buildIndustryMarkdown(s, '##').trim());
    lines.push('');
    lines.push(buildHotIndustriesMarkdown(s, '##').trim());
    lines.push('');
  } else {
    // Compact mode: keep only the headline industry fund flow summary (no Top5×Date grid).
    lines.push(
      buildIndustryMarkdown(s, '##')
        .split('\n')
        .slice(0, 6)
        .concat(['- note: compact mode — full industry tables omitted', ''])
        .join('\n'),
    );
    lines.push('');
    lines.push('- note: compact mode — Hot industries workflow omitted');
    lines.push('');
  }
  lines.push(buildSentimentMarkdown(s, '##').trim());
  lines.push('');
  lines.push(buildMarketAndMacroMarkdown(s, '##').trim());
  lines.push('');
  lines.push('## News brief');
  lines.push('');
  lines.push(`- hours: ${String((s as any)?.news?.hours ?? 24)}`);
  lines.push(`- total: ${String((s as any)?.news?.total ?? 0)}`);
  lines.push(
    '- note: 关键词白名单过滤（AI/算力/半导体/美联储/降准降息/原油/关税/…）；其他娱乐/边缘政治新闻剔除',
  );
  if (newsSummaryUpdatedAt) lines.push(`- summaryUpdatedAt: ${newsSummaryUpdatedAt}`);
  lines.push('');
  if (newsSummary?.trim()) lines.push(newsSummary.trim());
  else if (newsFallback?.trim()) lines.push(newsFallback.trim());
  else lines.push('No summary yet. Last news records are included above.');
  lines.push('');
  lines.push(screenersMd.trim());
  lines.push('');
  lines.push(alphaTrendsMd.trim());
  lines.push('');
  lines.push(catalystMd.trim());
  lines.push('');
  lines.push(watchlistMd.trim());
  lines.push('');
  return lines.join('\n').trim() + '\n';
}
