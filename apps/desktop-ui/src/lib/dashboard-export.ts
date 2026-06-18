/* eslint-disable @typescript-eslint/no-explicit-any */
import type { QueryClient } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import {
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
  fmtAmountCn,
  mdTable,
  escapeMarkdownCell,
  mdLines,
  mdNum,
  mdScore,
  mdPrice,
  buildHotIndustriesMarkdown,
} from '@/lib/dashboard-format';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import {
  getShanghaiTodayIso,
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
import { watchlistMarketQueryOptions } from '@/lib/queries/watchlist';
import { trendOkSummary, trendOkRuleLines, scoreRuleLines } from '@/lib/trendok-display';
import {
  WATCHLIST_MD_HEADERS,
  buildWatchlistRowMetrics,
  computePnLPct,
  formatGapUp,
  formatHotTop3,
  formatIntradayChgPct,
  formatPnLPct,
  formatRiskAlerts,
  formatVwap,
  industryDisplayName,
  isIntradaySurge,
  parseQuoteNumber,
  shouldRequireRealtimeQuote,
  tradeDateFromTradeTime,
} from '@/lib/watchlist-metrics';
import { loadWatchlist, ensureWatchlistHydrated, type WatchlistItem } from '@/lib/watchlist-storage';

type DashboardSummary = any;

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

export function buildIndustryMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const summary2: any = s ?? {};
  const ind: any = summary2?.industryFundFlow ?? {};
  const asOfDate = String(ind?.asOfDate ?? summary2?.asOfDate ?? '').trim();

  const datesAll: string[] = Array.isArray(ind?.dates) ? ind.dates : [];
  const rawShownDates = datesAll.slice(-5);
  const topByDateArr: any[] = Array.isArray(ind?.topByDate) ? ind.topByDate : [];
  const byDate: Record<string, string[]> = {};
  for (const it of topByDateArr) {
    const d = String(it?.date ?? '');
    const top = Array.isArray(it?.top) ? it.top.map((x: any) => String(x ?? '')) : [];
    if (d) byDate[d] = top;
  }
  const dedupedDates: string[] = [];
  let prevSig = '';
  for (const d of rawShownDates) {
    const sig = (byDate[d] || []).slice(0, 5).join('|');
    if (sig && sig === prevSig) continue;
    dedupedDates.push(d);
    prevSig = sig;
  }

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
    const dates: string[] = Array.isArray(block?.dates) ? block.dates : [];
    const cols: string[] = dates.length ? dates.slice(-5) : dedupedDates;
    const topRows: any[] = Array.isArray(block?.top) ? block.top : [];
    if (!topRows.length || !cols.length) return;
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
    const up = Number(latest?.upCount ?? 0);
    const down = Number(latest?.downCount ?? 0);
    if (up > 0 || down > 0) {
      lines.push(
        `- Market Breadth: ${up.toLocaleString()} Up / ${down.toLocaleString()} Down`,
      );
    }
    const total = fmtAmountCn(latest?.marketTurnoverCny);
    if (total && total !== '—') lines.push(`- totalTurnover: ${total}`);
    const rules = Array.isArray(latest?.rules)
      ? latest.rules.map((x: any) => String(x)).filter(Boolean)
      : [];
    if (rules.length)
      lines.push(`- rules: ${rules.slice(0, 6).join(' • ')}${rules.length > 6 ? '…' : ''}`);
  }
  lines.push('');

  if (indexSignals.length) {
    const headers0 = ['Index', 'Signal', 'Position', 'chg%', 'Close', 'MA5', 'MA20', 'AsOfDate'];
    const rows0: unknown[][] = indexSignals.map((it: any) => {
      const pc = it?.pctChg;
      const chg =
        typeof pc === 'number' && Number.isFinite(pc)
          ? `${pc >= 0 ? '+' : ''}${pc.toFixed(2)}%`
          : '—';
      return [
        String(it?.name ?? it?.tsCode ?? ''),
        String(it?.signal ?? ''),
        String(it?.positionRange ?? ''),
        chg,
        Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—',
        Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
        Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
        String(it?.asOfDate ?? ''),
      ];
    });
    lines.push(`${heading}# Index traffic lights`);
    lines.push('');
    lines.push(mdTable(headers0, rows0));
    lines.push('');
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

  const headers = ['Name', 'Close', 'Chg%', 'MA5', 'MA20', 'AsOfDate', 'Source'];
  const rows: unknown[][] = macroItems.map((it: any) => {
    const pct = it?.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    return [
      String(it?.name ?? it?.seriesId ?? ''),
      Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—',
      chg,
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
): Promise<string> {
  const summary2: any = s ?? {};
  const rows: any[] = Array.isArray(summary2?.screeners) ? summary2.screeners : [];
  const lines: string[] = [];
  lines.push(`${heading} Screener sync`);
  lines.push('');
  const headers = ['Name', 'capturedAt', 'rows', 'filters'];
  const rows2: unknown[][] = rows.map((r: any) => [
    String(r?.name ?? r?.id ?? ''),
    String(r?.capturedAt ?? ''),
    String(r?.rowCount ?? 0),
    String(r?.filtersCount ?? 0),
  ]);
  lines.push(mdTable(headers, rows2));
  lines.push('');

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
    const limit = 50;
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
    if (truncated) lines.push(`- note: showing first ${limit} rows (truncated)`);
    lines.push(
      '- scoreSource: TrendOK (same as Watchlist); Score>90 = candidate for forced research',
    );

    if (headersTv.length && rowsSlice.length) {
      const symbols = extractSymbolsFromSnapshotRows(rowsSlice, headersTv);
      const trendMap = await fetchTrendOkMap(symbols, {
        realtime: isShanghaiTradingTime(),
      });
      const enrichedRows = buildScreenerMarkdownRows(rowsSlice, headersTv, trendMap);
      const missingScore = countMissingScores(enrichedRows);
      if (missingScore > 0) lines.push(`- missingScore: ${missingScore}`);
      lines.push('');
      lines.push(
        mdTable([...SCREENER_MARKDOWN_HEADERS], screenerMarkdownRowsToTable(enrichedRows)),
      );
    } else {
      lines.push('');
      lines.push('_No rows._');
    }
    lines.push('');
  }

  return lines.join('\n').trim() + '\n';
}

export async function buildWatchlistMarkdown(queryClient?: QueryClient): Promise<string> {
  const itemsRaw = loadWatchlist();
  const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

  const heading = '##';
  if (!items.length) return `${heading} Watchlist\n\nNo items.\n`;

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
    const snapshot = await queryClient.fetchQuery(watchlistMarketQueryOptions(syms));
    trend = snapshot.trend;
    quotes = snapshot.quotes;
  } else {
    const symsChunks = chunk(syms, 200);

    const byTsCode = new Map<string, string>();
    const tsCodes = syms
      .map((s) => {
        const t = toTsCodeFromSymbol(s);
        if (t) byTsCode.set(t, s);
        return t;
      })
      .filter(Boolean) as string[];
    const tsCodesChunks = chunk(tsCodes, 50);

    const [trendResults, quoteResults] = await Promise.all([
      Promise.all(
        symsChunks.map(async (part) => {
          const sp = new URLSearchParams();
          sp.set('realtime', quoteWindow ? 'true' : 'false');
          for (const s of part) sp.append('symbols', s);
          return apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
        }),
      ),
      Promise.all(
        tsCodesChunks.map(async (part) => {
          return apiGetJson<QuoteResp>(
            `/quote?ts_codes=${encodeURIComponent(part.join(','))}`,
          ).catch(() => null);
        }),
      ),
    ]);

    trend = {};
    for (const trendRows of trendResults) {
      for (const r of Array.isArray(trendRows) ? trendRows : []) {
        if (r && r.symbol) trend[String(r.symbol).toUpperCase()] = r;
      }
    }

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
    const md = Array.isArray(t.missingData) ? t.missingData.filter(Boolean) : [];
    if (md.length) missingHistory.push(sym);
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

  const generatedAt = new Date().toISOString();
  const lines: string[] = [];
  lines.push(`${heading} Watchlist`);
  lines.push(`- generatedAt: ${generatedAt}`);
  lines.push(`- items: ${sorted.length}`);
  lines.push(`- shanghaiToday: ${todaySh}`);
  lines.push(`- tradingTime: ${tradingTime ? 'true' : 'false'}`);
  lines.push(`- quoteWindow: ${quoteWindow ? 'true' : 'false'}`);
  lines.push('');

  lines.push(`${heading}# TrendOK rules`);
  lines.push(mdLines(trendOkRuleLines()));
  lines.push('');
  lines.push(`${heading}# Score rules`);
  lines.push(mdLines(scoreRuleLines()));
  lines.push('');

  const headers = [...WATCHLIST_MD_HEADERS];
  const rows: unknown[][] = [];
  const blockAlerts: string[] = [];
  for (const it of sorted) {
    const t = trend[it.symbol];
    const q = quotes[it.symbol];
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
    const buy =
      t?.buyAction && t?.buyMode
        ? `${String(t.buyMode)}/${String(t.buyAction)}`
        : t?.buyAction
          ? String(t.buyAction)
          : '—';
    const values = (t?.values ?? {}) as Record<string, unknown>;
    const intradayCell = isIntradaySurge(rowMetrics.intradayChgPct)
      ? `⚠️ ${formatIntradayChgPct(rowMetrics.intradayChgPct)}`
      : formatIntradayChgPct(rowMetrics.intradayChgPct);
    const gapCell =
      rowMetrics.gapUp === true
        ? `⚠️ ${formatGapUp(true)}`
        : formatGapUp(rowMetrics.gapUp);
    for (const alert of rowMetrics.alerts) {
      if (alert.severity === 'block') blockAlerts.push(`${it.symbol}: ${alert.message}`);
    }
    rows.push([
      it.symbol,
      it.name ?? t?.name ?? '—',
      industryDisplayName(values),
      formatHotTop3(t),
      mdNum(it.positionPct ?? null, 1),
      mdPrice(it.costPrice ?? null),
      mdPrice(rowMetrics.current),
      formatVwap(rowMetrics.vwap),
      intradayCell,
      gapCell,
      formatRiskAlerts(rowMetrics.alerts),
      formatPnLPct(pnl),
      mdScore(t?.score ?? null),
      trendOkSummary(t),
      buy,
      mdPrice(t?.stopLossPrice ?? null),
      asOf,
    ]);
  }
  lines.push(mdTable(headers, rows));
  lines.push('');
  if (blockAlerts.length) {
    lines.push(`${heading}# Risk alerts`);
    lines.push(mdLines(blockAlerts.map((line) => `- ${line}`)));
    lines.push('');
  }

  return lines.join('\n').trim() + '\n';
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
};

export async function buildDashboardCopyAllMarkdown(
  options: DashboardCopyAllOptions,
): Promise<string> {
  await ensureWatchlistHydrated();
  const { summary: s, newsSummary, newsSummaryUpdatedAt, newsFallback, queryClient } = options;
  if (!s) {
    throw new Error('No data available. Please refresh first.');
  }
  const generatedAt = new Date().toISOString();
  const [screenersMd, watchlistMd, catalystMd, alphaTrendsMd] = await Promise.all([
    buildScreenersMarkdown(s, '##', queryClient),
    buildWatchlistMarkdown(queryClient),
    buildCompactCatalystMarkdown(s),
    fetchAlphaRadarTrendsForCopy(DATA_SYNC_BASE_URL, 20, DEFAULT_CATALYST_MAX_AGE_DAYS)
      .then(({ items, scope }) =>
        buildAlphaRadarTrendsMarkdown(items, {
          headingLevel: '##',
          mode: 'compact',
          scopeNote:
            scope === 'recent'
              ? `recent ${DEFAULT_CATALYST_MAX_AGE_DAYS}d (latest batch empty)`
              : 'latest batch',
        }),
      )
      .catch(() => '## Alpha Radar · Structured Trends\n\n- Alpha Radar trends: unavailable\n'),
  ]);
  const lines: string[] = [];
  lines.push(`# Copy all (Dashboard)`);
  lines.push(`- generatedAt: ${generatedAt}`);
  lines.push(`- asOfDate: ${String((s as any)?.asOfDate ?? '')}`);
  lines.push('');
  lines.push(buildIndustryMarkdown(s, '##').trim());
  lines.push('');
  lines.push(buildHotIndustriesMarkdown(s, '##').trim());
  lines.push('');
  lines.push(buildSentimentMarkdown(s, '##').trim());
  lines.push('');
  lines.push(buildMacroMarkdown(s, '##').trim());
  lines.push('');
  lines.push('## News brief');
  lines.push('');
  lines.push(`- hours: ${String((s as any)?.news?.hours ?? 24)}`);
  lines.push(`- total: ${String((s as any)?.news?.total ?? 0)}`);
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
