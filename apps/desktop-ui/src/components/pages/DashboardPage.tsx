/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

import {
  HotIndustryWorkflowCard,
  type HotIndustryPick,
} from '@/components/pages/HotIndustryWorkflowCard';
import { buildDashboardHotIndustryPicks } from '@/lib/hot-industry-picks';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { DATA_SYNC_BASE_URL, AI_BASE_URL } from '@/lib/endpoints';
import {
  buildCatalystStocksMarkdown,
  buildAlphaRadarTrendsMarkdown,
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchAlphaRadarTrends,
  fetchCatalystStocks,
} from '@/lib/alpha-radar-catalyst';
import { useChatStore } from '@/lib/chat/store';
import { loadJson } from '@/lib/storage';
import {
  downloadInvestmentDailyPdf,
  parseInvestmentDailyReportResponse,
  truncateMarkdownForReport,
} from '@/lib/investmentDailyPdf';
import {
  SCREENER_MARKDOWN_HEADERS,
  buildScreenerMarkdownRows,
  countMissingScores,
  extractSymbolsFromSnapshotRows,
  fetchTrendOkMap,
  screenerMarkdownRowsToTable,
} from '@/lib/screenerExport';
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
  type WatchlistRiskAlert,
} from '@/lib/watchlist-metrics';

type DashboardSummary = any;
type DashboardSyncResp = any;

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

function loadCardOrder(): string[] | null {
  try {
    const raw = window.localStorage.getItem('karios.dashboard.cardOrder.v0');
    if (!raw) return null;
    const arr = JSON.parse(raw) as unknown;
    return Array.isArray(arr) ? arr.filter((x) => typeof x === 'string') : null;
  } catch {
    return null;
  }
}

function saveCardOrder(ids: string[]) {
  try {
    window.localStorage.setItem('karios.dashboard.cardOrder.v0', JSON.stringify(ids));
  } catch {
    // ignore
  }
}

function fmtDateTime(x: string | null | undefined) {
  if (!x) return '—';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? x : d.toLocaleString();
}

function parseNum(x: unknown): number | null {
  const s = String(x ?? '').trim();
  if (!s) return null;
  const n = Number(s.replaceAll(',', ''));
  return Number.isFinite(n) ? n : null;
}

function fmtAmountCn(x: unknown): string {
  const n = parseNum(x);
  if (n == null) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e8) return `${(n / 1e8).toFixed(2)}亿`;
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}万`;
  return `${n.toFixed(0)}`;
}

function escapeMarkdownCell(x: unknown): string {
  const s0 = String(x ?? '');
  // Keep it single-line and avoid breaking Markdown table formatting.
  const s1 = s0.replaceAll('\r\n', '\n').replaceAll('\r', '\n').replaceAll('\n', '<br/>');
  return s1.replaceAll('|', '\\|');
}

function mdRow(cells: unknown[]): string {
  return `| ${cells.map(escapeMarkdownCell).join(' | ')} |`;
}

function mdTable(headers: string[], rows: unknown[][]): string {
  const out: string[] = [];
  out.push(mdRow(headers));
  out.push(mdRow(headers.map(() => '---')));
  for (const r of rows) out.push(mdRow(r));
  return out.join('\n');
}

function mdNum(v: number | null | undefined, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

function mdScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

function mdPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

function mdLines(items: string[]): string {
  return items.filter((x) => String(x || '').trim()).join('\n');
}

const BREADTH_PANIC_DOWN_THRESHOLD = 3000;

function signalRank(x: string): number {
  if (x === 'green' || x === 'light_green' || x === 'deep_green') return 3;
  if (x === 'yellow') return 2;
  if (x === 'red') return 1;
  return 0;
}

function buildIndexTrafficSummary(indexSignals: any[]): { title: string; detail: string } {
  const items = Array.isArray(indexSignals) ? indexSignals : [];
  if (items.length < 2) {
    return {
      title: '⚠️ 当前行情：弱势 (Weak)',
      detail: '缺少完整指数信号，保持防守。',
    };
  }
  const byName = new Map(
    items.map((x) => [String(x?.name ?? x?.tsCode ?? ''), String(x?.signal ?? '')]),
  );
  const sse = byName.get('上证指数') || String(items[0]?.signal ?? '');
  const cyb = byName.get('创业板指') || String(items[1]?.signal ?? '');
  const g1 = sse === 'green' || sse === 'light_green' || sse === 'deep_green';
  const g2 = cyb === 'green' || cyb === 'light_green' || cyb === 'deep_green';

  if (g1 && g2) {
    return {
      title: '✅ 当前行情：强势 (Strong)',
      detail: '双绿确认，顺势为主，控制仓位与回撤。',
    };
  }

  if (g1 || g2) {
    const r1 = signalRank(sse);
    const r2 = signalRank(cyb);
    const bias = r1 === r2 ? '分化' : r1 > r2 ? '主强创弱' : '创强主弱';
    return {
      title: '⚠️ 当前行情：震荡/分化 (Diverging)',
      detail: `震荡分化（${bias}），严禁追高，仅限防守型回踩；买入仅用反弹买入策略单。`,
    };
  }

  return {
    title: '⚠️ 当前行情：弱势 (Weak)',
    detail: '非绿环境，防守为主，严格控制风险；买入仅用反弹买入策略单。',
  };
}

const TREND_OK_CHECKS: Array<{ key: keyof TrendOkChecks; failText: string }> = [
  { key: 'emaOrder', failText: 'EMA order broken (Close <= EMA20 or EMA20 <= EMA60)' },
  { key: 'macdPositive', failText: 'MACD <= 0' },
  { key: 'macdHistExpanding', failText: 'MACD hist <= 0' },
  { key: 'closeNear20dHigh', failText: 'Close < 0.90 * High(20)' },
  { key: 'rsiInRange', failText: 'RSI(14) out of 50..90' },
  { key: 'volumeSurge', failText: 'AvgVol(5) < 0.9 * AvgVol(30)' },
];

function trendOkSummary(t?: TrendOkResult | null): string {
  if (!t) return '—';
  if (t.trendOk === true) return '✅';
  const checks = t.checks ?? null;
  if (!checks || typeof checks !== 'object') return t.trendOk === false ? '❌' : '—';
  const failed: string[] = [];
  for (const rule of TREND_OK_CHECKS) {
    const val = (checks as TrendOkChecks)[rule.key];
    if (val === false) failed.push(rule.failText);
  }
  if (failed.length) return failed.join('; ');
  return t.trendOk === false ? '❌' : '—';
}

function trendOkRuleLines(): string[] {
  return [
    '- Close > EMA20 and EMA20 > EMA60',
    '- MACD line > 0',
    '- MACD histogram > 0',
    '- Close >= 0.90 * High(20)',
    '- RSI(14) in [50, 90]',
    '- AvgVol(5) >= 0.9 * AvgVol(30)',
  ];
}

function scoreRuleLines(): string[] {
  return [
    '- Deterministic 0–100 score (CN daily, no LLM).',
    '- Subscores: EMA trend 25%, MACD strength 15%, breakout 25%, RSI 15%, volume 20%.',
    '- Bonus: +3 when Close >= High(20).',
    '- Penalties: high ATR/close (>7%) and Close < EMA20.',
    '- Optional industry flow adjustment when available.',
  ];
}

function buildHotIndustriesMarkdown(s: DashboardSummary | null, heading = '##'): string {
  const asOfDate = String(
    (s as any)?.industryFundFlow?.asOfDate ?? (s as any)?.asOfDate ?? '',
  ).trim();
  const picks = buildDashboardHotIndustryPicks(s);
  const lines: string[] = [];
  lines.push(`${heading} Hot industries workflow`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  lines.push(
    '- Rule V4.0: prioritize "momentum breakout" (今日净流入>20亿 且 排名提升>10名); fallback to daily top ∩ strong 5D ranking.',
  );
  lines.push(
    '- Momentum breakout sectors are often the first day of a new mainline, more explosive than sectors already in 5D ranking.',
  );
  lines.push(
    '- Action: only stocks from these 3 sectors and passing technical checks should be added to Watchlist.',
  );
  lines.push('');

  const headers = ['#', 'Industry', '1D rank', '5D rank', '1D net', '5D sum', 'RankΔ', 'Signal'];
  const rows: unknown[][] = picks
    .slice(0, 3)
    .map((p, idx) => [
      idx + 1,
      p.industryName || '—',
      typeof p.dailyRank === 'number' ? `#${p.dailyRank}` : '—',
      typeof p.fiveDayRank === 'number' ? `#${p.fiveDayRank}` : '—',
      fmtAmountCn(p.netInflow ?? null),
      fmtAmountCn(p.sum5d ?? null),
      typeof p.rankChange === 'number'
        ? p.rankChange > 0
          ? `+${p.rankChange}`
          : String(p.rankChange)
        : '—',
      p.momentumSignal ? '🚀 MOMENTUM' : '—',
    ]);
  if (!rows.length) rows.push([1, '—', '—', '—', '—', '—', '—', '—']);
  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}

const WATCHLIST_STORAGE_KEY = 'karios.watchlist.v1';
const NEWS_BRIEF_CACHE_KEY = 'karios.dashboard.newsBrief.v1';
const DASHBOARD_SUMMARY_CACHE_KEY = 'karios.dashboard.summary.v1';
const NEWS_BRIEF_MIN_REFRESH_MS = 4 * 60 * 60 * 1000;

type DashboardSummaryCache = {
  summary?: DashboardSummary;
  cachedAt?: string;
};

function loadDashboardSummaryCache(): DashboardSummary | null {
  try {
    const raw = window.localStorage.getItem(DASHBOARD_SUMMARY_CACHE_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw) as DashboardSummaryCache;
    const summary = obj?.summary;
    return summary && typeof summary === 'object' ? summary : null;
  } catch {
    return null;
  }
}

function saveDashboardSummaryCache(summary: DashboardSummary) {
  try {
    const payload: DashboardSummaryCache = {
      summary,
      cachedAt: new Date().toISOString(),
    };
    window.localStorage.setItem(DASHBOARD_SUMMARY_CACHE_KEY, JSON.stringify(payload));
  } catch {
    // ignore
  }
}

type WatchlistItem = {
  symbol: string;
  name?: string | null;
  addedAt: string;
  color?: string;
  positionPct?: number | null;
  costPrice?: number | null;
  maxPrice?: number | null;
};

type TrendOkChecks = {
  emaOrder?: boolean | null;
  macdPositive?: boolean | null;
  macdHistExpanding?: boolean | null;
  closeNear20dHigh?: boolean | null;
  rsiInRange?: boolean | null;
  volumeSurge?: boolean | null;
};

type TrendOkResult = {
  symbol: string;
  name?: string | null;
  asOfDate?: string | null;
  trendOk?: boolean | null;
  score?: number | null;
  scoreParts?: Record<string, number>;
  stopLossPrice?: number | null;
  buyMode?: string | null;
  buyAction?: string | null;
  buyZoneLow?: number | null;
  buyZoneHigh?: number | null;
  marketRegime?: string | null;
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  riskAlerts?: WatchlistRiskAlert[];
  checks?: TrendOkChecks | null;
  values?: Record<string, unknown> | null;
  missingData?: string[];
};

type WatchlistRiskRow = {
  symbol: string;
  name: string;
  intradayChgPct: number | null;
  gapUp: boolean | null;
  alerts: WatchlistRiskAlert[];
};

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

type NewsBriefCache = {
  summary?: string;
  updatedAt?: string;
  fallback?: string;
  fallbackUpdatedAt?: string;
};

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function toTsCodeFromSymbol(symbol: string): string | null {
  // Only handle CN A-shares for /quote.
  const s = symbol.trim().toUpperCase();
  if (!s.startsWith('CN:')) return null;
  const ticker = s.slice('CN:'.length).trim();
  if (!/^[0-9]{6}$/.test(ticker)) return null;
  const suffix = ticker.startsWith('6') ? 'SH' : 'SZ';
  return `${ticker}.${suffix}`;
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

function getShanghaiTodayIso(): string {
  // YYYY-MM-DD in Asia/Shanghai
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const map = new Map(parts.map((p) => [p.type, p.value]));
  const y = map.get('year') ?? '1970';
  const m = map.get('month') ?? '01';
  const d = map.get('day') ?? '01';
  return `${y}-${m}-${d}`;
}

function isShanghaiTradingTime(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  // CN A-share: 09:30-11:30, 13:00-15:00
  const inMorning = minutes >= 9 * 60 + 30 && minutes <= 11 * 60 + 30;
  const inAfternoon = minutes >= 13 * 60 && minutes <= 15 * 60;
  return inMorning || inAfternoon;
}

/** Trading hours + lunch + after-hours until 20:00 (matches data-sync-service). */
function isShanghaiSyncWindow(): boolean {
  const { weekday, hour, minute } = getShanghaiTimeParts();
  if (!['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday)) return false;
  const minutes = hour * 60 + minute;
  if (isShanghaiTradingTime()) return true;
  const inLunch = minutes > 11 * 60 + 30 && minutes < 13 * 60;
  const inAfterHours = minutes > 15 * 60 && minutes <= 20 * 60;
  return inLunch || inAfterHours;
}

type SyncStep = {
  name: string;
  ok: boolean | null;
  durationMs: number | null;
  message?: string | null;
};

async function fetchWatchlistRiskRows(): Promise<WatchlistRiskRow[]> {
  const itemsRaw = loadJson<WatchlistItem[]>(WATCHLIST_STORAGE_KEY, []);
  const items = (Array.isArray(itemsRaw) ? itemsRaw : [])
    .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
    .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));
  if (!items.length) return [];

  const tradingTime = isShanghaiTradingTime();
  const todaySh = getShanghaiTodayIso();
  const syms = items.map((x) => x.symbol);
  const byTsCode = new Map<string, string>();
  const tsCodes = syms
    .map((s) => {
      const t = toTsCodeFromSymbol(s);
      if (t) byTsCode.set(t, s);
      return t;
    })
    .filter(Boolean) as string[];

  const [trendResults, quoteResults] = await Promise.all([
    Promise.all(
      chunk(syms, 200).map(async (part) => {
        const sp = new URLSearchParams();
        sp.set('refresh', 'true');
        sp.set('realtime', tradingTime ? 'true' : 'false');
        for (const s of part) sp.append('symbols', s);
        return apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
      }),
    ),
    Promise.all(
      chunk(tsCodes, 50).map(async (part) => {
        return apiGetJson<QuoteResp>(`/quote?ts_codes=${encodeURIComponent(part.join(','))}`).catch(
          () => null,
        );
      }),
    ),
  ]);

  const trend: Record<string, TrendOkResult> = {};
  for (const trendRows of trendResults) {
    for (const r of Array.isArray(trendRows) ? trendRows : []) {
      if (r && r.symbol) trend[String(r.symbol).toUpperCase()] = r;
    }
  }

  const quotes: Record<
    string,
    {
      price: number | null;
      tradeTime: string | null;
      amount: number | null;
      volume: number | null;
      preClose: number | null;
      pctChg: number | null;
    }
  > = {};
  for (const r of quoteResults) {
    for (const it of r?.items ?? []) {
      const sym = byTsCode.get(it.ts_code);
      if (!sym) continue;
      quotes[sym] = parseDashboardQuoteItem(it);
    }
  }

  const out: WatchlistRiskRow[] = [];
  for (const it of items) {
    const t = trend[it.symbol];
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote: quotes[it.symbol],
      tradingTime,
      todaySh,
    });
    if (!rowMetrics.alerts.length) continue;
    out.push({
      symbol: it.symbol,
      name: it.name ?? t?.name ?? '—',
      intradayChgPct: rowMetrics.intradayChgPct,
      gapUp: rowMetrics.gapUp,
      alerts: rowMetrics.alerts,
    });
  }

  out.sort((a, b) => {
    const ab = a.alerts.some((x) => x.severity === 'block');
    const bb = b.alerts.some((x) => x.severity === 'block');
    if (ab !== bb) return ab ? -1 : 1;
    const ia = a.intradayChgPct ?? -Infinity;
    const ib = b.intradayChgPct ?? -Infinity;
    return ib - ia;
  });
  return out;
}

export function DashboardPage({ onNavigate }: { onNavigate?: (pageId: string) => void }) {
  const { addReference } = useChatStore();
  // Do not read localStorage during initial render — avoids SSR/CSR hydration mismatch.
  const [summary, setSummary] = React.useState<DashboardSummary | null>(null);
  const [summaryLoading, setSummaryLoading] = React.useState(false);
  const [syncResp, setSyncResp] = React.useState<DashboardSyncResp | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [sentimentBusy, setSentimentBusy] = React.useState(false);
  const [syncSteps, setSyncSteps] = React.useState<SyncStep[]>([]);
  const [syncProgress, setSyncProgress] = React.useState(0);
  const [industryCopyStatus, setIndustryCopyStatus] = React.useState<{
    ok: boolean;
    text: string;
  } | null>(null);
  const [sentimentCopyStatus, setSentimentCopyStatus] = React.useState<{
    ok: boolean;
    text: string;
  } | null>(null);
  const [copyAllBusy, setCopyAllBusy] = React.useState(false);
  const [copyAllStatus, setCopyAllStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [pdfReportBusy, setPdfReportBusy] = React.useState(false);
  const [pdfReportStatus, setPdfReportStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [error, setError] = React.useState<string | null>(null);
  const [editLayout, setEditLayout] = React.useState(false);
  const [newsSummary, setNewsSummary] = React.useState<string | null>(null);
  const [newsSummaryUpdatedAt, setNewsSummaryUpdatedAt] = React.useState<string | null>(null);
  const [newsFallback, setNewsFallback] = React.useState<string | null>(null);
  const [newsSummaryBusy, setNewsSummaryBusy] = React.useState(false);
  const [watchlistRiskRows, setWatchlistRiskRows] = React.useState<WatchlistRiskRow[]>([]);
  const [watchlistRiskBusy, setWatchlistRiskBusy] = React.useState(false);
  const [watchlistRiskUpdatedAt, setWatchlistRiskUpdatedAt] = React.useState<string | null>(null);
  const hotIndustryPicks = React.useMemo(() => buildDashboardHotIndustryPicks(summary), [summary]);

  const industryCopyTimerRef = React.useRef<number | null>(null);
  const sentimentCopyTimerRef = React.useRef<number | null>(null);
  const copyAllTimerRef = React.useRef<number | null>(null);
  const pdfReportTimerRef = React.useRef<number | null>(null);
  React.useEffect(() => {
    return () => {
      if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
      if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
      if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
      if (pdfReportTimerRef.current != null) window.clearTimeout(pdfReportTimerRef.current);
    };
  }, []);

  React.useEffect(() => {
    const cached = loadDashboardSummaryCache();
    if (cached) setSummary(cached);
  }, []);

  React.useEffect(() => {
    try {
      const raw = window.localStorage.getItem(NEWS_BRIEF_CACHE_KEY);
      if (!raw) return;
      const obj = JSON.parse(raw) as NewsBriefCache;
      const summary = typeof obj?.summary === 'string' ? obj.summary.trim() : '';
      const updatedAt = typeof obj?.updatedAt === 'string' ? obj.updatedAt.trim() : '';
      const fallback = typeof obj?.fallback === 'string' ? obj.fallback.trim() : '';
      if (summary) setNewsSummary(summary);
      if (updatedAt) setNewsSummaryUpdatedAt(updatedAt);
      if (fallback) setNewsFallback(fallback);
    } catch {
      // ignore
    }
  }, []);

  function saveNewsBriefCache(patch: NewsBriefCache) {
    try {
      const raw = window.localStorage.getItem(NEWS_BRIEF_CACHE_KEY);
      const prev = raw ? (JSON.parse(raw) as NewsBriefCache) : {};
      window.localStorage.setItem(NEWS_BRIEF_CACHE_KEY, JSON.stringify({ ...prev, ...patch }));
    } catch {
      // ignore
    }
  }

  function buildNewsFallback(items: any[]): string | null {
    const rows = (Array.isArray(items) ? items : [])
      .slice(0, 8)
      .map((it: any, idx: number) => {
        const title = String(it?.title ?? '').trim();
        if (!title) return null;
        const source = String(it?.sourceId ?? '').trim();
        const publishedAt = String(it?.publishedAt ?? '').trim();
        const meta = [source, publishedAt].filter(Boolean).join(' | ');
        return `${idx + 1}. ${title}${meta ? ` (${meta})` : ''}`;
      })
      .filter(Boolean) as string[];
    if (!rows.length) return null;
    return ['Latest headlines:', ...rows].join('\n');
  }

  function shouldRefreshNewsBrief(lastUpdatedAt: string | null): boolean {
    if (!lastUpdatedAt) return true;
    const t = new Date(lastUpdatedAt).getTime();
    if (!Number.isFinite(t)) return true;
    return Date.now() - t >= NEWS_BRIEF_MIN_REFRESH_MS;
  }

  function toastIndustryCopy(ok: boolean, text: string) {
    setIndustryCopyStatus({ ok, text });
    if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
    industryCopyTimerRef.current = window.setTimeout(() => setIndustryCopyStatus(null), 2400);
  }

  function toastSentimentCopy(ok: boolean, text: string) {
    setSentimentCopyStatus({ ok, text });
    if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
    sentimentCopyTimerRef.current = window.setTimeout(() => setSentimentCopyStatus(null), 2400);
  }

  function toastCopyAll(ok: boolean, text: string) {
    setCopyAllStatus({ ok, text });
    if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
    copyAllTimerRef.current = window.setTimeout(() => setCopyAllStatus(null), 2600);
  }

  function toastPdfReport(ok: boolean, text: string) {
    setPdfReportStatus({ ok, text });
    if (pdfReportTimerRef.current != null) window.clearTimeout(pdfReportTimerRef.current);
    pdfReportTimerRef.current = window.setTimeout(() => setPdfReportStatus(null), 3200);
  }

  const defaultCards = React.useMemo(
    () => [
      { id: 'industry', title: 'Industry fund flow' },
      { id: 'sentiment', title: 'Market sentiment' },
      { id: 'watchlistRisk', title: 'Watchlist 风险警报' },
      { id: 'news', title: 'News brief' },
      { id: 'screeners', title: 'Screener sync' },
    ],
    [],
  );

  const [cardOrder, setCardOrder] = React.useState<string[]>(() => []);
  React.useEffect(() => {
    const loaded = loadCardOrder();
    const ids = defaultCards.map((c) => c.id);
    const next = loaded
      ? [...loaded.filter((x) => ids.includes(x)), ...ids.filter((x) => !loaded.includes(x))]
      : ids;
    const nextIds = next.includes('industry')
      ? ['industry', ...next.filter((x) => x !== 'industry')]
      : next;
    setCardOrder(nextIds);
    saveCardOrder(nextIds);
  }, [defaultCards]);

  const refreshWatchlistRisk = React.useCallback(async () => {
    setWatchlistRiskBusy(true);
    try {
      const rows = await fetchWatchlistRiskRows();
      setWatchlistRiskRows(rows);
      setWatchlistRiskUpdatedAt(new Date().toISOString());
    } catch {
      // Keep last good snapshot on transient errors.
    } finally {
      setWatchlistRiskBusy(false);
    }
  }, []);

  React.useEffect(() => {
    void refreshWatchlistRisk();
  }, [refreshWatchlistRisk]);

  React.useEffect(() => {
    const id = window.setInterval(() => {
      if (isShanghaiTradingTime()) void refreshWatchlistRisk();
    }, 60 * 1000);
    return () => window.clearInterval(id);
  }, [refreshWatchlistRisk]);

  const refresh = React.useCallback(async () => {
    setError(null);
    setSummaryLoading(true);
    try {
      const lite = isShanghaiSyncWindow() ? '' : '?include_macro=false';
      const s = await apiGetJson<DashboardSummary>(`/dashboard/summary${lite}`);
      setSummary(s);
      saveDashboardSummaryCache(s);
      const fallback = buildNewsFallback((s as any)?.news?.items ?? []);
      if (fallback) {
        const fallbackUpdatedAt = new Date().toISOString();
        setNewsFallback(fallback);
        saveNewsBriefCache({ fallback, fallbackUpdatedAt });
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSummaryLoading(false);
    }
  }, []);

  React.useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  React.useEffect(() => {
    const id = window.setInterval(() => {
      if (isShanghaiTradingTime()) void refresh();
    }, 60 * 1000);
    return () => window.clearInterval(id);
  }, [refresh]);

  async function onSyncAll() {
    setBusy(true);
    setError(null);
    setSyncSteps([]);
    setSyncProgress(0);

    const stepNames = ['industryFundFlow', 'marketSentiment', 'screeners', 'news'];
    const forceSync = isShanghaiSyncWindow();

    return new Promise<void>((resolve) => {
      const es = new EventSource(
        `${DATA_SYNC_BASE_URL}/dashboard/sync/stream?force=${forceSync ? 'true' : 'false'}`,
      );

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'start') {
            setSyncSteps(
              stepNames.map((name) => ({
                name,
                ok: null,
                durationMs: null,
                message: null,
              })),
            );
          } else if (data.type === 'step') {
            const step = data.step as { name: string; ok: boolean; durationMs: number; message?: string };
            setSyncSteps((prev) => {
              const updated = prev.map((s) => (s.name === step.name ? { ...step } : s));
              const completed = updated.filter((s) => s.ok !== null).length;
              setSyncProgress(Math.round((completed / stepNames.length) * 100));
              return updated;
            });
          } else if (data.type === 'done') {
            es.close();
            const result = data.result as DashboardSyncResp;
            setSyncResp(result);
            setSyncProgress(100);

            const s = data.summary as DashboardSummary;
            if (s) {
              setSummary(s);
              saveDashboardSummaryCache(s);
              const newsData = (s as any)?.news;
              if (newsData && Array.isArray(newsData.items) && newsData.items.length > 0) {
                if (!shouldRefreshNewsBrief(newsSummaryUpdatedAt) && newsSummary?.trim()) {
                  setBusy(false);
                  resolve();
                  return;
                }
                setNewsSummaryBusy(true);
                fetch(`${AI_BASE_URL}/news/summary`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ items: newsData.items, hours: 24 }),
                })
                  .then((aiRes) => {
                    if (aiRes.ok) {
                      return aiRes.json();
                    }
                    return null;
                  })
                  .then((aiData) => {
                    const summaryText = typeof aiData?.summary === 'string' ? aiData.summary.trim() : '';
                    if (summaryText) {
                      const updatedAt = new Date().toISOString();
                      setNewsSummary(summaryText);
                      setNewsSummaryUpdatedAt(updatedAt);
                      saveNewsBriefCache({ summary: summaryText, updatedAt });
                    }
                  })
                  .catch(() => {})
                  .finally(() => {
                    setNewsSummaryBusy(false);
                    setBusy(false);
                    resolve();
                  });
              } else {
                setBusy(false);
                resolve();
              }
            } else {
              setBusy(false);
              resolve();
            }
          }
        } catch {
          // ignore parse errors
        }
      };

      es.onerror = () => {
        es.close();
        setError('Connection error during sync');
        setBusy(false);
        resolve();
      };
    });
  }

  async function onSyncSentiment() {
    setSentimentBusy(true);
    setError(null);
    try {
      await apiPostJson('/market/cn/sentiment/sync', { force: isShanghaiSyncWindow() });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSentimentBusy(false);
    }
  }

  async function regenerateNewsSummary() {
    setNewsSummaryBusy(true);
    setError(null);
    try {
      const s = await apiGetJson<DashboardSummary>(`/dashboard/summary`);
      setSummary(s);
      const newsData = (s as any)?.news;
      if (newsData && Array.isArray(newsData.items) && newsData.items.length > 0) {
        const aiRes = await fetch(`${AI_BASE_URL}/news/summary`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ items: newsData.items, hours: 24 }),
        });
        if (aiRes.ok) {
          const aiData = await aiRes.json();
          const summaryText = typeof aiData?.summary === 'string' ? aiData.summary.trim() : '';
          if (summaryText) {
            const updatedAt = new Date().toISOString();
            setNewsSummary(summaryText);
            setNewsSummaryUpdatedAt(updatedAt);
            saveNewsBriefCache({ summary: summaryText, updatedAt });
          }
        } else {
          const errText = await aiRes.text();
          setError(`AI error: ${errText}`);
        }
      } else {
        setError('No news items available');
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setNewsSummaryBusy(false);
    }
  }

  function buildIndustryMarkdown(s: DashboardSummary | null, heading = '##'): string {
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

  function buildSentimentMarkdown(s: DashboardSummary | null, heading = '##'): string {
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
    const headers = ['date', 'ratio', 'turnover', 'premium%', 'failed%', 'risk'];
    const rows: unknown[][] = last5.map((it: any) => [
      String(it?.date ?? ''),
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

  function buildMacroMarkdown(s: DashboardSummary | null, heading = '##'): string {
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

  async function buildScreenersMarkdown(
    s: DashboardSummary | null,
    heading = '##',
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

    const screenerResults = await Promise.all(
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

    for (const result of screenerResults) {
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

  async function buildWatchlistMarkdown(): Promise<string> {
    const itemsRaw = loadJson<WatchlistItem[]>(WATCHLIST_STORAGE_KEY, []);
    const items: WatchlistItem[] = (Array.isArray(itemsRaw) ? itemsRaw : [])
      .filter((x) => x && typeof x.symbol === 'string' && String(x.symbol).trim())
      .map((x) => ({ ...x, symbol: String(x.symbol).trim().toUpperCase() }));

    const heading = '##';
    if (!items.length) return `${heading} Watchlist\n\nNo items.\n`;

    const syms = items.map((x) => x.symbol);
    const tradingTime = isShanghaiTradingTime();
    const todaySh = getShanghaiTodayIso();

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
          sp.set('refresh', 'true');
          sp.set('realtime', tradingTime ? 'true' : 'false');
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

    const trend: Record<string, TrendOkResult> = {};
    for (const trendRows of trendResults) {
      for (const r of Array.isArray(trendRows) ? trendRows : []) {
        if (r && r.symbol) trend[String(r.symbol).toUpperCase()] = r;
      }
    }

    const quotes: Record<
      string,
      {
        price: number | null;
        tradeTime: string | null;
        amount: number | null;
        volume: number | null;
        preClose: number | null;
        pctChg: number | null;
      }
    > = {};
    for (const r of quoteResults) {
      for (const it of r?.items ?? []) {
        const sym = byTsCode.get(it.ts_code);
        if (!sym) continue;
        quotes[sym] = parseDashboardQuoteItem(it);
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
      const asOf =
        tradingTime && tradeDateFromTradeTime(q?.tradeTime ?? null)
          ? tradeDateFromTradeTime(q?.tradeTime ?? null)
          : String(t?.asOfDate ?? '');
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

  async function buildDashboardCopyAllMarkdown(): Promise<string> {
    const s = summary;
    if (!s) {
      throw new Error('No data available. Please refresh first.');
    }
    const generatedAt = new Date().toISOString();
    const [screenersMd, watchlistMd, catalystMd, alphaTrendsMd] = await Promise.all([
      buildScreenersMarkdown(s, '##'),
      buildWatchlistMarkdown(),
      fetchCatalystStocks(DATA_SYNC_BASE_URL, 10, DEFAULT_CATALYST_MAX_AGE_DAYS)
        .then((resp) => buildCatalystStocksMarkdown(resp, { headingLevel: '##' }))
        .catch(() => '## Alpha Radar · Top Catalyst Stocks\n\n- Alpha Radar: unavailable\n'),
      fetchAlphaRadarTrends(DATA_SYNC_BASE_URL, 20, true)
        .then((items) => buildAlphaRadarTrendsMarkdown(items, { headingLevel: '##' }))
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

  async function copyAllMarkdown() {
    setCopyAllBusy(true);
    setError(null);
    try {
      if (!summary) {
        toastCopyAll(false, 'No data available. Please refresh first.');
        return;
      }
      const text = await buildDashboardCopyAllMarkdown();
      await navigator.clipboard.writeText(text);
      toastCopyAll(true, 'Copied all Markdown to clipboard.');
    } catch (e) {
      toastCopyAll(false, e instanceof Error ? e.message : String(e));
    } finally {
      setCopyAllBusy(false);
    }
  }

  async function onDownloadInvestmentDailyPdf() {
    setPdfReportBusy(true);
    setError(null);
    try {
      if (!summary) {
        toastPdfReport(false, 'No data available. Please refresh first.');
        return;
      }
      const rawMd = await buildDashboardCopyAllMarkdown();
      const markdown = truncateMarkdownForReport(rawMd);
      const aiRes = await fetch(`${AI_BASE_URL}/report/investment-daily`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ markdown }),
      });
      const rawText = await aiRes.text();
      let data: unknown = null;
      try {
        data = rawText ? JSON.parse(rawText) : null;
      } catch {
        throw new Error(rawText || `AI error (${aiRes.status})`);
      }
      if (!aiRes.ok) {
        const errMsg =
          data && typeof data === 'object' && 'error' in data
            ? String((data as { error?: unknown }).error)
            : rawText;
        throw new Error(errMsg || `AI error (${aiRes.status})`);
      }
      const report = parseInvestmentDailyReportResponse(data);
      const subtitleTimeZh = new Date().toLocaleString('zh-CN', { hour12: false });
      const datePart = new Date().toISOString().slice(0, 10);
      await downloadInvestmentDailyPdf({
        report,
        subtitleTimeZh,
        filename: `投资要点日报-${datePart}.pdf`,
        summary,
        hotIndustryPicks,
      });
      toastPdfReport(true, 'PDF downloaded.');
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
      toastPdfReport(false, msg);
    } finally {
      setPdfReportBusy(false);
    }
  }

  const cardsById = React.useMemo(
    () => Object.fromEntries(defaultCards.map((c) => [c.id, c])),
    [defaultCards],
  );
  const orderedCards = cardOrder.map((id) => cardsById[id]).filter(Boolean);

  function moveCard(id: string, dir: -1 | 1) {
    const idx = cardOrder.indexOf(id);
    if (idx < 0) return;
    const j = idx + dir;
    if (j < 0 || j >= cardOrder.length) return;
    const next = [...cardOrder];
    const tmp = next[idx];
    next[idx] = next[j];
    next[j] = tmp;
    setCardOrder(next);
    saveCardOrder(next);
  }

  return (
    <div className="mx-auto w-full max-w-6xl p-6">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Dashboard</div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => void refresh()}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => void copyAllMarkdown()}
          >
            {copyAllBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            Copy all Markdown
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => void onDownloadInvestmentDailyPdf()}
          >
            {pdfReportBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            下载 PDF
          </Button>
          <Button
            variant="secondary"
            size="sm"
            disabled={busy || copyAllBusy || pdfReportBusy}
            onClick={() => {
              const asOfDate = String(summary?.asOfDate ?? '');
              const capturedAt = new Date().toISOString();
              addReference({
                kind: 'dashboardAll',
                refId: `dashboardAll:${asOfDate}:${Date.now()}`,
                asOfDate,
                title: 'Dashboard Overview',
                capturedAt,
              } as any);
            }}
          >
            Reference all
          </Button>
          <Button size="sm" className="gap-2" disabled={busy} onClick={() => void onSyncAll()}>
            {busy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            {busy
              ? 'Syncing…'
              : isShanghaiSyncWindow()
                ? 'Sync all (force)'
                : 'Sync all (cached)'}
          </Button>
          <Button size="sm" variant="secondary" onClick={() => setEditLayout((v) => !v)}>
            {editLayout ? 'Done' : 'Edit layout'}
          </Button>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-[var(--k-muted)]">
        <span>
          asOfDate: <span className="font-mono">{summary?.asOfDate ?? '—'}</span>
        </span>
        {summaryLoading ? (
          <span className="inline-flex items-center gap-1">
            <RefreshCw className="h-3 w-3 animate-spin" />
            Updating…
          </span>
        ) : null}
        {!summaryLoading && summary && !isShanghaiSyncWindow() ? (
          <span>盘后模式：仅读缓存，同步跳过实时抓取</span>
        ) : null}
      </div>
      {copyAllStatus ? (
        <div className={`mb-4 text-xs ${copyAllStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {copyAllStatus.text}
        </div>
      ) : null}
      {pdfReportStatus ? (
        <div
          className={`mb-4 text-xs ${pdfReportStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}
        >
          {pdfReportStatus.text}
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
        </div>
      ) : null}

      {busy && syncSteps.length > 0 ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <div className="text-sm font-medium">Syncing…</div>
            <div className="text-xs text-[var(--k-muted)]">{syncProgress}%</div>
          </div>
          <Progress value={syncProgress} className="mb-4" />
          <div className="space-y-2">
            {syncSteps.map((s) => (
              <div key={s.name} className="flex items-center gap-3 text-xs">
                {s.ok === null ? (
                  <span className="h-4 w-4 rounded-full bg-[var(--k-muted)]/30 animate-pulse" />
                ) : s.ok ? (
                  <span className="h-4 w-4 rounded-full bg-emerald-500" />
                ) : (
                  <span className="h-4 w-4 rounded-full bg-red-500" />
                )}
                <span className="font-mono">
                  {s.name === 'industryFundFlow'
                    ? 'Industry Fund Flow'
                    : s.name === 'marketSentiment'
                      ? 'Market Sentiment'
                      : s.name === 'screeners'
                        ? 'Screeners'
                        : s.name === 'news'
                          ? 'News'
                          : s.name}
                </span>
                {s.durationMs !== null ? (
                  <span className="text-[var(--k-muted)]">{s.durationMs}ms</span>
                ) : null}
                {s.message ? (
                  <span className="text-red-600 truncate">{s.message}</span>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {syncResp ? (
        <div className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
          <div className="mb-2 text-sm font-medium">Last sync result</div>
          <div className="text-xs text-[var(--k-muted)]">
            started: {fmtDateTime(syncResp.startedAt)} • finished:{' '}
            {fmtDateTime(syncResp.finishedAt)} • ok: {String(Boolean(syncResp.ok))}
          </div>
          <div className="mt-3 overflow-auto rounded-lg border border-[var(--k-border)]">
            <table className="w-full border-collapse text-xs">
              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2">Step</th>
                  <th className="px-3 py-2">OK</th>
                  <th className="px-3 py-2">Duration</th>
                  <th className="px-3 py-2">Message</th>
                </tr>
              </thead>
              <tbody>
                {(syncResp.steps ?? []).map((s: any) => (
                  <tr key={String(s.name)} className="border-t border-[var(--k-border)]">
                    <td className="px-3 py-2 font-mono">{String(s.name)}</td>
                    <td className="px-3 py-2">{String(Boolean(s.ok))}</td>
                    <td className="px-3 py-2 font-mono">{String(s.durationMs ?? 0)}ms</td>
                    <td className="px-3 py-2 text-[var(--k-muted)]">{String(s.message ?? '')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {syncResp.screener?.failed?.length || syncResp.screener?.missing?.length ? (
            <div className="mt-3 text-xs text-red-600">
              Screener issues: failed={syncResp.screener?.failed?.length ?? 0} missing=
              {syncResp.screener?.missing?.length ?? 0}
            </div>
          ) : null}
        </div>
      ) : null}

      {(() => {
        const weightOf = (id: string) => {
          if (id === 'industry') return 6;
          if (id === 'sentiment') return 3;
          if (id === 'watchlistRisk') return 2;
          if (id === 'news') return 2;
          if (id === 'screeners') return 2;
          return 2;
        };
        const left: any[] = [];
        const right: any[] = [];
        let wl = 0;
        let wr = 0;
        for (const c of orderedCards) {
          const id = String(c.id);
          const w = weightOf(id);
          if (wl <= wr) {
            left.push(c);
            wl += w;
          } else {
            right.push(c);
            wr += w;
          }
        }

        const renderCard = (c: any) => {
          const id = String(c.id);
          return (
            <section
              key={id}
              className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4"
            >
              <div className="mb-3 flex items-center justify-between gap-2">
                <div className="text-sm font-medium">{c.title}</div>
                {editLayout ? (
                  <div className="flex items-center gap-1">
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, -1)}
                    >
                      ↑
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      className="h-7 px-2 text-xs"
                      onClick={() => moveCard(id, 1)}
                    >
                      ↓
                    </Button>
                  </div>
                ) : null}
              </div>

              {id === 'sentiment' ? (
                <div>
                  {(() => {
                    const ms = summary?.marketSentiment ?? {};
                    const items: any[] = Array.isArray(ms.items) ? ms.items : [];
                    const latest = items.length ? items[items.length - 1] : null;
                    const indexSignals: any[] = Array.isArray(ms.indexSignals)
                      ? ms.indexSignals
                      : [];
                    const summaryLine = buildIndexTrafficSummary(indexSignals);
                    const risk = String(latest?.riskMode ?? '—');
                    const premium = Number.isFinite(latest?.yesterdayLimitUpPremium)
                      ? `${Number(latest.yesterdayLimitUpPremium).toFixed(2)}%`
                      : '—';
                    const failed = Number.isFinite(latest?.failedLimitUpRate)
                      ? `${Number(latest.failedLimitUpRate).toFixed(1)}%`
                      : '—';
                    const turnover = fmtAmountCn(latest?.marketTurnoverCny);
                    const ratio = Number.isFinite(latest?.upDownRatio)
                      ? Number(latest.upDownRatio).toFixed(2)
                      : '—';
                    const up = Number(latest?.upCount ?? 0);
                    const down = Number(latest?.downCount ?? 0);
                    const flat = Number(latest?.flatCount ?? 0);
                    const breadthPanic = down >= BREADTH_PANIC_DOWN_THRESHOLD;
                    const badge =
                      risk === 'extreme_caution' || breadthPanic
                        ? 'border-red-600/40 bg-red-600/15 text-red-700'
                        : risk === 'no_new_positions'
                          ? 'border-red-500/30 bg-red-500/10 text-red-600'
                          : risk === 'caution'
                            ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                            : risk === 'hot'
                              ? 'border-green-500/30 bg-green-500/10 text-green-700'
                              : risk === 'euphoric'
                                ? 'border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-700'
                                : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
                    return (
                      <>
                        <div className="mb-2 flex flex-wrap items-center gap-2">
                          <div className={`rounded-md border px-2 py-1 text-xs ${badge}`}>
                            risk: {risk}
                          </div>
                          {Array.isArray(latest?.rules) && latest.rules.length ? (
                            <div className="text-xs text-[var(--k-muted)]">
                              {latest.rules
                                .slice(0, 2)
                                .map((x: any) => String(x))
                                .join(' • ')}
                            </div>
                          ) : null}
                        </div>

                        <div
                          className={`mb-3 rounded-lg border px-3 py-2 text-sm ${
                            breadthPanic
                              ? 'border-red-500/40 bg-red-500/10'
                              : 'border-[var(--k-border)] bg-[var(--k-surface-2)]'
                          }`}
                        >
                          <div
                            className={`font-medium ${
                              breadthPanic ? 'text-red-700' : 'text-[var(--k-fg)]'
                            }`}
                          >
                            Market Breadth: {up.toLocaleString()} Up / {down.toLocaleString()}{' '}
                            Down
                          </div>
                          {breadthPanic ? (
                            <div className="mt-1 text-xs text-red-700">
                              Down &ge; {BREADTH_PANIC_DOWN_THRESHOLD.toLocaleString()}: force red
                              lights and extreme caution.
                            </div>
                          ) : null}
                        </div>

                        <div className="mb-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                          <div className="text-sm font-semibold text-amber-700">
                            {summaryLine.title}
                          </div>
                          <div className="mt-1 text-xs text-amber-800">{summaryLine.detail}</div>
                        </div>

                        <div className="grid grid-cols-2 gap-2 text-sm">
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Up/Down/Flat</div>
                            <div className="mt-1 font-mono">
                              {up}/{down}/{flat}
                            </div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">ratio: {ratio}</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              turnover: {turnover}
                            </div>
                          </div>
                          <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-3">
                            <div className="text-xs text-[var(--k-muted)]">Sentiment</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              yesterday limit-up premium
                            </div>
                            <div className="mt-0.5 font-mono">{premium}</div>
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              failed limit-up rate
                            </div>
                            <div className="mt-0.5 font-mono">{failed}</div>
                          </div>
                        </div>

                        {indexSignals.length ? (
                          <div className="mt-3">
                            <div className="mb-2 text-xs text-[var(--k-muted)]">
                              Index traffic lights
                            </div>
                            <div className="mb-3 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 py-2 text-xs text-[var(--k-muted)]">
                              <div className="font-medium text-[var(--k-fg)]">信号规则（简版）</div>
                              <div className="mt-1">
                                🔴 Red: Price &lt; MA20 或 MA5 &lt; MA20，仓位 0%-10%。
                              </div>
                              <div className="mt-1">
                                🟡 Yellow: Price &gt; MA20 但 MA20 斜率向下 或 预估全天量 &lt;
                                MA5_Vol * 0.8 或 MA5 &lt; MA20，仓位 30%。
                              </div>
                              <div className="mt-1">
                                🟢 Green: Price &gt; MA20 且 MA5 &gt; MA20 且 MA20
                                向上，且预估全天量 &gt; MA5_Vol * 0.8，仓位 50%-60%。
                              </div>
                              <div className="mt-1">
                                ❇️ Deep Green: MA5 &gt; MA20 &gt; MA60 且 Price &gt; EMA10，全市场成交额连续
                                &gt; 1.5万亿，Breadth &gt; 50% 或 单一板块流入 &gt; 50亿，仓位
                                80%-100%。
                              </div>
                            </div>
                            <div className="grid gap-2 md:grid-cols-2">
                              {indexSignals.map((it: any) => {
                                const signal = String(it?.signal ?? 'unknown');
                                const badge =
                                  signal === 'deep_green'
                                    ? 'border-emerald-600/40 bg-emerald-600/15 text-emerald-800'
                                    : signal === 'light_green' || signal === 'green'
                                      ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-700'
                                      : signal === 'red'
                                        ? 'border-red-500/30 bg-red-500/10 text-red-600'
                                        : signal === 'yellow'
                                          ? 'border-yellow-500/30 bg-yellow-500/10 text-yellow-700'
                                          : 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
                                return (
                                  <div
                                    key={String(it?.tsCode ?? it?.name)}
                                    className={`rounded-lg border px-3 py-2 text-xs ${badge}`}
                                  >
                                    <div className="font-medium">
                                      {String(it?.name ?? it?.tsCode ?? '')}
                                    </div>
                                    <div className="mt-1 font-mono">
                                      {signal} • pos {String(it?.positionRange ?? '—')}
                                    </div>
                                    <div className="mt-1 text-[var(--k-muted)]">
                                      chg{' '}
                                      {Number.isFinite(it?.pctChg)
                                        ? `${Number(it.pctChg) >= 0 ? '+' : ''}${Number(it.pctChg).toFixed(2)}%`
                                        : '—'}{' '}
                                      • close{' '}
                                      {Number.isFinite(it?.close)
                                        ? Number(it.close).toFixed(2)
                                        : '—'}{' '}
                                      • MA5{' '}
                                      {Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—'} •
                                      MA20{' '}
                                      {Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—'}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        ) : null}

                        <div className="mt-3">
                          <div className="mb-2 text-xs text-[var(--k-muted)]">Last 5 days</div>
                          <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                            <table className="w-full border-collapse text-xs">
                              <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                <tr className="text-left">
                                  <th className="px-2 py-2 font-mono">date</th>
                                  <th className="px-2 py-2 text-right">ratio</th>
                                  <th className="px-2 py-2 text-right">turnover</th>
                                  <th className="px-2 py-2 text-right">premium%</th>
                                  <th className="px-2 py-2 text-right">failed%</th>
                                  <th className="px-2 py-2">risk</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(items || []).slice(-5).map((it: any, idx: number) => (
                                  <tr key={idx} className="border-t border-[var(--k-border)]">
                                    <td className="px-2 py-2 font-mono">{String(it.date ?? '')}</td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.upDownRatio)
                                        ? Number(it.upDownRatio).toFixed(2)
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {fmtAmountCn(it.marketTurnoverCny)}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.yesterdayLimitUpPremium)
                                        ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2 text-right font-mono">
                                      {Number.isFinite(it.failedLimitUpRate)
                                        ? `${Number(it.failedLimitUpRate).toFixed(1)}%`
                                        : '—'}
                                    </td>
                                    <td className="px-2 py-2">{String(it.riskMode ?? '')}</td>
                                  </tr>
                                ))}
                                {!items.length ? (
                                  <tr>
                                    <td
                                      className="px-2 py-3 text-sm text-[var(--k-muted)]"
                                      colSpan={7}
                                    >
                                      No sentiment cached yet. Click “Sync all (force)”.
                                    </td>
                                  </tr>
                                ) : null}
                              </tbody>
                            </table>
                          </div>
                        </div>

                        <div className="mt-3 flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            disabled={sentimentBusy}
                            onClick={() => void onSyncSentiment()}
                          >
                            {sentimentBusy ? (
                              <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                              <RefreshCw className="mr-2 h-4 w-4" />
                            )}
                            Sync sentiment
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              try {
                                const md = buildSentimentMarkdown(summary, '#');
                                void navigator.clipboard
                                  .writeText(md)
                                  .then(() => toastSentimentCopy(true, 'Copied Markdown.'))
                                  .catch(() =>
                                    toastSentimentCopy(
                                      false,
                                      'Copy failed. Please allow clipboard access.',
                                    ),
                                  );
                              } catch (e) {
                                toastSentimentCopy(
                                  false,
                                  e instanceof Error ? e.message : String(e),
                                );
                              }
                            }}
                          >
                            Copy Markdown
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              const asOfDate = String(ms.asOfDate ?? summary?.asOfDate ?? '');
                              addReference({
                                kind: 'marketSentiment',
                                refId: `${asOfDate}:5`,
                                asOfDate,
                                days: 5,
                                title: 'CN market sentiment (breadth & limit-up)',
                                createdAt: new Date().toISOString(),
                              } as any);
                            }}
                          >
                            Reference
                          </Button>
                        </div>
                        {sentimentCopyStatus ? (
                          <div
                            className={`mt-2 text-xs ${
                              sentimentCopyStatus.ok ? 'text-emerald-600' : 'text-red-600'
                            }`}
                          >
                            {sentimentCopyStatus.text}
                          </div>
                        ) : null}
                      </>
                    );
                  })()}
                </div>
              ) : id === 'industry' ? (
                <div>
                  <div className="mb-4">
                    <HotIndustryWorkflowCard
                      picks={hotIndustryPicks}
                      asOfDate={String(
                        summary?.industryFundFlow?.asOfDate ?? summary?.asOfDate ?? '',
                      )}
                      compact
                      onOpenScreener={() => onNavigate?.('screener')}
                      onOpenWatchlist={() => onNavigate?.('watchlist')}
                    />
                  </div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    Top5×Date hotspots (names only)
                  </div>
                  {(() => {
                    const datesAll: string[] = Array.isArray(summary?.industryFundFlow?.dates)
                      ? summary.industryFundFlow.dates
                      : [];
                    const rawShownDates = datesAll.slice(-5);
                    const topByDateArr: any[] = Array.isArray(summary?.industryFundFlow?.topByDate)
                      ? summary.industryFundFlow.topByDate
                      : [];
                    const map: Record<string, string[]> = {};
                    for (const it of topByDateArr) {
                      const d = String(it?.date ?? '');
                      const top = Array.isArray(it?.top)
                        ? it.top.map((x: any) => String(x ?? ''))
                        : [];
                      if (d) map[d] = top;
                    }
                    const dedupedDates: string[] = [];
                    let prevSig = '';
                    let collapsed = 0;
                    for (const d of rawShownDates) {
                      const sig = (map[d] || []).slice(0, 5).join('|');
                      if (sig && sig === prevSig) {
                        collapsed += 1;
                        continue;
                      }
                      dedupedDates.push(d);
                      prevSig = sig;
                    }

                    async function copyIndustryMarkdown() {
                      try {
                        const asOfDate = String(
                          summary?.industryFundFlow?.asOfDate ?? summary?.asOfDate ?? '',
                        ).trim();

                        const lines: string[] = [];
                        lines.push(
                          `# Industry fund flow${asOfDate ? ` (asOfDate: ${asOfDate})` : ''}`,
                        );
                        lines.push('');

                        // Table 1: Top5×Date hotspots.
                        if (dedupedDates.length) {
                          const headers1 = ['#', ...dedupedDates.map((d) => String(d).slice(5))];
                          const rows1: unknown[][] = Array.from({ length: 5 }).map((_, i) => [
                            i + 1,
                            ...dedupedDates.map((d) => String((map[d] || [])[i] ?? '')),
                          ]);
                          lines.push('## Top5×Date hotspots (names only)');
                          lines.push('');
                          lines.push(mdTable(headers1, rows1));
                          lines.push('');
                        }

                        // Table 2: 5D net inflow.
                        const flow5d: any = (summary?.industryFundFlow as any)?.flow5d ?? null;
                        const flowDates: string[] = Array.isArray(flow5d?.dates)
                          ? flow5d.dates
                          : [];
                        const colDates: string[] = flowDates.length
                          ? flowDates.slice(-5)
                          : dedupedDates;
                        const topRows: any[] = Array.isArray(flow5d?.top) ? flow5d.top : [];
                        if (topRows.length && colDates.length) {
                          const headers2 = [
                            'Industry',
                            'Sum(5D)',
                            ...colDates.map((d) => String(d).slice(5)),
                          ];
                          const rows2: unknown[][] = topRows.slice(0, 10).map((r: any) => {
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
                              ...colDates.map((d) => fmtAmountCn(m2[d] ?? 0)),
                            ];
                          });
                          lines.push('## 5D net inflow (Top by 5D sum)');
                          lines.push('');
                          lines.push(mdTable(headers2, rows2));
                          lines.push('');
                        }

                        // Table 3: 5D net outflow.
                        const flow5dOut: any =
                          (summary?.industryFundFlow as any)?.flow5dOut ?? null;
                        const outDates: string[] = Array.isArray(flow5dOut?.dates)
                          ? flow5dOut.dates
                          : [];
                        const outColDates: string[] = outDates.length
                          ? outDates.slice(-5)
                          : dedupedDates;
                        const outRows: any[] = Array.isArray(flow5dOut?.top) ? flow5dOut.top : [];
                        if (outRows.length && outColDates.length) {
                          const headers3 = [
                            'Industry',
                            'Sum(5D)',
                            ...outColDates.map((d) => String(d).slice(5)),
                          ];
                          const rows3: unknown[][] = outRows.slice(0, 10).map((r: any) => {
                            const seriesArr: any[] = Array.isArray(r?.series) ? r.series : [];
                            const m3: Record<string, number> = {};
                            for (const p of seriesArr) {
                              const dd = String(p?.date ?? '');
                              const nv = Number(p?.netInflow ?? 0);
                              if (dd) m3[dd] = Number.isFinite(nv) ? nv : 0;
                            }
                            return [
                              String(r?.industryName ?? ''),
                              fmtAmountCn(r?.sum5d),
                              ...outColDates.map((d) => fmtAmountCn(m3[d] ?? 0)),
                            ];
                          });
                          lines.push('## 5D net outflow (Top by 5D sum)');
                          lines.push('');
                          lines.push(mdTable(headers3, rows3));
                          lines.push('');
                        }

                        if (
                          !dedupedDates.length &&
                          !(topRows.length && colDates.length) &&
                          !(outRows.length && outColDates.length)
                        ) {
                          toastIndustryCopy(false, 'Nothing to copy (no industry fund flow data).');
                          return;
                        }

                        await navigator.clipboard.writeText(lines.join('\n'));
                        toastIndustryCopy(true, 'Copied Markdown to clipboard.');
                      } catch (e) {
                        toastIndustryCopy(false, e instanceof Error ? e.message : String(e));
                      }
                    }

                    return (
                      <>
                        {collapsed ? (
                          <div className="mb-2 text-xs text-[var(--k-muted)]">
                            collapsed {collapsed} duplicate non-trading snapshot
                            {collapsed > 1 ? 's' : ''}
                          </div>
                        ) : null}
                        <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                          <table className="w-full border-collapse text-xs">
                            <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                              <tr className="text-left">
                                <th className="px-2 py-2">#</th>
                                {dedupedDates.map((d: string) => (
                                  <th key={d} className="px-2 py-2 font-mono">
                                    {String(d).slice(5)}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {Array.from({ length: 5 }).map((_, i) => (
                                <tr key={i} className="border-t border-[var(--k-border)]">
                                  <td className="px-2 py-2 font-mono">{i + 1}</td>
                                  {dedupedDates.map((d: string, j: number) => (
                                    <td key={j} className="px-2 py-2">
                                      {String((map[d] || [])[i] ?? '')}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        {(() => {
                          const flow5d: any = (summary?.industryFundFlow as any)?.flow5d ?? null;
                          const flowDates: string[] = Array.isArray(flow5d?.dates)
                            ? flow5d.dates
                            : [];
                          const cols: string[] = flowDates.length
                            ? flowDates.slice(-5)
                            : dedupedDates;
                          const topRows: any[] = Array.isArray(flow5d?.top) ? flow5d.top : [];
                          if (!topRows.length || !cols.length) return null;
                          const colDates = cols;
                          return (
                            <div className="mt-4">
                              <div className="mb-2 text-xs text-[var(--k-muted)]">
                                5D net inflow (Top by 5D sum)
                              </div>
                              <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                                <table className="w-full border-collapse text-xs">
                                  <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                    <tr className="text-left">
                                      <th className="px-2 py-2">Industry</th>
                                      <th className="px-2 py-2 text-right">Sum(5D)</th>
                                      {colDates.map((d: string) => (
                                        <th key={d} className="px-2 py-2 text-right font-mono">
                                          {String(d).slice(5)}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {topRows.slice(0, 10).map((r: any, idx: number) => {
                                      const seriesArr: any[] = Array.isArray(r?.series)
                                        ? r.series
                                        : [];
                                      const map: Record<string, number> = {};
                                      for (const p of seriesArr) {
                                        const dd = String(p?.date ?? '');
                                        const nv = Number(p?.netInflow ?? 0);
                                        if (dd) map[dd] = Number.isFinite(nv) ? nv : 0;
                                      }
                                      return (
                                        <tr
                                          key={`${String(r?.industryCode ?? 'unknown')}-${idx}`}
                                          className="border-t border-[var(--k-border)]"
                                        >
                                          <td className="px-2 py-2">
                                            {String(r?.industryName ?? '')}
                                          </td>
                                          <td className="px-2 py-2 text-right font-mono">
                                            {fmtAmountCn(r?.sum5d)}
                                          </td>
                                          {colDates.map((d: string) => (
                                            <td key={d} className="px-2 py-2 text-right font-mono">
                                              {fmtAmountCn(map[d] ?? 0)}
                                            </td>
                                          ))}
                                        </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          );
                        })()}
                        {(() => {
                          const flow5dOut: any =
                            (summary?.industryFundFlow as any)?.flow5dOut ?? null;
                          const flowDates: string[] = Array.isArray(flow5dOut?.dates)
                            ? flow5dOut.dates
                            : [];
                          const cols: string[] = flowDates.length
                            ? flowDates.slice(-5)
                            : dedupedDates;
                          const topRows: any[] = Array.isArray(flow5dOut?.top) ? flow5dOut.top : [];
                          if (!topRows.length || !cols.length) return null;
                          const colDates = cols;
                          return (
                            <div className="mt-4">
                              <div className="mb-2 text-xs text-[var(--k-muted)]">
                                5D net outflow (Top by 5D sum)
                              </div>
                              <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                                <table className="w-full border-collapse text-xs">
                                  <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                                    <tr className="text-left">
                                      <th className="px-2 py-2">Industry</th>
                                      <th className="px-2 py-2 text-right">Sum(5D)</th>
                                      {colDates.map((d: string) => (
                                        <th key={d} className="px-2 py-2 text-right font-mono">
                                          {String(d).slice(5)}
                                        </th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {topRows.slice(0, 10).map((r: any, idx: number) => {
                                      const seriesArr: any[] = Array.isArray(r?.series)
                                        ? r.series
                                        : [];
                                      const map: Record<string, number> = {};
                                      for (const p of seriesArr) {
                                        const dd = String(p?.date ?? '');
                                        const nv = Number(p?.netInflow ?? 0);
                                        if (dd) map[dd] = Number.isFinite(nv) ? nv : 0;
                                      }
                                      return (
                                        <tr
                                          key={`${String(r?.industryCode ?? 'unknown')}-${idx}`}
                                          className="border-t border-[var(--k-border)]"
                                        >
                                          <td className="px-2 py-2">
                                            {String(r?.industryName ?? '')}
                                          </td>
                                          <td className="px-2 py-2 text-right font-mono">
                                            {fmtAmountCn(r?.sum5d)}
                                          </td>
                                          {colDates.map((d: string) => (
                                            <td key={d} className="px-2 py-2 text-right font-mono">
                                              {fmtAmountCn(map[d] ?? 0)}
                                            </td>
                                          ))}
                                        </tr>
                                      );
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          );
                        })()}
                        <div className="mt-3 flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => onNavigate?.('industryFlow')}
                          >
                            Open Industry Flow
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => void copyIndustryMarkdown()}
                          >
                            Copy Markdown
                          </Button>
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => {
                              const asOfDate = String(
                                summary?.industryFundFlow?.asOfDate ?? summary?.asOfDate ?? '',
                              );
                              addReference({
                                kind: 'industryFundFlow',
                                refId: `${asOfDate}:5:10`,
                                asOfDate,
                                days: 5,
                                topN: 10,
                                view: 'dailyTopByDate',
                                title: 'CN industry fund flow (Top by date)',
                                createdAt: new Date().toISOString(),
                              } as any);
                            }}
                          >
                            Reference
                          </Button>
                        </div>
                        {industryCopyStatus ? (
                          <div
                            className={`mt-2 text-xs ${
                              industryCopyStatus.ok ? 'text-emerald-600' : 'text-red-600'
                            }`}
                          >
                            {industryCopyStatus.text}
                          </div>
                        ) : null}
                      </>
                    );
                  })()}
                </div>
              ) : id === 'news' ? (
                <div>
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <div className="text-xs text-[var(--k-muted)]">
                      24-hour news summary (AI-generated, finance/stock focused)
                    </div>
                    {newsSummaryUpdatedAt ? (
                      <div className="text-xs text-[var(--k-muted)]">
                        Generated: {fmtDateTime(newsSummaryUpdatedAt)}
                      </div>
                    ) : null}
                  </div>
                  {newsSummaryBusy ? (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      <RefreshCw className="mr-2 inline h-4 w-4 animate-spin" />
                      Generating AI summary...
                    </div>
                  ) : newsSummary || newsFallback ? (
                    <div className="rounded-lg border border-blue-500/30 bg-blue-500/10 p-4 text-sm">
                      {newsSummary?.trim() || newsFallback?.trim()}
                    </div>
                  ) : (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      No summary yet. Click "Sync all" to fetch news and generate summary.
                    </div>
                  )}
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('news')}>
                      Open News
                    </Button>
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={newsSummaryBusy}
                      onClick={() => void regenerateNewsSummary()}
                    >
                      {newsSummaryBusy ? (
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <RefreshCw className="mr-2 h-4 w-4" />
                      )}
                      Regenerate
                    </Button>
                  </div>
                </div>
              ) : id === 'watchlistRisk' ? (
                <div>
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2 text-xs text-[var(--k-muted)]">
                    <span>
                      Intraday &gt;6%、跳空缺口（弱势/震荡）、VWAP 溢价等建仓风险预警
                    </span>
                    <span>
                      {watchlistRiskUpdatedAt
                        ? `Updated ${fmtDateTime(watchlistRiskUpdatedAt)}`
                        : '—'}
                    </span>
                  </div>
                  {watchlistRiskBusy && !watchlistRiskRows.length ? (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      <RefreshCw className="mr-2 inline h-4 w-4 animate-spin" />
                      Loading watchlist risk alerts...
                    </div>
                  ) : watchlistRiskRows.length ? (
                    <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                      <table className="w-full border-collapse text-xs">
                        <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                          <tr className="text-left">
                            <th className="px-2 py-2">Symbol</th>
                            <th className="px-2 py-2">Name</th>
                            <th className="px-2 py-2">Intraday%</th>
                            <th className="px-2 py-2">Gap</th>
                            <th className="px-2 py-2">Alerts</th>
                          </tr>
                        </thead>
                        <tbody>
                          {watchlistRiskRows.map((row) => {
                            const hasBlock = row.alerts.some((a) => a.severity === 'block');
                            return (
                              <tr
                                key={row.symbol}
                                className={`border-t border-[var(--k-border)] ${
                                  hasBlock
                                    ? 'bg-red-50/70'
                                    : 'bg-amber-50/50'
                                }`}
                              >
                                <td className="px-2 py-2 font-mono text-red-700">{row.symbol}</td>
                                <td className="px-2 py-2">{row.name}</td>
                                <td
                                  className={`px-2 py-2 font-mono ${
                                    isIntradaySurge(row.intradayChgPct)
                                      ? 'font-semibold text-red-600'
                                      : ''
                                  }`}
                                >
                                  {formatIntradayChgPct(row.intradayChgPct)}
                                </td>
                                <td
                                  className={`px-2 py-2 font-mono ${
                                    row.gapUp === true ? 'font-semibold text-red-600' : ''
                                  }`}
                                >
                                  {formatGapUp(row.gapUp)}
                                </td>
                                <td className="px-2 py-2">
                                  {row.alerts.map((alert) => (
                                    <div
                                      key={alert.code}
                                      className={
                                        alert.severity === 'block'
                                          ? 'text-red-600'
                                          : 'text-amber-700'
                                      }
                                    >
                                      {alert.message}
                                    </div>
                                  ))}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-[var(--k-border)] bg-[var(--k-surface-2)] p-4 text-sm text-[var(--k-muted)]">
                      No watchlist risk alerts. Add symbols to Watchlist or refresh during session.
                    </div>
                  )}
                  <div className="mt-3 flex items-center gap-2">
                    <Button
                      size="sm"
                      variant="secondary"
                      disabled={watchlistRiskBusy}
                      onClick={() => void refreshWatchlistRisk()}
                    >
                      {watchlistRiskBusy ? (
                        <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      ) : (
                        <RefreshCw className="mr-2 h-4 w-4" />
                      )}
                      Refresh alerts
                    </Button>
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('watchlist')}>
                      Open Watchlist
                    </Button>
                  </div>
                </div>
              ) : id === 'screeners' ? (
                <div>
                  <div className="mb-2 text-xs text-[var(--k-muted)]">
                    Enabled screeners (no content). Missing/rowCount=0 will be highlighted.
                  </div>
                  <div className="overflow-auto rounded-lg border border-[var(--k-border)]">
                    <table className="w-full border-collapse text-xs">
                      <thead className="bg-[var(--k-surface-2)] text-[var(--k-muted)]">
                        <tr className="text-left">
                          <th className="px-2 py-2">Name</th>
                          <th className="px-2 py-2">capturedAt</th>
                          <th className="px-2 py-2 text-right">rows</th>
                          <th className="px-2 py-2 text-right">filters</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(summary?.screeners ?? []).map((s: any) => {
                          const bad = !s.capturedAt || Number(s.rowCount ?? 0) <= 0;
                          return (
                            <tr key={String(s.id)} className="border-t border-[var(--k-border)]">
                              <td className="px-2 py-2">{String(s.name ?? s.id)}</td>
                              <td className={`px-2 py-2 font-mono ${bad ? 'text-red-600' : ''}`}>
                                {String(s.capturedAt ?? '—')}
                              </td>
                              <td
                                className={`px-2 py-2 text-right font-mono ${bad ? 'text-red-600' : ''}`}
                              >
                                {String(s.rowCount ?? 0)}
                              </td>
                              <td className="px-2 py-2 text-right font-mono">
                                {String(s.filtersCount ?? 0)}
                              </td>
                            </tr>
                          );
                        })}
                        {!(summary?.screeners ?? []).length ? (
                          <tr>
                            <td className="px-2 py-3 text-sm text-[var(--k-muted)]" colSpan={4}>
                              No enabled screeners.
                            </td>
                          </tr>
                        ) : null}
                      </tbody>
                    </table>
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <Button size="sm" variant="secondary" onClick={() => onNavigate?.('screener')}>
                      Open Screener
                    </Button>
                  </div>
                </div>
              ) : null}
            </section>
          );
        };

        return (
          <>
            <div className="space-y-4 lg:hidden">{orderedCards.map(renderCard)}</div>
            <div className="hidden lg:grid lg:grid-cols-2 lg:gap-4">
              <div className="space-y-4">{left.map(renderCard)}</div>
              <div className="space-y-4">{right.map(renderCard)}</div>
            </div>
          </>
        );
      })()}

      {editLayout ? (
        <div className="mt-4 text-xs text-[var(--k-muted)]">
          Layout config is saved locally. Drag-and-drop UI can be added later; for now use ↑/↓.
        </div>
      ) : null}
    </div>
  );
}
