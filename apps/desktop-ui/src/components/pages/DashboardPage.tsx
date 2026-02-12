/* eslint-disable @typescript-eslint/no-explicit-any */
'use client';

import * as React from 'react';
import { RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { useChatStore } from '@/lib/chat/store';
import { loadJson } from '@/lib/storage';

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

function fmtPerfLine(r: any): string {
  const p = Number(r?.todayChangePct);
  if (Number.isFinite(p)) {
    const sign = p > 0 ? '+' : '';
    return `${sign}${p.toFixed(2)}%`;
  }
  return '—';
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

function mdBool(v: boolean | null | undefined): string {
  if (v == null) return '—';
  return v ? '✅' : '❌';
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

function mdScoreParts(parts: Record<string, number> | undefined): string[] {
  if (!parts) return [];
  const entries = Object.entries(parts).filter(([, v]) => typeof v === 'number' && Number.isFinite(v));
  entries.sort((a, b) => Number(b[1]) - Number(a[1]));
  return entries.map(([k, v]) => `- ${k}: ${v}`);
}

const WATCHLIST_STORAGE_KEY = 'karios.watchlist.v1';

type WatchlistItem = {
  symbol: string;
  name?: string | null;
  addedAt: string;
  color?: string;
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
  checks?: Record<string, unknown> | null;
  values?: Record<string, unknown> | null;
  missingData?: string[];
};

type QuoteResp = {
  ok: boolean;
  error?: string;
  items: Array<{
    ts_code: string;
    price: string | null;
    trade_time: string | null;
  }>;
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

function tradeDateFromTradeTime(tradeTime: string | null | undefined): string | null {
  const s = String(tradeTime ?? '').trim();
  if (!s) return null;
  const m1 = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m1) return m1[1];
  const m2 = s.match(/^(\d{8})$/);
  if (m2) return `${m2[1].slice(0, 4)}-${m2[1].slice(4, 6)}-${m2[1].slice(6, 8)}`;
  return null;
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



export function DashboardPage({
  onNavigate,
}: {
  onNavigate?: (pageId: string) => void;
}) {
  const { addReference } = useChatStore();
  const [summary, setSummary] = React.useState<DashboardSummary | null>(null);
  const [syncResp, setSyncResp] = React.useState<DashboardSyncResp | null>(null);
  const [busy, setBusy] = React.useState(false);
  const [sentimentBusy, setSentimentBusy] = React.useState(false);
  const [industryCopyStatus, setIndustryCopyStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [sentimentCopyStatus, setSentimentCopyStatus] = React.useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [copyAllBusy, setCopyAllBusy] = React.useState(false);
  const [copyAllStatus, setCopyAllStatus] = React.useState<{ ok: boolean; text: string } | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [editLayout, setEditLayout] = React.useState(false);

  const industryCopyTimerRef = React.useRef<number | null>(null);
  const sentimentCopyTimerRef = React.useRef<number | null>(null);
  const copyAllTimerRef = React.useRef<number | null>(null);
  React.useEffect(() => {
    return () => {
      if (industryCopyTimerRef.current != null) window.clearTimeout(industryCopyTimerRef.current);
      if (sentimentCopyTimerRef.current != null) window.clearTimeout(sentimentCopyTimerRef.current);
      if (copyAllTimerRef.current != null) window.clearTimeout(copyAllTimerRef.current);
    };
  }, []);

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

  const defaultCards = React.useMemo(
    () => [
      { id: 'industry', title: 'Industry fund flow' },
      { id: 'sentiment', title: 'Market sentiment' },
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

  const refresh = React.useCallback(async () => {
    setError(null);
    try {
      const s = await apiGetJson<DashboardSummary>(`/dashboard/summary`);
      setSummary(s);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  React.useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onSyncAll() {
    setBusy(true);
    setError(null);
    try {
      const r = await apiPostJson<DashboardSyncResp>('/dashboard/sync?force=true', {});
      setSyncResp(r);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function onSyncSentiment() {
    setSentimentBusy(true);
    setError(null);
    try {
      await apiPostJson('/market/cn/sentiment/sync', { force: true });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSentimentBusy(false);
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
    lines.push(`${heading} Market sentiment`);
    if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
    if (latest) {
      const risk = String(latest?.riskMode ?? '');
      if (risk) lines.push(`- risk: ${risk}`);
    const total = fmtAmountCn(latest?.marketTurnoverCny);
    if (total && total !== '—') lines.push(`- totalTurnover: ${total}`);
      const rules = Array.isArray(latest?.rules) ? latest.rules.map((x: any) => String(x)).filter(Boolean) : [];
      if (rules.length) lines.push(`- rules: ${rules.slice(0, 6).join(' • ')}${rules.length > 6 ? '…' : ''}`);
    }
    lines.push('');

  if (indexSignals.length) {
    const headers0 = ['Index', 'Signal', 'Position', 'Close', 'MA5', 'MA20', 'AsOfDate'];
    const rows0: unknown[][] = indexSignals.map((it: any) => [
      String(it?.name ?? it?.tsCode ?? ''),
      String(it?.signal ?? ''),
      String(it?.positionRange ?? ''),
      Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—',
      Number.isFinite(it?.ma5) ? Number(it.ma5).toFixed(2) : '—',
      Number.isFinite(it?.ma20) ? Number(it.ma20).toFixed(2) : '—',
      String(it?.asOfDate ?? ''),
    ]);
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
      Number.isFinite(it?.yesterdayLimitUpPremium) ? `${Number(it.yesterdayLimitUpPremium).toFixed(2)}%` : '—',
      Number.isFinite(it?.failedLimitUpRate) ? `${Number(it.failedLimitUpRate).toFixed(1)}%` : '—',
      String(it?.riskMode ?? ''),
    ]);
    lines.push(mdTable(headers, rows));
    lines.push('');
    return lines.join('\n').trim() + '\n';
  }

  async function buildScreenersMarkdown(s: DashboardSummary | null, heading = '##'): Promise<string> {
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

    // Also include latest snapshot tables (DB content) for each screener.
    for (const sc of rows) {
      const sid = String(sc?.id ?? '').trim();
      if (!sid) continue;
      try {
        const list = await apiGetJson<{ items: Array<{ id: string; capturedAt?: string; rowCount?: number }> }>(
          `/integrations/tradingview/screeners/${encodeURIComponent(sid)}/snapshots?limit=1`,
        );
        const snapId = String(list?.items?.[0]?.id ?? '').trim();
        if (!snapId) continue;
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

        const title = String(snap?.screenTitle ?? sc?.name ?? sid).trim() || sid;
        const capturedAt = String(snap?.capturedAt ?? '').trim();
        const headersTv: string[] = Array.isArray(snap?.headers) ? snap.headers.map((h) => String(h ?? '')) : [];
        const rowsTv: Array<Record<string, string>> = Array.isArray(snap?.rows) ? snap.rows : [];
        const limit = 50;
        const truncated = rowsTv.length > limit;
        const bodyRows: unknown[][] = rowsTv.slice(0, limit).map((r) => headersTv.map((h) => String(r?.[h] ?? '')));

        lines.push(`${heading}# ${escapeMarkdownCell(title)}`);
        if (capturedAt) lines.push(`- capturedAt: ${capturedAt}`);
        lines.push(`- rows: ${String(snap?.rowCount ?? rowsTv.length ?? 0)}`);
        if (Array.isArray(snap?.filters) && snap.filters.length) {
          lines.push(`- filters: ${snap.filters.slice(0, 8).map((x) => escapeMarkdownCell(String(x))).join(' • ')}${snap.filters.length > 8 ? '…' : ''}`);
        }
        if (truncated) lines.push(`- note: showing first ${limit} rows (truncated)`);
        lines.push('');
        if (headersTv.length) lines.push(mdTable(headersTv, bodyRows));
        else lines.push('_No headers._');
        lines.push('');
      } catch (e) {
        lines.push(`${heading}# ${escapeMarkdownCell(String(sc?.name ?? sid))}`);
        lines.push(`- error: ${escapeMarkdownCell(e instanceof Error ? e.message : String(e))}`);
        lines.push('');
      }
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

    // 1) TrendOK
    const trend: Record<string, TrendOkResult> = {};
    for (const part of chunk(syms, 200)) {
      const sp = new URLSearchParams();
      sp.set('refresh', 'true');
      sp.set('realtime', tradingTime ? 'true' : 'false');
      for (const s of part) sp.append('symbols', s);
      const trendRows = await apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
      for (const r of Array.isArray(trendRows) ? trendRows : []) {
        if (r && r.symbol) trend[String(r.symbol).toUpperCase()] = r;
      }
    }

    // 2) Quotes (CN only)
    const byTsCode = new Map<string, string>();
    const tsCodes = syms
      .map((s) => {
        const t = toTsCodeFromSymbol(s);
        if (t) byTsCode.set(t, s);
        return t;
      })
      .filter(Boolean) as string[];

    const quotes: Record<string, { price: number | null; tradeTime: string | null }> = {};
    for (const part of chunk(tsCodes, 50)) {
      const r = await apiGetJson<QuoteResp>(`/quote?ts_codes=${encodeURIComponent(part.join(','))}`).catch(
        () => null,
      );
      for (const it of r?.items ?? []) {
        const sym = byTsCode.get(it.ts_code);
        if (!sym) continue;
        const p = it.price != null ? Number(it.price) : NaN;
        quotes[sym] = {
          price: Number.isFinite(p) ? p : null,
          tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
        };
      }
    }

    // 3) Sort by score desc (unknown scores at bottom)
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

    // 4) Strict validation (same spirit as Watchlist page).
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
      if (tradingTime && sym.startsWith('CN:')) {
        const q = quotes[sym];
        const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
        if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
          missingRealtime.push(sym);
        }
      }
    }
    if (missingTrend.length || missingHistory.length || missingRealtime.length) {
      const parts: string[] = [];
      if (missingRealtime.length) parts.push(`missing realtime quote (today): ${missingRealtime.slice(0, 6).join(', ')}${missingRealtime.length > 6 ? '…' : ''}`);
      if (missingHistory.length) parts.push(`missing history/indicators: ${missingHistory.slice(0, 6).join(', ')}${missingHistory.length > 6 ? '…' : ''}`);
      if (missingTrend.length) parts.push(`missing TrendOK result: ${missingTrend.slice(0, 6).join(', ')}${missingTrend.length > 6 ? '…' : ''}`);
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

    const headers = ['Symbol', 'Name', 'Score', 'TrendOK', 'Buy', 'Current', 'StopLoss', 'AsOfDate'];
    const rows: unknown[][] = [];
    for (const it of sorted) {
      const t = trend[it.symbol];
      const q = quotes[it.symbol];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      const close0 = (t?.values as any)?.close;
      const current =
        q?.price ?? (typeof close0 === 'number' && Number.isFinite(close0) ? (close0 as number) : null);
      const asOf = tradingTime && qDate ? qDate : String(t?.asOfDate ?? '');
      const buy =
        t?.buyAction && t?.buyMode ? `${String(t.buyMode)}/${String(t.buyAction)}` : t?.buyAction ? String(t.buyAction) : '—';
      rows.push([
        it.symbol,
        it.name ?? t?.name ?? '—',
        mdScore(t?.score ?? null),
        mdBool(t?.trendOk ?? null),
        buy,
        mdPrice(typeof current === 'number' ? current : null),
        mdPrice(t?.stopLossPrice ?? null),
        asOf,
      ]);
    }
    lines.push(mdTable(headers, rows));
    lines.push('');

    for (const it of sorted) {
      const t = trend[it.symbol];
      const q = quotes[it.symbol];
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      lines.push(`${heading}# ${escapeMarkdownCell(it.symbol)}${it.name ? ` ${escapeMarkdownCell(it.name)}` : ''}`);
      if (it.color) lines.push(`- color: ${String(it.color)}`);
      if (qDate) lines.push(`- quoteDate: ${qDate}`);
      if (q?.tradeTime) lines.push(`- quoteTradeTime: ${String(q.tradeTime)}`);
      if (typeof q?.price === 'number' && Number.isFinite(q.price)) lines.push(`- current(realtime): ${mdPrice(q.price)}`);
      lines.push(`- trendOk: ${mdBool(t?.trendOk ?? null)}`);
      lines.push(`- score: ${mdScore(t?.score ?? null)}`);
      if (t?.asOfDate) lines.push(`- asOfDate: ${String(t.asOfDate)}`);
      if (t?.buyMode || t?.buyAction) lines.push(`- buy: ${String(t?.buyMode ?? '')} / ${String(t?.buyAction ?? '')}`);
      if (typeof t?.stopLossPrice === 'number') lines.push(`- stopLossPrice: ${mdNum(t.stopLossPrice, 2)}`);

      const parts = mdScoreParts(t?.scoreParts);
      if (parts.length) {
        lines.push('');
        lines.push('Score parts:');
        lines.push(mdLines(parts));
      }

      const missing = (t?.missingData ?? []).filter(Boolean);
      if (missing.length) {
        lines.push('');
        lines.push(`Missing data: ${missing.map((x) => escapeMarkdownCell(String(x))).join(', ')}`);
      }
      lines.push('');
    }

    return lines.join('\n').trim() + '\n';
  }

  async function copyAllMarkdown() {
    setCopyAllBusy(true);
    setError(null);
    try {
      const s = await apiGetJson<DashboardSummary>(`/dashboard/summary`);
      setSummary(s);
      const generatedAt = new Date().toISOString();
      const lines: string[] = [];
      lines.push(`# Copy all (Dashboard)`);
      lines.push(`- generatedAt: ${generatedAt}`);
      lines.push(`- asOfDate: ${String((s as any)?.asOfDate ?? '')}`);
      lines.push('');
      lines.push(buildIndustryMarkdown(s, '##').trim());
      lines.push('');
      lines.push(buildSentimentMarkdown(s, '##').trim());
      lines.push('');
      lines.push((await buildScreenersMarkdown(s, '##')).trim());
      lines.push('');
      lines.push((await buildWatchlistMarkdown()).trim());
      lines.push('');
      await navigator.clipboard.writeText(lines.join('\n').trim() + '\n');
      toastCopyAll(true, 'Copied all Markdown to clipboard.');
    } catch (e) {
      toastCopyAll(false, e instanceof Error ? e.message : String(e));
    } finally {
      setCopyAllBusy(false);
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
            disabled={busy || copyAllBusy}
            onClick={() => void refresh()}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
          <Button
            variant="secondary"
            size="sm"
            className="gap-2"
            disabled={busy || copyAllBusy}
            onClick={() => void copyAllMarkdown()}
          >
            {copyAllBusy ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            Copy all Markdown
          </Button>
          <Button size="sm" className="gap-2" disabled={busy} onClick={() => void onSyncAll()}>
            {busy ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            {busy ? 'Syncing…' : 'Sync all (force)'}
          </Button>
          <Button size="sm" variant="secondary" onClick={() => setEditLayout((v) => !v)}>
            {editLayout ? 'Done' : 'Edit layout'}
          </Button>
        </div>
      </div>

      <div className="mb-4 text-xs text-[var(--k-muted)]">
        asOfDate: <span className="font-mono">{summary?.asOfDate ?? '—'}</span>
      </div>
      {copyAllStatus ? (
        <div className={`mb-4 text-xs ${copyAllStatus.ok ? 'text-emerald-600' : 'text-red-600'}`}>
          {copyAllStatus.text}
        </div>
      ) : null}

      {error ? (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-600">
          {error}
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
                    const indexSignals: any[] = Array.isArray(ms.indexSignals) ? ms.indexSignals : [];
                    const risk = String(latest?.riskMode ?? '—');
                    const premium = Number.isFinite(latest?.yesterdayLimitUpPremium)
                      ? `${Number(latest.yesterdayLimitUpPremium).toFixed(2)}%`
                      : '—';
                    const failed = Number.isFinite(latest?.failedLimitUpRate)
                      ? `${Number(latest.failedLimitUpRate).toFixed(1)}%`
                      : '—';
                    const turnover = fmtAmountCn(latest?.marketTurnoverCny);
                    const totalVolume = fmtAmountCn(latest?.marketTurnoverCny);
                    const ratio = Number.isFinite(latest?.upDownRatio)
                      ? Number(latest.upDownRatio).toFixed(2)
                      : '—';
                    const up = Number(latest?.upCount ?? 0);
                    const down = Number(latest?.downCount ?? 0);
                    const flat = Number(latest?.flatCount ?? 0);
                    const badge =
                      risk === 'no_new_positions'
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
                            <div className="mt-1 text-xs text-[var(--k-muted)]">
                              total volume: {totalVolume}
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
                            <div className="mb-2 text-xs text-[var(--k-muted)]">Index traffic lights</div>
                            <div className="grid gap-2 md:grid-cols-2">
                              {indexSignals.map((it: any) => {
                                const signal = String(it?.signal ?? 'unknown');
                                const badge =
                                  signal === 'green'
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
                                      close {Number.isFinite(it?.close) ? Number(it.close).toFixed(2) : '—'} • MA20{' '}
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
                                  .catch(() => toastSentimentCopy(false, 'Copy failed. Please allow clipboard access.'));
                              } catch (e) {
                                toastSentimentCopy(false, e instanceof Error ? e.message : String(e));
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
                        lines.push(`# Industry fund flow${asOfDate ? ` (asOfDate: ${asOfDate})` : ''}`);
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
                        const flowDates: string[] = Array.isArray(flow5d?.dates) ? flow5d.dates : [];
                        const colDates: string[] = flowDates.length ? flowDates.slice(-5) : dedupedDates;
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
                        const flow5dOut: any = (summary?.industryFundFlow as any)?.flow5dOut ?? null;
                        const outDates: string[] = Array.isArray(flow5dOut?.dates) ? flow5dOut.dates : [];
                        const outColDates: string[] = outDates.length ? outDates.slice(-5) : dedupedDates;
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
                                          key={`${String(r?.industryCode ?? idx)}`}
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
                          const flow5dOut: any = (summary?.industryFundFlow as any)?.flow5dOut ?? null;
                          const flowDates: string[] = Array.isArray(flow5dOut?.dates)
                            ? flow5dOut.dates
                            : [];
                          const cols: string[] = flowDates.length ? flowDates.slice(-5) : dedupedDates;
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
                                          key={`${String(r?.industryCode ?? idx)}`}
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
                          <Button size="sm" variant="secondary" onClick={() => void copyIndustryMarkdown()}>
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
