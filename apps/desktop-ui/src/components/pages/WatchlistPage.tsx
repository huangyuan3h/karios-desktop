'use client';

import * as React from 'react';
import { ArrowDown, ArrowUp, ArrowUpDown, CircleX, ExternalLink, Info, RefreshCw, Trash2 } from 'lucide-react';
import { createPortal } from 'react-dom';

import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { loadJson, saveJson } from '@/lib/storage';
import { useChatStore } from '@/lib/chat/store';

type WatchlistItem = {
  symbol: string; // e.g. "CN:600000" or "HK:0700"
  name?: string | null;
  nameStatus?: 'resolved' | 'not_found';
  addedAt: string; // ISO
  color?: string; // hex color for lightweight flag, default white (#ffffff)
  positionPct?: number | null; // 0..100 (%)
  costPrice?: number | null;
  maxPrice?: number | null;
};

const STORAGE_KEY = 'karios.watchlist.v1';
const COST_PRICE_RE = /^\d+(\.\d{0,2})?$/;

const FLAG_COLORS: Array<{ label: string; hex: string }> = [
  { label: 'White', hex: '#ffffff' },
  { label: 'Red', hex: '#fee2e2' },
  { label: 'Orange', hex: '#ffedd5' },
  { label: 'Yellow', hex: '#fef9c3' },
  { label: 'Green', hex: '#dcfce7' },
  { label: 'Blue', hex: '#dbeafe' },
  { label: 'Purple', hex: '#f3e8ff' },
  { label: 'Gray', hex: '#f4f4f5' },
];

function escapeMarkdownCell(value: string): string {
  return String(value ?? '').replace(/\|/g, '\\|').replace(/\r?\n/g, '<br>').trim();
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

function VisibilitySection({
  visible,
  className,
  children,
}: {
  visible: boolean;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={className} style={{ display: visible ? 'block' : 'none' }} aria-hidden={!visible}>
      {children}
    </div>
  );
}

const TREND_OK_CHECKS: Array<{ key: keyof TrendOkChecks; failText: string }> = [
  { key: 'emaOrder', failText: 'EMA order broken (Close <= EMA20 or EMA20 <= EMA60)' },
  { key: 'macdPositive', failText: 'MACD <= 0' },
  { key: 'macdHistExpanding', failText: 'MACD hist <= 0' },
  { key: 'closeNear20dHigh', failText: 'Close < 0.95 * High(20)' },
  { key: 'rsiInRange', failText: 'RSI(14) out of 50..82' },
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
    '- Close >= 0.95 * High(20)',
    '- RSI(14) in [50, 82]',
    '- AvgVol(5) >= 0.9 * AvgVol(30)',
  ];
}

function scoreRuleLines(): string[] {
  return [
    '- Deterministic 0–100 score (CN daily, no LLM).',
    '- Subscores: EMA trend 25%, MACD strength 15%, breakout 25%, RSI 15%, volume 20%.',
    '- Bonus: +3 when Close >= High(20).',
    '- Penalties: high ATR/close (>3%) and Close < EMA20.',
    '- Optional industry flow adjustment when available.',
  ];
}

type MarketStockBasicRow = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
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

type TrendOkChecks = {
  emaOrder?: boolean | null;
  macdPositive?: boolean | null;
  macdHistExpanding?: boolean | null;
  closeNear20dHigh?: boolean | null;
  rsiInRange?: boolean | null;
  volumeSurge?: boolean | null;
};

type TrendOkValues = {
  close?: number | null;
  ema5?: number | null;
  ema20?: number | null;
  ema60?: number | null;
  macd?: number | null;
  macdSignal?: number | null;
  macdHist?: number | null;
  macdHist4?: number[];
  rsi14?: number | null;
  high20?: number | null;
  avgVol5?: number | null;
  avgVol30?: number | null;
};

type TrendOkResult = {
  symbol: string;
  name?: string | null;
  asOfDate?: string | null;
  trendOk?: boolean | null;
  score?: number | null; // 0..100, formula-based (no LLM)
  scoreParts?: Record<string, number>; // points breakdown (positive parts and penalties)
  stopLossPrice?: number | null;
  stopLossParts?: Record<string, unknown>;
  buyMode?: string | null;
  buyAction?: string | null;
  buyZoneLow?: number | null;
  buyZoneHigh?: number | null;
  buyRefPrice?: number | null;
  buyWhy?: string | null;
  buyChecks?: Record<string, unknown>;
  checks?: TrendOkChecks;
  values?: TrendOkValues;
  missingData?: string[];
};

type ScreenerImportDebugState = {
  updatedAt: string | null;
  scanned: number;
  trendOkCount: number;
  rows: TrendOkResult[];
};

type TvScreener = {
  id: string;
  name: string;
  url: string;
  enabled: boolean;
  updatedAt: string;
};

type TvSnapshotSummary = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
};

type TvSnapshotDetail = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  filters: string[];
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

async function apiGetJson<T>(path: string): Promise<T> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiGetJsonFrom<T>(baseUrl: string, path: string): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`, { cache: 'no-store' });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

async function apiPostJsonFrom<T>(baseUrl: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

function normalizeSymbolInput(input: string): { symbol: string } | { error: string } {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return { error: 'Empty input' };

  // Accept already-normalized market prefix forms.
  // Examples: "CN:600000", "HK:0700"
  if (/^(CN|HK):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  // CN A-share ticker (6 digits)
  if (/^\d{6}$/.test(raw)) {
    return { symbol: `CN:${raw}` };
  }

  // HK ticker (4-5 digits), allow leading zeros
  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return {
    error:
      'Unsupported code format. Use 6-digit CN ticker, 4-5 digit HK ticker, or CN:/HK: prefixed symbol.',
  };
}

function normalizeScreenerSymbol(raw: string): string | null {
  const s = String(raw || '')
    .trim()
    .toUpperCase();
  if (!s) return null;

  // Try the same rules as manual input first.
  const parsed = normalizeSymbolInput(s);
  if (!('error' in parsed)) return parsed.symbol;

  // TradingView forms like "SSE:600000" / "SZSE:000001" / "HKEX:0700"
  const m = s.match(/^[A-Z]+:(\d{4,6})$/);
  if (m) {
    const code = m[1];
    if (/^\d{6}$/.test(code)) return `CN:${code}`;
    if (/^\d{4,5}$/.test(code)) return `HK:${code.padStart(4, '0')}`;
  }
  return null;
}

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function toTsCodeFromSymbol(symbol: string): string | null {
  // Only handle CN A-shares for now: "CN:000001" -> "000001.SZ/SH"
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

function fmtPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

function fmtScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

function fmtNum(v: unknown, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

function fmtBuyCell(t: TrendOkResult | undefined | null): {
  text: string;
  tone: 'buy' | 'wait' | 'avoid' | 'none';
} {
  if (!t || !t.buyMode || !t.buyAction) return { text: '—', tone: 'none' };
  if (t.buyAction === 'avoid') return { text: '回避', tone: 'avoid' };
  const zl = typeof t.buyZoneLow === 'number' ? t.buyZoneLow : null;
  const zh = typeof t.buyZoneHigh === 'number' ? t.buyZoneHigh : null;
  const zone =
    zl != null && zh != null
      ? `${zl.toFixed(2)}–${zh.toFixed(2)}`
      : zl != null
        ? `${zl.toFixed(2)}`
        : '—';
  if (t.buyMode === 'A_pullback') {
    const prefix = t.buyAction === 'buy' ? 'A 买' : 'A 等';
    return { text: `${prefix} 回踩 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  if (t.buyMode === 'B_momentum') {
    const prefix = t.buyAction === 'buy' ? 'B 买' : 'B 等';
    return { text: `${prefix} 新高 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  return { text: '无', tone: 'none' };
}

function rowTone(t: TrendOkResult | undefined | null): 'green' | 'red' | 'none' {
  if (!t) return 'none';
  const stopParts = t.stopLossParts as Record<string, unknown> | null | undefined;
  const exitNow = Boolean(stopParts && typeof stopParts === 'object' && stopParts['exit_now']);
  if (exitNow || t.buyAction === 'avoid') return 'red';
  const score = typeof t.score === 'number' && Number.isFinite(t.score) ? t.score : null;
  const buyModeOk = t.buyMode === 'A_pullback' || t.buyMode === 'B_momentum';
  if (t.trendOk === true && t.buyAction === 'buy' && buyModeOk && score != null && score >= 85) {
    return 'green';
  }
  return 'none';
}

export function WatchlistPage({ onOpenStock }: { onOpenStock?: (symbol: string) => void } = {}) {
  const { addReference } = useChatStore();
  const [items, setItems] = React.useState<WatchlistItem[]>([]);
  const [code, setCode] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const [trend, setTrend] = React.useState<Record<string, TrendOkResult>>({});
  const [quotes, setQuotes] = React.useState<
    Record<string, { price: number | null; tsCode: string; tradeTime: string | null }>
  >({});
  const [trendBusy, setTrendBusy] = React.useState(false);
  const [trendUpdatedAt, setTrendUpdatedAt] = React.useState<string | null>(null);
  const [syncBusy, setSyncBusy] = React.useState(false);
  const [syncMsg, setSyncMsg] = React.useState<string | null>(null);
  const [syncStage, setSyncStage] = React.useState<string | null>(null);
  const [syncProgress, setSyncProgress] = React.useState<{ cur: number; total: number } | null>(null);
  const [syncLogs, setSyncLogs] = React.useState<string[]>([]);
  const [copyMdStatus, setCopyMdStatus] = React.useState<{ ok: boolean; text: string } | null>(null);
  const copyMdTimerRef = React.useRef<number | null>(null);

  // Keep the last screener import inspection table visible for manual follow-ups.
  const [importDebugOpen, setImportDebugOpen] = React.useState(true);
  const [importDebugFilter, setImportDebugFilter] = React.useState('');
  const [importDebugScoreSortDir, setImportDebugScoreSortDir] = React.useState<'desc' | 'asc'>('desc');
  const [importDebug, setImportDebug] = React.useState<ScreenerImportDebugState>({
    updatedAt: null,
    scanned: 0,
    trendOkCount: 0,
    rows: [],
  });

  const [scoreSortDir, setScoreSortDir] = React.useState<'desc' | 'asc'>('desc');
  const [scoreSortEnabled, setScoreSortEnabled] = React.useState(true);
  const [costPriceDrafts, setCostPriceDrafts] = React.useState<Record<string, string>>({});
  const nameBySymbol = React.useMemo(() => {
    const map = new Map<string, string>();
    for (const it of items) {
      if (it.symbol) map.set(it.symbol, it.name || '');
    }
    return map;
  }, [items]);
  const [tooltip, setTooltip] = React.useState<{
    open: boolean;
    x: number;
    y: number;
    w: number;
    placement: 'top-end' | 'bottom-end';
    content: React.ReactNode;
  }>({ open: false, x: 0, y: 0, w: 0, placement: 'top-end', content: null });

  const [colorPicker, setColorPicker] = React.useState<{
    open: boolean;
    x: number;
    y: number;
    placement: 'top-end' | 'bottom-end';
    symbol: string | null;
  }>({ open: false, x: 0, y: 0, placement: 'bottom-end', symbol: null });

  const trendReqRef = React.useRef(0);

  React.useEffect(() => {
    const saved = loadJson<WatchlistItem[]>(STORAGE_KEY, []);
    // Backward-compatible migration: drop deprecated fields (e.g. note).
    const arr = Array.isArray(saved) ? saved : [];
    const migrated: WatchlistItem[] = arr
      .filter((x) => x && typeof x === 'object')
      .map((x) => {
        const it = x as Partial<WatchlistItem> & { note?: unknown };
        const rawColor = typeof it.color === 'string' ? it.color.trim().toLowerCase() : '';
        const color = FLAG_COLORS.some((c) => c.hex === rawColor) ? rawColor : '#ffffff';
        return {
          symbol: String(it.symbol ?? '').trim(),
          name: it.name ?? null,
          nameStatus:
            it.nameStatus === 'resolved' || it.nameStatus === 'not_found'
              ? it.nameStatus
              : undefined,
          addedAt: String(it.addedAt ?? new Date().toISOString()),
          color,
          positionPct:
            typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
              ? Math.max(0, Math.min(100, it.positionPct))
              : null,
          costPrice:
            typeof it.costPrice === 'number' && Number.isFinite(it.costPrice) ? it.costPrice : null,
          maxPrice:
            typeof it.maxPrice === 'number' && Number.isFinite(it.maxPrice) ? it.maxPrice : null,
        };
      })
      .filter((x) => Boolean(x.symbol));
    setItems(migrated);
    saveJson(STORAGE_KEY, migrated);
  }, []);

  React.useEffect(
    () => () => {
      if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    },
    [],
  );

  function persist(next: WatchlistItem[]) {
    setItems(next);
    saveJson(STORAGE_KEY, next);
  }

  React.useEffect(() => {
    let cancelled = false;
    async function resolveMissingNames() {
      const missing = items
        .filter((x) => !x.name && x.nameStatus !== 'not_found')
        .map((x) => x.symbol);
      if (!missing.length) return;

      try {
        const sp = new URLSearchParams();
        for (const s of missing) sp.append('symbols', s);
        const rows = await apiGetJsonFrom<MarketStockBasicRow[]>(
          DATA_SYNC_BASE_URL,
          `/market/stocks/resolve?${sp.toString()}`,
        );
        if (cancelled) return;
        const bySym = new Map<string, MarketStockBasicRow>();
        for (const r of Array.isArray(rows) ? rows : []) bySym.set(r.symbol, r);

        const next = items.map((it) => {
          if (it.name || it.nameStatus === 'resolved') return it;
          const hit = bySym.get(it.symbol);
          if (hit) return { ...it, name: hit.name, nameStatus: 'resolved' as const };
          if (missing.includes(it.symbol)) return { ...it, nameStatus: 'not_found' as const };
          return it;
        });
        persist(next);
      } catch (e) {
        // If Market is not synced or service is unavailable, keep silent; user can still manage codes.
        if (!cancelled) console.warn('Watchlist name resolve failed:', e);
      }
    }
    void resolveMissingNames();
    return () => {
      cancelled = true;
    };
  }, [items]);

  const refreshTrend = React.useCallback(
    async (reason: 'items_changed' | 'manual' | 'timer', opts: { forceMarket?: boolean } = {}) => {
      const syms = items.map((x) => x.symbol).filter(Boolean);
      if (!syms.length) {
        setTrend({});
        setQuotes({});
        setTrendUpdatedAt(null);
        return;
      }

      const reqId = (trendReqRef.current += 1);
      setTrendBusy(true);
      try {
        // If requested, force-refresh latest daily bars (and optional chips) from network first.
        if (opts.forceMarket) {
          // Keep it lightweight: daily bars are sufficient for score/trend/buy/stoploss.
          // Also, do sequential requests to avoid spiky traffic / upstream throttling.
          let failures = 0;
          for (const sym of syms) {
            const enc = encodeURIComponent(sym);
            const ok = await apiGetJsonFrom(
              DATA_SYNC_BASE_URL,
              `/market/stocks/${enc}/bars?days=60&force=true`,
            )
              .then(() => true)
              .catch(() => false);
            if (!ok) failures += 1;
            await new Promise((r) => window.setTimeout(r, 120));
          }
          if (reason === 'manual' && failures > 0) {
            setSyncMsg(`Network sync failed for ${failures}/${syms.length} symbols; using cached data.`);
          }
        }

        const sp = new URLSearchParams();
        // Always request a best-effort refresh so Watchlist is based on the latest daily bar.
        // The backend will fall back to cache if upstream is blocked.
        sp.set('refresh', 'true');
        sp.set('realtime', isShanghaiTradingTime() ? 'true' : 'false');
        for (const s of syms) sp.append('symbols', s);
        const rows = await apiGetJsonFrom<TrendOkResult[]>(
          DATA_SYNC_BASE_URL,
          `/market/stocks/trendok?${sp.toString()}`,
        );
        if (reqId !== trendReqRef.current) return;
        const next: Record<string, TrendOkResult> = {};
        for (const r of Array.isArray(rows) ? rows : []) {
          if (r && r.symbol) next[r.symbol] = r;
        }
        setTrend(next);
        setTrendUpdatedAt(new Date().toISOString());

        // Best-effort realtime quotes (CN only) for the "Current" column.
        try {
          const cn = syms.map(toTsCodeFromSymbol).filter(Boolean) as string[];
          const byTsCode = new Map<string, string>();
          for (const s of syms) {
            const tsCode = toTsCodeFromSymbol(s);
            if (tsCode) byTsCode.set(tsCode, s);
          }
          const nextQuotes: Record<string, { price: number | null; tsCode: string; tradeTime: string | null }> = {};
          for (const part of chunk(cn, 50)) {
            const r = await apiGetJsonFrom<QuoteResp>(
              DATA_SYNC_BASE_URL,
              `/quote?ts_codes=${encodeURIComponent(part.join(','))}`,
            ).catch(() => null);
            for (const it of r?.items ?? []) {
              const sym = byTsCode.get(it.ts_code);
              if (!sym) continue;
              const p = it.price != null ? Number(it.price) : NaN;
              nextQuotes[sym] = {
                tsCode: it.ts_code,
                price: Number.isFinite(p) ? p : null,
                tradeTime: typeof it.trade_time === 'string' ? it.trade_time : null,
              };
            }
          }
          if (reqId === trendReqRef.current) {
            setQuotes(nextQuotes);
            const nextItems = items.map((it) => {
              if (!(it.positionPct && it.positionPct > 0)) return it;
              if (!it.costPrice) return it;
              const q = nextQuotes[it.symbol];
              const price =
                (typeof q?.price === 'number' && Number.isFinite(q.price))
                  ? q.price
                  : (typeof next[it.symbol]?.values?.close === 'number' ? next[it.symbol]?.values?.close : null);
              if (price == null) return it;
              const maxPrice = typeof it.maxPrice === 'number' ? it.maxPrice : 0;
              if (price > maxPrice) return { ...it, maxPrice: price };
              if (!it.maxPrice) return { ...it, maxPrice: price };
              return it;
            });
            if (nextItems.some((x, i) => x.maxPrice !== items[i]?.maxPrice)) {
              persist(nextItems);
            }
          }
        } catch {
          // ignore quote failures
        }

        if (reason === 'manual') setError(null);
      } catch (e) {
        if (reqId === trendReqRef.current) console.warn('Watchlist trendok load failed:', e);
      } finally {
        if (reqId === trendReqRef.current) setTrendBusy(false);
      }
    },
    [items],
  );

  React.useEffect(() => {
    void refreshTrend('items_changed');
  }, [refreshTrend]);

  React.useEffect(() => {
    // Auto refresh every 10 minutes to reflect DB updates without reloading the app.
    if (!items.length) return;
    const id = window.setInterval(() => {
      // Auto refresh only recomputes from cache; manual refresh can force network sync.
      void refreshTrend('timer', { forceMarket: false });
    }, 10 * 60 * 1000);
    return () => window.clearInterval(id);
  }, [items.length, refreshTrend]);

  function addSymbolToWatchlist(symRaw: string) {
    setError(null);
    setSyncMsg(null);
    const parsed = normalizeSymbolInput(symRaw);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) return;
    const next: WatchlistItem[] = [
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
        color: '#ffffff',
      },
      ...items,
    ];
    persist(next);
  }

  function onAdd() {
    setError(null);
    setSyncMsg(null);
    const parsed = normalizeSymbolInput(code);
    if ('error' in parsed) {
      setError(parsed.error);
      return;
    }
    const sym = parsed.symbol;
    if (items.some((x) => x.symbol === sym)) {
      setError('Already in watchlist.');
      return;
    }
    const next: WatchlistItem[] = [
      {
        symbol: sym,
        name: null,
        addedAt: new Date().toISOString(),
        color: '#ffffff',
      },
      ...items,
    ];
    persist(next);
    setCode('');
  }

  function onRemove(sym: string) {
    setSyncMsg(null);
    persist(items.filter((x) => x.symbol !== sym));
  }

  async function onSyncFromScreener() {
    setError(null);
    setSyncMsg(null);
    setSyncBusy(true);
    setSyncStage('Loading enabled screeners');
    setSyncProgress(null);
    setSyncLogs([]);
    setImportDebugFilter('');

    // UI-only helpers: show progress & last few steps.
    const pushLog = (line: string) => {
      setSyncLogs((prev) => [...prev, line].slice(-6));
    };
    const setStep = (label: string, cur?: number, total?: number) => {
      setSyncStage(label);
      if (typeof cur === 'number' && typeof total === 'number') setSyncProgress({ cur, total });
      else setSyncProgress(null);
      pushLog(label + (typeof cur === 'number' && typeof total === 'number' ? ` (${cur}/${total})` : ''));
    };
    try {
      const s = await apiGetJson<{ items: TvScreener[] }>('/integrations/tradingview/screeners');
      const enabled = (s.items || []).filter((x) => x && x.enabled);
      if (!enabled.length) {
        setSyncMsg('No enabled screeners.');
        return;
      }

      setStep('Loading latest snapshots (DB)', 0, enabled.length);
      // Load snapshot details from DB (no TradingView sync).
      const snapshotDetails: TvSnapshotDetail[] = [];
      for (let i = 0; i < enabled.length; i++) {
        const sc = enabled[i]!;
        setSyncProgress({ cur: i + 1, total: enabled.length });
        try {
          let snapId: string | null = null;
          const list = await apiGetJson<{ items: TvSnapshotSummary[] }>(
            `/integrations/tradingview/screeners/${encodeURIComponent(sc.id)}/snapshots?limit=1`,
          );
          const latest = list.items?.[0];
          if (latest?.id) snapId = String(latest.id);
          if (!snapId) continue;
          const d = await apiGetJson<TvSnapshotDetail>(
            `/integrations/tradingview/snapshots/${encodeURIComponent(snapId)}`,
          );
          snapshotDetails.push(d);
        } catch {
          // ignore per-screener
        }
      }

      const candidates: string[] = [];
      for (const snap of snapshotDetails) {
        if (!snap) continue;
        for (const r of snap.rows || []) {
          const raw = String(r['Ticker'] || r['Symbol'] || '').trim();
          const sym = normalizeScreenerSymbol(raw);
          if (sym) candidates.push(sym);
        }
      }

      const uniq = Array.from(new Set(candidates)).slice(0, 2000);
      if (!uniq.length) {
        setSyncMsg('No symbols found in latest screener snapshots.');
        setImportDebug({
          updatedAt: new Date().toISOString(),
          scanned: 0,
          trendOkCount: 0,
          rows: [],
        });
        return;
      }

      setStep('TrendOK check', 0, uniq.length);
      // Check TrendOK from data-sync-service DB cache (no network fetch in data-sync-service).
      const okSymsCached: string[] = [];
      const debugBySym: Record<string, TrendOkResult> = {};
      for (const part of chunk(uniq, 200)) {
        const sp = new URLSearchParams();
        sp.set('refresh', 'true');
        sp.set('realtime', isShanghaiTradingTime() ? 'true' : 'false');
        for (const s2 of part) sp.append('symbols', s2);
        const rows = await apiGetJsonFrom<TrendOkResult[]>(
          DATA_SYNC_BASE_URL,
          `/market/stocks/trendok?${sp.toString()}`,
        );
        for (const rr of Array.isArray(rows) ? rows : []) {
          if (!rr || !rr.symbol) continue;
          debugBySym[rr.symbol] = rr;
          if (rr.trendOk === true) okSymsCached.push(rr.symbol);
        }
        setSyncProgress((p) => {
          const prev = p?.cur ?? 0;
          return { cur: Math.min(uniq.length, prev + part.length), total: uniq.length };
        });
      }
      const okUniqCached = Array.from(new Set(okSymsCached));
      const okUniq = okUniqCached;

      // Persist debug table for manual review (never auto-cleared).
      setImportDebug({
        updatedAt: new Date().toISOString(),
        scanned: uniq.length,
        trendOkCount: okUniq.length,
        rows: uniq.map(
          (sym) =>
            debugBySym[sym] ?? ({
              symbol: sym,
              trendOk: null,
              score: null,
              missingData: ['no_result'],
            } satisfies TrendOkResult),
        ),
      });

      const existing = new Set(items.map((x) => x.symbol));
      const now = new Date().toISOString();
      const added: WatchlistItem[] = okUniq
        .filter((sym) => !existing.has(sym))
        .map((sym) => ({ symbol: sym, name: null, addedAt: now, color: '#ffffff' }));

      if (!added.length) {
        setSyncMsg(
          `Screener scanned ${uniq.length} symbols (latest snapshots); TrendOK ✅: ${okUniq.length}; nothing new to add.`,
        );
        return;
      }

      persist([...added, ...items]);
      setSyncMsg(
        `Added ${added.length} TrendOK ✅ stocks from screener (latest snapshots; scanned ${uniq.length}).`,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSyncBusy(false);
      setSyncStage(null);
      setSyncProgress(null);
    }
  }

  function showTooltip(el: HTMLElement, content: React.ReactNode, width = 360) {
    // Render via portal to avoid clipping, but anchor near the hovered element.
    // Place the tooltip at the element's top-right corner (top-end). If there isn't
    // enough room above, flip to bottom-end.
    const r = el.getBoundingClientRect();
    const pad = 12;
    const w = Math.min(width, Math.max(240, window.innerWidth - pad * 2));
    const x = Math.max(pad, Math.min(window.innerWidth - w - pad, r.right - w));
    const preferTop = r.top > 140;
    const placement: 'top-end' | 'bottom-end' = preferTop ? 'top-end' : 'bottom-end';
    const y = preferTop
      ? Math.max(pad, r.top - 8)
      : Math.min(window.innerHeight - pad, r.bottom + 8);
    setTooltip({ open: true, x, y, w, placement, content });
  }

  function hideTooltip() {
    setTooltip((prev) => (prev.open ? { ...prev, open: false } : prev));
  }

  function showColorPicker(el: HTMLElement, sym: string) {
    // Anchor near the clicked button, but clamp within viewport.
    // Flip to open upward when near the bottom to keep all items clickable.
    const r = el.getBoundingClientRect();
    const pad = 10;
    const panelW = 220;
    const panelH = 220; // heuristic, enough for header + 2 rows of 4 color buttons

    const x0 = r.right - panelW;
    const x = Math.max(pad, Math.min(window.innerWidth - panelW - pad, x0));

    const shouldOpenDown = r.bottom + 8 + panelH <= window.innerHeight - pad;
    const placement: 'top-end' | 'bottom-end' = shouldOpenDown ? 'bottom-end' : 'top-end';

    // y is the anchor point. For top-end, we use translateY(-100%) so y refers to the bottom edge.
    let y = placement === 'bottom-end' ? r.bottom + 8 : r.top - 8;
    if (placement === 'bottom-end') {
      y = Math.max(pad, Math.min(window.innerHeight - panelH - pad, y));
    } else {
      // Ensure y is not so small that the panel would go above the viewport when translated.
      y = Math.max(pad + panelH, Math.min(window.innerHeight - pad, y));
    }

    setColorPicker({ open: true, x, y, placement, symbol: sym });
  }

  function hideColorPicker() {
    setColorPicker((prev) => (prev.open ? { ...prev, open: false, symbol: null } : prev));
  }

  function setItemColor(symbol: string, color: string) {
    const next = items.map((it) => (it.symbol === symbol ? { ...it, color } : it));
    persist(next);
  }

  function setItemPositionPct(symbol: string, value: string) {
    const raw = value.trim();
    const num = raw === '' ? null : Number(raw);
    const nextVal =
      typeof num === 'number' && Number.isFinite(num) ? Math.max(0, Math.min(100, num)) : null;
    const next = items.map((it) => (it.symbol === symbol ? { ...it, positionPct: nextVal } : it));
    persist(next);
  }

  function setItemCostPriceValue(symbol: string, value: number | null) {
    const nextVal =
      typeof value === 'number' && Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
    const next = items.map((it) =>
      it.symbol === symbol ? { ...it, costPrice: nextVal, maxPrice: nextVal ?? it.maxPrice } : it,
    );
    persist(next);
  }

  function setItemCostPriceDraft(symbol: string, value: string) {
    setCostPriceDrafts((prev) => ({ ...prev, [symbol]: value }));
  }

  function commitItemCostPriceDraft(symbol: string) {
    const raw = costPriceDrafts[symbol];
    setCostPriceDrafts((prev) => {
      const next = { ...prev };
      delete next[symbol];
      return next;
    });
    if (raw == null) return;
    const trimmed = raw.trim();
    if (!trimmed) {
      setItemCostPriceValue(symbol, null);
      return;
    }
    const num = Number(trimmed);
    if (Number.isFinite(num)) {
      setItemCostPriceValue(symbol, num);
    }
  }


  React.useEffect(() => {
    if (!colorPicker.open) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') hideColorPicker();
    }
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [colorPicker.open]);

  function checkLine(label: string, ok: boolean | null | undefined, detail: string) {
    if (ok == null) return { label, state: '—', detail };
    return { label, state: ok ? '✅' : '❌', detail };
  }

  function renderTrendOkCell(sym: string) {
    const t = trend[sym];
    const ok = t?.trendOk ?? null;
    const icon = ok == null ? '—' : ok ? '✅' : '❌';
    const rsiNow =
      typeof t?.values?.rsi14 === 'number' && Number.isFinite(t.values.rsi14)
        ? t.values.rsi14
        : null;
    const h4 =
      Array.isArray(t?.values?.macdHist4) && t?.values?.macdHist4?.length === 4
        ? t.values.macdHist4
        : null;
    const hpos = h4 ? h4.map((x) => Math.max(0, Number(x))) : null;
    const d1 = hpos ? hpos[1] > hpos[0] : null;
    const d2 = hpos ? hpos[2] > hpos[1] : null;
    const d3 = hpos ? hpos[3] > hpos[2] : null;
    const hLastPos = hpos ? hpos[3] > 0 : null;
    const macdHistDetail = h4
      ? `need h_last>0: ${hLastPos ? '✅' : '❌'}; d1 ${d1 ? '✅' : '❌'}; d2 ${
          d2 ? '✅' : '❌'
        }; d3 ${d3 ? '✅' : '❌'} (h: ${h4
          .map((x) => (Number.isFinite(Number(x)) ? Number(x).toFixed(3) : '—'))
          .join(', ')})`
      : 'need last 4 histogram values';
    const lines = [
      checkLine('EMA trend', t?.checks?.emaOrder ?? null, 'Close > EMA(20) AND EMA(20) > EMA(60)'),
      checkLine('MACD > 0', t?.checks?.macdPositive ?? null, 'macdLine > 0'),
      checkLine(
        'MACD hist',
        t?.checks?.macdHistExpanding ?? null,
        `histogram > 0 (red bar above zero axis). Expansion is scored separately; ${macdHistDetail}`,
      ),
      checkLine('Near 20D high', t?.checks?.closeNear20dHigh ?? null, 'Close >= 0.95 * High(20)'),
      checkLine(
        'RSI(14)',
        t?.checks?.rsiInRange ?? null,
        `50 <= RSI <= 82${rsiNow == null ? '' : ` (now: ${rsiNow.toFixed(1)})`}`,
      ),
      checkLine('Volume', t?.checks?.volumeSurge ?? null, 'AvgVol(5) > 0.9 * AvgVol(30)'),
    ];
    const missing = (t?.missingData ?? []).filter(Boolean);
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">TrendOK checks</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="space-y-1">
          {lines.map((x) => (
            <div key={x.label} className="flex items-start justify-between gap-3">
              <div className="text-[var(--k-muted)]">{x.label}</div>
              <div className="flex-1 text-right">
                <span className="font-mono">{x.state}</span>{' '}
                <span className="text-[var(--k-muted)]">{x.detail}</span>
              </div>
            </div>
          ))}
        </div>
        {missing.length ? (
          <div className="mt-2 text-[var(--k-muted)]">
            Missing: <span className="font-mono">{missing.join(', ')}</span>
          </div>
        ) : null}
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 360)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 360)}
        onBlur={hideTooltip}
        aria-label="TrendOK details"
      >
        <span className="font-mono">{icon}</span>
      </button>
    );
  }

  function renderStopLossCell(sym: string) {
    const t = trend[sym];
    const p = t?.stopLossPrice ?? null;
    const parts = t?.stopLossParts ?? null;
    const get = (k: string) =>
      parts && typeof parts === 'object' ? (parts as Record<string, unknown>)[k] : undefined;
    const exitNow = Boolean(get('exit_now'));
    const exitDisplay =
      typeof get('exit_display') === 'string' ? String(get('exit_display')) : null;
    const warnHalf = Boolean(get('warn_reduce_half'));
    const warnDisplay =
      typeof get('warn_display') === 'string' ? String(get('warn_display')) : null;
    const exitChecks = {
      ema5_lt_ema20: Boolean(get('exit_check_ema5_lt_ema20')),
      close_lt_ema20: Boolean(get('exit_check_close_lt_ema20')),
      momentum_exhaustion: Boolean(get('exit_check_momentum_exhaustion')),
      volume_dry: Boolean(get('exit_check_volume_dry')),
    };
    // Semantics: ✅ means "NOT triggered" (safe), ❌ means "triggered" (exit-now condition hit).
    const ok = (triggered: boolean) => (triggered ? '❌' : '✅');
    const exitMomAndVol = Boolean(exitChecks.momentum_exhaustion && exitChecks.volume_dry);
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">StopLoss</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        {exitNow ? (
          <div className="mb-2 rounded border border-red-500/30 bg-red-500/10 px-2 py-1 text-red-600">
            {exitDisplay || '立刻离场'}
          </div>
        ) : warnHalf ? (
          <div className="mb-2 rounded border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-amber-700">
            {warnDisplay || '警告：MACD柱缩小但未转负，建议至少卖出一半'}
          </div>
        ) : null}
        <div className="text-[var(--k-muted)]">
          Formula: max(final_support - atr_k×ATR14, hard_stop)
        </div>
        <div className="mt-2 rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="mb-1 font-medium">立刻离场检查</div>
          <div className="text-[10px] text-[var(--k-muted)]">
            ✅ 安全 / ❌ 触发。任一条为 ❌ 即“立刻离场”（止损价=当前价）。
          </div>
          <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-1">
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">EMA5 &lt; EMA20</span>
              <span className="font-mono">{ok(exitChecks.ema5_lt_ema20)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">收盘价 &lt; EMA20</span>
              <span className="font-mono">{ok(exitChecks.close_lt_ema20)}</span>
            </div>
            <div className="flex items-center justify-between gap-2">
              <span className="text-[var(--k-muted)]">动能衰竭 + 量能萎缩</span>
              <span className="font-mono">{ok(exitMomAndVol)}</span>
            </div>
          </div>
        </div>
        <div className="mt-2 space-y-1">
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">StopLoss</div>
            <div className="font-mono">{fmtPrice(p)}</div>
          </div>
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">final_support</div>
            <div className="font-mono">{fmtNum(get('final_support'), 2)}</div>
          </div>
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">buffer</div>
            <div className="font-mono">{fmtNum(get('buffer'), 3)}</div>
          </div>
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">hard_stop</div>
            <div className="font-mono">{fmtNum(get('hard_stop'), 2)}</div>
          </div>
        </div>
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 380)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 380)}
        onBlur={hideTooltip}
        aria-label="StopLoss details"
      >
        {exitNow ? (
          <span className="inline-flex items-center gap-1 font-mono text-red-600">
            <CircleX className="h-4 w-4" aria-hidden />
            {fmtPrice(p)}
          </span>
        ) : warnHalf ? (
          <span className="inline-flex items-center gap-1 font-mono text-amber-700">
            <span aria-hidden>⚠︎</span>
            {fmtPrice(p)}
          </span>
        ) : (
          <span className="font-mono">{fmtPrice(p)}</span>
        )}
      </button>
    );
  }

  function renderScoreCell(sym: string) {
    const t = trend[sym];
    const score = t?.score ?? null;
    const parts = t?.scoreParts ?? null;
    const entries =
      parts && typeof parts === 'object'
        ? Object.entries(parts).filter(([, v]) => typeof v === 'number' && Number.isFinite(v))
        : [];
    entries.sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">Score (0–100)</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="text-[var(--k-muted)]">
          Deterministic formula (CN daily, no LLM). Higher means better short-horizon setup.
        </div>
        <div className="mt-2 space-y-1">
          <div className="flex items-center justify-between">
            <div className="text-[var(--k-muted)]">Total</div>
            <div className="font-mono">{fmtScore(score)}</div>
          </div>
          {entries.length ? (
            <div className="mt-2">
              {entries.map(([k, v]) => (
                <div key={k} className="flex items-center justify-between gap-3">
                  <div className="text-[var(--k-muted)]">{k}</div>
                  <div className="font-mono">{v > 0 ? `+${v.toFixed(1)}` : v.toFixed(1)}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-2 text-[var(--k-muted)]">
              No breakdown available (insufficient data).
            </div>
          )}
        </div>
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 360)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 360)}
        onBlur={hideTooltip}
        aria-label="Score details"
      >
        <span className="font-mono">{fmtScore(score)}</span>
      </button>
    );
  }

  function renderBuyCell(sym: string) {
    const t = trend[sym];
    const { text, tone } = fmtBuyCell(t);
    const why = typeof t?.buyWhy === 'string' ? t.buyWhy : null;
    const tip = (
      <>
        <div className="mb-2 flex items-center justify-between">
          <div className="font-medium">买入</div>
          <div className="font-mono text-[var(--k-muted)]">{sym}</div>
        </div>
        <div className="text-[var(--k-muted)]">{why || '—'}</div>
        <div className="mt-2 flex items-center justify-between">
          <div className="text-[var(--k-muted)]">建议</div>
          <div className="font-mono">{text}</div>
        </div>
      </>
    );
    return (
      <button
        type="button"
        className="inline-flex items-center"
        onMouseEnter={(e) => showTooltip(e.currentTarget, tip, 380)}
        onMouseLeave={hideTooltip}
        onFocus={(e) => showTooltip(e.currentTarget, tip, 380)}
        onBlur={hideTooltip}
        aria-label="Buy details"
      >
        <span
          className={
            tone === 'buy'
              ? 'font-mono text-emerald-700'
              : tone === 'avoid'
                ? 'font-mono text-red-600'
                : tone === 'wait'
                  ? 'font-mono text-[var(--k-muted)]'
                  : 'font-mono'
          }
        >
          {text}
        </span>
      </button>
    );
  }

  const sortedItems = React.useMemo(() => {
    if (!scoreSortEnabled) return items;
    const arr = [...items];
    arr.sort((a, b) => {
      const sa = trend[a.symbol]?.score;
      const sb = trend[b.symbol]?.score;
      const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
      const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
      if (va == null && vb == null) return 0;
      if (va == null) return 1; // push unknown to bottom
      if (vb == null) return -1;
      const d = va - vb;
      return scoreSortDir === 'asc' ? d : -d;
    });
    return arr;
  }, [items, trend, scoreSortEnabled, scoreSortDir]);

  function referenceTable() {
    const capturedAt = new Date().toISOString();
    const rows = sortedItems.slice(0, 50).map((it) => {
      const t = trend[it.symbol];
      return {
        symbol: it.symbol,
        name: it.name ?? null,
        asOfDate: t?.asOfDate ?? null,
        close: t?.values?.close ?? null,
        trendOk: t?.trendOk ?? null,
        score: t?.score ?? null,
        stopLossPrice: t?.stopLossPrice ?? null,
        buyMode: t?.buyMode ?? null,
        buyAction: t?.buyAction ?? null,
        buyZoneLow: t?.buyZoneLow ?? null,
        buyZoneHigh: t?.buyZoneHigh ?? null,
      };
    });
    addReference({
      kind: 'watchlistTable',
      refId: `${capturedAt}:${sortedItems.length}`,
      capturedAt,
      total: sortedItems.length,
      items: rows,
    });
  }

  function toastCopyMd(ok: boolean, text: string) {
    setCopyMdStatus({ ok, text });
    if (copyMdTimerRef.current) window.clearTimeout(copyMdTimerRef.current);
    copyMdTimerRef.current = window.setTimeout(() => setCopyMdStatus(null), 2400);
  }

  async function copyWatchlistMarkdown() {
    if (!sortedItems.length) {
      toastCopyMd(false, 'No items to copy.');
      return;
    }
    const tradingTime = isShanghaiTradingTime();
    const todaySh = getShanghaiTodayIso();
    const missingRealtime: string[] = [];
    const missingTrend: string[] = [];
    const missingHistory: string[] = [];
    for (const it of sortedItems) {
      const sym = it.symbol;
      const t = trend[sym];
      if (!t) {
        missingTrend.push(sym);
        continue;
      }
      const md = Array.isArray(t.missingData) ? t.missingData.filter(Boolean) : [];
      if (md.length) {
        missingHistory.push(sym);
      }
      if (tradingTime && sym.toUpperCase().startsWith('CN:')) {
        const q = quotes[sym];
        const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
        if (!(q && typeof q.price === 'number' && Number.isFinite(q.price) && qDate === todaySh)) {
          missingRealtime.push(sym);
        }
      }
    }
    if (missingTrend.length || missingHistory.length || missingRealtime.length) {
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
      toastCopyMd(false, `Copy aborted: ${parts.join(' | ')}`);
      return;
    }
    const generatedAt = new Date().toISOString();
    const lines: string[] = [];
    lines.push('## Watchlist');
    lines.push(`- generatedAt: ${generatedAt}`);
    lines.push(`- items: ${sortedItems.length}`);
    lines.push(`- scoresUpdatedAt: ${trendUpdatedAt ? new Date(trendUpdatedAt).toLocaleString() : '—'}`);
    lines.push(`- shanghaiToday: ${todaySh}`);
    lines.push(`- tradingTime: ${tradingTime ? 'true' : 'false'}`);
    lines.push('');

    lines.push('### TrendOK rules');
    lines.push(mdLines(trendOkRuleLines()));
    lines.push('');
    lines.push('### Score rules');
    lines.push(mdLines(scoreRuleLines()));
    lines.push('');

    // Summary table
    const headers = ['Symbol', 'Name', 'Score', 'TrendOK', 'Buy', 'Current', 'StopLoss', 'AsOfDate'];
    lines.push(`| ${headers.join(' | ')} |`);
    lines.push(`| ${headers.map(() => '---').join(' | ')} |`);
    for (const it of sortedItems) {
      const t = trend[it.symbol];
      const buy = fmtBuyCell(t).text;
      const q = quotes[it.symbol];
      const current = q?.price ?? t?.values?.close ?? null;
      const qDate = tradeDateFromTradeTime(q?.tradeTime ?? null);
      const asOf = tradingTime && qDate ? qDate : String(t?.asOfDate ?? '');
      const row = [
        escapeMarkdownCell(it.symbol),
        escapeMarkdownCell(it.name || '—'),
        escapeMarkdownCell(mdScore(t?.score ?? null)),
        escapeMarkdownCell(trendOkSummary(t)),
        escapeMarkdownCell(buy),
        escapeMarkdownCell(mdPrice(typeof current === 'number' ? current : null)),
        escapeMarkdownCell(mdPrice(t?.stopLossPrice ?? null)),
        escapeMarkdownCell(asOf),
      ];
      lines.push(`| ${row.join(' | ')} |`);
    }
    lines.push('');

    const md = lines.join('\n').trim() + '\n';
    try {
      await navigator.clipboard.writeText(md);
      toastCopyMd(true, 'Copied Markdown.');
    } catch {
      toastCopyMd(false, 'Copy failed. Please allow clipboard access.');
    }
  }

  const watchlistSet = React.useMemo(() => new Set(items.map((x) => x.symbol)), [items]);
  const importDebugRows = React.useMemo(() => {
    const q = importDebugFilter.trim().toUpperCase();
    const base = (importDebug.rows || []).filter((r) => {
      if (!q) return true;
      const sym = String(r?.symbol || '').toUpperCase();
      const name = String(r?.name || '').toUpperCase();
      return sym.includes(q) || name.includes(q);
    });
    const arr = [...base];
    arr.sort((a, b) => {
      const sa = a?.score;
      const sb = b?.score;
      const va = typeof sa === 'number' && Number.isFinite(sa) ? sa : null;
      const vb = typeof sb === 'number' && Number.isFinite(sb) ? sb : null;
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const d = va - vb;
      return importDebugScoreSortDir === 'asc' ? d : -d;
    });
    return arr;
  }, [importDebug.rows, importDebugFilter, importDebugScoreSortDir]);

  const headerTip = (
    <>
      <div className="mb-2 font-medium">Definition (CN daily)</div>
      <div className="space-y-1 text-[var(--k-muted)]">
        <div>✅ only when ALL rules are satisfied.</div>
        <div>— when data/indicators are insufficient.</div>
      </div>
      <div className="mt-2 space-y-1">
        <div>1) Close &gt; EMA(20) and EMA(20) &gt; EMA(60)</div>
        <div>2) MACD line &gt; 0</div>
        <div>3) MACD histogram &gt; 0</div>
        <div>4) Close ≥ 0.95 × High(20)</div>
        <div>5) RSI(14) in [50, 82]</div>
        <div>6) AvgVol(5) &gt; 0.9 × AvgVol(30)</div>
      </div>
    </>
  );

  return (
    <div className="mx-auto w-full max-w-none p-6">
      <div className="mb-6 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-lg font-semibold">Watchlist</div>
          <div className="mt-1 text-sm text-[var(--k-muted)]">
            Manage the stocks you are watching.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            Names are resolved from Market cache. If names are missing, go to Market and click Sync
            once.
          </div>
          <div className="mt-1 text-xs text-[var(--k-muted)]">
            {trendUpdatedAt
              ? `Scores updated at ${new Date(trendUpdatedAt).toLocaleString()} (auto refresh: 10 min)`
              : 'Scores not loaded yet.'}
          </div>
          {syncBusy && syncStage ? (
            <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
              <div className="flex items-center justify-between gap-2">
                <div className="font-medium">Import from screener</div>
                <div className="text-[var(--k-muted)]">
                  {syncProgress ? `${syncProgress.cur}/${syncProgress.total}` : '…'}
                </div>
              </div>
              <div className="mt-1 text-[var(--k-muted)]">{syncStage}</div>
              {syncProgress && syncProgress.total > 0 ? (
                <div className="mt-2 h-2 w-full overflow-hidden rounded bg-[var(--k-surface-2)]">
                  <div
                    className="h-full bg-[var(--k-accent)]"
                    style={{
                      width: `${Math.max(
                        0,
                        Math.min(100, (syncProgress.cur / Math.max(1, syncProgress.total)) * 100),
                      ).toFixed(1)}%`,
                    }}
                  />
                </div>
              ) : null}
              {syncLogs.length ? (
                <div className="mt-2 space-y-0.5 text-[var(--k-muted)]">
                  {syncLogs.slice(-4).map((l, i) => (
                    <div key={i} className="truncate">
                      {l}
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="mt-2 rounded-md border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <div className="font-medium">Import debug table</div>
                <Switch
                  checked={importDebugOpen}
                  onCheckedChange={setImportDebugOpen}
                  aria-label="Toggle import debug table"
                />
              </div>
              <div className="text-[var(--k-muted)]">
                {importDebug.updatedAt ? new Date(importDebug.updatedAt).toLocaleString() : 'No import yet'}
              </div>
            </div>
            <div className="mt-1 flex flex-wrap items-center justify-between gap-2">
              <div className="text-[var(--k-muted)]">
                Scanned {importDebug.scanned} • TrendOK ✅ {importDebug.trendOkCount} • Showing {importDebugRows.length}
              </div>
              <div className="flex items-center gap-2">
                <input
                  className="h-8 w-[220px] rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-xs outline-none"
                  placeholder="Filter (symbol/name)"
                  value={importDebugFilter}
                  onChange={(e) => setImportDebugFilter(e.target.value)}
                />
                <Button
                  size="sm"
                  variant="secondary"
                  onClick={() => setImportDebugFilter('')}
                  disabled={!importDebugFilter.trim()}
                >
                  Clear
                </Button>
              </div>
            </div>

            <VisibilitySection
              visible={importDebugOpen}
              className="mt-2 max-h-[520px] overflow-auto rounded border border-[var(--k-border)]"
            >
              <table className="w-full border-collapse text-sm">
                <thead className="sticky top-0 bg-[var(--k-surface)] text-[var(--k-muted)]">
                  <tr className="text-left">
                    <th className="px-3 py-2 w-[150px]">Symbol</th>
                    <th className="px-3 py-2 w-[140px]">Name</th>
                    <th className="px-3 py-2 w-[80px]">TrendOK</th>
                    <th className="px-3 py-2 w-[90px]">
                      <button
                        type="button"
                        className="inline-flex items-center gap-1 hover:text-[var(--k-text)]"
                        onClick={() =>
                          setImportDebugScoreSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
                        }
                        aria-label="Sort by score"
                        title="Sort by score"
                      >
                        <span>Score</span>
                        {importDebugScoreSortDir === 'desc' ? (
                          <ArrowDown className="h-3.5 w-3.5" />
                        ) : (
                          <ArrowUp className="h-3.5 w-3.5" />
                        )}
                      </button>
                    </th>
                    <th className="px-3 py-2 w-[180px]">Buy</th>
                    <th className="px-3 py-2 w-[110px]">StopLoss</th>
                    <th className="px-3 py-2 w-[120px]">Action</th>
                    <th className="px-3 py-2 min-w-[320px]">Notes</th>
                  </tr>
                </thead>
                <tbody>
                  {importDebugRows.length ? (
                    importDebugRows.map((r) => {
                      const sym = String(r?.symbol || '');
                      const ok = r?.trendOk ?? null;
                      const icon = ok == null ? '—' : ok ? '✅' : '❌';
                      const buy = fmtBuyCell(r);
                      const notes =
                        (typeof r?.buyWhy === 'string' && r.buyWhy) ||
                        (Array.isArray(r?.missingData) && r.missingData.length ? r.missingData.join(', ') : '');
                      const inWl = sym ? watchlistSet.has(sym) : false;
                      return (
                        <tr key={sym} className="border-t border-[var(--k-border)]">
                          <td className="px-3 py-2 font-mono">
                            <button
                              type="button"
                              className="hover:underline"
                              onClick={() => {
                                setCode(sym);
                                setError(null);
                              }}
                              title="Fill the Add input with this symbol"
                            >
                              {sym || '—'}
                            </button>
                          </td>
                          <td className="px-3 py-2">
                            <div className="truncate" title={String(r?.name || '')}>
                              {r?.name || '—'}
                            </div>
                          </td>
                          <td className="px-3 py-2 font-mono">{icon}</td>
                          <td className="px-3 py-2 font-mono">{fmtScore(r?.score ?? null)}</td>
                          <td
                            className={
                              buy.tone === 'buy'
                                ? 'px-3 py-2 font-mono text-emerald-700'
                                : buy.tone === 'avoid'
                                  ? 'px-3 py-2 font-mono text-red-600'
                                  : buy.tone === 'wait'
                                    ? 'px-3 py-2 font-mono text-[var(--k-muted)]'
                                    : 'px-3 py-2 font-mono'
                            }
                          >
                            {buy.text}
                          </td>
                          <td className="px-3 py-2 font-mono">{fmtPrice(r?.stopLossPrice ?? null)}</td>
                          <td className="px-3 py-2">
                            {inWl ? (
                              <span className="text-[var(--k-muted)]">In watchlist</span>
                            ) : (
                              <Button
                                size="sm"
                                variant="secondary"
                                onClick={() => sym && addSymbolToWatchlist(sym)}
                                disabled={!sym}
                              >
                                Add
                              </Button>
                            )}
                          </td>
                          <td className="px-3 py-2 text-[var(--k-muted)]">
                            <div className="truncate" title={notes}>
                              {notes || '—'}
                            </div>
                          </td>
                        </tr>
                      );
                    })
                  ) : (
                    <tr>
                      <td className="px-3 py-3 text-[var(--k-muted)]" colSpan={8}>
                        No import results yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </VisibilitySection>
          </div>

          {syncMsg ? <div className="mt-2 text-xs text-[var(--k-muted)]">{syncMsg}</div> : null}
          {copyMdStatus ? (
            <div className="mt-2 text-xs">
              <span className={copyMdStatus.ok ? 'text-emerald-600' : 'text-red-600'}>
                {copyMdStatus.text}
              </span>
            </div>
          ) : null}
          {error ? <div className="mt-2 text-sm text-red-600">{error}</div> : null}
        </div>
        <div className="flex items-center gap-2">
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void refreshTrend('manual', { forceMarket: true })}
            disabled={trendBusy || !items.length}
            className="gap-2"
            aria-label="Refresh watchlist scores"
            title="Fetch latest daily bars from network and recompute"
          >
            <RefreshCw className={trendBusy ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
            Refresh
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => referenceTable()}
            disabled={!sortedItems.length}
            className="gap-2"
          >
            <ExternalLink className="h-4 w-4" />
            Reference table
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void copyWatchlistMarkdown()}
            disabled={!sortedItems.length}
          >
            Copy Markdown
          </Button>
          <Button
            size="sm"
            variant="secondary"
            onClick={() => void onSyncFromScreener()}
            disabled={syncBusy}
            className="gap-2"
          >
            <RefreshCw className="h-4 w-4" />
            Import from screener
          </Button>
        </div>
      </div>

      <section className="mb-4 rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 text-sm font-medium">Add</div>
        <div className="grid gap-2 md:grid-cols-12">
          <input
            className="h-9 md:col-span-10 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-sm outline-none"
            placeholder="Ticker (e.g. 600000 / 0700 / CN:600000)"
            value={code}
            onChange={(e) => setCode(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') onAdd();
            }}
          />
          <div className="md:col-span-2 flex gap-2">
            <Button size="sm" onClick={onAdd} disabled={!code.trim()}>
              Add
            </Button>
            <Button
              size="sm"
              variant="secondary"
              onClick={() => {
                setCode('');
                setError(null);
              }}
              disabled={!code.trim() && !error}
            >
              Clear
            </Button>
          </div>
        </div>
        <div className="mt-2 text-xs text-[var(--k-muted)]">
          Supported inputs: CN 6-digit ticker, HK 4-5 digit ticker, or prefixed symbol (CN:/HK:).
        </div>
      </section>

      <section className="rounded-xl border border-[var(--k-border)] bg-[var(--k-surface)] p-4">
        <div className="mb-2 flex items-center justify-between">
          <div className="text-sm font-medium">List</div>
          <div className="text-xs text-[var(--k-muted)]">{items.length} items</div>
        </div>

        {items.length ? (
          <div className="overflow-auto rounded border border-[var(--k-border)]">
            <table className="w-full border-collapse text-sm">
              <thead className="bg-[var(--k-surface)] text-[var(--k-muted)]">
                <tr className="text-left">
                  <th className="px-3 py-2 w-[44px]" title="Color flag">
                    <span className="sr-only">Color</span>
                  </th>
                  <th className="px-3 py-2">Symbol</th>
                  <th className="px-3 py-2">Name</th>
                  <th className="px-3 py-2">成本价</th>
                  <th className="px-3 py-2">最高价</th>
                  <th className="px-3 py-2">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => {
                        setScoreSortEnabled(true);
                        setScoreSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
                      }}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        setScoreSortEnabled((v) => !v);
                      }}
                      title="Click to toggle sort. Right-click to enable/disable sorting."
                      aria-label="Sort by score"
                    >
                      <span>Score</span>
                      {scoreSortEnabled ? (
                        scoreSortDir === 'desc' ? (
                          <ArrowDown className="h-3.5 w-3.5" />
                        ) : (
                          <ArrowUp className="h-3.5 w-3.5" />
                        )
                      ) : (
                        <ArrowUpDown className="h-3.5 w-3.5" />
                      )}
                    </button>
                  </th>
                  <th className="px-3 py-2">买入</th>
                  <th className="px-3 py-2">仓位%</th>
                  <th className="px-3 py-2">Current</th>
                  <th className="px-3 py-2">止损</th>
                  <th className="px-3 py-2">
                    <div className="inline-flex items-center gap-2">
                      <span>TrendOK</span>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-8 w-8 rounded-full"
                        onMouseEnter={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                        onMouseLeave={hideTooltip}
                        onFocus={(e) => showTooltip(e.currentTarget, headerTip, 380)}
                        onBlur={hideTooltip}
                        aria-label="TrendOK definition"
                      >
                        <Info className="h-4 w-4" />
                      </Button>
                    </div>
                  </th>
                  <th className="px-3 py-2 w-[54px] text-right"> </th>
                </tr>
              </thead>
              <tbody>
                {sortedItems.map((it) => (
                  (() => {
                    const t = trend[it.symbol];
                    const tone = rowTone(t);
                    const rowClass =
                      tone === 'green'
                        ? 'border-t border-[var(--k-border)] bg-emerald-50/60 hover:bg-emerald-100/60'
                        : tone === 'red'
                          ? 'border-t border-[var(--k-border)] bg-red-50/60 hover:bg-red-100/60'
                          : 'border-t border-[var(--k-border)] hover:bg-[var(--k-surface-2)]';
                    return (
                  <tr
                    key={it.symbol}
                    className={rowClass}
                  >
                    <td className="px-3 py-2">
                      <button
                        type="button"
                        className="grid h-6 w-6 place-items-center rounded hover:bg-[var(--k-surface-2)]"
                        onClick={(e) => {
                          e.stopPropagation();
                          showColorPicker(e.currentTarget, it.symbol);
                        }}
                        aria-label="Set color flag"
                        title="Set color flag"
                      >
                        <span
                          className="h-3.5 w-3.5 rounded-sm border border-[var(--k-border)]"
                          style={{ backgroundColor: it.color || '#ffffff' }}
                        />
                      </button>
                    </td>
                    <td className="px-3 py-2 font-mono">
                      <button
                        type="button"
                        className="inline-flex items-center rounded px-1 py-0.5 hover:underline"
                        onClick={() => onOpenStock?.(it.symbol)}
                        disabled={!onOpenStock}
                        aria-label={`Open ${it.symbol}`}
                      >
                        {it.symbol}
                      </button>
                    </td>
                    <td className="px-3 py-2">{it.name || '—'}</td>
                    <td className="px-3 py-2">
                      <input
                        className="h-8 w-24 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-xs outline-none"
                        placeholder="成本"
                        inputMode="decimal"
                        value={
                          costPriceDrafts[it.symbol] ??
                          (typeof it.costPrice === 'number' && Number.isFinite(it.costPrice)
                            ? it.costPrice.toFixed(2)
                            : '')
                        }
                        onChange={(e) => {
                          const raw = e.target.value;
                          if (raw === '' || COST_PRICE_RE.test(raw)) {
                            setItemCostPriceDraft(it.symbol, raw);
                          }
                        }}
                        onFocus={() => {
                          if (costPriceDrafts[it.symbol] != null) return;
                          if (typeof it.costPrice === 'number' && Number.isFinite(it.costPrice)) {
                            setItemCostPriceDraft(it.symbol, it.costPrice.toFixed(2));
                          }
                        }}
                        onBlur={() => commitItemCostPriceDraft(it.symbol)}
                      />
                    </td>
                    <td className="px-3 py-2 font-mono">
                      {typeof it.maxPrice === 'number' && Number.isFinite(it.maxPrice)
                        ? it.maxPrice.toFixed(2)
                        : '—'}
                    </td>
                    <td className="px-3 py-2">{renderScoreCell(it.symbol)}</td>
                    <td className="px-3 py-2">{renderBuyCell(it.symbol)}</td>
                    <td className="px-3 py-2">
                      <input
                        className="h-8 w-20 rounded-md border border-[var(--k-border)] bg-[var(--k-surface-2)] px-2 font-mono text-xs outline-none"
                        placeholder="0"
                        value={
                          typeof it.positionPct === 'number' && Number.isFinite(it.positionPct)
                            ? String(it.positionPct)
                            : ''
                        }
                        onChange={(e) => setItemPositionPct(it.symbol, e.target.value)}
                      />
                    </td>
                    <td
                      className="px-3 py-2 font-mono"
                      title={
                        trend[it.symbol]?.asOfDate
                          ? `as of ${trend[it.symbol]?.asOfDate}`
                          : trend[it.symbol]
                            ? 'as of latest cached daily bar'
                            : '—'
                      }
                    >
                      {fmtPrice(quotes[it.symbol]?.price ?? trend[it.symbol]?.values?.close)}
                    </td>
                    <td className="px-3 py-2">{renderStopLossCell(it.symbol)}</td>
                    <td className="px-3 py-2">{renderTrendOkCell(it.symbol)}</td>
                    <td className="px-3 py-2 text-right">
                      <div className="flex justify-end">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8"
                          onClick={() => {
                            const t = trend[it.symbol];
                            const capturedAt = new Date().toISOString();
                            addReference({
                              kind: 'watchlistStock',
                              refId: `${it.symbol}:${capturedAt}`,
                              symbol: it.symbol,
                              name: it.name ?? null,
                              capturedAt,
                              asOfDate: t?.asOfDate ?? null,
                              close: t?.values?.close ?? null,
                              trendOk: t?.trendOk ?? null,
                              score: t?.score ?? null,
                              stopLossPrice: t?.stopLossPrice ?? null,
                              buyMode: t?.buyMode ?? null,
                              buyAction: t?.buyAction ?? null,
                              buyZoneLow: t?.buyZoneLow ?? null,
                              buyZoneHigh: t?.buyZoneHigh ?? null,
                              buyWhy: t?.buyWhy ?? null,
                            });
                          }}
                          aria-label="Reference to chat"
                          title="Reference to chat"
                        >
                          <ExternalLink className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8"
                          onClick={() => onRemove(it.symbol)}
                          aria-label="Remove"
                          title="Remove"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                    );
                  })()
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-sm text-[var(--k-muted)]">No items yet. Add a ticker above.</div>
        )}
      </section>

      {tooltip.open
        ? createPortal(
            <div
              className="fixed z-[9999] max-h-[70vh] overflow-auto rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-xs text-[var(--k-text)] shadow-lg"
              style={{
                left: tooltip.x,
                top: tooltip.y,
                width: tooltip.w,
                transform: tooltip.placement === 'top-end' ? 'translateY(-100%)' : undefined,
              }}
            >
              {tooltip.content}
            </div>,
            document.body,
          )
        : null}

      {colorPicker.open
        ? createPortal(
            <div className="fixed inset-0 z-[9999]" onMouseDown={hideColorPicker}>
              <div
                className="fixed w-[220px] rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-2 text-xs text-[var(--k-text)] shadow-lg"
                style={{
                  left: colorPicker.x,
                  top: colorPicker.y,
                  transform: colorPicker.placement === 'top-end' ? 'translateY(-100%)' : undefined,
                }}
                onMouseDown={(e) => e.stopPropagation()}
              >
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-medium text-[var(--k-muted)]">Color flag</div>
                  <button
                    type="button"
                    className="grid h-7 w-7 place-items-center rounded hover:bg-[var(--k-surface-2)]"
                    onClick={hideColorPicker}
                    aria-label="Close"
                  >
                    <CircleX className="h-4 w-4" />
                  </button>
                </div>
                <div className="grid grid-cols-4 gap-2">
                  {FLAG_COLORS.map((c) => (
                    <button
                      key={c.hex}
                      type="button"
                      className="group flex h-9 items-center justify-center rounded-md border border-[var(--k-border)] hover:bg-[var(--k-surface-2)]"
                      onClick={() => {
                        if (colorPicker.symbol) setItemColor(colorPicker.symbol, c.hex);
                        hideColorPicker();
                      }}
                      aria-label={c.label}
                      title={c.label}
                    >
                      <span
                        className="h-5 w-5 rounded-sm border border-[var(--k-border)]"
                        style={{ backgroundColor: c.hex }}
                      />
                    </button>
                  ))}
                </div>
                <div className="mt-2 text-[11px] text-[var(--k-muted)]">
                  Tip: Press Esc or click outside to close.
                </div>
              </div>
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}
