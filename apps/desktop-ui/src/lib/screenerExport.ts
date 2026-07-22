import { apiGetJson } from '@/lib/api/client';
import type { TrendOkResult } from '@/lib/api/types';
import { getShanghaiTodayIso } from '@/lib/market-hours';
import { formatGapUp, formatIntradayChgPct } from '@/lib/watchlist-metrics';

export type { TrendOkResult } from '@/lib/api/types';
export { fetchTrendOkMap } from '@/lib/api/trendok';
export { getShanghaiTodayIso } from '@/lib/market-hours';

export type ScreenerMarkdownRow = {
  symbol: string;
  name: string;
  industry: string;
  price: string;
  changePct: string;
  relVolume: string;
  score: number | null;
  intradayPct: string;
  gapUp: string;
  flags: string;
};

const TV_FIELD_ALIASES: Record<string, string[]> = {
  ticker: ['Ticker', 'Symbol'],
  name: ['Name'],
  price: ['Price', 'Last', 'Close'],
  changePct: ['Change %', 'Price Change % 1 day'],
  relVolume: ['Rel Volume', 'Relative Volume 1 day', 'Relative Volume'],
  flags: ['Flags'],
};

export const SCREENER_MARKDOWN_HEADERS = [
  'Symbol',
  'Name',
  'Industry',
  'Price',
  'Chg %',
  'Rel vol',
  'Score',
  'Intraday%',
  'GapUp',
  'Flags',
] as const;

export const SCREENER_TITLE_PATTERNS = [
  'karios pullback',
  'pullback',
  'falcon launch',
  'institutional trend',
] as const;

export function shanghaiDateFromIso(iso: string): string | null {
  const dt = new Date(iso);
  if (Number.isNaN(dt.getTime())) return null;
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(dt);
  const map = new Map(parts.map((p) => [p.type, p.value]));
  const y = map.get('year');
  const m = map.get('month');
  const d = map.get('day');
  if (!y || !m || !d) return null;
  return `${y}-${m}-${d}`;
}

export function isTodayShanghai(iso: string, todayIso = getShanghaiTodayIso()): boolean {
  const localDate = shanghaiDateFromIso(iso);
  return localDate != null && localDate === todayIso;
}

export function matchesScreenerTitlePattern(
  screenTitle: string | null | undefined,
  patterns: readonly string[] = SCREENER_TITLE_PATTERNS,
): boolean {
  const title = String(screenTitle ?? '').trim().toLowerCase();
  if (!title) return false;
  return patterns.some((pattern) => title.includes(pattern.toLowerCase()));
}

type ScreenerSummaryRow = { id?: string; name?: string };

type ScreenerSnapshotListItem = { id: string; capturedAt?: string; rowCount?: number };

type ScreenerSnapshotDetail = {
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

export async function fetchTodayScreenerSymbolsByTitle(
  screeners: ScreenerSummaryRow[],
  options?: {
    patterns?: readonly string[];
    todayIso?: string;
    apiGetJson?: <T>(path: string) => Promise<T>;
  },
): Promise<Set<string>> {
  const patterns = options?.patterns ?? SCREENER_TITLE_PATTERNS;
  const todayIso = options?.todayIso ?? getShanghaiTodayIso();
  const fetchJson = options?.apiGetJson ?? apiGetJson;

  const out = new Set<string>();
  const rows = Array.isArray(screeners) ? screeners : [];
  const screenerIds = rows
    .map((sc) => String(sc?.id ?? '').trim())
    .filter((sid) => sid);

  const results = await Promise.all(
    screenerIds.map(async (sid) => {
      try {
        const list = await fetchJson<{ items: ScreenerSnapshotListItem[] }>(
          `/integrations/tradingview/screeners/${encodeURIComponent(sid)}/snapshots?limit=1`,
        );
        const snapId = String(list?.items?.[0]?.id ?? '').trim();
        if (!snapId) return null;
        const snap = await fetchJson<ScreenerSnapshotDetail>(
          `/integrations/tradingview/snapshots/${encodeURIComponent(snapId)}`,
        );
        return snap;
      } catch {
        return null;
      }
    }),
  );

  for (const snap of results) {
    if (!snap) continue;
    if (!isTodayShanghai(String(snap.capturedAt ?? ''), todayIso)) continue;
    if (!matchesScreenerTitlePattern(snap.screenTitle, patterns)) continue;
    const headers = Array.isArray(snap.headers) ? snap.headers.map((h) => String(h ?? '')) : [];
    const rowsTv = Array.isArray(snap.rows) ? snap.rows : [];
    for (const sym of extractSymbolsFromSnapshotRows(rowsTv, headers)) {
      out.add(sym);
    }
  }

  return out;
}

function normalizeSymbolInput(input: string): { symbol: string } | { error: string } {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return { error: 'Empty input' };

  if (/^(CN|HK):[0-9A-Z.\-]{1,16}$/.test(raw)) {
    return { symbol: raw };
  }

  if (/^\d{6}$/.test(raw)) {
    return { symbol: `CN:${raw}` };
  }

  if (/^\d{4,5}$/.test(raw)) {
    return { symbol: `HK:${raw.padStart(4, '0')}` };
  }

  return { error: 'Unsupported code format' };
}

export function normalizeScreenerSymbol(raw: string): string | null {
  const s = String(raw || '')
    .trim()
    .toUpperCase();
  if (!s) return null;

  const parsed = normalizeSymbolInput(s);
  if (!('error' in parsed)) return parsed.symbol;

  const m = s.match(/^[A-Z]+:(\d{4,6})$/);
  if (m) {
    const code = m[1];
    if (/^\d{6}$/.test(code)) return `CN:${code}`;
    if (/^\d{4,5}$/.test(code)) return `HK:${code.padStart(4, '0')}`;
  }
  return null;
}

function resolveHeaderKey(headers: string[], aliases: string[]): string | null {
  const byNorm = new Map(headers.map((h) => [String(h).trim().toLowerCase(), h]));
  for (const alias of aliases) {
    const hit = byNorm.get(alias.trim().toLowerCase());
    if (hit) return hit;
  }
  return null;
}

export function pickScreenerField(
  row: Record<string, string>,
  headers: string[],
  field: keyof typeof TV_FIELD_ALIASES,
): string {
  const key = resolveHeaderKey(headers, TV_FIELD_ALIASES[field] ?? []);
  if (!key) return '';
  return String(row[key] ?? '').trim();
}

export function extractSymbolsFromSnapshotRows(
  rows: Array<Record<string, string>>,
  headers: string[],
): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const raw = pickScreenerField(row, headers, 'ticker');
    const sym = normalizeScreenerSymbol(raw);
    if (!sym || seen.has(sym)) continue;
    seen.add(sym);
    out.push(sym);
  }
  return out;
}

export function isHotspotTop3Industry(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const reasonsRaw = (t.values as Record<string, unknown> | undefined)?.industryFlowReasons;
  const reasons = Array.isArray(reasonsRaw) ? reasonsRaw.map((x) => String(x ?? '')) : [];
  if (reasons.includes('hotspots_today_top3')) return true;
  const parts = t.scoreParts as Record<string, unknown> | null | undefined;
  const v = parts?.hotspots_today_top3;
  return typeof v === 'number' && Number.isFinite(v) && v > 0;
}

function formatFlags(tvFlags: string, trend: TrendOkResult | undefined): string {
  const parts = tvFlags.trim() ? [tvFlags.trim()] : [];
  if (isHotspotTop3Industry(trend)) parts.push('Top3');
  return parts.join(' ');
}

export function buildScreenerMarkdownRows(
  rows: Array<Record<string, string>>,
  headers: string[],
  trendMap: Map<string, TrendOkResult>,
): ScreenerMarkdownRow[] {
  const enriched: ScreenerMarkdownRow[] = [];

  for (const row of rows) {
    const rawTicker = pickScreenerField(row, headers, 'ticker');
    const sym = normalizeScreenerSymbol(rawTicker);
    const trend = sym ? trendMap.get(sym) : undefined;
    const industryRaw = (trend?.values as Record<string, unknown> | undefined)?.industry;
    const industry = typeof industryRaw === 'string' && industryRaw.trim() ? industryRaw.trim() : '—';
    const score =
      typeof trend?.score === 'number' && Number.isFinite(trend.score) ? trend.score : null;

    enriched.push({
      symbol: sym ?? rawTicker,
      name: pickScreenerField(row, headers, 'name') || trend?.name || '—',
      industry,
      price: pickScreenerField(row, headers, 'price') || '—',
      changePct: pickScreenerField(row, headers, 'changePct') || '—',
      relVolume: pickScreenerField(row, headers, 'relVolume') || '—',
      score,
      intradayPct: formatIntradayChgPct(trend?.intradayChgPct ?? null),
      gapUp: formatGapUp(trend?.gapUp ?? null),
      flags: formatFlags(pickScreenerField(row, headers, 'flags'), trend),
    });
  }

  enriched.sort((a, b) => {
    const va = a.score;
    const vb = b.score;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return vb - va;
  });

  return enriched;
}

export function countMissingScores(rows: ScreenerMarkdownRow[]): number {
  return rows.filter((r) => r.score == null).length;
}

export function screenerMarkdownRowsToTable(rows: ScreenerMarkdownRow[]): unknown[][] {
  return rows.map((r) => [
    r.symbol,
    r.name,
    r.industry,
    r.price,
    r.changePct,
    r.relVolume,
    r.score != null ? String(Math.round(r.score)) : '—',
    r.intradayPct,
    r.gapUp,
    r.flags || '—',
  ]);
}
