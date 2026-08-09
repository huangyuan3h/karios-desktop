/**
 * Execution source attribution (TIP-011).
 *
 * Three provenances are tracked end-to-end so the Dashboard Copy markdown
 * and the /v1/execution/source-stats endpoint can break out BUY/ADD
 * win-rate by origin:
 *
 *   - 'TV'      — TV screener (funnel path)
 *   - 'ALPHA'   — Alpha Radar catalyst
 *   - 'MANUAL'  — user / external AI agent added to watchlist directly
 *
 * Inference rule (defensive — the canonical attribution is at write-time
 * via deriveActionCard / compute_alpha_additions):
 *   - If symbol appears in TV screener rows → 'TV'
 *   - Else if symbol appears in current Alpha Radar catalyst → 'ALPHA'
 *   - Else → 'MANUAL'
 */

import {
  DEFAULT_CATALYST_MAX_AGE_DAYS,
  fetchCatalystStocks,
  normalizeCatalystSymbol,
} from '@/lib/alpha-radar-catalyst';
import {
  fetchEnabledScreeners,
  fetchScreenerSnapshotsMap,
} from '@/lib/queries/screener';
import { extractSymbolsFromSnapshotRows } from '@/lib/screenerExport';

export type ExecutionSource = 'TV' | 'ALPHA' | 'MANUAL';

export const EXECUTION_SOURCES: readonly ExecutionSource[] = ['TV', 'ALPHA', 'MANUAL'] as const;

export interface SourceContext {
  /** Symbols in today's TV screener snapshot rows (any score). */
  tvSymbols: ReadonlySet<string>;
  /** Symbols in current Alpha Radar catalyst list (Top N). */
  alphaSymbols: ReadonlySet<string>;
}

/** Defensive source inference for backfill scripts and edge cases. */
export function inferSource(
  symbol: string,
  ctx: Pick<SourceContext, 'tvSymbols' | 'alphaSymbols'>,
): ExecutionSource {
  const sym = (symbol || '').trim().toUpperCase();
  if (!sym) return 'MANUAL';
  if (ctx.tvSymbols.has(sym)) return 'TV';
  if (ctx.alphaSymbols.has(sym)) return 'ALPHA';
  return 'MANUAL';
}

/** Build the SourceContext from already-loaded screener + catalyst payloads. */
export function buildSourceContext(payload: {
  tvSymbols?: Iterable<string>;
  alphaSymbols?: Iterable<string>;
}): SourceContext {
  return {
    tvSymbols: new Set<string>(
      Array.from(payload.tvSymbols ?? []).map((s) => s.toUpperCase()),
    ),
    alphaSymbols: new Set<string>(
      Array.from(payload.alphaSymbols ?? []).map((s) => s.toUpperCase()),
    ),
  };
}

/** Merge symbol into the appropriate set on a SourceContext (immutable). */
export function withSymbol(
  ctx: SourceContext,
  symbol: string,
  source: ExecutionSource,
): SourceContext {
  const sym = symbol.toUpperCase();
  if (source === 'TV') {
    return {
      tvSymbols: new Set<string>([...ctx.tvSymbols, sym]),
      alphaSymbols: ctx.alphaSymbols,
    };
  }
  if (source === 'ALPHA') {
    return {
      tvSymbols: ctx.tvSymbols,
      alphaSymbols: new Set<string>([...ctx.alphaSymbols, sym]),
    };
  }
  return ctx;
}

/** Raw response of GET /v1/execution/source-stats (TIP-011). */
export interface SourceStatsBucket {
  buySignals: number;
  closed: number;
  wins: number;
  losses: number;
  winRate: number;
}

export interface SourceStatsResponse {
  sinceDays: number;
  lookbackDays: number;
  generatedAt: string;
  bySource: Record<string, SourceStatsBucket>;
  openTradesBySource: Record<string, number>;
}

/**
 * Fetch per-source BUY-signal volume + paper-trade win-rate (TIP-011).
 * Uses the direct data-sync URL (same pattern as fetchExecutionJournalMarkdown).
 */
export async function fetchSourceStats(
  baseUrl: string,
  sinceDays = 30,
): Promise<SourceStatsResponse> {
  const res = await fetch(
    `${baseUrl}/v1/execution/source-stats?sinceDays=${encodeURIComponent(String(sinceDays))}`,
    { cache: 'no-store', signal: AbortSignal.timeout(30_000) },
  );
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (await res.json()) as SourceStatsResponse;
}

/** Stable display order for the Copy markdown section. */
export const SOURCE_DISPLAY_ORDER = ['TV', 'ALPHA', 'MANUAL', 'UNKNOWN'] as const;
export function formatSourceAttributionMarkdown(
  stats: SourceStatsResponse,
  opts: { heading?: string; openOnly?: boolean } = {},
): string {
  const heading = opts.heading ?? '##';
  const rows = SOURCE_DISPLAY_ORDER.filter((name) => stats.bySource[name]);
  const lines: string[] = [];
  lines.push(`${heading} Execution · Source attribution (${stats.sinceDays}d)`);
  lines.push('');
  if (!rows.length) {
    lines.push('- note: no BUY signals / closed trades in window yet (TIP-011 attribution active)');
    lines.push('');
    return lines.join('\n') + '\n';
  }
  lines.push('| Source | BUY signals | Closed | Wins | Losses | Win rate | Open |');
  lines.push('|--------|-------------|--------|------|--------|----------|------|');
  for (const name of rows) {
    const b = stats.bySource[name];
    const total = b.wins + b.losses;
    const winRate =
      total > 0 ? `${((b.wins / total) * 100).toFixed(1)}%` : '—';
    const open = stats.openTradesBySource[name] ?? 0;
    lines.push(
      `| ${name} | ${b.buySignals} | ${b.closed} | ${b.wins} | ${b.losses} | ${winRate} | ${open} |`,
    );
  }
  lines.push('');
  return lines.join('\n') + '\n';
}

/** Symbols in the latest snapshot rows of all enabled screeners (TV funnel). */
export async function fetchTvSourceSymbols(): Promise<Set<string>> {
  const screeners = await fetchEnabledScreeners();
  const ids = screeners.map((s) => String(s?.id ?? '').trim()).filter(Boolean);
  if (!ids.length) return new Set<string>();
  const snaps = await fetchScreenerSnapshotsMap(ids);
  const out = new Set<string>();
  for (const id of ids) {
    const snap = snaps[id];
    if (!snap || !Array.isArray(snap.rows)) continue;
    const headers = Array.isArray(snap.headers) ? snap.headers.map((h) => String(h ?? '')) : [];
    for (const sym of extractSymbolsFromSnapshotRows(snap.rows, headers)) {
      out.add(sym.toUpperCase());
    }
  }
  return out;
}

/** Symbols in the current Alpha Radar catalyst list (Top N). */
export async function fetchAlphaSourceSymbols(
  baseUrl: string,
  limit = 50,
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
): Promise<Set<string>> {
  const resp = await fetchCatalystStocks(baseUrl, limit, maxAgeDays);
  const out = new Set<string>();
  for (const it of Array.isArray(resp.items) ? resp.items : []) {
    const sym = normalizeCatalystSymbol(String(it?.symbol ?? ''));
    if (sym) out.add(sym.toUpperCase());
  }
  return out;
}

/** Combined TV + Alpha symbol sets for write-time source attribution. */
export async function fetchSourceContext(
  baseUrl: string,
  opts?: { limit?: number; maxAgeDays?: number },
): Promise<SourceContext> {
  const [tv, alpha] = await Promise.all([
    fetchTvSourceSymbols().catch(() => new Set<string>()),
    fetchAlphaSourceSymbols(
      baseUrl,
      opts?.limit ?? 50,
      opts?.maxAgeDays ?? DEFAULT_CATALYST_MAX_AGE_DAYS,
    ).catch(() => new Set<string>()),
  ]);
  return buildSourceContext({ tvSymbols: tv, alphaSymbols: alpha });
}