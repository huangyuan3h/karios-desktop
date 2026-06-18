import { apiFetchJson } from '@/lib/api/client';

export type CatalystArticle = {
  trendId: string;
  trendName: string;
  macroTheme?: string | null;
  catalystGrade?: string | null;
  driverType?: string | null;
  eventFocus?: string | null;
  logicSummary?: string | null;
  catalyst?: string | null;
  globalTarget?: string | null;
  documentId: string;
  relevance: number;
  contribution: number;
  documentTitle?: string | null;
  documentUrl?: string | null;
  summary?: string | null;
  publishedAt?: string | null;
  urgencyLevel: string;
};

export type AlphaRadarTrendExport = {
  id: string;
  trendName: string;
  macroTheme?: string | null;
  catalystGrade?: string | null;
  driverType?: string | null;
  eventFocus?: string | null;
  logicSummary?: string | null;
  catalyst?: string | null;
  globalTarget?: string | null;
  keywordsForMapping?: string[];
  cnSymbols?: Array<{
    symbol: string;
    name: string;
    confidence: number;
    rationale: string;
  }>;
  riskStatus?: string;
  documentTitle?: string | null;
  documentUrl?: string | null;
  documentPublishedAt?: string | null;
};

export type AlphaRadarTrendsResponse = {
  total: number;
  items: AlphaRadarTrendExport[];
};

export type CatalystStock = {
  symbol: string;
  name: string;
  catalystScore: number;
  articleCount: number;
  latestArticleAt?: string | null;
  articles: CatalystArticle[];
};

export type CatalystStocksResponse = {
  stalenessBasis: string;
  maxAgeDays: number;
  total: number;
  items: CatalystStock[];
};

export type CatalystTrendOkSnapshot = {
  symbol: string;
  trendOk?: boolean | null;
  score?: number | null;
};

export type CatalystCopyContext = {
  watchlistSymbols: Set<string>;
  watchlistScores: Map<string, number>;
  screenerTrendOkSymbols: Set<string>;
  trendMap: Map<string, CatalystTrendOkSnapshot>;
};

export const DEFAULT_CATALYST_MAX_AGE_DAYS = 30;
export const CATALYST_NEWS_MAX_HOURS = 72;
export const CATALYST_NEWS_MAX_ITEMS = 3;
export const WATCHLIST_CATALYST_SCORE_THRESHOLD = 80;

const GRADE_RANK: Record<string, number> = { S: 4, A: 3, B: 2, C: 1 };

export function formatCatalystScore(score: number): string {
  return Number.isFinite(score) ? score.toFixed(1) : '—';
}

export function formatRelevancePct(relevance: number): string {
  return `${Math.round(Math.max(0, Math.min(relevance, 1)) * 100)}%`;
}

export function displaySymbol(symbol: string): string {
  const text = String(symbol || '').trim();
  return text.startsWith('CN:') ? text.slice(3) : text;
}

export function normalizeCatalystSymbol(symbol: string): string {
  const text = String(symbol || '').trim().toUpperCase();
  if (!text) return '';
  if (text.startsWith('CN:') || text.startsWith('HK:')) return text;
  if (/^\d{6}$/.test(text)) return `CN:${text}`;
  if (/^\d{4,5}$/.test(text)) return `HK:${text.padStart(4, '0')}`;
  return text;
}

export function maxGradeArticle(
  articles: CatalystArticle[],
): { grade: string; theme: string } | null {
  if (!articles.length) return null;
  let best: CatalystArticle | null = null;
  let bestRank = -1;
  for (const article of articles) {
    const grade = trendCatalystGrade(article);
    const rank = GRADE_RANK[grade.toUpperCase()] ?? 0;
    if (rank > bestRank) {
      bestRank = rank;
      best = article;
    }
  }
  if (!best) return null;
  return { grade: trendCatalystGrade(best), theme: trendMacroTheme(best) };
}

export function formatCatalystStockSummaryLine(stock: CatalystStock): string {
  const sym = normalizeCatalystSymbol(stock.symbol);
  const maxGrade = maxGradeArticle(stock.articles);
  const gradePart = maxGrade ? `Max Grade: ${maxGrade.grade} (${maxGrade.theme})` : 'Max Grade: —';
  return `${sym} ${stock.name} | Score: ${formatCatalystScore(stock.catalystScore)} | ${gradePart}`;
}

export function articleAgeHours(publishedAt?: string | null, now = Date.now()): number | null {
  const eventAt = parseEventDate(publishedAt);
  if (!eventAt) return null;
  return Math.max(0, (now - eventAt.getTime()) / 3_600_000);
}

export function filterRecentArticles(
  articles: CatalystArticle[],
  maxHours = CATALYST_NEWS_MAX_HOURS,
  limit = CATALYST_NEWS_MAX_ITEMS,
  now = Date.now(),
): CatalystArticle[] {
  const recent = articles.filter((article) => {
    const ageHours = articleAgeHours(article.publishedAt, now);
    return ageHours != null && ageHours <= maxHours;
  });
  recent.sort((a, b) => floatOrZero(b.contribution) - floatOrZero(a.contribution));
  return recent.slice(0, Math.max(0, limit));
}

function floatOrZero(value: number | undefined | null): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

export function formatCatalystNewsLine(article: CatalystArticle): string {
  const grade = trendCatalystGrade(article);
  const theme = trendMacroTheme(article);
  const driver = article.driverType ? `${article.driverType} · ` : '';
  const body =
    (article.eventFocus && String(article.eventFocus).trim()) ||
    (article.catalyst && String(article.catalyst).trim()) ||
    (article.summary && String(article.summary).trim()) ||
    (article.documentTitle && String(article.documentTitle).trim()) ||
    '—';
  const logic = article.logicSummary ? ` (${article.logicSummary})` : '';
  return `- ${grade} · ${driver}${theme} · ${body}${logic}`;
}

export function isTechnicallyBroken(trend: CatalystTrendOkSnapshot | undefined | null): boolean {
  return trend?.trendOk === false;
}

export function isCatalystEligible(
  symbol: string,
  ctx: CatalystCopyContext,
): boolean {
  const sym = normalizeCatalystSymbol(symbol);
  if (ctx.watchlistSymbols.has(sym)) {
    const score = ctx.watchlistScores.get(sym);
    if (typeof score === 'number' && Number.isFinite(score) && score > WATCHLIST_CATALYST_SCORE_THRESHOLD) {
      return true;
    }
  }
  if (ctx.screenerTrendOkSymbols.has(sym)) return true;
  return false;
}

export function shouldShowCatalystNews(symbol: string, ctx: CatalystCopyContext): boolean {
  const sym = normalizeCatalystSymbol(symbol);
  if (!isCatalystEligible(sym, ctx)) return false;
  const trend = ctx.trendMap.get(sym);
  if (isTechnicallyBroken(trend)) return false;
  return true;
}

export function parseEventDate(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

export function articleAgeDays(
  publishedAt?: string | null,
  fetchedAt?: string | null,
  now = Date.now(),
): number | null {
  const eventAt = parseEventDate(publishedAt) ?? parseEventDate(fetchedAt);
  if (!eventAt) return null;
  return Math.max(0, Math.floor((now - eventAt.getTime()) / 86_400_000));
}

export function isStaleArticle(
  publishedAt?: string | null,
  fetchedAt?: string | null,
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
): boolean {
  const age = articleAgeDays(publishedAt, fetchedAt);
  return age != null && age > maxAgeDays;
}

export function trendMacroTheme(trend: {
  macroTheme?: string | null;
  trendName?: string | null;
}): string {
  return String(trend.macroTheme || trend.trendName || '').trim() || 'Unknown theme';
}

export function trendCatalystGrade(trend: {
  catalystGrade?: string | null;
  urgencyLevel?: string | null;
}): string {
  return String(trend.catalystGrade || trend.urgencyLevel || 'B').trim() || 'B';
}

export function formatStructuredTrendJson(trend: {
  macroTheme?: string | null;
  trendName?: string | null;
  catalystGrade?: string | null;
  urgencyLevel?: string | null;
  driverType?: string | null;
  eventFocus?: string | null;
  logicSummary?: string | null;
}): string {
  return JSON.stringify({
    Macro_Theme: trendMacroTheme(trend),
    Driver_Type: trend.driverType || 'Global_Tech',
    Catalyst_Grade: trendCatalystGrade(trend),
    Event_Focus: trend.eventFocus || null,
    Logic_Summary: trend.logicSummary || null,
  });
}

function formatCnSymbols(
  symbols: AlphaRadarTrendExport['cnSymbols'] | undefined,
): string {
  if (!symbols?.length) return '—';
  return symbols
    .map((s) => `${s.name} (${displaySymbol(s.symbol)}, ${Math.round(s.confidence * 100)}%)`)
    .join('; ');
}

export function buildAlphaRadarTrendsMarkdown(
  trends: AlphaRadarTrendExport[],
  opts?: {
    headingLevel?: '##' | '###';
    limit?: number;
    mode?: 'full' | 'compact';
    scopeNote?: string;
  },
): string {
  const heading = opts?.headingLevel ?? '##';
  const limit = opts?.limit ?? trends.length;
  const mode = opts?.mode ?? 'full';
  const rows = trends.slice(0, Math.max(0, limit));
  const lines: string[] = [];
  lines.push(`${heading} Alpha Radar · Structured Trends`);
  lines.push(`- count: ${rows.length}`);
  if (opts?.scopeNote) lines.push(`- scope: ${opts.scopeNote}`);
  lines.push('');

  if (!rows.length) {
    lines.push('No structured trends in the latest batch. Run Alpha Radar pipeline first.');
    return lines.join('\n').trim() + '\n';
  }

  lines.push('| Macro Theme | Driver | Grade | A-share Mapping |');
  lines.push('| --- | --- | :---: | --- |');
  for (const trend of rows) {
    const theme = trendMacroTheme(trend);
    const grade = trendCatalystGrade(trend);
    const driver = trend.driverType || 'Global_Tech';
    lines.push(
      `| ${theme} | ${driver} | ${grade} | ${formatCnSymbols(trend.cnSymbols)} |`,
    );
  }
  lines.push('');

  if (mode === 'compact') {
    return lines.join('\n').trim() + '\n';
  }

  for (const trend of rows) {
    const theme = trendMacroTheme(trend);
    const grade = trendCatalystGrade(trend);
    lines.push(`### ${theme} (${grade})`);
    lines.push(`- structured: \`${formatStructuredTrendJson(trend)}\``);
    if (trend.driverType) lines.push(`- driverType: ${trend.driverType}`);
    if (trend.eventFocus) lines.push(`- eventFocus: ${trend.eventFocus}`);
    else if (trend.catalyst) lines.push(`- catalyst: ${trend.catalyst}`);
    if (trend.logicSummary) lines.push(`- logicSummary: ${trend.logicSummary}`);
    if (trend.keywordsForMapping?.length) {
      lines.push(`- keywords: ${trend.keywordsForMapping.join(', ')}`);
    }
    if (trend.documentTitle) lines.push(`- source: ${trend.documentTitle}`);
    if (trend.documentUrl) lines.push(`- url: ${trend.documentUrl}`);
    if (trend.riskStatus) lines.push(`- riskStatus: ${trend.riskStatus}`);
    if (trend.cnSymbols?.length) {
      lines.push('- cnMapping:');
      for (const s of trend.cnSymbols) {
        lines.push(
          `  - ${s.name} (${displaySymbol(s.symbol)}) confidence=${Math.round(s.confidence * 100)}% · ${s.rationale}`,
        );
      }
    }
    lines.push('');
  }

  return lines.join('\n').trim() + '\n';
}

export function buildCatalystStocksMarkdown(
  resp: CatalystStocksResponse,
  opts?: {
    headingLevel?: '##' | '###';
    includeDetails?: boolean;
    mode?: 'full' | 'compact';
    context?: CatalystCopyContext;
    now?: number;
  },
): string {
  const heading = opts?.headingLevel ?? '##';
  const mode = opts?.mode ?? 'full';
  const includeDetails = opts?.includeDetails ?? mode === 'full';
  const now = opts?.now ?? Date.now();
  const lines: string[] = [];
  lines.push(`${heading} Alpha Radar · Top Catalyst Stocks`);
  lines.push(`- maxAgeDays: ${resp.maxAgeDays}`);
  if (mode === 'full') lines.push(`- stalenessBasis: ${resp.stalenessBasis}`);
  lines.push(`- total: ${resp.total}`);
  lines.push('');

  if (!resp.items.length) {
    lines.push('No catalyst stocks in the current window.');
    return lines.join('\n').trim() + '\n';
  }

  if (mode === 'compact') {
    for (const row of resp.items) {
      lines.push(formatCatalystStockSummaryLine(row));
      const ctx = opts?.context;
      if (ctx && shouldShowCatalystNews(row.symbol, ctx)) {
        const recent = filterRecentArticles(row.articles, CATALYST_NEWS_MAX_HOURS, CATALYST_NEWS_MAX_ITEMS, now);
        if (recent.length) {
          lines.push('====');
          for (const article of recent) {
            lines.push(formatCatalystNewsLine(article));
          }
        }
      }
      lines.push('');
    }
    return lines.join('\n').trim() + '\n';
  }

  lines.push('| Rank | Symbol | Name | Score | Articles | Latest |');
  lines.push('| --- | --- | --- | ---: | ---: | --- |');
  resp.items.forEach((row, idx) => {
    const latest = row.latestArticleAt ? row.latestArticleAt.slice(0, 10) : '—';
    lines.push(
      `| ${idx + 1} | ${displaySymbol(row.symbol)} | ${row.name} | ${formatCatalystScore(row.catalystScore)} | ${row.articleCount} | ${latest} |`,
    );
  });
  lines.push('');

  if (includeDetails) {
    for (const row of resp.items) {
      lines.push(
        `### ${displaySymbol(row.symbol)} ${row.name} (${formatCatalystScore(row.catalystScore)})`,
      );
      for (const article of row.articles) {
        const theme = trendMacroTheme(article);
        const grade = trendCatalystGrade(article);
        const title = article.documentTitle || theme;
        lines.push(`- **${theme}** | Grade **${grade}** | ${title} (${formatRelevancePct(article.relevance)})`);
        lines.push(`  - structured: \`${formatStructuredTrendJson(article)}\``);
        if (article.driverType) lines.push(`  - driverType: ${article.driverType}`);
        if (article.eventFocus) lines.push(`  - eventFocus: ${article.eventFocus}`);
        else if (article.catalyst) lines.push(`  - catalyst: ${article.catalyst}`);
        else if (article.summary) lines.push(`  - sourceSummary: ${article.summary}`);
        if (article.logicSummary) lines.push(`  - logicSummary: ${article.logicSummary}`);
        if (article.documentUrl) lines.push(`  - url: ${article.documentUrl}`);
      }
      lines.push('');
    }
  }

  return lines.join('\n').trim() + '\n';
}

export async function fetchCatalystStocks(
  baseUrl: string,
  limit = 50,
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
): Promise<CatalystStocksResponse> {
  return apiFetchJson<CatalystStocksResponse>(
    `/api/alpha-radar/catalyst-stocks?limit=${limit}&maxAgeDays=${maxAgeDays}`,
    { baseUrl, signal: AbortSignal.timeout(60_000) },
  );
}

export async function fetchAlphaRadarTrends(
  baseUrl: string,
  limit = 20,
  latestBatch = true,
  maxAgeDays?: number,
): Promise<AlphaRadarTrendExport[]> {
  const params = new URLSearchParams({
    limit: String(limit),
    latest_batch: String(latestBatch),
  });
  if (maxAgeDays != null) {
    params.set('maxAgeDays', String(maxAgeDays));
  }
  const body = await apiFetchJson<AlphaRadarTrendsResponse>(
    `/api/alpha-radar/trends?${params.toString()}`,
    { baseUrl, signal: AbortSignal.timeout(60_000) },
  );
  return Array.isArray(body.items) ? body.items : [];
}

/** Prefer latest batch; fall back to recent window when the batch marker is empty. */
export async function fetchAlphaRadarTrendsForCopy(
  baseUrl: string,
  limit = 20,
  maxAgeDays = DEFAULT_CATALYST_MAX_AGE_DAYS,
): Promise<{ items: AlphaRadarTrendExport[]; scope: 'latest_batch' | 'recent' }> {
  const latest = await fetchAlphaRadarTrends(baseUrl, limit, true);
  if (latest.length > 0) {
    return { items: latest, scope: 'latest_batch' };
  }
  const recent = await fetchAlphaRadarTrends(baseUrl, limit, false, maxAgeDays);
  return { items: recent, scope: 'recent' };
}
