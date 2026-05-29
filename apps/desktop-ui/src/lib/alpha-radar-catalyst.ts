export type CatalystArticle = {
  trendId: string;
  trendName: string;
  macroTheme?: string | null;
  catalystGrade?: string | null;
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

export const DEFAULT_CATALYST_MAX_AGE_DAYS = 30;

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
}): string {
  return JSON.stringify({
    Macro_Theme: trendMacroTheme(trend),
    Catalyst_Grade: trendCatalystGrade(trend),
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
  opts?: { headingLevel?: '##' | '###'; limit?: number },
): string {
  const heading = opts?.headingLevel ?? '##';
  const limit = opts?.limit ?? trends.length;
  const rows = trends.slice(0, Math.max(0, limit));
  const lines: string[] = [];
  lines.push(`${heading} Alpha Radar · Structured Trends`);
  lines.push(`- count: ${rows.length}`);
  lines.push('');

  if (!rows.length) {
    lines.push('No structured trends in the latest batch. Run Alpha Radar pipeline first.');
    return lines.join('\n').trim() + '\n';
  }

  lines.push('| Macro Theme | Catalyst Grade | Global Target | A-share Mapping |');
  lines.push('| --- | :---: | --- | --- |');
  for (const trend of rows) {
    const theme = trendMacroTheme(trend);
    const grade = trendCatalystGrade(trend);
    lines.push(
      `| ${theme} | ${grade} | ${trend.globalTarget || 'N/A'} | ${formatCnSymbols(trend.cnSymbols)} |`,
    );
  }
  lines.push('');

  for (const trend of rows) {
    const theme = trendMacroTheme(trend);
    const grade = trendCatalystGrade(trend);
    lines.push(`### ${theme} (${grade})`);
    lines.push(`- structured: \`${formatStructuredTrendJson(trend)}\``);
    if (trend.catalyst) lines.push(`- catalyst: ${trend.catalyst}`);
    if (trend.globalTarget) lines.push(`- globalTarget: ${trend.globalTarget}`);
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
  opts?: { headingLevel?: '##' | '###'; includeDetails?: boolean },
): string {
  const heading = opts?.headingLevel ?? '##';
  const includeDetails = opts?.includeDetails ?? true;
  const lines: string[] = [];
  lines.push(`${heading} Alpha Radar · Top Catalyst Stocks`);
  lines.push(`- maxAgeDays: ${resp.maxAgeDays}`);
  lines.push(`- stalenessBasis: ${resp.stalenessBasis}`);
  lines.push(`- total: ${resp.total}`);
  lines.push('');

  if (!resp.items.length) {
    lines.push('No catalyst stocks in the current window.');
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
        if (article.catalyst) lines.push(`  - catalyst: ${article.catalyst}`);
        else if (article.summary) lines.push(`  - sourceSummary: ${article.summary}`);
        if (article.globalTarget) lines.push(`  - globalTarget: ${article.globalTarget}`);
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
  const res = await fetch(
    `${baseUrl}/api/alpha-radar/catalyst-stocks?limit=${limit}&maxAgeDays=${maxAgeDays}`,
    { cache: 'no-store', signal: AbortSignal.timeout(60_000) },
  );
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return txt ? (JSON.parse(txt) as CatalystStocksResponse) : { stalenessBasis: '', maxAgeDays, total: 0, items: [] };
}

export async function fetchAlphaRadarTrends(
  baseUrl: string,
  limit = 20,
  latestBatch = true,
): Promise<AlphaRadarTrendExport[]> {
  const params = new URLSearchParams({
    limit: String(limit),
    latest_batch: String(latestBatch),
  });
  const res = await fetch(`${baseUrl}/api/alpha-radar/trends?${params.toString()}`, {
    cache: 'no-store',
    signal: AbortSignal.timeout(60_000),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  const body = txt ? (JSON.parse(txt) as AlphaRadarTrendsResponse) : { total: 0, items: [] };
  return Array.isArray(body.items) ? body.items : [];
}
