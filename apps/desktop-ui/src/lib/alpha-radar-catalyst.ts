export type CatalystArticle = {
  trendId: string;
  trendName: string;
  macroTheme?: string | null;
  documentId: string;
  relevance: number;
  contribution: number;
  documentTitle?: string | null;
  documentUrl?: string | null;
  summary?: string | null;
  publishedAt?: string | null;
  urgencyLevel: string;
  catalystGrade?: string | null;
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
        const title = article.documentTitle || article.trendName || 'Untitled';
        const summary = article.summary ? ` — ${article.summary}` : '';
        lines.push(
          `- ${title} (${formatRelevancePct(article.relevance)})${summary}`,
        );
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
