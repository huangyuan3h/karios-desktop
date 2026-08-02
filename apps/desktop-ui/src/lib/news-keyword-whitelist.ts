/**
 * News keyword whitelist (2026-08-01 · wife feedback).
 * Drop news titles that don't match any keyword (CN + EN).
 * Used by:
 *   - useDashboardSummary.ts: buildNewsFallback filters items
 *   - dashboard-export.ts (Copy all): same filter applied to newsSummary
 */

export const NEWS_KEYWORD_WHITELIST: readonly string[] = [
  // 中文核心词
  'AI',
  '算力',
  '半导体',
  '美联储',
  '降准',
  '降息',
  '原油',
  '关税',
  // 中文常见变体
  '芯片',
  '人工智能',
  '英伟达',
  'NVDA',
  '台积电',
  'TSMC',
  '央行',
  'PBOC',
  '人行',
  '人民币',
  '美元',
  'CPI',
  'PPI',
  'GDP',
  'PMI',
  '非农',
  '加息',
  '减息',
  '政治局',
  '国务院',
  '财政部',
  '证监会',
  '股市',
  'A股',
  '港股',
  '美股',
  '欧股',
  '黄金',
  '白银',
  '铜',
  '油价',
  'WTI',
  '布伦特',
  'OPEC',
  '俄乌',
  '中东',
  '伊朗',
  '以色列',
  '欧洲',
  '欧盟',
  '特朗普',
  '拜登',
  // 英文核心词
  'Fed',
  'Federal Reserve',
  'Powell',
  'rate cut',
  'rate hike',
  'tariff',
  'trade war',
  'semiconductor',
  'chip',
  'GPU',
  'data center',
  'data center',
  'OPEC',
  'Brent',
  'WTI',
  'crude',
  'oil',
  'gold',
  'copper',
  'silver',
  'iron ore',
  'lithium',
  'rare earth',
  'EV',
  'battery',
  'solar',
  'wind',
  'nuclear',
  'yuan',
  'renminbi',
  'dollar',
  'euro',
  'yen',
  'Treasury',
  'bond',
  'yield',
  'yield curve',
  'recession',
  'soft landing',
  'inflation',
  'deflation',
  'stagflation',
  'earnings',
  'guidance',
  'IPO',
  'M&A',
  'merger',
  'acquisition',
  'antitrust',
  'sanction',
  'export control',
  'export ban',
  'entity list',
  'TikTok',
  'Huawei',
  'Tencent',
  'Alibaba',
  'BYD',
  'CATL',
  'Apple',
  'Microsoft',
  'Google',
  'Meta',
  'Tesla',
  'Amazon',
];

function normalize(title: string): string {
  return String(title || '').toLowerCase();
}

// Pre-built lookup: short keywords (<=4 chars) need word boundaries to avoid
// false matches (e.g. "Chipotle" contains "IPO" as substring).
const SHORT_KEYWORDS = NEWS_KEYWORD_WHITELIST.filter((k) => k.length <= 4).map((k) =>
  k.toLowerCase(),
);
const LONG_KEYWORDS = NEWS_KEYWORD_WHITELIST.filter((k) => k.length > 4).map((k) =>
  k.toLowerCase(),
);

// Cached regex per short keyword.
const shortKeywordRegexes = SHORT_KEYWORDS.map(
  (k) => new RegExp(`(?<![a-z0-9])${escapeRegExp(k)}(?![a-z0-9])`, 'i'),
);

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Returns true if `title` contains any whitelisted keyword.
 * - Long keywords (>4 chars): case-insensitive substring match.
 * - Short keywords (<=4 chars): case-insensitive whole-word match.
 */
export function isNewsTitleWhitelisted(title: string): boolean {
  const lower = normalize(title);
  if (!lower) return false;
  for (let i = 0; i < LONG_KEYWORDS.length; i++) {
    const k = LONG_KEYWORDS[i];
    if (k && lower.includes(k)) return true;
  }
  for (let i = 0; i < shortKeywordRegexes.length; i++) {
    const re = shortKeywordRegexes[i];
    if (re && re.test(lower)) return true;
  }
  return false;
}

/**
 * Pure filter helper for testing.
 */
export function filterNewsByKeyword<T extends { title?: string | null }>(items: T[]): T[] {
  if (!Array.isArray(items)) return [];
  return items.filter((it) => isNewsTitleWhitelisted(String(it?.title ?? '')));
}
