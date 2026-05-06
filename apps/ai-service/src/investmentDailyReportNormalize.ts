import { InvestmentDailyReportResponseSchema } from './schemas';

const FALLBACK = {
  traffic: '依据 Markdown 信息不足，暂无法给出红绿灯与情绪解读。',
  bullets: '- 依据不足，待数据更新后再提炼要点。',
  hot: '依据不足：热点行业工作流表格或规则信息不完整，无法展开书面分析。',
  mainline: '依据不足：行业五日流向与热点矩阵信息不完整，暂无法综合评述主线与资金。',
  stockRationale: '依据不足，未能从筛选器与自选中提取有效理由。',
  newsSummary: '依据不足，未能从新闻摘要中提炼有效内容。',
} as const;

function clip(s: string, max: number): string {
  if (s.length <= max) return s;
  return s.slice(0, Math.max(0, max - 1)) + '…';
}

function nonEmpty(s: string, fallback: string): string {
  const t = s.trim();
  return t.length > 0 ? t : fallback;
}

function asRecord(x: unknown): Record<string, unknown> {
  return x && typeof x === 'object' && !Array.isArray(x) ? (x as Record<string, unknown>) : {};
}

/**
 * Coerce common LLM mistakes (wrong array lengths, empty strings, snake_case keys)
 * before Zod validation so PDF generation can proceed.
 */
export function normalizeInvestmentDailyReportPayload(raw: unknown): unknown {
  const r = asRecord(raw);
  const pick = (camel: string, snake: string): unknown => {
    if (camel in r && r[camel] !== undefined) return r[camel];
    if (snake in r && r[snake] !== undefined) return r[snake];
    return undefined;
  };

  const fixStocks = (arr: unknown): unknown[] => {
    if (!Array.isArray(arr)) return [];
    const rows = arr.filter((x) => x && typeof x === 'object').map((x) => asRecord(x));
    const out = rows.slice(0, 3).map((row) => ({
      symbol: clip(nonEmpty(String(row.symbol ?? ''), '—'), 32),
      name: clip(nonEmpty(String(row.name ?? ''), '—'), 64),
      rationale: clip(nonEmpty(String(row.rationale ?? ''), FALLBACK.stockRationale), 2000),
    }));
    while (out.length < 3) {
      out.push({
        symbol: '—',
        name: '—',
        rationale: FALLBACK.stockRationale,
      });
    }
    return out;
  };

  const fixNews = (arr: unknown): unknown[] => {
    if (!Array.isArray(arr)) return [];
    const rows = arr.filter((x) => x && typeof x === 'object').map((x) => asRecord(x));
    const out = rows.slice(0, 5).map((row) => ({
      title: clip(nonEmpty(String(row.title ?? ''), '—'), 500),
      summary: clip(nonEmpty(String(row.summary ?? ''), FALLBACK.newsSummary), 1200),
    }));
    while (out.length < 5) {
      out.push({
        title: '—',
        summary: FALLBACK.newsSummary,
      });
    }
    return out;
  };

  return {
    trafficLightPositionAndSentiment: clip(
      nonEmpty(String(pick('trafficLightPositionAndSentiment', 'traffic_light_position_and_sentiment') ?? ''), FALLBACK.traffic),
      8000,
    ),
    marketEnvironmentHighlights: clip(
      nonEmpty(String(pick('marketEnvironmentHighlights', 'market_environment_highlights') ?? ''), FALLBACK.bullets),
      4000,
    ),
    hotIndustriesFormalAnalysis: clip(
      nonEmpty(String(pick('hotIndustriesFormalAnalysis', 'hot_industries_formal_analysis') ?? ''), FALLBACK.hot),
      10000,
    ),
    capitalFlowAndMainline: clip(
      nonEmpty(String(pick('capitalFlowAndMainline', 'capital_flow_and_mainline') ?? ''), FALLBACK.mainline),
      12000,
    ),
    topStocks: fixStocks(pick('topStocks', 'top_stocks')),
    topNews: fixNews(pick('topNews', 'top_news')),
  };
}

export function parseInvestmentDailyReportAfterNormalize(raw: unknown) {
  const normalized = normalizeInvestmentDailyReportPayload(raw);
  return InvestmentDailyReportResponseSchema.safeParse(normalized);
}
