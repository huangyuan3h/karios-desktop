import * as React from 'react';

import type { HotIndustryPick } from '@/components/pages/HotIndustryWorkflowCard';

export {
  INVESTMENT_DAILY_MARKDOWN_MAX_CHARS,
  type DownloadInvestmentDailyPdfArgs,
  type InvestmentDailyPdfLayout,
  type InvestmentDailyReportPayload,
  type PdfTableBlock,
} from './investmentDailyPdfTypes';

import type {
  DownloadInvestmentDailyPdfArgs,
  InvestmentDailyPdfLayout,
  InvestmentDailyReportPayload,
  PdfTableBlock,
} from './investmentDailyPdfTypes';
import { INVESTMENT_DAILY_MARKDOWN_MAX_CHARS } from './investmentDailyPdfTypes';

export function truncateMarkdownForReport(markdown: string): string {
  if (markdown.length <= INVESTMENT_DAILY_MARKDOWN_MAX_CHARS) return markdown;
  return (
    markdown.slice(0, INVESTMENT_DAILY_MARKDOWN_MAX_CHARS) +
    '\n\n---\n\n[Truncated: input exceeded ' +
    INVESTMENT_DAILY_MARKDOWN_MAX_CHARS +
    ' characters]\n'
  );
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

const HOT_INDUSTRY_STATIC_RULES_ZH = [
  'Rule V4.0：优先「动量突破」（今日净流入>20亿 且 排名提升>10名）；否则采用当日 Top ∩ 强势 5D 排名。',
  '动量突破板块常为新主线首日，爆发力通常强于已长期在 5D 排名中的板块。',
  '执行纪律：仅考虑来自上述观测行业、且通过技术筛选的股票加入自选。',
];

const SENTIMENT_STATIC_NOTES_ZH =
  '说明：上表为各主要指数「信号灯」与建议仓位区间；下表为全市场近五个交易日的涨跌家数比、成交额、涨停溢价与炸板率。' +
  'risk 字段为系统根据成交额与广度等规则判定的情绪档位（如 hot / normal / caution）。';

function asRecord(x: unknown): Record<string, unknown> {
  return x && typeof x === 'object' ? (x as Record<string, unknown>) : {};
}

function buildIndustryLayout(summary: unknown): {
  industryTopByDate: PdfTableBlock | null;
  industryInflow: PdfTableBlock | null;
  industryOutflow: PdfTableBlock | null;
} {
  const s = asRecord(summary);
  const ind = asRecord(s.industryFundFlow);
  const datesAll: string[] = Array.isArray(ind.dates) ? (ind.dates as string[]) : [];
  const rawShownDates = datesAll.slice(-5);
  const topByDateArr: unknown[] = Array.isArray(ind.topByDate) ? (ind.topByDate as unknown[]) : [];
  const byDate: Record<string, string[]> = {};
  for (const it of topByDateArr) {
    const r = asRecord(it);
    const d = String(r.date ?? '');
    const top = Array.isArray(r.top) ? (r.top as unknown[]).map((x) => String(x ?? '')) : [];
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

  let industryTopByDate: PdfTableBlock | null = null;
  if (dedupedDates.length) {
    const headers1 = ['#', ...dedupedDates.map((d) => String(d).slice(5))];
    const rows1: string[][] = Array.from({ length: 5 }).map((_, i) => [
      String(i + 1),
      ...dedupedDates.map((d) => String((byDate[d] || [])[i] ?? '')),
    ]);
    industryTopByDate = { title: '行业 Top5×日期（热点名称）', headers: headers1, rows: rows1 };
  }

  let industryInflow: PdfTableBlock | null = null;
  let industryOutflow: PdfTableBlock | null = null;

  const buildFlow = (block: unknown, titleZh: string): PdfTableBlock | null => {
    const b = asRecord(block);
    const dates: string[] = Array.isArray(b.dates) ? (b.dates as string[]) : [];
    const cols: string[] = dates.length ? dates.slice(-5) : dedupedDates;
    const topRows: unknown[] = Array.isArray(b.top) ? (b.top as unknown[]) : [];
    if (!topRows.length || !cols.length) return null;
    const headers = ['行业', '5日合计', ...cols.map((d) => String(d).slice(5))];
    const rows: string[][] = topRows.slice(0, 10).map((row) => {
      const r = asRecord(row);
      const seriesArr: unknown[] = Array.isArray(r.series) ? (r.series as unknown[]) : [];
      const m2: Record<string, number> = {};
      for (const p of seriesArr) {
        const pr = asRecord(p);
        const dd = String(pr.date ?? '');
        const nv = Number(pr.netInflow ?? 0);
        if (dd) m2[dd] = Number.isFinite(nv) ? nv : 0;
      }
      return [
        String(r.industryName ?? ''),
        fmtAmountCn(r.sum5d),
        ...cols.map((d) => fmtAmountCn(m2[d] ?? 0)),
      ];
    });
    return { title: titleZh, headers, rows };
  };

  industryInflow = buildFlow(ind.flow5d ?? null, '五日主力净流入（按 5 日合计 Top）');
  industryOutflow = buildFlow(ind.flow5dOut ?? null, '五日主力净流出（按 5 日合计 Top）');

  return { industryTopByDate, industryInflow, industryOutflow };
}

function buildMacroTable(summary: unknown): PdfTableBlock | null {
  const s = asRecord(summary);
  const macroSnapshot = asRecord(s.macroSnapshot);
  const macroItems: unknown[] = Array.isArray(macroSnapshot.macro) ? (macroSnapshot.macro as unknown[]) : [];
  if (!macroItems.length) return null;
  const headers = ['名称', '收盘', '涨跌%', 'MA5', 'MA20', '日期', '来源'];
  const rows: string[][] = macroItems.map((it) => {
    const x = asRecord(it);
    const pct = x.pctChg;
    const chg =
      typeof pct === 'number' && Number.isFinite(pct)
        ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`
        : '—';
    return [
      String(x.name ?? x.seriesId ?? ''),
      Number.isFinite(x.close as number) ? Number(x.close).toFixed(2) : '—',
      chg,
      Number.isFinite(x.ma5 as number) ? Number(x.ma5).toFixed(2) : '—',
      Number.isFinite(x.ma20 as number) ? Number(x.ma20).toFixed(2) : '—',
      String(x.asOfDate ?? ''),
      String(x.source ?? ''),
    ];
  });
  return { headers, rows };
}

function buildSentimentLayout(summary: unknown): {
  sentimentIndexTable: PdfTableBlock | null;
  sentimentDailyTable: PdfTableBlock | null;
  sentimentRuleLines: string[];
} {
  const s = asRecord(summary);
  const ms = asRecord(s.marketSentiment);
  const items: unknown[] = Array.isArray(ms.items) ? (ms.items as unknown[]) : [];
  const latest = items.length ? asRecord(items[items.length - 1]) : null;
  const asOfDate = String(ms.asOfDate ?? s.asOfDate ?? '').trim();
  const indexSignals: unknown[] = Array.isArray(ms.indexSignals) ? (ms.indexSignals as unknown[]) : [];

  let sentimentIndexTable: PdfTableBlock | null = null;
  if (indexSignals.length) {
    const headers0 = ['指数', '信号', '建议仓位', '涨跌%', '收盘', 'MA5', 'MA20', '日期'];
    const rows0: string[][] = indexSignals.map((it) => {
      const x = asRecord(it);
      const pc = x.pctChg;
      const chg =
        typeof pc === 'number' && Number.isFinite(pc)
          ? `${pc >= 0 ? '+' : ''}${pc.toFixed(2)}%`
          : '—';
      return [
        String(x.name ?? x.tsCode ?? ''),
        String(x.signal ?? ''),
        String(x.positionRange ?? ''),
        chg,
        Number.isFinite(x.close as number) ? Number(x.close).toFixed(2) : '—',
        Number.isFinite(x.ma5 as number) ? Number(x.ma5).toFixed(2) : '—',
        Number.isFinite(x.ma20 as number) ? Number(x.ma20).toFixed(2) : '—',
        String(x.asOfDate ?? ''),
      ];
    });
    sentimentIndexTable = { title: '主要指数红绿灯', headers: headers0, rows: rows0 };
  }

  let sentimentDailyTable: PdfTableBlock | null = null;
  const last5 = items.slice(-5);
  if (last5.length) {
    const headers = ['日期', '涨跌比', '成交额', '涨停溢价%', '炸板率%', 'risk'];
    const rows: string[][] = last5.map((it) => {
      const x = asRecord(it);
      return [
        String(x.date ?? ''),
        Number.isFinite(x.upDownRatio as number) ? Number(x.upDownRatio).toFixed(2) : '—',
        fmtAmountCn(x.marketTurnoverCny),
        Number.isFinite(x.yesterdayLimitUpPremium as number)
          ? `${Number(x.yesterdayLimitUpPremium).toFixed(2)}%`
          : '—',
        Number.isFinite(x.failedLimitUpRate as number) ? `${Number(x.failedLimitUpRate).toFixed(1)}%` : '—',
        String(x.riskMode ?? ''),
      ];
    });
    sentimentDailyTable = { title: '全市场日度情绪', headers, rows };
  }

  const sentimentRuleLines: string[] = [];
  if (asOfDate) sentimentRuleLines.push(`数据截至：${asOfDate}`);
  if (latest) {
    const risk = String(latest.riskMode ?? '');
    if (risk) sentimentRuleLines.push(`最新 risk 档位：${risk}`);
    const total = fmtAmountCn(latest.marketTurnoverCny);
    if (total && total !== '—') sentimentRuleLines.push(`最新全市场成交额：${total}`);
    const rules = Array.isArray(latest.rules)
      ? (latest.rules as unknown[]).map((x) => String(x)).filter(Boolean)
      : [];
    if (rules.length) sentimentRuleLines.push(`系统规则摘要：${rules.slice(0, 8).join(' · ')}`);
  }

  return { sentimentIndexTable, sentimentDailyTable, sentimentRuleLines };
}

function buildHotPicksTable(picks: HotIndustryPick[]): PdfTableBlock {
  const headers = ['#', '行业', '1D 排名', '5D 排名', '1D 净流入', '5D 合计', '排名变化', '信号'];
  const rows: string[][] = (picks.length ? picks : []).slice(0, 3).map((p, idx) => [
    String(idx + 1),
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
    p.momentumSignal ? '动量突破' : '—',
  ]);
  if (!rows.length) {
    return {
      title: '当前观测行业',
      headers,
      rows: [['1', '—', '—', '—', '—', '—', '—', '—']],
    };
  }
  return { title: '当前观测行业', headers, rows };
}

export function buildInvestmentDailyPdfLayout(args: {
  report: InvestmentDailyReportPayload;
  subtitleTimeZh: string;
  summary: unknown;
  hotIndustryPicks: HotIndustryPick[];
}): InvestmentDailyPdfLayout {
  const { report, subtitleTimeZh, summary, hotIndustryPicks } = args;
  const s = asRecord(summary);
  const asOfDate = String(s.asOfDate ?? '').trim();
  const envZh = String(s.marketEnvironmentZh ?? '').trim();

  const sentiment = buildSentimentLayout(summary);
  const industry = buildIndustryLayout(summary);
  const macroTable = buildMacroTable(summary);

  return {
    subtitleTimeZh,
    asOfDate,
    report,
    envZh,
    sentimentIndexTable: sentiment.sentimentIndexTable,
    sentimentDailyTable: sentiment.sentimentDailyTable,
    sentimentRuleLines: sentiment.sentimentRuleLines,
    sentimentStaticNotes: SENTIMENT_STATIC_NOTES_ZH,
    industryTopByDate: industry.industryTopByDate,
    industryInflow: industry.industryInflow,
    industryOutflow: industry.industryOutflow,
    macroTable,
    hotStaticRules: HOT_INDUSTRY_STATIC_RULES_ZH,
    hotPicksTable: buildHotPicksTable(hotIndustryPicks),
  };
}

export function parseInvestmentDailyReportResponse(data: unknown): InvestmentDailyReportPayload {
  if (!data || typeof data !== 'object') throw new Error('Invalid AI response');
  const o = data as Record<string, unknown>;
  const a = o.trafficLightPositionAndSentiment;
  const mh = o.marketEnvironmentHighlights;
  const hi = o.hotIndustriesFormalAnalysis;
  const b = o.capitalFlowAndMainline;
  const stocks = o.topStocks;
  const news = o.topNews;
  if (typeof a !== 'string' || typeof mh !== 'string' || typeof hi !== 'string' || typeof b !== 'string') {
    throw new Error('Invalid AI response: text sections');
  }
  if (!Array.isArray(stocks) || stocks.length !== 3) throw new Error('Invalid AI response: topStocks');
  if (!Array.isArray(news) || news.length !== 5) throw new Error('Invalid AI response: topNews');
  const topStocks = stocks.map((row, i) => {
    if (!row || typeof row !== 'object') throw new Error(`Invalid topStocks[${i}]`);
    const r = row as Record<string, unknown>;
    if (typeof r.symbol !== 'string' || typeof r.name !== 'string' || typeof r.rationale !== 'string') {
      throw new Error(`Invalid topStocks[${i}] fields`);
    }
    return { symbol: r.symbol, name: r.name, rationale: r.rationale };
  });
  const topNews = news.map((row, i) => {
    if (!row || typeof row !== 'object') throw new Error(`Invalid topNews[${i}]`);
    const r = row as Record<string, unknown>;
    if (typeof r.title !== 'string' || typeof r.summary !== 'string') {
      throw new Error(`Invalid topNews[${i}] fields`);
    }
    return { title: r.title, summary: r.summary };
  });
  return {
    trafficLightPositionAndSentiment: a,
    marketEnvironmentHighlights: mh,
    hotIndustriesFormalAnalysis: hi,
    capitalFlowAndMainline: b,
    topStocks,
    topNews,
  };
}

/**
 * Renders the investment daily report to a PDF Blob (browser). Use for tests or custom download UX.
 */
export async function renderInvestmentDailyPdfToBlob(
  args: DownloadInvestmentDailyPdfArgs,
): Promise<Blob> {
  const layout = buildInvestmentDailyPdfLayout({
    report: args.report,
    subtitleTimeZh: args.subtitleTimeZh,
    summary: args.summary,
    hotIndustryPicks: args.hotIndustryPicks,
  });

  const [rpdf, docMod] = await Promise.all([
    import('@react-pdf/renderer'),
    // Relative path so Vitest resolves without Next.js `paths` alias.
    import('./InvestmentDailyPdfDocument'),
  ]);
  const doc = React.createElement(docMod.InvestmentDailyPdfDocument, { layout });
  return rpdf.pdf(doc as Parameters<typeof rpdf.pdf>[0]).toBlob();
}

/**
 * Vector PDF via @react-pdf/renderer (multi-page text flow, smaller than rasterized HTML).
 */
export async function downloadInvestmentDailyPdf(args: DownloadInvestmentDailyPdfArgs): Promise<void> {
  const blob = await renderInvestmentDailyPdfToBlob(args);
  const name = args.filename.endsWith('.pdf') ? args.filename : `${args.filename}.pdf`;
  const url = URL.createObjectURL(blob);
  try {
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.rel = 'noopener';
    document.body.appendChild(a);
    a.click();
    a.remove();
  } finally {
    URL.revokeObjectURL(url);
  }
}
