import type { HotIndustryPick } from '@/components/pages/HotIndustryWorkflowCard';

/** Must match ai-service `InvestmentDailyReportRequestSchema` max. */
export const INVESTMENT_DAILY_MARKDOWN_MAX_CHARS = 100_000;

export type InvestmentDailyReportPayload = {
  trafficLightPositionAndSentiment: string;
  marketEnvironmentHighlights: string;
  hotIndustriesFormalAnalysis: string;
  capitalFlowAndMainline: string;
  topStocks: { symbol: string; name: string; rationale: string }[];
  topNews: { title: string; summary: string }[];
};

export type PdfTableBlock = {
  title?: string;
  headers: string[];
  rows: string[][];
};

export type InvestmentDailyPdfLayout = {
  subtitleTimeZh: string;
  asOfDate: string;
  report: InvestmentDailyReportPayload;
  envZh: string;
  sentimentIndexTable: PdfTableBlock | null;
  sentimentDailyTable: PdfTableBlock | null;
  sentimentRuleLines: string[];
  sentimentStaticNotes: string;
  industryTopByDate: PdfTableBlock | null;
  industryInflow: PdfTableBlock | null;
  industryOutflow: PdfTableBlock | null;
  macroTable: PdfTableBlock | null;
  hotStaticRules: string[];
  hotPicksTable: PdfTableBlock;
};

export type DownloadInvestmentDailyPdfArgs = {
  report: InvestmentDailyReportPayload;
  subtitleTimeZh: string;
  filename: string;
  summary: unknown;
  hotIndustryPicks: HotIndustryPick[];
};
