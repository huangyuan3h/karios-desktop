export type { WatchlistRiskAlert } from '@/lib/watchlist-metrics';

export type TrendOkResult = {
  symbol: string;
  name?: string | null;
  asOfDate?: string | null;
  trendOk?: boolean | null;
  score?: number | null;
  scoreParts?: Record<string, number>;
  stopLossPrice?: number | null;
  stopLossParts?: Record<string, unknown>;
  buyMode?: string | null;
  buyAction?: string | null;
  buyZoneLow?: number | null;
  buyZoneHigh?: number | null;
  buyRefPrice?: number | null;
  buyWhy?: string | null;
  buyChecks?: Record<string, unknown>;
  marketRegime?: string | null;
  intradayChgPct?: number | null;
  gapUp?: boolean | null;
  riskAlerts?: import('@/lib/watchlist-metrics').WatchlistRiskAlert[];
  riskMetricsLive?: boolean | null;
  checks?: Record<string, unknown> | null;
  values?: Record<string, unknown> | null;
  missingData?: string[];
};

export type WatchlistQuote = {
  price: number | null;
  tsCode: string;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose: number | null;
  pctChg: number | null;
};
