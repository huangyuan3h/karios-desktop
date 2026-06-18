export type { TrendOkResult, WatchlistRiskAlert } from '@karios/shared';

export type WatchlistQuote = {
  price: number | null;
  tsCode: string;
  tradeTime: string | null;
  amount: number | null;
  volume: number | null;
  preClose: number | null;
  pctChg: number | null;
};
