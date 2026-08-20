import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

/** Raw response of GET /v1/agent/portfolio-health (S-3 holdings health). */
export interface PortfolioHolding {
  symbol: string;
  name?: string;
  positionPct?: number;
  costPrice?: number;
  lastClose?: number;
  lastDate?: string;
  peakPrice?: number;
  peakDate?: string;
  pnlPct?: number;
  drawdownFromPeakPct?: number;
  holdingDays?: number;
  stopLossLine?: number;
  trailingLine?: number;
  pyramidTriggerLine?: number;
  pyramidAdded?: boolean;
  maxHoldDate?: string;
  expireDate?: string;
  action?: 'EXIT' | 'HOLD';
  reason?: string;
  note?: string;
  status?: string;
  /** OPT-101: realtime line breach not yet confirmed by the close. */
  realtimeWarning?: boolean;
  realtimeAlert?: string;
  /** OPT-105: which stop rule produced the lines ('atr' | 'fixed'). */
  stopRule?: string;
  stopRuleDetail?: string;
  /** 2026-08-12 info layer — alpha radar events mapped to this holding. */
  alphaEvents?: Array<{
    trend?: string;
    grade?: string;
    confidence?: number | null;
    daysAgo?: number | null;
    riskStatus?: string;
    focus?: string;
  }>;
  /** 2026-08-12 info layer — CN SW L1 industry 5-day net inflow. */
  industryFlow?: {
    industry?: string;
    netInflow5d?: number;
    rank5d?: number;
    total?: number;
  } | null;
}

export interface PortfolioCandidate {
  symbol?: string;
  name?: string | null;
  ts_code?: string;
  industry?: string;
  score?: number;
  rs?: number;
  regime?: string | null;
  alphaEvents?: Array<{
    trend?: string;
    grade?: string;
    confidence?: number | null;
    daysAgo?: number | null;
    riskStatus?: string;
    focus?: string;
  }>;
  industryFlow?: {
    industry?: string;
    netInflow5d?: number;
    rank5d?: number;
    total?: number;
  } | null;
}

export interface PortfolioHealthResponse {
  tradeDate?: string;
  regime?: string | null;
  /** T2 regime strength 0-100 (shared CN/HK ruler; allocation hint, not a gate). */
  strength?: number;
  sentiment?: string | null;
  panicCooldown?: {
    lastPanicDate?: string | null;
    cooldownEndDate?: string | null;
    active?: boolean;
  } | null;
  /** 2026-08-12: drawdown circuit breaker on (CN line) — trailing 30d realized pnl <= -25% → new S-3 entries halted. */
  circuitBlocked?: boolean | null;
  s3Candidates?: PortfolioCandidate[] | null;
  /** 2026-08-10 badge: full candidate pool size before top-N collapse. */
  s3CandidateTotal?: number;
  s3Rules?: Record<string, unknown>;
  holdings?: PortfolioHolding[];
  /**
   * 2026-08-11: latest trade_date present in watchlist_score_daily for this
   * market (scores are written post-close at 17:30, plus intraday realtime
   * passes at 10:30 / 14:00). When < tradeDate, "no candidates" means
   * "scores not computed yet", NOT a real gate decision.
   */
  scoreDataAsOfDate?: string | null;
  /** True when scoreDataAsOfDate === tradeDate (scores are as-of today). */
  scoreFresh?: boolean;
  /** 2026-08-12 info layer P2 — per-market signal summary line. */
  infoSummary?: {
    holdingsCount?: number;
    eventHoldings?: number;
    industryOutflow?: number;
    industryInflow?: number;
  } | null;
  /**
   * 2026-08-19 (T6) — NASDAQ-100 ETF (513100) idle-cash sleeve hint.
   * Active when the CN line has idle capital and the ETF is above its 200d MA
   * (buy) or when it must be sold (A-share buy point / broke the MA line).
   */
  thirdAssetSleeve?: {
    active?: boolean;
    action?: 'BUY_513100' | 'SELL_TO_A_SHARE' | 'SELL_TO_REPO' | 'DONT_BUY' | 'NONE';
    label?: string;
    message?: string;
    etf?: string;
    tsCode?: string;
    price?: number;
    ma200?: number;
    aboveMa200?: boolean;
    asOfDate?: string;
    pctChg?: number | null;
    idlePct?: number;
    s3BuySetup?: boolean;
    gateOpen?: boolean;
    holding513100?: boolean;
    note?: string;
  } | null;
  /**
   * 2026-08-20 (T6): the HELD NASDAQ-100 ETF (513110/513100/...) tracked as a
   * separate "third asset / US" region with the sleeve rules (200d MA line),
   * NOT as a CN A-share holding.
   */
  thirdAssetHolding?: {
    active?: boolean;
    symbol?: string;
    tsCode?: string;
    name?: string | null;
    entryDate?: string;
    costPrice?: number | null;
    positionPct?: number | null;
    price?: number;
    ma200?: number;
    aboveMa200?: boolean;
    asOfDate?: string;
    pctChg?: number | null;
    pnlPct?: number | null;
    action?: 'HOLD' | 'SELL_TO_A_SHARE' | 'SELL_TO_REPO' | 'NONE';
    label?: string;
    message?: string;
    s3BuySetup?: boolean;
    gateOpen?: boolean;
    note?: string;
  } | null;
  /** 2026-08-10 HK parallel line — HK strategy-line block (null when not requested). */
  hkHealth?: PortfolioHealthResponse | null;
}

/**
 * True when this market's S-3 line is closed to NEW entries today
 * (regime Weak / unknown / panic cooldown / drawdown circuit breaker).
 * Shared by PortfolioHealthCard (闸门关闭 badge) and BehaviorAuditBanner
 * (hide 该持没买 suggestions the user cannot act on).
 */
export function isMarketGateClosed(
  block: Pick<PortfolioHealthResponse, 'regime' | 'panicCooldown' | 'circuitBlocked'> | null | undefined,
): boolean {
  return (
    block != null &&
    (block.regime === 'Weak' ||
      block.regime == null ||
      block.panicCooldown?.active === true ||
      block.circuitBlocked === true)
  );
}

/**
 * Fetch the S-3-aligned health check (holdings vs exit rules + market state).
 * `markets=CN,HK` returns both the CN line (top-level) and the HK line (hkHealth).
 */
export async function fetchPortfolioHealth(
  baseUrl: string = DATA_SYNC_BASE_URL,
  signal?: AbortSignal,
): Promise<PortfolioHealthResponse> {
  const res = await fetch(`${baseUrl}/v1/agent/portfolio-health?markets=CN,HK`, {
    cache: 'no-store',
    signal: signal ?? AbortSignal.timeout(30_000),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (await res.json()) as PortfolioHealthResponse;
}
