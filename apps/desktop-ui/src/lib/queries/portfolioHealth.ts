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
  /** 2026-08-10 HK parallel line — HK strategy-line block (null when not requested). */
  hkHealth?: PortfolioHealthResponse | null;
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
