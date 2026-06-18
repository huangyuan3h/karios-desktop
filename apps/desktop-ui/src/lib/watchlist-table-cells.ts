import type { TrendOkResult } from '@/lib/api/types';
import {
  hasBlockingWatchlistRisk,
  rowHasWatchlistRiskHighlight,
  type WatchlistRiskAlert,
} from '@/lib/watchlist-metrics';

function isGreenZoneSignal(x: unknown): boolean {
  const s = String(x ?? '').trim();
  return s === 'green' || s === 'light_green' || s === 'deep_green';
}

function isHotspotTop3Industry(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const reasonsRaw = (t.values as Record<string, unknown> | undefined)?.industryFlowReasons;
  const reasons = Array.isArray(reasonsRaw) ? reasonsRaw.map((x) => String(x ?? '')) : [];
  if (reasons.includes('hotspots_today_top3')) return true;
  const parts = t.scoreParts as Record<string, unknown> | null | undefined;
  const v = parts?.hotspots_today_top3;
  return typeof v === 'number' && Number.isFinite(v) && v > 0;
}

function isGreenZoneMarket(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const regime = String(
    (t as TrendOkResult & { marketRegime?: unknown }).marketRegime ?? '',
  ).trim();
  if (regime === 'Strong') return true;

  const checks = t.buyChecks as Record<string, unknown> | null | undefined;
  if (!checks || typeof checks !== 'object') return false;
  const directCandidates = [
    checks.marketSignal,
    checks.signal,
    checks.sseSignal,
    checks.cybSignal,
    checks.sse_signal,
    checks.cyb_signal,
  ];
  if (directCandidates.some((x) => isGreenZoneSignal(x))) return true;
  const indexSignals = checks.indexSignals;
  if (Array.isArray(indexSignals)) {
    for (const it of indexSignals) {
      if (isGreenZoneSignal((it as Record<string, unknown>)?.signal)) return true;
    }
  }
  return false;
}

function shouldForceNoAPullbackWait(t: TrendOkResult | undefined | null): boolean {
  if (!t) return false;
  const score = typeof t.score === 'number' && Number.isFinite(t.score) ? t.score : null;
  if (score == null || score <= 90) return false;
  if (t.buyMode !== 'A_pullback' || t.buyAction !== 'wait') return false;
  if (!isGreenZoneMarket(t)) return false;
  if (!isHotspotTop3Industry(t)) return false;
  return true;
}

export function fmtBuyCell(t: TrendOkResult | undefined | null): {
  text: string;
  tone: 'buy' | 'wait' | 'avoid' | 'none';
  forced?: boolean;
  forcedReason?: string;
} {
  if (!t || !t.buyMode || !t.buyAction) return { text: '—', tone: 'none' };
  if (shouldForceNoAPullbackWait(t)) {
    return {
      text: 'B 买 强势热点',
      tone: 'buy',
      forced: true,
      forcedReason: 'Green zone + score>90 + industry in today top3: A_pullback/wait is blocked.',
    };
  }
  if (t.buyAction === 'avoid') return { text: '回避', tone: 'avoid' };
  const zl = typeof t.buyZoneLow === 'number' ? t.buyZoneLow : null;
  const zh = typeof t.buyZoneHigh === 'number' ? t.buyZoneHigh : null;
  const zone =
    zl != null && zh != null
      ? `${zl.toFixed(2)}–${zh.toFixed(2)}`
      : zl != null
        ? `${zl.toFixed(2)}`
        : '—';
  if (t.buyMode === 'A_pullback') {
    const prefix = t.buyAction === 'buy' ? 'A 买' : 'A 等';
    return { text: `${prefix} 回踩 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  if (t.buyMode === 'B_momentum') {
    const prefix = t.buyAction === 'buy' ? 'B 买' : 'B 等';
    return { text: `${prefix} 新高 ${zone}`, tone: t.buyAction === 'buy' ? 'buy' : 'wait' };
  }
  return { text: '无', tone: 'none' };
}

export function rowTone(
  t: TrendOkResult | undefined | null,
  alerts: WatchlistRiskAlert[] = [],
): 'green' | 'red' | 'none' {
  if (!t) return 'none';
  const stopParts = t.stopLossParts as Record<string, unknown> | null | undefined;
  const exitNow = Boolean(stopParts && typeof stopParts === 'object' && stopParts['exit_now']);
  if (exitNow || t.buyAction === 'avoid' || hasBlockingWatchlistRisk(alerts)) return 'red';
  if (rowHasWatchlistRiskHighlight(alerts)) return 'red';
  const score = typeof t.score === 'number' && Number.isFinite(t.score) ? t.score : null;
  const buy = fmtBuyCell(t);
  const buyModeOk = t.buyMode === 'A_pullback' || t.buyMode === 'B_momentum';
  if (t.trendOk === true && buy.tone === 'buy' && buyModeOk && score != null && score >= 85) {
    return 'green';
  }
  return 'none';
}

export function fmtPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

export function fmtScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

export function fmtNum(v: unknown, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}
