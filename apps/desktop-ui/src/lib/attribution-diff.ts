/**
 * Attribution diff — essential gap between pick-strong leg contributions
 * and the user's book (realized sells + open MTM), not shallow "wrong pick today".
 */

export const TRACK_PICKS = ['STOCK', 'GOLD', 'OIL', 'NASDAQ', 'BOND10', 'REPO'] as const;
export type TrackPick = (typeof TRACK_PICKS)[number];

export type TrackLegStat = {
  days: number;
  pctDays?: number;
  contribAddPct: number;
  contribGeoPct: number;
};

export type UserBucketStat = {
  count: number;
  sumPnlPct: number;
};

export type OpenHolding = {
  symbol: string;
  positionPct?: number | null;
  pnlPct?: number | null;
};

export type LegDiffRow = {
  pick: TrackPick;
  trackDays: number;
  trackAddPct: number;
  trackGeoPct: number;
  /** Share of |track additive| mass (0–100). */
  trackSharePct: number;
  openWeightPct: number;
  /** Σ (pnlPct × weight/100) — portfolio points from cost, not NAV path. */
  openPnlPoints: number;
  realizedSumPct: number;
  realizedCount: number;
  /** openWeight − trackShare (percentage points). */
  weightGapPt: number;
  kind: 'under_capture' | 'over_weight' | 'aligned' | 'track_drag' | 'idle_leg';
};

export type AttributionInsight = {
  id: string;
  priority: number;
  title: string;
  detail: string;
};

export type AttributionDiffReport = {
  rows: LegDiffRow[];
  insights: AttributionInsight[];
  trackEngine: TrackPick | null;
  userTopWeight: TrackPick | null;
};

const ETF_BUCKET: Record<string, TrackPick> = {
  '518880': 'GOLD',
  '513350': 'OIL',
  '513100': 'NASDAQ',
  '513110': 'NASDAQ',
  '513500': 'NASDAQ',
  '159941': 'NASDAQ',
  '511260': 'BOND10',
};

export function symbolToTrackPick(symbol: string): TrackPick | 'OTHER' {
  const s = (symbol || '').toUpperCase();
  const bare = s.replace('ETF:', '').replace('.SH', '').replace('.SZ', '').replace('.HK', '');
  if (s.startsWith('CN:') || (bare.length === 6 && /^\d+$/.test(bare) && !s.startsWith('ETF:'))) {
    return 'STOCK';
  }
  if (s.startsWith('HK:')) return 'STOCK';
  if (bare in ETF_BUCKET) return ETF_BUCKET[bare];
  return 'OTHER';
}

function userBucketToPick(bucket: string): TrackPick | null {
  const b = bucket.toUpperCase();
  if (b === 'STOCK_CN' || b === 'STOCK_HK' || b === 'STOCK') return 'STOCK';
  if ((TRACK_PICKS as readonly string[]).includes(b)) return b as TrackPick;
  return null;
}

export function buildOpenByPick(holdings: OpenHolding[]): Record<TrackPick, { weight: number; pnlPoints: number }> {
  const out = Object.fromEntries(
    TRACK_PICKS.map((p) => [p, { weight: 0, pnlPoints: 0 }]),
  ) as Record<TrackPick, { weight: number; pnlPoints: number }>;
  for (const h of holdings) {
    const pick = symbolToTrackPick(h.symbol);
    if (pick === 'OTHER' || pick === 'REPO') continue;
    const w = Number(h.positionPct) || 0;
    const pnl = Number(h.pnlPct) || 0;
    out[pick].weight += w;
    out[pick].pnlPoints += pnl * (w / 100);
  }
  return out;
}

export function buildRealizedByPick(
  byBucket: Record<string, UserBucketStat> | undefined,
): Record<TrackPick, { count: number; sumPnlPct: number }> {
  const out = Object.fromEntries(
    TRACK_PICKS.map((p) => [p, { count: 0, sumPnlPct: 0 }]),
  ) as Record<TrackPick, { count: number; sumPnlPct: number }>;
  if (!byBucket) return out;
  for (const [bucket, st] of Object.entries(byBucket)) {
    const pick = userBucketToPick(bucket);
    if (!pick) continue;
    out[pick].count += st.count;
    out[pick].sumPnlPct += st.sumPnlPct;
  }
  return out;
}

function classifyLeg(row: Omit<LegDiffRow, 'kind'>): LegDiffRow['kind'] {
  const { trackAddPct, trackSharePct, openWeightPct, trackDays } = row;
  if (trackDays === 0 && openWeightPct < 5) return 'idle_leg';
  if (trackAddPct < -2 && openWeightPct >= 15) return 'track_drag';
  // Under-capture: track was a real engine but book light on it
  if (trackSharePct >= 15 && openWeightPct + 20 < trackSharePct) return 'under_capture';
  if (trackAddPct >= 5 && openWeightPct < 25 && trackSharePct >= 20) return 'under_capture';
  // Over-weight: heavy book on a leg that was not the engine
  if (openWeightPct >= 30 && trackSharePct + 15 < openWeightPct && trackAddPct < trackSharePct) {
    return 'over_weight';
  }
  if (openWeightPct >= 40 && trackSharePct < 15) return 'over_weight';
  return 'aligned';
}

export function buildAttributionDiff(input: {
  byPick: Record<string, TrackLegStat> | undefined;
  userByBucket: Record<string, UserBucketStat> | undefined;
  holdings: OpenHolding[];
}): AttributionDiffReport {
  const byPick = input.byPick ?? {};
  const open = buildOpenByPick(input.holdings);
  const realized = buildRealizedByPick(input.userByBucket);

  const absSum = TRACK_PICKS.reduce((s, p) => s + Math.abs(byPick[p]?.contribAddPct ?? 0), 0) || 1;

  const rows: LegDiffRow[] = TRACK_PICKS.map((pick) => {
    const t = byPick[pick] ?? { days: 0, contribAddPct: 0, contribGeoPct: 0 };
    const o = open[pick];
    const r = realized[pick];
    const trackSharePct = (Math.abs(t.contribAddPct) / absSum) * 100;
    const base = {
      pick,
      trackDays: t.days,
      trackAddPct: t.contribAddPct,
      trackGeoPct: t.contribGeoPct,
      trackSharePct: Math.round(trackSharePct * 10) / 10,
      openWeightPct: Math.round(o.weight * 10) / 10,
      openPnlPoints: Math.round(o.pnlPoints * 100) / 100,
      realizedSumPct: Math.round(r.sumPnlPct * 100) / 100,
      realizedCount: r.count,
      weightGapPt: Math.round((o.weight - trackSharePct) * 10) / 10,
    };
    return { ...base, kind: classifyLeg(base) };
  });

  const trackEngine =
    [...rows].filter((r) => r.pick !== 'REPO').sort((a, b) => b.trackAddPct - a.trackAddPct)[0]?.pick ??
    null;
  const userTopWeight =
    [...rows].filter((r) => r.pick !== 'REPO').sort((a, b) => b.openWeightPct - a.openWeightPct)[0]
      ?.pick ?? null;

  const insights = buildInsights(rows, trackEngine, userTopWeight);
  return { rows, insights, trackEngine, userTopWeight };
}

function buildInsights(
  rows: LegDiffRow[],
  trackEngine: TrackPick | null,
  userTopWeight: TrackPick | null,
): AttributionInsight[] {
  const out: AttributionInsight[] = [];
  const by = Object.fromEntries(rows.map((r) => [r.pick, r])) as Record<TrackPick, LegDiffRow>;

  if (trackEngine && trackEngine !== 'REPO') {
    const eng = by[trackEngine];
    if (eng.kind === 'under_capture') {
      out.push({
        id: 'engine_under',
        priority: 100,
        title: `单轨主贡献是 ${trackEngine}（加法 ${fmt(eng.trackAddPct)}），你仓位仅 ${eng.openWeightPct}%`,
        detail: `归因份额约 ${eng.trackSharePct}% 的「发动机」腿，实盘明显欠配。今天是否 OIL/纳指满仓是战术问题；长期跑不赢曲线，优先看有没有吃到这条腿的涨跌。退出机制管的是单票风险，补不齐「没坐上主引擎」的缺口。`,
      });
    } else if (eng.openWeightPct >= 40) {
      out.push({
        id: 'engine_ok',
        priority: 40,
        title: `主贡献腿 ${trackEngine} 你有在场（仓 ${eng.openWeightPct}% · 浮盈点 ${fmt(eng.openPnlPoints)}）`,
        detail: `已实现 ${eng.realizedCount} 笔合计 ${fmt(eng.realizedSumPct)}。若仍低于单轨几何，差额更多来自时点/仓位比例，而不是「完全没碰对的资产」。`,
      });
    }
  }

  const overs = rows.filter((r) => r.kind === 'over_weight' && r.pick !== 'REPO');
  for (const r of overs.slice(0, 2)) {
    out.push({
      id: `over_${r.pick}`,
      priority: 80,
      title: `超配 ${r.pick}（仓 ${r.openWeightPct}%），单轨加法仅 ${fmt(r.trackAddPct)}（份额 ${r.trackSharePct}%）`,
      detail: `实盘 beta 压在这条腿上，但区间归因显示它不是主发动机。条件单/止损可以改善单票结局，改变不了「权重押在次要贡献腿」的结构差。`,
    });
  }

  const stock = by.STOCK;
  if (stock && stock.realizedCount > 0 && stock.trackAddPct > 5) {
    const exitHelp = stock.realizedSumPct > 0 && stock.openPnlPoints < stock.realizedSumPct;
    out.push({
      id: 'stock_exit',
      priority: 55,
      title: `股票腿：单轨加法 ${fmt(stock.trackAddPct)} · 你已实现 ${fmt(stock.realizedSumPct)}（${stock.realizedCount} 笔）· 浮盈点 ${fmt(stock.openPnlPoints)}`,
      detail: exitHelp
        ? '退出机制可能已经兑现过一部分股票收益；若开仓权重长期偏低，仍会相对单轨 STOCK 袖欠捕获。'
        : '把已实现与浮盈放在一起看股票腿，不要用「今天 OIL 不够 100%」代替股票引擎是否在账上。',
    });
  }

  if (userTopWeight && trackEngine && userTopWeight !== trackEngine) {
    out.push({
      id: 'engine_mismatch',
      priority: 90,
      title: `结构错位：单轨发动机 ${trackEngine} ≠ 你最重仓 ${userTopWeight}`,
      detail: '这是归因对照的核心结论——不是「今日 pick 提示你没满仓」，而是区间收益来源与实盘风险预算不一致。',
    });
  }

  const drags = rows.filter((r) => r.kind === 'track_drag');
  for (const r of drags.slice(0, 1)) {
    out.push({
      id: `drag_${r.pick}`,
      priority: 70,
      title: `单轨在 ${r.pick} 上加法为负（${fmt(r.trackAddPct)}），你仍持有 ${r.openWeightPct}%`,
      detail: '若浮盈/已实现也为负，说明共担了拖累腿；若你已通过退出做成正已实现，说明纪律在这条腿上优于「死扛硬切日」。',
    });
  }

  out.push({
    id: 'method',
    priority: 1,
    title: '口径（必读）',
    detail: '单轨列 = 100% 硬切日收益归因。你的列 = 现仓权重/浮盈点 + 区间已实现毛盈亏。两者不是同一 NAV，但足够回答「钱主要从哪条腿来、你有没有接到」。14:30/条件单是残差，排在结构错位之后。',
  });

  return out.sort((a, b) => b.priority - a.priority);
}

function fmt(n: number): string {
  const s = n.toFixed(1);
  return n > 0 ? `+${s}%` : `${s}%`;
}
