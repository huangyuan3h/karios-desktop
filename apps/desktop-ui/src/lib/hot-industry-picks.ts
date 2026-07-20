import type { HotIndustryPick } from '@/components/pages/HotIndustryWorkflowCard';

type DailyRankingEntry = {
  industryName: string;
  value: number;
  rank: number;
};

type DailyRankingByDate = {
  date: string;
  ranked: DailyRankingEntry[];
};

type TopByDateEntry = string | { industryName?: string; value?: number };

const MOMENTUM_THRESHOLD_YI = 20e8;
const MOMENTUM_RANK_CHANGE = 10;
const DAILY_TOP_N = 50;

function parseTopEntry(entry: TopByDateEntry): { name: string; value: number } | null {
  if (typeof entry === 'string') {
    const name = entry.trim();
    return name ? { name, value: 0 } : null;
  }
  if (entry && typeof entry === 'object') {
    const name = String(entry.industryName ?? '').trim();
    const val = Number(entry.value ?? 0);
    return name ? { name, value: Number.isFinite(val) ? val : 0 } : null;
  }
  return null;
}

function normalizeRankedEntries(rankedRaw: unknown[]): DailyRankingEntry[] {
  const parsed: Array<{ industryName: string; value: number }> = [];
  for (const row of rankedRaw) {
    const rec = row && typeof row === 'object' ? (row as Record<string, unknown>) : null;
    const industryName = String(rec?.industryName ?? '').trim();
    const value = Number(rec?.value ?? 0);
    if (!industryName) continue;
    parsed.push({
      industryName,
      value: Number.isFinite(value) ? value : 0,
    });
  }
  parsed.sort((a, b) => b.value - a.value);
  return parsed.map((r, i) => ({ ...r, rank: i + 1 }));
}

function resolveCalendarDates(args: {
  flow5dDates: string[];
  summaryDates: string[];
  rankingDates: string[];
}): { latestDate: string; prevDate: string } {
  const calendar =
    args.flow5dDates.length > 0
      ? args.flow5dDates
      : args.summaryDates.length > 0
        ? args.summaryDates
        : args.rankingDates;
  const latestDate = calendar.length ? String(calendar[calendar.length - 1] ?? '') : '';
  const prevDate = calendar.length >= 2 ? String(calendar[calendar.length - 2] ?? '') : '';
  return { latestDate, prevDate };
}

function buildRankMapsFromDailyRankings(
  dailyRankings: DailyRankingByDate[],
  latestDate: string,
  prevDate: string,
): {
  dailyNames: string[];
  yesterdayRankMap: Map<string, number>;
  todayRankMap: Map<string, number>;
  todayValueMap: Map<string, number>;
} {
  const latestRanked =
    dailyRankings.find((x) => x.date === latestDate)?.ranked ?? [];
  const prevRanked = dailyRankings.find((x) => x.date === prevDate)?.ranked ?? [];

  const dailyNames = latestRanked
    .filter((r) => r.value > 0)
    .slice(0, DAILY_TOP_N)
    .map((r) => r.industryName);
  const yesterdayRankMap = new Map(prevRanked.map((r) => [r.industryName, r.rank]));
  const todayRankMap = new Map(latestRanked.map((r) => [r.industryName, r.rank]));
  const todayValueMap = new Map(latestRanked.map((r) => [r.industryName, r.value]));

  return { dailyNames, yesterdayRankMap, todayRankMap, todayValueMap };
}

function buildRankMapsFromTopByDate(
  topByDateArr: Array<{ date?: string; top?: TopByDateEntry[] }>,
  latestDate: string,
  prevDate: string,
): {
  dailyNames: string[];
  yesterdayRankMap: Map<string, number>;
  todayRankMap: Map<string, number>;
  todayValueMap: Map<string, number>;
} {
  const namesByDate = new Map<string, string[]>();
  const rankByDate = new Map<string, Map<string, number>>();
  const valueByDate = new Map<string, Map<string, number>>();

  for (const it of topByDateArr) {
    const d = String(it?.date ?? '');
    const topArr = Array.isArray(it?.top) ? it.top : [];
    const names: string[] = [];
    const rankMap = new Map<string, number>();
    const valueMap = new Map<string, number>();
    for (let i = 0; i < topArr.length; i += 1) {
      const parsed = parseTopEntry(topArr[i]);
      if (!parsed) continue;
      names.push(parsed.name);
      rankMap.set(parsed.name, i + 1);
      valueMap.set(parsed.name, parsed.value);
    }
    if (d && names.length) {
      namesByDate.set(d, names);
      rankByDate.set(d, rankMap);
      valueByDate.set(d, valueMap);
    }
  }

  const latestNames = namesByDate.get(latestDate) ?? [];
  return {
    dailyNames: latestNames.slice(0, DAILY_TOP_N),
    yesterdayRankMap: prevDate ? (rankByDate.get(prevDate) ?? new Map()) : new Map(),
    todayRankMap: rankByDate.get(latestDate) ?? new Map(),
    todayValueMap: valueByDate.get(latestDate) ?? new Map(),
  };
}

export type MainlineTag = 'MOMENTUM' | '5D_TOP3';

export type MainlineAllowSet = {
  /** True when industryFundFlow data was present enough to evaluate. */
  ready: boolean;
  names: Set<string>;
  byName: Map<string, MainlineTag>;
};

type IndustryFlowContext = {
  latestDate: string;
  dailyNames: string[];
  yesterdayRankMap: Map<string, number>;
  todayRankMap: Map<string, number>;
  todayValueMap: Map<string, number>;
  fiveRank: Map<string, { rank: number; sum5d: number | null; latestNet: number | null }>;
  rows5dTopNames: string[];
};

function resolveIndustryFlowContext(summary: unknown): IndustryFlowContext | null {
  const root = summary && typeof summary === 'object' ? (summary as Record<string, unknown>) : null;
  const indRaw = root?.industryFundFlow;
  if (!indRaw || typeof indRaw !== 'object') return null;
  const ind = indRaw as Record<string, unknown>;

  const dailyRankingsRaw: unknown[] = Array.isArray(ind.dailyRankings) ? ind.dailyRankings : [];
  const dailyRankings: DailyRankingByDate[] = dailyRankingsRaw
    .map((it: unknown) => {
      const row = it && typeof it === 'object' ? (it as Record<string, unknown>) : null;
      const date = String(row?.date ?? '');
      const rankedRaw: unknown[] = Array.isArray(row?.ranked) ? row.ranked : [];
      const ranked = normalizeRankedEntries(rankedRaw);
      return date ? { date, ranked } : null;
    })
    .filter((x): x is DailyRankingByDate => x != null);

  const datesAll: string[] = Array.isArray(ind.dates) ? ind.dates.map(String) : [];
  const flow5dObj =
    ind.flow5d && typeof ind.flow5d === 'object' ? (ind.flow5d as Record<string, unknown>) : null;
  const flow5dDates: string[] = Array.isArray(flow5dObj?.dates)
    ? flow5dObj.dates.map(String)
    : [];
  const rankingDates =
    dailyRankings.length > 0
      ? dailyRankings.map((x) => x.date)
      : datesAll;
  const { latestDate, prevDate } = resolveCalendarDates({
    flow5dDates,
    summaryDates: datesAll,
    rankingDates,
  });

  const topByDateArr: Array<{ date?: string; top?: TopByDateEntry[] }> = Array.isArray(
    ind.topByDate,
  )
    ? (ind.topByDate as Array<{ date?: string; top?: TopByDateEntry[] }>)
    : [];
  const hasTopByDate = topByDateArr.length > 0;
  const hasFlow5d = Array.isArray(flow5dObj?.top) && flow5dObj.top.length > 0;
  if (!dailyRankings.length && !hasTopByDate && !hasFlow5d) return null;

  const rankMaps =
    dailyRankings.length > 0
      ? buildRankMapsFromDailyRankings(dailyRankings, latestDate, prevDate)
      : buildRankMapsFromTopByDate(topByDateArr, latestDate, prevDate);

  const { dailyNames, yesterdayRankMap, todayRankMap, todayValueMap } = rankMaps;

  const rows5d: Array<Record<string, unknown>> = Array.isArray(flow5dObj?.top)
    ? (flow5dObj.top as Array<Record<string, unknown>>)
    : [];
  const fiveRank = new Map<
    string,
    { rank: number; sum5d: number | null; latestNet: number | null }
  >();
  const rows5dTopNames: string[] = [];
  for (let i = 0; i < rows5d.length; i += 1) {
    const r = rows5d[i];
    const name = String(r?.industryName ?? '').trim();
    if (!name || fiveRank.has(name)) continue;
    const sum5dRaw = Number(r?.sum5d);
    const sum5d = Number.isFinite(sum5dRaw) ? sum5dRaw : null;
    const seriesArr: Array<Record<string, unknown>> = Array.isArray(r?.series)
      ? (r.series as Array<Record<string, unknown>>)
      : [];
    let latestNet: number | null = null;
    if (latestDate) {
      const p = seriesArr.find((x) => String(x?.date ?? '') === latestDate);
      const v = Number(p?.netInflow);
      latestNet = Number.isFinite(v) ? v : null;
    }
    fiveRank.set(name, { rank: i + 1, sum5d, latestNet });
    rows5dTopNames.push(name);
  }

  return {
    latestDate,
    dailyNames,
    yesterdayRankMap,
    todayRankMap,
    todayValueMap,
    fiveRank,
    rows5dTopNames,
  };
}

/**
 * Mainline allow-set for BUY/ADD: 5D net-inflow Top3 ∪ Momentum Breakout industries.
 * Wider than Dashboard Hot industries workflow Top3 (which is capped at 3).
 */
/**
 * True when latest daily industry ranking has data and every net inflow ≤ 0
 * (no positive sector flow → Mainline stays no; Why uses SECTOR_OUTFLOW_BLOCK).
 */
export function isSectorOutflowBlock(summary: unknown): boolean {
  const ctx = resolveIndustryFlowContext(summary);
  if (!ctx || !ctx.latestDate) return false;
  if (ctx.todayValueMap.size === 0) return false;
  for (const v of ctx.todayValueMap.values()) {
    if (v > 0) return false;
  }
  return true;
}

export function buildMainlineAllowSet(summary: unknown): MainlineAllowSet {
  const empty: MainlineAllowSet = {
    ready: false,
    names: new Set(),
    byName: new Map(),
  };
  const ctx = resolveIndustryFlowContext(summary);
  if (!ctx) return empty;

  const names = new Set<string>();
  const byName = new Map<string, MainlineTag>();

  // 5D Top3 first (tag can be overwritten by MOMENTUM below if both apply)
  for (const name of ctx.rows5dTopNames.slice(0, 3)) {
    names.add(name);
    byName.set(name, '5D_TOP3');
  }

  for (let i = 0; i < ctx.dailyNames.length; i += 1) {
    const name = ctx.dailyNames[i];
    const todayNetInflow = ctx.todayValueMap.get(name) ?? 0;
    const todayRank = ctx.todayRankMap.get(name) ?? i + 1;
    const yesterdayRank = ctx.yesterdayRankMap.get(name) ?? null;
    const rankChange = yesterdayRank != null ? yesterdayRank - todayRank : null;
    const isMomentumSignal =
      todayNetInflow >= MOMENTUM_THRESHOLD_YI &&
      rankChange != null &&
      rankChange >= MOMENTUM_RANK_CHANGE;
    if (!isMomentumSignal) continue;
    names.add(name);
    byName.set(name, 'MOMENTUM');
  }

  return { ready: true, names, byName };
}

export function buildDashboardHotIndustryPicks(summary: unknown): HotIndustryPick[] {
  const ctx = resolveIndustryFlowContext(summary);
  if (!ctx) return [];

  const {
    dailyNames,
    yesterdayRankMap,
    todayRankMap,
    todayValueMap,
    fiveRank,
  } = ctx;

  const picks: HotIndustryPick[] = [];
  const momentumPicks: HotIndustryPick[] = [];

  for (let i = 0; i < dailyNames.length; i += 1) {
    const name = dailyNames[i];
    const five = fiveRank.get(name);
    const todayNetInflow = todayValueMap.get(name) ?? 0;
    const todayRank = todayRankMap.get(name) ?? i + 1;
    const yesterdayRank = yesterdayRankMap.get(name) ?? null;
    const rankChange = yesterdayRank != null ? yesterdayRank - todayRank : null;
    const isMomentumSignal =
      todayNetInflow >= MOMENTUM_THRESHOLD_YI &&
      rankChange != null &&
      rankChange >= MOMENTUM_RANK_CHANGE;

    const pick: HotIndustryPick = {
      industryName: name,
      dailyRank: todayRank,
      fiveDayRank: five?.rank ?? null,
      netInflow: five?.latestNet ?? todayNetInflow,
      sum5d: five?.sum5d ?? null,
      yesterdayRank,
      rankChange,
      momentumSignal: isMomentumSignal,
    };

    if (isMomentumSignal) {
      momentumPicks.push(pick);
    } else if (five) {
      picks.push(pick);
    }
  }

  const result: HotIndustryPick[] = [];
  const seen = new Set<string>();
  for (const p of momentumPicks) {
    if (seen.has(p.industryName)) continue;
    seen.add(p.industryName);
    result.push(p);
    if (result.length >= 3) return result;
  }
  for (const p of picks) {
    if (seen.has(p.industryName)) continue;
    seen.add(p.industryName);
    result.push(p);
    if (result.length >= 3) break;
  }
  return result;
}
