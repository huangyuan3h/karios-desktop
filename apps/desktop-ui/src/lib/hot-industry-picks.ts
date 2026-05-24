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
    const industryName = String((row as any)?.industryName ?? '').trim();
    const value = Number((row as any)?.value ?? 0);
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

export function buildDashboardHotIndustryPicks(summary: unknown): HotIndustryPick[] {
  const ind: any = (summary as any)?.industryFundFlow ?? {};
  const dailyRankingsRaw: unknown[] = Array.isArray(ind?.dailyRankings) ? ind.dailyRankings : [];
  const dailyRankings: DailyRankingByDate[] = dailyRankingsRaw
    .map((it: any) => {
      const date = String(it?.date ?? '');
      const rankedRaw: unknown[] = Array.isArray(it?.ranked) ? it.ranked : [];
      const ranked = normalizeRankedEntries(rankedRaw);
      return date ? { date, ranked } : null;
    })
    .filter((x): x is DailyRankingByDate => x != null);

  const datesAll: string[] = Array.isArray(ind?.dates) ? ind.dates : [];
  const flow5dDates: string[] = Array.isArray(ind?.flow5d?.dates) ? ind.flow5d.dates : [];
  const rankingDates =
    dailyRankings.length > 0
      ? dailyRankings.map((x) => x.date)
      : datesAll;
  const { latestDate, prevDate } = resolveCalendarDates({
    flow5dDates,
    summaryDates: datesAll,
    rankingDates,
  });

  const topByDateArr: any[] = Array.isArray(ind?.topByDate) ? ind.topByDate : [];
  const rankMaps =
    dailyRankings.length > 0
      ? buildRankMapsFromDailyRankings(dailyRankings, latestDate, prevDate)
      : buildRankMapsFromTopByDate(topByDateArr, latestDate, prevDate);

  const { dailyNames, yesterdayRankMap, todayRankMap, todayValueMap } = rankMaps;

  const flow5d: any = ind?.flow5d ?? null;
  const rows5d: any[] = Array.isArray(flow5d?.top) ? flow5d.top : [];
  const fiveRank = new Map<
    string,
    { rank: number; sum5d: number | null; latestNet: number | null }
  >();
  for (let i = 0; i < rows5d.length; i += 1) {
    const r = rows5d[i];
    const name = String(r?.industryName ?? '').trim();
    if (!name || fiveRank.has(name)) continue;
    const sum5dRaw = Number(r?.sum5d);
    const sum5d = Number.isFinite(sum5dRaw) ? sum5dRaw : null;
    const seriesArr: any[] = Array.isArray(r?.series) ? r.series : [];
    let latestNet: number | null = null;
    if (latestDate) {
      const p = seriesArr.find((x) => String(x?.date ?? '') === latestDate);
      const v = Number(p?.netInflow);
      latestNet = Number.isFinite(v) ? v : null;
    }
    fiveRank.set(name, { rank: i + 1, sum5d, latestNet });
  }

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
