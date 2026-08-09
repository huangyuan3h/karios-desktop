import type { ExecutionActionCard, ExecutionGate } from '@karios/shared';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { mdPrice, mdScore, mdTable } from '@/lib/dashboard-format';
import {
  buildDefensiveSleeveExposurePct,
  buildSectorExposureFromWatchlist,
  buildSleeveExposureByMarket,
  buildSleeveExposurePct,
  computePnLPct,
  countHeldMissingPositionPct,
  deriveActionCard,
  isHeldPosition,
  isLockedT1,
  isMissingEntryDate,
  marketOfSymbol,
  parsePositionRangeHintMaxPct,
  sleeveExposureForSymbol,
  type CatalystPurgeHint,
} from '@/lib/execution-action';
import type { MainlineAllowSet } from '@/lib/hot-industry-picks';
import type { TrendOkResult } from '@/lib/api/types';
import { formatPnLPct, formatRs, buildWatchlistRowMetrics } from '@/lib/watchlist-metrics';
import type { WatchlistQuoteSlice } from '@/lib/watchlist-metrics';
import type { WatchlistItem } from '@/lib/watchlist-storage';
import {
  WATCHLIST_TABLE_VISIBILITY_NOTE,
  shouldShowInWatchlistTable,
} from '@/lib/watchlist-table-filter';

function formatTrendOkCell(t: TrendOkResult | undefined): string {
  if (!t) return '—';
  const status = String(t.trendStatus || '')
    .trim()
    .toLowerCase();
  if (status === 'recovering') return 'recovering';
  if (status === 'ok' || t.trendOk === true) return 'ok';
  if (status === 'no' || t.trendOk === false) return 'no';
  return '—';
}

function formatPosPct(v: number | null | undefined): string {
  return typeof v === 'number' && Number.isFinite(v) ? v.toFixed(1) : '—';
}

function formatEntryDate(v: string | null | undefined): string {
  const s = String(v || '').trim();
  return s || '—';
}

function formatLockedT1(entryDate: string | null | undefined, todaySh: string): string {
  if (isMissingEntryDate(entryDate)) return 'MISSING';
  return isLockedT1(entryDate, todaySh) ? 'True' : 'False';
}

/** S-3 panic protection status (backtest: no entries panic day + 3 trade days). */
export type PanicCooldownStatus = {
  lastPanicDate: string | null;
  cooldownEndDate: string | null;
  active: boolean;
};

/** Fetch S-3 panic cooldown from the backend (fail-open: null on any error). */
export async function fetchPanicCooldown(): Promise<PanicCooldownStatus | null> {
  try {
    const res = await fetch(`${DATA_SYNC_BASE_URL}/market/cn/sentiment/panic-cooldown?days=10&cooldownDays=3`, {
      cache: 'no-store',
    });
    if (!res.ok) return null;
    return (await res.json()) as PanicCooldownStatus;
  } catch {
    return null;
  }
}

export type PositionsExecutionMarkdownResult = {
  markdown: string;
  /** Flat symbols with Action=PURGE this round (remove after report is built). */
  purgeSymbols: string[];
};

/**
 * Unified combat table for Copy all / Watchlist copy.
 * Merges quant factors (RS/Score/TrendOK/Current) with execution contract
 * (Action/Suggest%/Entry_Trigger/Exit_Stop) into one LLM payload table.
 */
export function buildPositionsExecutionMarkdown(
  items: WatchlistItem[],
  trend: Record<string, TrendOkResult | undefined>,
  quotes: Record<
    string,
    | {
        price?: number | null;
        preClose?: number | null;
        pctChg?: number | null;
        tradeTime?: string | null;
        amount?: number | null;
        volume?: number | null;
      }
    | undefined
  >,
  gate: ExecutionGate | null,
  heading = '##',
  mainlineAllow: MainlineAllowSet | null = null,
  tradingTime = false,
  todaySh = '',
  sectorOutflowBlock = false,
  catalystBySymbol: Map<string, CatalystPurgeHint> | null = null,
  rsRanks: Record<string, number> | null = null,
  panicCooldown: PanicCooldownStatus | null = null,
): PositionsExecutionMarkdownResult {
  const lines: string[] = [];
  lines.push(`${heading} Combat Positions & Watchlist（A股 / 港股 分表）`);
  lines.push(...WATCHLIST_TABLE_VISIBILITY_NOTE);
  lines.push(
    '- note: 两市场独立核算 — A股(CN个股+ETF篮子)对照 CN Gate、港股(HK个股/基金+HK指数ETF如513180)对照 hkGate；ETF 免单票15%上限，计入对应 sleeve 总额',
  );
  if (!gate?.allowNewEntries) {
    lines.push('- note: BUY/ADD only valid when Execution Gate allowNewEntries=true');
  }
  lines.push(
    '- note: BUY/ADD 前提=主线绑定(5D Top3/Momentum)+非防御板块; 全部板块净流出时阻断(SECTOR_OUTFLOW_BLOCK)',
  );
  lines.push(
    '- note: BUY/ADD 阻断=盘中>6%(INTRADAY_SURGE_BLOCK; ATTACK+主线+B动量+Score≥85 可至9%=MOMENTUM_SURGE_ALLOW) / 弱势或背离跳空(GAP_UP_WEAK_BLOCK) / Entry_Trigger≤HardStop(ENTRY_BELOW_STOP)',
  );
  lines.push(
    '- note: 仓位上限=单票15%(SIZE_CAP_BLOCK; ETF豁免) / 板块合计30%(SECTOR_CONC_BLOCK) / sleeve 达 Gate hint 上限(SLEEVE_CAP_BLOCK)',
  );
  lines.push(
    '- note: Suggest% = min(5%截断, 0.5%风险预算/止损距离, 单票15%余量, 板块30%余量, sleeve余量); 风险绑定=缩量; <2.5%不出建议; WEAK_ATTACK 封顶5%',
  );
  lines.push(
    '- note: Dist% 平仓=(Entry_Trigger−现价)/现价; 持仓=(现价−Exit_Stop)/现价; stop-dist% 同口径(fallback 2×ATR%)',
  );
  lines.push(
    '- note: PURGE=Pos%0 & Score<30 & TrendOK=no → 移除; Alpha S 级→WATCH_SILENT; 恢复中→WATCH(TREND_RECOVERING)',
  );
  lines.push(
    '- note: T+1 锁: entryDate=今天→EXIT/TRIM 禁(T1_LOCK); 缺 entryDate→fail-closed(ENTRY_DATE_MISSING)',
  );
  lines.push(
    '- note: DEFEND/Weak 时段锁: BUY/ADD 仅 14:30–14:50(TIME_LOCK_WEAK_REGIME/MARKET_CLOSING_LOCK); 防御仓白名单+5D Top3+Score≥70→DEFENSIVE_SLEEVE_ALLOW(10% sleeve/5% 单票); V6.3 溢出: 单日净流入>500亿+涨>4000家+≥14:30→allowNewEntries≤5%',
  );
  if (sectorOutflowBlock) {
    lines.push(
      '- note: Mainline=no + SECTOR_OUTFLOW_BLOCK when all sectors net outflow',
    );
  }
  const sleeveExposurePct = buildSleeveExposurePct(items);
  const sleeveByMarket = buildSleeveExposureByMarket(items);
  const cnCap = parsePositionRangeHintMaxPct(gate?.positionRangeHint);
  const hkCap = parsePositionRangeHintMaxPct(gate?.hkGate?.positionRangeHint ?? null);
  const capLabel = (v: number | null) => (v == null ? '—' : `${v}%`);
  lines.push(
    `- sleeve: 卫星仓 ${sleeveExposurePct.toFixed(1)}%（CN 上限 ${capLabel(cnCap)}）` +
      `｜A股 ${sleeveByMarket.cn.toFixed(1)}% + ETF ${sleeveByMarket.etf.toFixed(1)}%（计入 CN 上限）` +
      `｜港股 ${sleeveByMarket.hk.toFixed(1)}%（hkGate 上限 ${capLabel(hkCap)}）`,
  );
  lines.push(
    '- note: ETF 是指数/板块篮子，不是单票：不受 15% 单票上限约束（SIZE_CAP_BLOCK 豁免），只计入 CN 市场 sleeve 总额；HK 指数 ETF（如 ETF:513180 华夏恒生科技）按港股 exposure 归入港股 sleeve',
  );
  const missingSize = countHeldMissingPositionPct(items);
  if (missingSize > 0) {
    lines.push(
      `- note: ${missingSize} held missing positionPct (sector/sleeve caps fail-open)`,
    );
  }
  const missingEntryDate = items.filter(
    (it) => isHeldPosition(it) && isMissingEntryDate(it.entryDate),
  ).length;
  if (missingEntryDate > 0) {
    lines.push(
      `- note: ALERT ${missingEntryDate} held missing entryDate — sells blocked (fail-closed); set EntryDate before EXIT/TRIM`,
    );
  }
  lines.push('');
  const sectorExposureByIndustry = buildSectorExposureFromWatchlist(items, trend);
  const defensiveSleeveExposurePct = buildDefensiveSleeveExposurePct(items, trend);
  const headers = [
    'Symbol',
    'Name',
    'RS',
    'Score',
    'TrendOK',
    'Current',
    'Pos%',
    'CostPrice',
    'P&L%',
    'EntryDate',
    'Locked_T1',
    'Action',
    'Suggest%',
    'Entry_Trigger',
    'Exit_Stop',
    'HardStop',
    'TrailStop',
    'Dist%',
    'Mainline',
    'Why',
  ];
  const cnRows: unknown[][] = [];
  const hkRows: unknown[][] = [];
  const purgeSymbols: string[] = [];
  let hiddenRows = 0;
  // S-3 candidate set (used for the sector/cluster-cap exemption and the
  // S-3 section below) — computed once, before the per-row loop.
  const s3Candidates = buildS3Candidates({
    items,
    trend,
    rsRanks,
    gate,
    mainlineAllow,
    sectorOutflowBlock,
    cnCap,
    sleeveExposurePct,
  });
  const s3Symbols = new Set(s3Candidates.map((c) => c.symbol));
  for (const it of items) {
    const t = trend[it.symbol];
    const q = quotes[it.symbol];
    const quote: WatchlistQuoteSlice | null = q
      ? {
          price: q.price ?? null,
          tradeTime: q.tradeTime ?? null,
          amount: q.amount ?? null,
          volume: q.volume ?? null,
          preClose: q.preClose ?? null,
          pctChg: q.pctChg ?? null,
        }
      : null;
    const rowMetrics = buildWatchlistRowMetrics({
      symbol: it.symbol,
      trend: t,
      quote,
      tradingTime,
      todaySh,
    });
    const catalyst = catalystBySymbol?.get(it.symbol) ?? null;
    // Per-market sleeve accounting: HK rows size against the HK sleeve,
    // CN/ETF rows against the CN sleeve (incl. ETFs). Position budgets are
    // evaluated against the market's own gate hint (hkGate for HK).
    const rowGate =
      marketOfSymbol(it.symbol) === 'hk' && gate?.hkGate
        ? { ...gate, ...gate.hkGate }
        : gate;
    const card: ExecutionActionCard = deriveActionCard({
      symbol: it.symbol,
      gate: rowGate,
      trendok: t ?? null,
      position: it,
      currentPrice: rowMetrics.current,
      mainlineAllow,
      intradayChgPct: rowMetrics.intradayChgPct,
      gapUp: typeof t?.gapUp === 'boolean' ? t.gapUp : null,
      marketRegime: t?.marketRegime ?? null,
      sectorExposureByIndustry,
      sleeveExposurePct: sleeveExposureForSymbol(sleeveByMarket, it.symbol),
      defensiveSleeveExposurePct,
      sectorOutflowBlock,
      catalyst,
      todaySh,
      isS3Candidate: s3Symbols.has(it.symbol),
    });
    if (card.action === 'PURGE') {
      purgeSymbols.push(it.symbol);
    }
    // Visibility filter (2026-08-01): drop silent dead rows (WATCH_SILENT & no signal)
    // but keep PURGE rows so the post-report GC can still remove them from storage.
    if (
      card.action !== 'PURGE' &&
      !shouldShowInWatchlistTable(it, t ?? null, card.action)
    ) {
      hiddenRows += 1;
      continue;
    }
    const dist =
      typeof card.distPct === 'number' && Number.isFinite(card.distPct)
        ? card.distPct.toFixed(1)
        : '—';
    const mainlineCell = card.mainlineOk ? card.mainlineTag || 'ok' : 'no';
    const suggest =
      typeof card.suggestAddPct === 'number' && Number.isFinite(card.suggestAddPct)
        ? `+${card.suggestAddPct.toFixed(1)}${card.suggestSizeNote ? ` (${card.suggestSizeNote})` : ''}`
        : '—';
    const pnl = computePnLPct(
      typeof it.costPrice === 'number' ? it.costPrice : null,
      rowMetrics.current,
    );
    const row = [
      it.symbol,
      it.name ?? t?.name ?? '—',
      formatRs(t),
      mdScore(t?.score ?? null),
      formatTrendOkCell(t),
      mdPrice(rowMetrics.current),
      formatPosPct(it.positionPct),
      mdPrice(it.costPrice),
      formatPnLPct(pnl),
      formatEntryDate(it.entryDate),
      formatLockedT1(it.entryDate, todaySh),
      card.action,
      suggest,
      mdPrice(card.entryTrigger ?? null),
      mdPrice(card.exitStop ?? null),
      mdPrice(card.hardStop ?? null),
      mdPrice(card.trailStop ?? null),
      dist,
      mainlineCell,
      card.why ?? '—',
    ];
    if (marketOfSymbol(it.symbol) === 'hk') hkRows.push(row);
    else cnRows.push(row);
  }
  if (panicCooldown?.active) {
    lines.push(`${heading} S-3 回测口径买入候选（趋势跟随）`);
    lines.push(
      `- ⚠️ 恐慌冷却期：最近恐慌日 ${panicCooldown.lastPanicDate ?? '—'}，冷却至 ${panicCooldown.cooldownEndDate ?? '—'} —— 回测证明：恐慌后 3 个交易日禁开新仓（纪律）`,
    );
    lines.push('');
  } else if (s3Candidates.length) {
    lines.push(`${heading} S-3 回测口径买入候选（趋势跟随）`);
    lines.push(
      mdTable(
        ['Symbol', 'Name', 'Score', 'RS%', '仓位%'],
        s3Candidates.map((c) => [c.symbol, c.name, mdScore(c.score), c.rsLabel, c.sizePct]),
      ),
    );
    lines.push(
      '- note: S-3=回测定案规则（score≥65 · RS前50% · regime非Weak · 主线白名单 · 移动止损-8% · 持有60天 · 不止盈 · 恐慌保护）；仓位=回测口径 10%/笔（受 sleeve 上限约束）',
    );
    lines.push('');
  } else if (gate && rsRanks != null) {
    lines.push(`${heading} S-3 回测口径买入候选（趋势跟随）`);
    lines.push('- 当前无满足 S-3 全部条件的候选 —— 空仓等待（纪律）');
    lines.push('');
  }
  if (!cnRows.length && !hkRows.length) {
    lines.push('- No watchlist items.');
    lines.push('');
    return { markdown: lines.join('\n'), purgeSymbols: [] };
  }
  if (cnRows.length) {
    lines.push(`${heading} A股 卫星仓（CN: 个股 + ETF: 篮子）`);
    lines.push(mdTable(headers, cnRows));
    lines.push('');
  }
  if (hkRows.length) {
    lines.push(`${heading} 港股 卫星仓（HK: 个股/基金）`);
    lines.push(mdTable(headers, hkRows));
    lines.push('');
  }
  if (hiddenRows > 0) {
    lines.push(`- note: ${hiddenRows} silent dead rows hidden (Pos%=— & Score<70 & TrendOK≠ok/recovering & Action=WATCH_SILENT); kept in DB`);
  }
  if (purgeSymbols.length) {
    lines.push(
      `- note: Purged ${purgeSymbols.length} symbols (Score<30 & TrendOK=no & flat, not Alpha S) — removed after this report`,
    );
  }
  lines.push('');
  return { markdown: lines.join('\n'), purgeSymbols };
}

/**
 * S-3 backtest-derived buy candidates (2026-08-09).
 * All conditions from docs/modules/trading-system.md §3:
 * score>=65, whole-market RS >= top 50%, regime != Weak, mainline ok,
 * no sector-outflow block. Position = backtest size 10% per sleeve,
 * capped by the CN sleeve budget. CN only (HK has no score).
 */
export function buildS3Candidates(opts: {
  items: WatchlistItem[];
  trend: Record<string, TrendOkResult | undefined>;
  rsRanks: Record<string, number> | null;
  gate: ExecutionGate | null;
  mainlineAllow: MainlineAllowSet | null;
  sectorOutflowBlock: boolean;
  cnCap: number | null;
  sleeveExposurePct: number;
}): Array<{ symbol: string; name: string; score: number; rsLabel: string; sizePct: string }> {
  const { items, trend, rsRanks, gate, mainlineAllow, sectorOutflowBlock, cnCap, sleeveExposurePct } = opts;
  if (!gate || rsRanks == null || sectorOutflowBlock) return [];
  const regime = String(gate.marketRegime ?? '');
  if (regime !== 'Strong' && regime !== 'Diverging') return [];
  const maxSize = cnCap ?? 100;
  let remaining = Math.max(0, maxSize - sleeveExposurePct);
  const out: Array<{ symbol: string; name: string; score: number; rsLabel: string; sizePct: string }> = [];
  for (const it of items) {
    if (marketOfSymbol(it.symbol) !== 'cn') continue;
    if (isHeldPosition(it)) continue;
    const t = trend[it.symbol];
    const score = typeof t?.score === 'number' ? t.score : null;
    if (score == null || score < 65) continue;
    const rs = rsRanks[it.symbol];
    if (typeof rs !== 'number' || rs < 0.5) continue;
    if (!mainlineAllow?.ready) continue;
    const ind = String(t?.values?.emIndustry ?? '');
    if (!ind || !mainlineAllow.names.has(ind)) continue;
    if (remaining <= 0) break;
    const size = Math.min(10, remaining);
    out.push({
      symbol: it.symbol,
      name: it.name ?? t?.name ?? '—',
      score,
      rsLabel: `前${Math.round(rs * 100)}%`,
      sizePct: `${size.toFixed(0)}%`,
    });
    remaining -= size;
  }
  return out;
}
