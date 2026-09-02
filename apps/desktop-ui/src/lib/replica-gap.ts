/**
 * Replica gap — live book vs today's recipe.
 *
 * single_track: t-1 mom → 100% hard switch at next session.
 * twin_star: core sleeve at coreTargetPct (100 idle / 50 satActive);
 *   CN stocks are the satellite book, not a hard-switch deviation.
 */

import { TWIN_STAR_CLIP4 } from '@karios/shared';

export type HoldingSnap = {
  symbol: string;
  positionPct?: number | null;
  name?: string | null;
};

export type GapSeverity = 'block' | 'warn' | 'info';

export type ReplicaGapReason = {
  id: string;
  severity: GapSeverity;
  title: string;
  detail: string;
};

export type ReplicaGapVerdict = 'aligned' | 'partial' | 'diverged';

export type ReplicaGapMode = 'twin_star' | 'single_track';

export type ReplicaGapReport = {
  pick: string;
  verdict: ReplicaGapVerdict;
  targetWeightPct: number;
  stockWeightPct: number;
  etfWeightPct: number;
  idlePct: number;
  mode: ReplicaGapMode;
  coreTargetPct: number;
  reasons: ReplicaGapReason[];
};

const ETF_PICK_PREFIX: Record<string, string[]> = {
  GOLD: ['ETF:518880', '518880'],
  OIL: ['ETF:513350', '513350'],
  NASDAQ: ['ETF:513100', 'ETF:513110', 'ETF:513500', 'ETF:159941', '513100', '513110'],
  BOND10: ['ETF:511260', '511260'],
};

export function classifyHoldingSymbol(symbol: string): 'STOCK' | 'ETF' | 'OTHER' {
  const s = (symbol || '').toUpperCase();
  if (s.startsWith('CN:') || s.startsWith('HK:')) return 'STOCK';
  if (s.startsWith('ETF:')) return 'ETF';
  return 'OTHER';
}

export function holdingMatchesPick(symbol: string, pick: string): boolean {
  const s = (symbol || '').toUpperCase();
  const p = (pick || 'REPO').toUpperCase();
  if (p === 'STOCK') return s.startsWith('CN:') || s.startsWith('HK:');
  if (p === 'REPO') return false;
  const aliases = ETF_PICK_PREFIX[p] ?? [];
  return aliases.some((a) => s === a.toUpperCase() || s.includes(a.replace('ETF:', '')));
}

function weightSum(holdings: HoldingSnap[]): number {
  return holdings.reduce((s, h) => s + (Number(h.positionPct) || 0), 0);
}

function resolveCoreTargetPct(input: {
  mode: ReplicaGapMode;
  coreTargetPct?: number | null;
  stockWeightPct: number;
}): number {
  if (input.mode !== 'twin_star') return 100;
  const n = Number(input.coreTargetPct);
  if (Number.isFinite(n) && n > 0) return n;
  return input.stockWeightPct > 5 ? 50 : 100;
}

function timingInfos(mode: ReplicaGapMode, corePct: number, satBudget: number): ReplicaGapReason[] {
  const infos: ReplicaGapReason[] = [
    {
      id: 'timing_1430',
      severity: 'info',
      title: '时点：你约 14:30 操作 ≠ 回测收盘/次日开盘',
      detail: '定案用 t−1 收盘算 pick，净值按日收益复利。下午盘中买卖会吃到当日剩余波动，与 Timeline 日线归因天然有差。',
    },
  ];
  if (mode === 'twin_star') {
    infos.push({
      id: 'clip4_structure',
      severity: 'info',
      title: `机会双子星 clip4：核心 ${corePct}% · 卫星套筒 ${satBudget}%（最多 4×${TWIN_STAR_CLIP4.satSlotNavPct}%）`,
      detail: '开闸或隔夜卫星 = 50/50；关闸无仓 = 核心 100%。卫星股票不是偏离，不要用单轨 100% 硬切对照。',
    });
    infos.push({
      id: 'conditional_orders',
      severity: 'info',
      title: '卫星纪律：到期卖 / 保护止损 −5%，不是收盘瞬间换仓',
      detail: 'body 未到期的卫星仓关闸日也可以留着。条件单按触发价成交，会拉开与回测 T 开盘的距离。',
    });
  } else {
    infos.push({
      id: 'conditional_orders',
      severity: 'info',
      title: '股票条件单 = 延迟/部分成交',
      detail: '止损/移动/到期条件单按触发价成交，不是「收盘瞬间 100% 换仓」。这是纪律工具，但会拉开与硬切 NAV 的距离。',
    });
    infos.push({
      id: 'not_full_switch',
      severity: 'info',
      title: '未做 100% 硬切就会系统性跑输展示曲线',
      detail: '展示收益假设每天把全部资金放在当日 pick。部分换仓、留底仓、多腿并存，都会让实盘无法「复制」那条曲线。',
    });
  }
  return infos;
}

function finish(
  pick: string,
  mode: ReplicaGapMode,
  corePct: number,
  satBudget: number,
  weights: { targetWeightPct: number; stockWeightPct: number; etfWeightPct: number; idlePct: number; total: number },
  reasons: ReplicaGapReason[],
): ReplicaGapReport {
  reasons.push(...timingInfos(mode, corePct, satBudget));

  const hasBlock = reasons.some((r) => r.severity === 'block');
  const hasWarn = reasons.some((r) => r.severity === 'warn');
  let verdict: ReplicaGapVerdict = 'aligned';
  if (hasBlock) {
    verdict = 'diverged';
  } else if (hasWarn) {
    verdict = 'partial';
  } else if (mode === 'twin_star') {
    const coreOk =
      pick === 'REPO'
        ? satBudget <= 0
          ? weights.total <= 5
          : weights.stockWeightPct <= satBudget + 10
        : weights.targetWeightPct >= corePct - 20;
    const satOk = weights.stockWeightPct <= satBudget + 10 || pick === 'STOCK';
    verdict = coreOk && satOk ? 'aligned' : 'partial';
  } else if (pick !== 'REPO' && weights.targetWeightPct < 80 && weights.total > 5) {
    verdict = 'partial';
  } else if (pick !== 'REPO' && weights.targetWeightPct >= 80) {
    verdict = 'aligned';
  } else if (pick === 'REPO' && weights.total <= 5) {
    verdict = 'aligned';
  }

  return {
    pick,
    verdict,
    targetWeightPct: Math.round(weights.targetWeightPct * 10) / 10,
    stockWeightPct: Math.round(weights.stockWeightPct * 10) / 10,
    etfWeightPct: Math.round(weights.etfWeightPct * 10) / 10,
    idlePct: Math.round(weights.idlePct * 10) / 10,
    mode,
    coreTargetPct: corePct,
    reasons,
  };
}

function detectSingleTrackGaps(
  pick: string,
  holdings: HoldingSnap[],
  weights: { targetWeightPct: number; stockWeightPct: number; etfWeightPct: number; idlePct: number; total: number },
): ReplicaGapReport {
  const reasons: ReplicaGapReason[] = [];

  if (pick === 'REPO') {
    if (weights.total > 5) {
      reasons.push({
        id: 'should_be_cash',
        severity: 'block',
        title: '单轨今日 = REPO，实仓仍有风险仓',
        detail: `应接近空仓/逆回购；当前已部署约 ${weights.total.toFixed(0)}%。硬切要求先清股票/ETF。`,
      });
    }
  } else if (pick === 'STOCK') {
    if (weights.stockWeightPct < 50 && weights.total > 10) {
      reasons.push({
        id: 'missing_stock',
        severity: 'block',
        title: '单轨 = STOCK，实仓股票权重不足',
        detail: `股票约 ${weights.stockWeightPct.toFixed(0)}%（目标接近 100%）；ETF ${weights.etfWeightPct.toFixed(0)}%。应先卖 ETF 再配股票篮。`,
      });
    }
    if (weights.etfWeightPct > 15) {
      reasons.push({
        id: 'etf_while_stock',
        severity: 'block',
        title: '仍持有 ETF，偏离 STOCK 硬切',
        detail: `ETF 约 ${weights.etfWeightPct.toFixed(0)}%。单轨不会一边拿纳指/黄金一边拿股票篮。`,
      });
    }
  } else {
    if (weights.targetWeightPct < 50 && weights.total > 10) {
      reasons.push({
        id: 'wrong_etf_or_light',
        severity: 'block',
        title: `单轨 = ${pick}，目标 ETF 权重不足`,
        detail: `匹配 ${pick} 的仓位约 ${weights.targetWeightPct.toFixed(0)}%（目标 ~100%）。应卖出其他腿后买入对应 ETF。`,
      });
    }
    if (weights.stockWeightPct > 15) {
      reasons.push({
        id: 'stock_while_etf',
        severity: 'block',
        title: `仍持有股票，偏离 ${pick} 硬切`,
        detail: `股票约 ${weights.stockWeightPct.toFixed(0)}%。14:30 若只加 ETF 不加清股票，收益路径会与 Timeline 分叉。`,
      });
    }
    const otherEtf = weights.etfWeightPct - weights.targetWeightPct;
    if (otherEtf > 15) {
      reasons.push({
        id: 'wrong_etf',
        severity: 'warn',
        title: '持有非今日最强 ETF',
        detail: `其他 ETF 约 ${Math.max(0, otherEtf).toFixed(0)}%。单轨只持 argmax 那一只（或 REPO）。`,
      });
    }
  }

  if (weights.idlePct > 25 && pick !== 'REPO' && weights.targetWeightPct < 70) {
    reasons.push({
      id: 'idle_cash',
      severity: 'warn',
      title: '闲置现金偏多',
      detail: `约 ${weights.idlePct.toFixed(0)}% 未部署。回测按 100% 满仓硬切计收益；现金拖累是常见「跟不上」来源。`,
    });
  }

  if (pick === 'STOCK' && holdings.some((h) => classifyHoldingSymbol(h.symbol) === 'STOCK')) {
    reasons.push({
      id: 'stock_basket_vs_equal',
      severity: 'info',
      title: '股票腿 = 多票等权篮，不是一只龙头',
      detail: 'Timeline 的 STOCK 收益是当日持仓篮等权日收益；条件单分批成交会导致权重/时点与回测不一致。',
    });
  }

  return finish(pick, 'single_track', 100, 0, weights, reasons);
}

function detectTwinStarGaps(
  pick: string,
  holdings: HoldingSnap[],
  corePct: number,
  weights: { targetWeightPct: number; stockWeightPct: number; etfWeightPct: number; idlePct: number; total: number },
): ReplicaGapReport {
  const satBudget = Math.max(0, 100 - corePct);
  const reasons: ReplicaGapReason[] = [];

  if (pick === 'REPO') {
    if (satBudget <= 0 && weights.total > 5) {
      reasons.push({
        id: 'should_be_cash',
        severity: 'block',
        title: '核心腿今日 = REPO，实仓仍有风险仓',
        detail: `关闸无卫星时核心应接近空仓/逆回购；当前已部署约 ${weights.total.toFixed(0)}%。`,
      });
    } else if (satBudget > 0 && weights.etfWeightPct > 15) {
      reasons.push({
        id: 'etf_while_repo',
        severity: 'warn',
        title: 'REPO 日仍持 ETF',
        detail: `核心目标现金 ${corePct}%。ETF 约 ${weights.etfWeightPct.toFixed(0)}%，应先砍到核心目标再配卫星。`,
      });
    }
  } else if (pick === 'STOCK') {
    if (weights.stockWeightPct < Math.min(50, corePct) - 5 && weights.total > 10) {
      reasons.push({
        id: 'missing_stock',
        severity: 'block',
        title: '核心 = STOCK，实仓股票权重不足',
        detail: `股票约 ${weights.stockWeightPct.toFixed(0)}%（核心目标 ${corePct}%；开闸时核心+卫星都是股票）。`,
      });
    }
    if (weights.etfWeightPct > 15) {
      reasons.push({
        id: 'etf_while_stock',
        severity: 'warn',
        title: 'STOCK 日仍持 ETF',
        detail: `ETF 约 ${weights.etfWeightPct.toFixed(0)}%。核心股票篮 + 卫星都是 CN/HK，ETF 应先腾出。`,
      });
    }
  } else {
    if (weights.targetWeightPct < corePct - 20 && weights.total > 10) {
      reasons.push({
        id: 'wrong_etf_or_light',
        severity: 'block',
        title: `核心 = ${pick}，目标 ETF 低于 ${corePct}%`,
        detail: `匹配 ${pick} 的仓位约 ${weights.targetWeightPct.toFixed(0)}%（核心目标 ${corePct}%，不是 100%）。先把核心腿接到再买卖卫星。`,
      });
    } else if (weights.targetWeightPct < corePct - 10 && weights.total > 10) {
      reasons.push({
        id: 'wrong_etf_or_light',
        severity: 'warn',
        title: `核心 = ${pick} 略轻于 ${corePct}%`,
        detail: `匹配 ${pick} 的仓位约 ${weights.targetWeightPct.toFixed(0)}%。核心目标 ${corePct}%。`,
      });
    }
    if (satBudget > 0 && weights.targetWeightPct > corePct + 15 && weights.stockWeightPct < 5) {
      reasons.push({
        id: 'core_over_sat_empty',
        severity: 'warn',
        title: '核心超目标、卫星套筒空着',
        detail: `核心 ETF 约 ${weights.targetWeightPct.toFixed(0)}%（目标 ${corePct}%）。开闸时卫星套筒 ${satBudget}%（最多 4×${TWIN_STAR_CLIP4.satSlotNavPct}%），不要按单轨 100% 硬切把钱全放 ETF。`,
      });
    }
    if (corePct >= 95 && weights.stockWeightPct > 15) {
      reasons.push({
        id: 'leftover_sat_idle',
        severity: 'warn',
        title: '关闸日仍持卫星股票',
        detail: `股票约 ${weights.stockWeightPct.toFixed(0)}%。关闸核心 100%。若是 body 未到期仓则留到到期；不要当单轨硬切清掉。`,
      });
    }
    const otherEtf = weights.etfWeightPct - weights.targetWeightPct;
    if (otherEtf > 15) {
      reasons.push({
        id: 'wrong_etf',
        severity: 'warn',
        title: '持有非今日核心 ETF',
        detail: `其他 ETF 约 ${Math.max(0, otherEtf).toFixed(0)}%。核心腿只持 argmax 那一只。`,
      });
    }
  }

  if (satBudget > 0 && pick !== 'STOCK' && weights.stockWeightPct > satBudget + 10) {
    reasons.push({
      id: 'sat_over_sleeve',
      severity: 'warn',
      title: '卫星仓超套筒',
      detail: `股票约 ${weights.stockWeightPct.toFixed(0)}%，套筒上限 ${satBudget}%（最多 4×${TWIN_STAR_CLIP4.satSlotNavPct}%）。不要金字塔折进卫星。`,
    });
  }

  if (weights.idlePct > 25 && pick !== 'REPO') {
    reasons.push({
      id: 'idle_cash',
      severity: 'warn',
      title: satBudget > 0 ? '核心已接、卫星套筒空着' : '闲置现金偏多',
      detail:
        satBudget > 0
          ? `约 ${weights.idlePct.toFixed(0)}% 未部署。开闸时卫星套筒 ${satBudget}%（最多 4×${TWIN_STAR_CLIP4.satSlotNavPct}%）。`
          : `约 ${weights.idlePct.toFixed(0)}% 未部署。关闸日核心目标 ${corePct}%。`,
    });
  }

  if (pick === 'STOCK' && holdings.some((h) => classifyHoldingSymbol(h.symbol) === 'STOCK')) {
    reasons.push({
      id: 'stock_basket_vs_equal',
      severity: 'info',
      title: '核心 STOCK = S-3 篮；其余 CN 仓是卫星，不要混成一只龙头',
      detail: '核心腿按股票篮等权；卫星按 clip4 四槽。条件单分批成交会拉开与回测的距离。',
    });
  }

  return finish(pick, 'twin_star', corePct, satBudget, weights, reasons);
}

/**
 * Detect gaps between live holdings and today's recipe.
 * Always appends structural timing notes (info).
 *
 * Default mode is single_track so existing 100% hard-switch tests stay valid.
 * Live Watchlist must pass mode='twin_star' plus coreTargetPct from the action API.
 */
export function detectReplicaGaps(input: {
  pick: string | null | undefined;
  holdings: HoldingSnap[];
  mode?: ReplicaGapMode;
  coreTargetPct?: number | null;
}): ReplicaGapReport {
  const pick = (input.pick || 'REPO').toUpperCase();
  const mode: ReplicaGapMode = input.mode === 'twin_star' ? 'twin_star' : 'single_track';
  const holdings = (input.holdings || []).filter((h) => (Number(h.positionPct) || 0) > 0);
  const total = weightSum(holdings);
  const idlePct = Math.max(0, 100 - Math.min(100, total));

  let targetWeightPct = 0;
  let stockWeightPct = 0;
  let etfWeightPct = 0;
  for (const h of holdings) {
    const w = Number(h.positionPct) || 0;
    const kind = classifyHoldingSymbol(h.symbol);
    if (kind === 'STOCK') stockWeightPct += w;
    if (kind === 'ETF') etfWeightPct += w;
    if (holdingMatchesPick(h.symbol, pick)) targetWeightPct += w;
  }

  const weights = { targetWeightPct, stockWeightPct, etfWeightPct, idlePct, total };
  const corePct = resolveCoreTargetPct({ mode, coreTargetPct: input.coreTargetPct, stockWeightPct });

  if (mode === 'twin_star') {
    return detectTwinStarGaps(pick, holdings, corePct, weights);
  }
  return detectSingleTrackGaps(pick, holdings, weights);
}
