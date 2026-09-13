/**
 * Replica gap — live book vs today's S-3 core recipe.
 *
 * The harbor baseline keeps the S-3 pick-strong core: t-1 mom → 100% switch
 * at next session, with idle cash parked in the core ETF. This module flags
 * live-book deviations from that instruction.
 */

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

export type ReplicaGapReport = {
  pick: string;
  verdict: ReplicaGapVerdict;
  targetWeightPct: number;
  stockWeightPct: number;
  etfWeightPct: number;
  idlePct: number;
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

function timingInfos(): ReplicaGapReason[] {
  return [
    {
      id: 'timing_1430',
      severity: 'info',
      title: '时点：实盘操作 ≠ 回测收盘/次日开盘',
      detail:
        '定案用 t−1 收盘算 pick，净值按日收益复利。盘中买卖会吃到当日剩余波动，与 Timeline 日线归因天然有差。',
    },
    {
      id: 'conditional_orders',
      severity: 'info',
      title: '股票条件单 = 延迟/部分成交',
      detail:
        '止损/移动/到期条件单按触发价成交，不是「收盘瞬间 100% 换仓」。这是纪律工具，但会拉开与硬切 NAV 的距离。',
    },
    {
      id: 'not_full_switch',
      severity: 'info',
      title: '未做 100% 硬切就会系统性跑输展示曲线',
      detail:
        '展示收益假设每天把全部资金放在当日 pick。部分换仓、留底仓、多腿并存，都会让实盘无法「复制」那条曲线。',
    },
  ];
}

function finish(
  pick: string,
  weights: {
    targetWeightPct: number;
    stockWeightPct: number;
    etfWeightPct: number;
    idlePct: number;
    total: number;
  },
  reasons: ReplicaGapReason[],
): ReplicaGapReport {
  reasons.push(...timingInfos());

  const hasBlock = reasons.some((r) => r.severity === 'block');
  const hasWarn = reasons.some((r) => r.severity === 'warn');
  let verdict: ReplicaGapVerdict = 'aligned';
  if (hasBlock) {
    verdict = 'diverged';
  } else if (hasWarn) {
    verdict = 'partial';
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
    reasons,
  };
}

/**
 * Detect gaps between live holdings and today's core recipe.
 * Always appends structural timing notes (info).
 */
export function detectReplicaGaps(input: {
  pick: string | null | undefined;
  holdings: HoldingSnap[];
}): ReplicaGapReport {
  const pick = (input.pick || 'REPO').toUpperCase();
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
  const reasons: ReplicaGapReason[] = [];

  if (pick === 'REPO') {
    if (total > 5) {
      reasons.push({
        id: 'should_be_cash',
        severity: 'block',
        title: '港湾今日 = REPO，实仓仍有风险仓',
        detail: `应接近空仓/逆回购；当前已部署约 ${total.toFixed(0)}%。硬切要求先清股票/ETF。`,
      });
    }
  } else if (pick === 'STOCK') {
    if (stockWeightPct < 50 && total > 10) {
      reasons.push({
        id: 'missing_stock',
        severity: 'block',
        title: '港湾 = STOCK，实仓股票权重不足',
        detail: `股票约 ${stockWeightPct.toFixed(0)}%（目标接近 100%）；ETF ${etfWeightPct.toFixed(0)}%。应先卖 ETF 再配股票篮。`,
      });
    }
    if (etfWeightPct > 15) {
      reasons.push({
        id: 'etf_while_stock',
        severity: 'block',
        title: '仍持有 ETF，偏离 STOCK 硬切',
        detail: `ETF 约 ${etfWeightPct.toFixed(0)}%。港湾核心不会一边拿纳指/黄金一边拿股票篮。`,
      });
    }
  } else {
    if (targetWeightPct < 50 && total > 10) {
      reasons.push({
        id: 'wrong_etf_or_light',
        severity: 'block',
        title: `港湾 = ${pick}，目标 ETF 权重不足`,
        detail: `匹配 ${pick} 的仓位约 ${targetWeightPct.toFixed(0)}%（目标 ~100%）。应卖出其他腿后买入对应 ETF。`,
      });
    }
    if (stockWeightPct > 15) {
      reasons.push({
        id: 'stock_while_etf',
        severity: 'block',
        title: `仍持有股票，偏离 ${pick} 硬切`,
        detail: `股票约 ${stockWeightPct.toFixed(0)}%。若只加 ETF 不清股票，收益路径会与 Timeline 分叉。`,
      });
    }
    const otherEtf = etfWeightPct - targetWeightPct;
    if (otherEtf > 15) {
      reasons.push({
        id: 'wrong_etf',
        severity: 'warn',
        title: '持有非今日核心 ETF',
        detail: `其他 ETF 约 ${Math.max(0, otherEtf).toFixed(0)}%。港湾核心只持 argmax 那一只（或 REPO）。`,
      });
    }
  }

  if (idlePct > 25 && pick !== 'REPO' && targetWeightPct < 70) {
    reasons.push({
      id: 'idle_cash',
      severity: 'warn',
      title: '闲置现金偏多',
      detail: `约 ${idlePct.toFixed(0)}% 未部署。港湾按满仓计收益，闲置现金会拖累复刻。`,
    });
  }

  if (pick === 'STOCK' && holdings.some((h) => classifyHoldingSymbol(h.symbol) === 'STOCK')) {
    reasons.push({
      id: 'stock_basket_vs_equal',
      severity: 'info',
      title: '股票腿 = 多票等权篮，不是一只龙头',
      detail:
        'Timeline 的 STOCK 收益是当日持仓篮等权日收益；条件单分批成交会导致权重/时点与回测不一致。',
    });
  }

  return finish(pick, weights, reasons);
}
