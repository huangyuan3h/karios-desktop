/** Integer-lot sizing for manual orders (OPT-204, 2026-09-15).
 *
 * Exchange rules encoded here:
 *  - 沪深主板 / 创业板: 100 股整数倍
 *  - 科创板 (688xxx): 申报 ≥ 200 股，超出部分以 1 股递增
 *  - 北交所 (43/83/87/88/92xxxx): 申报 ≥ 100 股，超出部分以 1 股递增
 *  - 场内 ETF: 100 份整数倍
 *  - 港股: 每手股数由发行人决定（静态表覆盖常用标的，其余按 100 估算，以券商为准）
 *
 * Display / execution aid only: it converts a suggested position % into the
 * nearest valid integer order. It never changes strategy position sizing
 * (that stays the frozen engine's — a rank tilt belongs in a prereg).
 */

export type LotRule = {
  /** Minimum order size (股/份). */
  min: number;
  /** Increment above min (100 for board lots; 1 for STAR/BSE). */
  step: number;
  /** 股 (stocks) or 份 (ETF). */
  unit: '股' | '份';
  note: string;
  source: 'rule' | 'table' | 'default';
};

/** Board lots for common HK names (varies by issuer; fallback below). */
export const HK_BOARD_LOTS: Record<string, number> = {
  '00005': 400, // 汇丰
  '00388': 100, // 港交所
  '00700': 100, // 腾讯
  '00939': 1000, // 建设银行
  '00941': 500, // 中国移动
  '01024': 100, // 快手
  '01109': 500, // 华润置地
  '01299': 200, // 友邦
  '01398': 1000, // 工商银行
  '01810': 200, // 小米
  '02020': 200, // 安踏
  '02318': 500, // 中国平安
  '03690': 100, // 美团
  '09618': 50, // 京东
  '09888': 50, // 百度
  '09988': 100, // 阿里
  '09999': 100, // 网易
};

export function lotRuleFor(symbol: string): LotRule {
  const s = String(symbol || '').trim().toUpperCase();
  if (s.startsWith('ETF:')) {
    return { min: 100, step: 100, unit: '份', source: 'rule', note: '场内 ETF：100 份整数倍' };
  }
  if (s.startsWith('HK:')) {
    const code = s.slice(3).replace(/\D/g, '').padStart(5, '0');
    const lot = HK_BOARD_LOTS[code];
    if (lot) {
      return {
        min: lot,
        step: lot,
        unit: '股',
        source: 'table',
        note: `港股每手 ${lot} 股（以券商为准）`,
      };
    }
    return {
      min: 100,
      step: 100,
      unit: '股',
      source: 'default',
      note: '港股每手不同：按 100 股估算（以券商为准）',
    };
  }
  const code = s.replace(/^CN:/, '');
  if (/^688\d{3}$/.test(code)) {
    return {
      min: 200,
      step: 1,
      unit: '股',
      source: 'rule',
      note: '科创板：≥200 股，超出部分 1 股递增',
    };
  }
  if (/^(43|83|87|88|92)\d{4}$/.test(code)) {
    return {
      min: 100,
      step: 1,
      unit: '股',
      source: 'rule',
      note: '北交所：≥100 股，超出部分 1 股递增',
    };
  }
  return { min: 100, step: 100, unit: '股', source: 'rule', note: '沪深：100 股整数倍' };
}

/** Nearest valid order size for a raw (unrounded) share count. */
export function roundToLot(rawShares: number, rule: LotRule): number {
  const raw = Number(rawShares);
  if (!Number.isFinite(raw) || raw <= 0) return 0;
  if (rule.min === rule.step) {
    return Math.round(raw / rule.step) * rule.step;
  }
  const rounded = Math.round(raw);
  if (rounded < rule.min) return raw >= rule.min / 2 ? rule.min : 0;
  return rounded;
}

/** Mild rank tilt for the manual size suggestion ("排名靠前多买一点").
 * Display-only; capped downstream by POSITION_SIZE_CAP_PCT. */
export function rankSizeBoost(rank?: number | null): number {
  if (rank === 1) return 1.2;
  if (rank === 2) return 1.1;
  if (rank === 3) return 1.05;
  return 1;
}

export const POSITION_SIZE_CEILING_PCT = 15;

/** Integer order size for a fixed equal-weight slot (satellite 4×25%).
 *
 * Unlike ``suggestLotSizing`` there is NO rank tilt and NO concentration cap:
 * the satellite slots are equal-weight by design, so 25% > the 15% stock
 * ceiling must not bind. Pure/display-only. */
export function slotLotShares(opts: {
  symbol: string;
  price: number;
  capital: number;
  slotPct: number;
}): { shares: number; valueCny: number; rule: LotRule } | null {
  const price = Number(opts.price);
  const capital = Number(opts.capital);
  const slotPct = Number(opts.slotPct);
  if (!(price > 0) || !(capital > 0) || !(slotPct > 0)) return null;
  const rule = lotRuleFor(opts.symbol);
  const valueCny = (capital * slotPct) / 100;
  const shares = roundToLot(valueCny / price, rule);
  return { shares, valueCny, rule };
}

export type LotSizing = {
  shares: number;
  valueLocal: number;
  valueCny: number;
  localCurrency: 'CNY' | 'HKD';
  targetPct: number;
  actualPct: number;
  boost: number;
  rule: LotRule;
  minLotCny: number;
  affordable: boolean;
  warning?: string;
};

export function suggestLotSizing(opts: {
  symbol: string;
  price: number;
  targetPct: number;
  capital: number;
  rank?: number | null;
  fxRate?: number;
}): LotSizing | null {
  const price = Number(opts.price);
  const capital = Number(opts.capital);
  const targetPct = Number(opts.targetPct);
  if (!(price > 0) || !(capital > 0) || !(targetPct > 0)) return null;

  const symbol = String(opts.symbol || '').toUpperCase();
  const isHk = symbol.startsWith('HK:');
  const fxRate = Number(opts.fxRate) > 0 ? Number(opts.fxRate) : 1;
  const boost = rankSizeBoost(opts.rank);
  const rule = lotRuleFor(symbol);
  const localCurrency: 'CNY' | 'HKD' = isHk ? 'HKD' : 'CNY';

  // The concentration cap is a stock rule; the ETF parking leg legitimately
  // takes the whole idle fraction (up to 100%).
  const ceiling = rule.unit === '份' ? 100 : POSITION_SIZE_CEILING_PCT;
  const effectivePct = Math.min(targetPct * boost, ceiling);
  const valueCny = (capital * effectivePct) / 100;
  const valueLocal = isHk ? valueCny / fxRate : valueCny;
  const shares = roundToLot(valueLocal / price, rule);
  const actualValueLocal = shares * price;
  const actualPct = (actualValueLocal * (isHk ? fxRate : 1) * 100) / capital;
  const minLotCny = rule.min * price * (isHk ? fxRate : 1);

  return {
    shares,
    valueLocal: actualValueLocal,
    valueCny: actualValueLocal * (isHk ? fxRate : 1),
    localCurrency,
    targetPct: effectivePct,
    actualPct,
    boost,
    rule,
    minLotCny,
    affordable: actualValueLocal * (isHk ? fxRate : 1) <= capital,
    warning:
      shares === 0
        ? `目标仓位不足 1 手（1 手 ≈ ¥${Math.round(minLotCny).toLocaleString('zh-CN')}）`
        : undefined,
  };
}
