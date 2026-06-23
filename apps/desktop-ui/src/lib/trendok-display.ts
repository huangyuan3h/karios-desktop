import type { TrendOkResult } from '@/lib/api/types';

export type TrendOkChecks = {
  emaOrder?: boolean | null;
  macdPositive?: boolean | null;
  macdHistExpanding?: boolean | null;
  closeNear20dHigh?: boolean | null;
  rsiInRange?: boolean | null;
  volumeSurge?: boolean | null;
};

export const TREND_OK_CHECKS: Array<{ key: keyof TrendOkChecks; failText: string }> = [
  { key: 'emaOrder', failText: 'EMA order broken (Close <= EMA20 or EMA20 <= EMA60)' },
  { key: 'macdPositive', failText: 'MACD <= 0' },
  { key: 'macdHistExpanding', failText: 'MACD hist <= 0' },
  { key: 'closeNear20dHigh', failText: 'Close < 0.90 * High(20)' },
  { key: 'rsiInRange', failText: 'RSI(14) out of 50..90' },
  { key: 'volumeSurge', failText: 'AvgVol(5) < 0.9 * AvgVol(30)' },
];

export function trendOkSummary(t?: TrendOkResult | null): string {
  if (!t) return '—';
  if (t.trendOk === true) return '✅';
  const checks = t.checks ?? null;
  if (!checks || typeof checks !== 'object') return t.trendOk === false ? '❌' : '—';
  const failed: string[] = [];
  for (const rule of TREND_OK_CHECKS) {
    const val = (checks as TrendOkChecks)[rule.key];
    if (val === false) failed.push(rule.failText);
  }
  if (failed.length) return failed.join('; ');
  return t.trendOk === false ? '❌' : '—';
}

export function trendOkRuleLines(): string[] {
  return [
    '- Close > EMA20 and EMA20 > EMA60',
    '- MACD line > 0',
    '- MACD histogram > 0',
    '- Close >= 0.90 * High(20)',
    '- RSI(14) in [50, 90]',
    '- AvgVol(5) >= 0.9 * AvgVol(30)',
  ];
}

/** Score (0–100) rules for UI / Markdown; aligned with backend trendok.py. */
export function scoreExplainZhLines(): string[] {
  return [
    'Score 为 0～100 的确定性公式分（A 股日线、无 LLM）。先算「基础分」并限制在 0～100；若有行业资金流上下文，再累加行业调整 delta，再次限制在 0～100。',
    '基础分 = 五项加权子分之和 + EMA20 五日正斜率奖励 − Anti-Spike 剥离惩罚。每项子分先把信号压到 0～1，再乘以「100 × 该项权重」。',
    '权重：EMA 趋势连贯 40%；MACD 动能稳定 20%；量能一致性 20%；突破平滑 10%；RSI 舒适带 10%。',
    'EMA：EMA5>EMA20（0.4）+ EMA20>EMA60（0.4）+ EMA20 日斜率>0.1%（0.2），合计 0～1 后乘 40 分。',
    'MACD：MACD 线 <0 时该项为 0；否则需 MACD 柱连续 2 日为正且今日柱>昨日柱，满分映射后乘 20 分。',
    'Breakout：收盘价 ÷ 近 20 日最高价，从约 0.85～1.0 线性映射到 0～1（clip）后乘 10 分。',
    'RSI：以 RSI=65 为最高分，随 |RSI−65| 增大线性衰减（15 点尺度 clip）后乘 10 分；RSI>80 额外加速衰减。',
    'Volume：VR=AvgVol5÷AvgVol30，[1.2, 2.0] 满分 20；<1.0 按比例衰减；>3.0 子分为 0。VR<1.2 触发日内低量比硬风控：TrendOK=False，最终 Score 封顶 79。',
    '右侧加分：EMA20 连续 5 日上升 → +5（scoreParts 中 bonus_ema20_slope_5d）。',
    'Anti-Spike 剥离：① 日内涨幅>6% → −20（penalty_intraday_spike）。② ATR14/收盘价>5% 起按 (ratio−0.05)×1000  steep 扣分（penalty_volatility_atr）。③ 当日量/AvgVol30>3 → −15（penalty_volume_climax）。④ 收盘<EMA20 → −30（penalty_below_ema20）。',
    '行业资金流（可选）：如 5 日净流入行业 Top3 +10、当日热点 Top3 +5、Top4–5 +3；5 日弱势榜等可 −10～−20；细节以返回的 scoreParts 与 industryFlowReasons 为准。',
  ];
}

export function scoreRuleLines(): string[] {
  return [
    '- Deterministic 0–100 score (CN daily, no LLM).',
    '- Subscores: EMA trend 25%, MACD strength 15%, breakout 25%, RSI 15%, volume 20%.',
    '- Bonus: +3 when Close >= High(20).',
    '- Penalties: high ATR/close (>7%) and Close < EMA20.',
    '- Optional industry flow adjustment when available; VR<1.2 hard-caps final score at 79 and forces TrendOK=false.',
  ];
}
