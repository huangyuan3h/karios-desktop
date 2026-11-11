import { afterEach, describe, expect, it } from 'vitest';

import { STRATEGY_MODES } from './strategy-settings';
import {
  b3TotalPct,
  buildM30Targets,
  cadenceStatus,
  getFollowCadence,
  M30_DRIFT_LEG_PT,
  M30_DRIFT_LINE_PT,
  M30_MIN_TRADE_PCT,
  parkingSymbolForPick,
  pendingParkSellFromSleeve,
  rebalanceGate,
  setFollowCadence,
} from './m30-follow';

afterEach(() => {
  window.localStorage.removeItem('karios.m30FollowCadence.v1');
});

describe('m30 strategy order/default', () => {
  it('lists homeport (M30) first, rest in relative order', () => {
    expect(STRATEGY_MODES[0]).toBe('homeport');
    expect([...STRATEGY_MODES]).toEqual([
      'homeport',
      'starship_b',
      'starship_robust',
      'starship',
      'starport',
      'harbor',
      'twin_star',
    ]);
  });
});

describe('m30 follow cadence setting', () => {
  it('defaults to weekly and persists explicit picks', () => {
    expect(getFollowCadence()).toBe('weekly');
    setFollowCadence('monthly');
    expect(getFollowCadence()).toBe('monthly');
    setFollowCadence('daily');
    expect(getFollowCadence()).toBe('daily');
  });
});

describe('parkingSymbolForPick', () => {
  it('maps OIL to its ETF symbol and REPO/STOCK to cash', () => {
    expect(parkingSymbolForPick('OIL', 'ETF:513350')).toBe('ETF:513350');
    expect(parkingSymbolForPick('OIL', null)).toBe('ETF:513350');
    expect(parkingSymbolForPick('REPO', null)).toBeNull();
    expect(parkingSymbolForPick('STOCK', null)).toBeNull();
  });
});

describe('b3TotalPct', () => {
  it('scales within-leg % by 0.3 and sums to 30', () => {
    // backend `targetPct` sums ~100 within the B3 leg.
    const within = [40, 25, 15, 12, 8];
    const totals = within.map(b3TotalPct);
    expect(totals.reduce((s, v) => s + v, 0)).toBeCloseTo(30, 5);
    expect(totals[0]).toBeCloseTo(12, 5);
  });
});

const B3 = [
  { symbol: '510300.SH', name: '沪深300', targetPct: 20, px: 4.6 },
  { symbol: '510500.SH', name: '中证500', targetPct: 20, px: 7.0 },
  { symbol: '518880.SH', name: '黄金', targetPct: 20, px: 6.0 },
  { symbol: '513100.SH', name: '纳指100', targetPct: 20, px: 1.6 },
  { symbol: '511260.SH', name: '10年国债', targetPct: 20, px: 134.0 },
];

describe('buildM30Targets', () => {
  it('weights sum to 100% (70 harbor + 30 B3) with no stocks', () => {
    const r = buildM30Targets({
      capital: 1_160_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [],
    });
    const targetSum = r.targets.reduce((s, t) => s + t.targetPct, 0);
    expect(targetSum).toBeCloseTo(100, 5);
    expect(r.harborTargetPct).toBe(70);
    expect(r.b3TargetPct).toBe(30);
    // No stocks → whole Harbor leg parks in OIL.
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    expect(park?.targetPct).toBeCloseTo(70, 5);
    expect(park?.symbol).toBe('ETF:513350');
    // Amounts derive from 总资金.
    expect(park?.targetAmt).toBe(Math.round(1_160_000 * 0.7));
  });

  it('sizes one HOLD stock at 7% and parks the 63% remainder', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [{ symbol: 'CN:600519', positionPct: 7, action: 'HOLD', lastClose: 72.5 }],
    });
    const stock = r.targets.find((t) => t.symbol === 'CN:600519');
    expect(stock?.targetPct).toBeCloseTo(7, 5);
    expect(stock?.side).toBe('HOLD');
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    expect(park?.targetPct).toBeCloseTo(63, 5);
    expect(r.stockLegCount).toBe(1);
  });

  it('treats B3 ETFs as targets (never "非今日停车 ETF") and lists stale ETFs separately', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [
        { symbol: 'ETF:510300', positionPct: 6 },
        { symbol: 'ETF:159915', positionPct: 4 },
      ],
    });
    // 510300 is a B3 target line (6% of total each here).
    const b3line = r.targets.find((t) => t.symbol === 'ETF:510300');
    expect(b3line).toBeDefined();
    expect(b3line?.targetPct).toBeCloseTo(6, 5);
    // Unknown 159915 is a non-target → SELL action.
    expect(r.nonTargets.map((h) => h.symbol)).toContain('ETF:159915');
    expect(r.actions.some((a) => a.symbol === 'ETF:159915' && a.side === 'SELL')).toBe(true);
  });

  it('sells EXIT stocks in full (hard exits bypass the cadence gate)', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [{ symbol: 'CN:600519', positionPct: 7, action: 'EXIT', lastClose: 72.5 }],
    });
    const line = r.targets.find((t) => t.symbol === 'CN:600519');
    expect(line?.targetPct).toBe(0);
    expect(line?.side).toBe('SELL');
  });

  it('suppresses dust below the min-trade rule', () => {
    expect(M30_MIN_TRADE_PCT).toBe(0.5);
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      // B3 line targeting 6.00% with 5.90% held → 0.10pt drift < 0.5 → HOLD.
      b3: [{ symbol: '510300.SH', name: '沪深300', targetPct: 20, px: 4.6 }],
      holdings: [{ symbol: 'ETF:510300', positionPct: 5.9 }],
    });
    const line = r.targets.find((t) => t.symbol === 'ETF:510300');
    expect(line?.suppressed).toBe(true);
    expect(line?.side).toBe('HOLD');
    expect(r.actions.some((a) => a.symbol === 'ETF:510300')).toBe(false);
  });

  it('rounds lots to board lots (ETF 100份)', () => {
    const r = buildM30Targets({
      capital: 1_160_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: [],
      holdings: [],
    });
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    // 70% × 1,160,000 = 812,000 / 1.42 = 571,830 → 571,800 (100 lots).
    expect(park?.targetShares).toBe(571_800);
    expect((park?.targetShares ?? 0) % 100).toBe(0);
  });

  it('orders sells before buys', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'GOLD',
      pickSymbol: 'ETF:518880',
      pickPrice: 6,
      b3: [{ symbol: '510300.SH', name: '沪深300', targetPct: 100, px: 4.6 }],
      holdings: [
        { symbol: 'CN:600519', positionPct: 7, action: 'EXIT', lastClose: 72.5 },
        { symbol: 'ETF:510300', positionPct: 0 },
      ],
    });
    expect(r.actions.length).toBeGreaterThan(1);
    expect(r.actions[0]?.side).toBe('SELL');
  });
  it('labels a leg-drift B3 overweight sell as harbor refill, not a monthly weight change', () => {
    // 511260 实持 69.3% 远超 B3 月频 6%，而港湾腿 0%/70% 欠配：这笔卖出是把钱
    // 还给港湾停车（腿偏离），原因必须与纯月频权重区分。目标/方向/份数不动。
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [
        { symbol: 'ETF:511260', positionPct: 69.3 },
        { symbol: 'ETF:513100', positionPct: 3.6 },
      ],
    });
    const sell = r.actions.find((a) => a.symbol === 'ETF:511260');
    expect(sell?.side).toBe('SELL');
    expect(sell?.pct).toBeCloseTo(63.3, 5);
    expect(sell?.reason).toMatch(/腿偏离腾资金补港湾停车/);
    expect(sell?.reason).not.toBe('B3 月频目标');
    // 同一目标权重与方向口径：B3 线目标仍是 6%。
    const line = r.targets.find((t) => t.symbol === 'ETF:511260');
    expect(line?.targetPct).toBeCloseTo(6, 5);
    expect(line?.side).toBe('SELL');
  });

  it('keeps the plain monthly label for B3 sells when the harbor leg is balanced', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: [{ symbol: '510300.SH', name: '沪深300', targetPct: 100, px: 4.6 }],
      holdings: [
        { symbol: 'ETF:513350', positionPct: 70 },
        { symbol: 'ETF:510300', positionPct: 35 },
      ],
    });
    expect(r.harborCurrentPct).toBeCloseTo(70, 5);
    const sell = r.actions.find((a) => a.symbol === 'ETF:510300');
    expect(sell?.side).toBe('SELL');
    expect(sell?.reason).toBe('B3 月频目标');
  });
});

describe('buildM30Targets — overlap / HK / REPO (review fixes)', () => {
  it('merges the Harbor park ETF with its B3 line when the pick is a B3 member (no double count)', () => {
    // pick=BOND10 → 511260, which is also a B3 member.
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'BOND10',
      pickSymbol: 'ETF:511260',
      pickPrice: 134,
      b3: B3,
      holdings: [{ symbol: 'ETF:511260', positionPct: 69.3 }],
    });
    const lines = r.targets.filter((t) => t.symbol === 'ETF:511260');
    expect(lines).toHaveLength(1);
    expect(r.targets.some((t) => t.leg === 'harbor-park')).toBe(false);
    // 70 (Harbor park) + 0.3 × 20 (B3) = 76.
    expect(lines[0]?.targetPct).toBeCloseTo(76, 5);
    expect(lines[0]?.harborSharePct).toBeCloseTo(70, 5);
    expect(lines[0]?.currentPct).toBeCloseTo(69.3, 5);
    expect(r.targets.reduce((s, t) => s + t.targetPct, 0)).toBeCloseTo(100, 5);
    // Holding counted once across both legs.
    expect(r.harborCurrentPct + r.b3CurrentPct).toBeCloseTo(69.3, 1);
  });

  it('counts a B3-member NASDAQ alias (513100) on its B3 line only', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'NASDAQ',
      pickSymbol: 'ETF:513110',
      pickPrice: 1.5,
      b3: B3,
      holdings: [{ symbol: 'ETF:513100', positionPct: 6 }],
    });
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    expect(park?.symbol).toBe('ETF:513110');
    expect(park?.currentPct).toBe(0);
    expect(r.targets.find((t) => t.symbol === 'ETF:513100')?.currentPct).toBeCloseTo(6, 5);
    expect(r.harborCurrentPct + r.b3CurrentPct).toBeCloseTo(6, 5);
  });

  it('treats HK names as non-M30 (Harbor leg is CN-only)', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [{ symbol: 'HK:00700', positionPct: 5, action: 'HOLD', lastClose: 400 }],
    });
    expect(r.targets.some((t) => t.symbol === 'HK:00700')).toBe(false);
    expect(r.stockLegCount).toBe(0);
    expect(r.nonTargets.map((h) => h.symbol)).toContain('HK:00700');
    const a = r.actions.find((x) => x.symbol === 'HK:00700');
    expect(a?.side).toBe('SELL');
    expect(a?.shares).toBeNull();
  });

  it('pick=REPO: idle cash counts toward the Harbor leg (aligned book has no leg drift)', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'REPO',
      pickSymbol: null,
      pickPrice: null,
      b3: B3,
      holdings: B3.map((u) => ({ symbol: `ETF:${u.symbol.split('.')[0]}`, positionPct: 6 })),
    });
    expect(r.cashTargetPct).toBeCloseTo(70, 5);
    expect(r.cashCurrentPct).toBeCloseTo(70, 5);
    expect(r.harborCurrentPct).toBeCloseTo(70, 5);
    expect(r.legDriftPt).toBe(0);
    expect(r.actions).toHaveLength(0);
  });
});

describe('buildM30Targets — pending Harbor exit (trail8 / MA200 wipe)', () => {
  // 2026-11-05 live case: 63.5% parked in OIL, the 2026-11-03 close breached the
  // 8% trail → sleeve says SELL_TO_REPO while pick.key is still OIL.
  const PENDING = {
    symbol: 'ETF:513350',
    reason: 'ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03，待开盘执行）',
    signalDate: '2026-11-03',
    executeHint: '待开盘卖出转逆回购（信号已触发，未执行）',
  };
  const HOLDINGS = [
    { symbol: 'ETF:513350', positionPct: 63.5 },
    { symbol: 'ETF:513100', positionPct: 3.6 },
  ];

  it('sells the held park leg → repo and never buys the same ETF', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: HOLDINGS,
      pendingParkSell: PENDING,
    });
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    expect(park?.symbol).toBe('ETF:513350');
    // Target is repo (0), not the 70% harbor park weight.
    expect(park?.targetPct).toBe(0);
    expect(park?.side).toBe('SELL');
    // Must-act first row, even before the (big b3) sells.
    const first = r.actions[0];
    expect(first?.mustAct).toBe(true);
    expect(first?.side).toBe('SELL');
    expect(first?.symbol).toBe('ETF:513350');
    expect(first?.pct).toBeCloseTo(63.5, 5);
    expect(first?.reason).toMatch(/逆回购/);
    expect(first?.execHint).toContain('待开盘卖出');
    // Hard rule: no BUY on the exiting symbol anywhere.
    expect(r.actions.some((a) => a.side === 'BUY' && a.symbol === 'ETF:513350')).toBe(false);
    // Park weight moves to cash (repo), not to a live ETF leg.
    expect(r.cashTargetPct).toBeCloseTo(70, 5);
  });

  it('keeps the B3 share when the exiting park leg is also a B3 member', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'BOND10',
      pickSymbol: 'ETF:511260',
      pickPrice: 134,
      b3: B3,
      holdings: [{ symbol: 'ETF:511260', positionPct: 45 }],
      pendingParkSell: { ...PENDING, symbol: 'ETF:511260' },
    });
    const line = r.targets.find((t) => t.symbol === 'ETF:511260');
    // 6% B3 target stays; only the parked overage is sold.
    expect(line?.targetPct).toBeCloseTo(6, 5);
    expect(line?.side).toBe('SELL');
    expect(r.actions[0]?.symbol).toBe('ETF:511260');
    expect(r.actions[0]?.mustAct).toBe(true);
  });

  it('without a pending exit the park leg still targets the park weight', () => {
    const r = buildM30Targets({
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [{ symbol: 'ETF:513350', positionPct: 63.5 }],
    });
    const park = r.targets.find((t) => t.leg === 'harbor-park');
    expect(park?.targetPct).toBeCloseTo(70, 5);
    expect(park?.side).toBe('BUY');
    expect(r.actions.some((a) => a.mustAct)).toBe(false);
  });
});

describe('pendingParkSellFromSleeve', () => {
  it('extracts a SELL_TO_REPO sleeve action and ignores HOLD/ROTATE', () => {
    expect(
      pendingParkSellFromSleeve({
        action: 'SELL_TO_REPO',
        symbol: 'ETF:513350',
        signalDate: '2026-11-03',
        executeHint: '次日开盘卖出转逆回购',
        message: '峰值回撤8.5%',
      }),
    ).toEqual({
      symbol: 'ETF:513350',
      reason: '峰值回撤8.5%',
      signalDate: '2026-11-03',
      executeHint: '次日开盘卖出转逆回购',
    });
    expect(pendingParkSellFromSleeve({ action: 'HOLD', symbol: 'ETF:513350' })).toBeNull();
    expect(pendingParkSellFromSleeve({ action: 'ROTATE', symbol: 'ETF:513110' })).toBeNull();
    expect(pendingParkSellFromSleeve(null)).toBeNull();
  });
});

describe('cadenceStatus', () => {
  it('weekly: Monday is the rebalance day', () => {
    // 2026-10-05 is a Monday.
    const mon = cadenceStatus('weekly', '2026-10-05');
    expect(mon.isRebalanceDay).toBe(true);
    expect(mon.last).toBe('2026-10-05');
    expect(mon.next).toBe('2026-10-12');
    const wed = cadenceStatus('weekly', '2026-10-07');
    expect(wed.isRebalanceDay).toBe(false);
    expect(wed.last).toBe('2026-10-05');
    expect(wed.next).toBe('2026-10-12');
  });

  it('weekly: weekends are never rebalance days (holiday limitation documented)', () => {
    const sat = cadenceStatus('weekly', '2026-10-10');
    expect(sat.isRebalanceDay).toBe(false);
    expect(sat.last).toBe('2026-10-05');
    expect(sat.next).toBe('2026-10-12');
    const sun = cadenceStatus('weekly', '2026-10-11');
    expect(sun.isRebalanceDay).toBe(false);
  });

  it('monthly: first weekday of the month', () => {
    // 2026-10-01 is a Thursday.
    const first = cadenceStatus('monthly', '2026-10-01');
    expect(first.isRebalanceDay).toBe(true);
    expect(first.last).toBe('2026-10-01');
    const mid = cadenceStatus('monthly', '2026-10-07');
    expect(mid.isRebalanceDay).toBe(false);
    expect(mid.last).toBe('2026-10-01');
    expect(mid.next).toBe('2026-11-02'); // 2026-11-01 is a Sunday → Monday
  });

  it('daily: every weekday', () => {
    expect(cadenceStatus('daily', '2026-10-07').isRebalanceDay).toBe(true);
    expect(cadenceStatus('daily', '2026-10-10').isRebalanceDay).toBe(false);
  });
});

describe('rebalanceGate', () => {
  it('fires on the rebalance day or past the 5pt drift thresholds', () => {
    expect(M30_DRIFT_LEG_PT).toBe(5);
    expect(M30_DRIFT_LINE_PT).toBe(5);
    expect(
      rebalanceGate({ isRebalanceDay: true, legDriftPt: 0, maxLineDriftPt: 0 }).needsRebalance,
    ).toBe(true);
    expect(
      rebalanceGate({ isRebalanceDay: false, legDriftPt: 5, maxLineDriftPt: 0 }).needsRebalance,
    ).toBe(true);
    expect(
      rebalanceGate({ isRebalanceDay: false, legDriftPt: 0, maxLineDriftPt: 5 }).needsRebalance,
    ).toBe(true);
    expect(
      rebalanceGate({ isRebalanceDay: false, legDriftPt: 1.2, maxLineDriftPt: 2.1 }).needsRebalance,
    ).toBe(false);
  });
});

describe('buildM30Targets — 14:30 warning blocks adds (clock unchanged)', () => {
  it('suppresses the park BUY while a print is at/under the trail line', () => {
    const input = {
      capital: 1_000_000,
      pickKey: 'OIL',
      pickSymbol: 'ETF:513350',
      pickPrice: 1.42,
      b3: B3,
      holdings: [{ symbol: 'ETF:518880', positionPct: 20 }],
    };
    const before = buildM30Targets(input);
    expect(before.actions.some((a) => a.side === 'BUY' && a.symbol === 'ETF:513350')).toBe(true);
    const after = buildM30Targets({ ...input, parkBuyBlockedSymbol: 'ETF:513350' });
    expect(after.actions.some((a) => a.side === 'BUY' && a.symbol === 'ETF:513350')).toBe(false);
    // The clock is unchanged: a warning alone never creates a SELL.
    expect(after.actions.some((a) => a.symbol === 'ETF:513350' && a.side === 'SELL')).toBe(false);
    expect(after.actions.some((a) => a.mustAct)).toBe(false);
  });
});
