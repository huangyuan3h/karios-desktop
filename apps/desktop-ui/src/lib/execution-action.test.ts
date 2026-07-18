import { describe, expect, it } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import {
  BUY_SCORE_MIN,
  buildSectorExposureByIndustry,
  deriveActionCard,
  deriveTriggerAndTrail,
  evaluateHeldTrimGates,
  evaluateNewEntryGates,
  isAtOrOverPositionSizeCap,
  isDefenseSector,
  isHeldPosition,
  isSectorConcentrationBlocked,
} from './execution-action';
import type { MainlineAllowSet } from './hot-industry-picks';

const attackGate: ExecutionGate = {
  mode: 'ATTACK',
  allowNewEntries: true,
  marketRegime: 'Strong',
  indexLight: 'green',
  srvLevel: 'Stable',
  srvOverlapCount: 3,
  downCount: 1000,
  reasons: ['REGIME_STRONG'],
  positionRangeHint: '50%-60%',
  satelliteNote: 'ok',
};

const holdGate: ExecutionGate = {
  ...attackGate,
  mode: 'HOLD_ONLY',
  allowNewEntries: false,
  marketRegime: 'Diverging',
  reasons: ['REGIME_DIVERGING'],
};

const defendGate: ExecutionGate = {
  ...attackGate,
  mode: 'DEFEND',
  allowNewEntries: false,
  marketRegime: 'Weak',
  indexLight: 'red',
  srvLevel: 'Extreme_High',
  srvOverlapCount: 0,
  reasons: ['SRV_EXTREME_HIGH'],
  positionRangeHint: '0%-10%',
  satelliteNote: '防守优先',
};

function allowSet(names: Array<[string, 'MOMENTUM' | '5D_TOP3']>): MainlineAllowSet {
  const byName = new Map(names);
  return { ready: true, names: new Set(names.map(([n]) => n)), byName };
}

describe('deriveTriggerAndTrail', () => {
  it('arms chandelier when pnl >= 10% and has atr/peak', () => {
    const out = deriveTriggerAndTrail({
      hardStop: 10,
      costPrice: 10,
      maxPrice: 12,
      current: 11.5,
      atr14: 0.5,
    });
    expect(out.trailArmed).toBe(true);
    expect(out.trailStop).toBeCloseTo(11, 6);
    expect(out.trigger).toBeCloseTo(11, 6);
  });

  it('uses hardStop only when not armed', () => {
    const out = deriveTriggerAndTrail({
      hardStop: 9.5,
      costPrice: 10,
      maxPrice: 10.5,
      current: 10.2,
      atr14: 0.4,
    });
    expect(out.trailArmed).toBe(false);
    expect(out.trigger).toBe(9.5);
  });
});

describe('evaluateNewEntryGates', () => {
  it('blocks defense sectors', () => {
    expect(isDefenseSector('银行')).toBe(true);
    expect(evaluateNewEntryGates({ industryName: '股份制银行', mainlineAllow: allowSet([['股份制银行', '5D_TOP3']]) }).why).toBe(
      'DEFENSE_SECTOR_BLOCK',
    );
  });

  it('blocks missing industry', () => {
    expect(evaluateNewEntryGates({ industryName: null, mainlineAllow: allowSet([['半导体', '5D_TOP3']]) }).why).toBe(
      'MISSING_INDUSTRY',
    );
  });

  it('blocks when mainline data unavailable', () => {
    expect(
      evaluateNewEntryGates({
        industryName: '半导体',
        mainlineAllow: { ready: false, names: new Set(), byName: new Map() },
      }).why,
    ).toBe('MAINLINE_DATA_UNAVAILABLE');
  });

  it('blocks intraday surge before mainline check', () => {
    expect(
      evaluateNewEntryGates({
        industryName: '半导体',
        mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
        intradayChgPct: 6.1,
      }).why,
    ).toBe('INTRADAY_SURGE_BLOCK');
  });

  it('blocks gap-up in Weak before mainline check', () => {
    expect(
      evaluateNewEntryGates({
        industryName: '半导体',
        mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
        gapUp: true,
        marketRegime: 'Weak',
      }).why,
    ).toBe('GAP_UP_WEAK_BLOCK');
  });

  it('surge takes priority over gap-up weak', () => {
    expect(
      evaluateNewEntryGates({
        industryName: '半导体',
        mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
        intradayChgPct: 6.1,
        gapUp: true,
        marketRegime: 'Weak',
      }).why,
    ).toBe('INTRADAY_SURGE_BLOCK');
  });
});

describe('deriveActionCard', () => {
  const mainline = allowSet([['半导体', '5D_TOP3'], ['AI应用', 'MOMENTUM']]);

  it('marks BUY when attack + buy + score + mainline', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
    expect(card.mainlineOk).toBe(true);
    expect(isHeldPosition({ symbol: 'CN:600000' })).toBe(false);
  });

  it('downgrades BUY to WATCH when not mainline', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 90,
        buyAction: 'buy',
        stopLossPrice: 9,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('NOT_MAINLINE');
    expect(card.mainlineOk).toBe(false);
  });

  it('blocks defense sector even if in mainline set', () => {
    const card = deriveActionCard({
      symbol: 'CN:002142',
      gate: attackGate,
      trendok: {
        score: 90,
        buyAction: 'buy',
        stopLossPrice: 9,
        values: { emIndustry: '银行' },
      },
      position: { symbol: 'CN:002142' },
      currentPrice: 30,
      mainlineAllow: allowSet([['银行', '5D_TOP3']]),
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('DEFENSE_SECTOR_BLOCK');
  });

  it('downgrades BUY to WATCH when gate blocks new entries', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: holdGate,
      trendok: {
        score: 90,
        buyAction: 'buy',
        stopLossPrice: 9,
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('GATE_BLOCK_NEW');
  });

  it('EXIT on exit_now ignores mainline', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('ADD when held + attack + buy + mainline', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 85,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: 'AI应用' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 10.5 },
      currentPrice: 10.2,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('ADD');
    expect(card.why).toBe('MAINLINE_MOMENTUM');
  });

  it('TRIMs held position when industry leaves mainline', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 85,
        buyAction: 'buy',
        stopLossPrice: 9,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 10.5 },
      currentPrice: 10.2,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('TRIM');
    expect(card.why).toBe('MAINLINE_FADE');
  });

  it('TRIM on warn_reduce_half', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'wait',
        stopLossPrice: 9,
        stopLossParts: { warn_reduce_half: true, atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 8, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('TRIM');
  });

  it('TRIMs held position when Gate is DEFEND', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: defendGate,
      trendok: {
        score: 90,
        buyAction: 'buy',
        stopLossPrice: 9,
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('TRIM');
    expect(card.why).toBe('GATE_DEFEND');
  });

  it('EXIT still beats DEFEND TRIM', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: defendGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('HOLD_ONLY held + still on mainline stays HOLD (no gate TRIM)', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: holdGate,
      trendok: {
        score: 70,
        buyAction: 'wait',
        stopLossPrice: 9,
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('GATE_BLOCK_NEW');
  });

  it('HOLD_ONLY held + mainline fade still TRIMs', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: holdGate,
      trendok: {
        score: 70,
        buyAction: 'wait',
        stopLossPrice: 9,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('TRIM');
    expect(card.why).toBe('MAINLINE_FADE');
  });

  it('does not mainline-fade TRIM when allow-set not ready', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'wait',
        stopLossPrice: 9,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: { ready: false, names: new Set(), byName: new Map() },
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('HOLD');
  });

  it('blocks BUY to WATCH on intraday surge >6%', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      intradayChgPct: 6.1,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('INTRADAY_SURGE_BLOCK');
    expect(card.mainlineOk).toBe(true);
  });

  it('blocks ADD to HOLD on intraday surge (not TRIM)', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      intradayChgPct: 6.1,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('INTRADAY_SURGE_BLOCK');
  });

  it('allows BUY at exactly 6.0% intraday (strict >)', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      intradayChgPct: 6.0,
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('does not surge-block when intradayChgPct is null', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      intradayChgPct: null,
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('EXIT on exit_now ignores intraday surge', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      intradayChgPct: 8,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('blocks BUY to WATCH on gap-up in Weak', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('GAP_UP_WEAK_BLOCK');
    expect(card.mainlineOk).toBe(true);
  });

  it('blocks BUY to WATCH on gap-up in Diverging', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Diverging',
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('GAP_UP_WEAK_BLOCK');
  });

  it('blocks ADD to HOLD on gap-up weak (not TRIM)', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('GAP_UP_WEAK_BLOCK');
  });

  it('allows BUY on gap-up in Strong', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Strong',
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('allows BUY when gapUp false in Weak', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      gapUp: false,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('does not gap-block when gapUp is null', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      gapUp: null,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('surge Why wins over gap-up weak when both fire', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      intradayChgPct: 6.1,
      gapUp: true,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('INTRADAY_SURGE_BLOCK');
  });

  it('EXIT on exit_now ignores gap-up weak', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('blocks ADD to HOLD at positionPct 15% (SIZE_CAP_BLOCK)', () => {
    expect(isAtOrOverPositionSizeCap(15)).toBe(true);
    expect(isAtOrOverPositionSizeCap(14.9)).toBe(false);
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 15, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('SIZE_CAP_BLOCK');
  });

  it('allows ADD below size cap at 14.9%', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 14.9, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('ADD');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('blocks ADD at positionPct 20%', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('SIZE_CAP_BLOCK');
  });

  it('fail-open ADD when held via costPrice only (no positionPct)', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('ADD');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('candidate BUY ignores size cap', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('BUY');
    expect(isHeldPosition({ symbol: 'CN:600000' })).toBe(false);
  });

  it('EXIT ignores size cap', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('entryGate failure takes priority over size cap', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      gapUp: true,
      marketRegime: 'Weak',
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('GAP_UP_WEAK_BLOCK');
  });

  it('blocks BUY when sector concentration >= 30%', () => {
    const exposure = buildSectorExposureByIndustry([
      { industryName: '半导体', position: { symbol: 'CN:1', positionPct: 15 } },
      { industryName: '半导体', position: { symbol: 'CN:2', positionPct: 15 } },
    ]);
    expect(exposure.get('半导体')).toBe(30);
    expect(isSectorConcentrationBlocked('半导体', exposure)).toBe(true);
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      sectorExposureByIndustry: exposure,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('SECTOR_CONC_BLOCK');
    expect(card.mainlineOk).toBe(true);
  });

  it('blocks ADD to HOLD on sector concentration (not TRIM)', () => {
    const exposure = new Map([['半导体', 32]]);
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      sectorExposureByIndustry: exposure,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('SECTOR_CONC_BLOCK');
  });

  it('allows BUY when sector sum is 29.9%', () => {
    const exposure = new Map([['半导体', 29.9]]);
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
      sectorExposureByIndustry: exposure,
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('does not sector-block when exposure map omitted', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: BUY_SCORE_MIN,
        buyAction: 'buy',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('BUY');
  });

  it('excludes holdings without positionPct from sector sum', () => {
    const exposure = buildSectorExposureByIndustry([
      { industryName: '半导体', position: { symbol: 'CN:1', costPrice: 10 } },
      { industryName: '半导体', position: { symbol: 'CN:2', positionPct: 10 } },
    ]);
    expect(exposure.get('半导体')).toBe(10);
    expect(isSectorConcentrationBlocked('半导体', exposure)).toBe(false);
  });

  it('EXIT ignores sector concentration', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        buyAction: 'avoid',
        stopLossPrice: 9,
        stopLossParts: { exit_now: true, atr14: 0.2 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11 },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      sectorExposureByIndustry: new Map([['半导体', 40]]),
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });
});

describe('evaluateHeldTrimGates', () => {
  it('DEFEND trims first', () => {
    expect(
      evaluateHeldTrimGates({
        mode: 'DEFEND',
        industryName: '半导体',
        mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
      }),
    ).toEqual({ trim: true, why: 'GATE_DEFEND' });
  });

  it('mainline fade when ready and industry out', () => {
    expect(
      evaluateHeldTrimGates({
        mode: 'ATTACK',
        industryName: '白酒',
        mainlineAllow: allowSet([['半导体', '5D_TOP3']]),
      }),
    ).toEqual({ trim: true, why: 'MAINLINE_FADE' });
  });
});
