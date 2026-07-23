import { describe, expect, it } from 'vitest';

import type { ExecutionGate } from '@karios/shared';

import {
  BUY_SCORE_MIN,
  buildSectorExposureByIndustry,
  buildSleeveExposurePct,
  countHeldMissingPositionPct,
  deriveActionCard,
  deriveTriggerAndTrail,
  evaluateHeldTrimGates,
  evaluateNewEntryGates,
  formatSleeveBudgetLabel,
  isAtOrOverPositionSizeCap,
  isDefenseSector,
  isHeldMissingPositionPct,
  isHeldPosition,
  isSectorConcentrationBlocked,
  isSleeveCapBlocked,
  parsePositionRangeHintMaxPct,
  suggestFireSizePct,
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
    expect(out.exitStop).toBeCloseTo(11, 6);
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
    expect(out.exitStop).toBe(9.5);
    expect(out.trigger).toBe(9.5);
  });
});

describe('evaluateNewEntryGates', () => {
  it('blocks defense sectors', () => {
    expect(isDefenseSector('银行')).toBe(true);
    expect(isDefenseSector('电力设备')).toBe(false);
    expect(isDefenseSector('电力')).toBe(true);
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

  it('uses SECTOR_OUTFLOW_BLOCK when not mainline and all sectors outflow', () => {
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
      sectorOutflowBlock: true,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('SECTOR_OUTFLOW_BLOCK');
  });

  it('marks PURGE for flat low-score TrendOK=no', () => {
    const card = deriveActionCard({
      symbol: 'CN:603019',
      gate: attackGate,
      trendok: {
        score: 0,
        trendOk: false,
        buyAction: 'avoid',
        buyZoneHigh: 55,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:603019', positionPct: 0 },
      currentPrice: 50,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('PURGE');
    expect(card.why).toBe('PURGE_GC');
    expect(card.entryTrigger).toBe(55);
    expect(card.exitStop).toBeNull();
    expect(card.distPct).toBeCloseTo(((55 - 50) / 50) * 100, 6);
  });

  it('exempts PURGE to WATCH_SILENT for Alpha Max Grade S regardless of catalystScore', () => {
    const card = deriveActionCard({
      symbol: 'CN:603019',
      gate: attackGate,
      trendok: {
        score: 0,
        trendOk: false,
        buyAction: 'avoid',
        buyZoneHigh: 55,
        values: { emIndustry: '白酒' },
      },
      position: { symbol: 'CN:603019', positionPct: 0 },
      currentPrice: 50,
      mainlineAllow: mainline,
      catalyst: { maxGrade: 'S', catalystScore: 99.7 },
    });
    expect(card.action).toBe('WATCH_SILENT');
    expect(card.why).toBe('ALPHA_S_WATCH');
  });

  it('keeps WATCH_SILENT for Alpha S even when catalystScore is low or null', () => {
    const low = deriveActionCard({
      symbol: 'CN:002230',
      gate: attackGate,
      trendok: {
        score: 10,
        trendOk: false,
        buyAction: 'avoid',
        values: { emIndustry: '软件开发' },
      },
      position: { symbol: 'CN:002230', positionPct: 0 },
      currentPrice: 40,
      mainlineAllow: mainline,
      catalyst: { maxGrade: 'S', catalystScore: 0 },
    });
    expect(low.action).toBe('WATCH_SILENT');
    expect(low.why).toBe('ALPHA_S_WATCH');

    const missing = deriveActionCard({
      symbol: 'CN:002230',
      gate: attackGate,
      trendok: {
        score: 10,
        trendOk: false,
        buyAction: 'avoid',
        values: { emIndustry: '软件开发' },
      },
      position: { symbol: 'CN:002230', positionPct: 0 },
      currentPrice: 40,
      mainlineAllow: mainline,
      catalyst: { maxGrade: 'S', catalystScore: null },
    });
    expect(missing.action).toBe('WATCH_SILENT');
    expect(missing.why).toBe('ALPHA_S_WATCH');
  });

  it('still PURGEs when Max Grade is not S', () => {
    const card = deriveActionCard({
      symbol: 'CN:002230',
      gate: attackGate,
      trendok: {
        score: 10,
        trendOk: false,
        buyAction: 'avoid',
        values: { emIndustry: '软件开发' },
      },
      position: { symbol: 'CN:002230', positionPct: 0 },
      currentPrice: 40,
      mainlineAllow: mainline,
      catalyst: { maxGrade: 'A', catalystScore: 99 },
    });
    expect(card.action).toBe('PURGE');
  });

  it('blocks EXIT with T1_LOCK when entryDate is today', () => {
    const card = deriveActionCard({
      symbol: 'CN:002821',
      gate: attackGate,
      trendok: {
        score: 70,
        trendOk: true,
        buyAction: 'wait',
        stopLossPrice: 100,
        stopLossParts: { atr14: 1 },
        values: { emIndustry: '化学制药' },
      },
      position: {
        symbol: 'CN:002821',
        positionPct: 7,
        costPrice: 110,
        entryDate: '2026-07-18',
      },
      currentPrice: 95,
      mainlineAllow: mainline,
      todaySh: '2026-07-18',
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('T1_LOCK');
    expect(card.exitStop).toBe(100);
  });

  it('allows EXIT next calendar day after entryDate', () => {
    const card = deriveActionCard({
      symbol: 'CN:002821',
      gate: attackGate,
      trendok: {
        score: 70,
        trendOk: true,
        buyAction: 'wait',
        stopLossPrice: 100,
        stopLossParts: { atr14: 1 },
        values: { emIndustry: '化学制药' },
      },
      position: {
        symbol: 'CN:002821',
        positionPct: 7,
        costPrice: 110,
        entryDate: '2026-07-17',
      },
      currentPrice: 95,
      mainlineAllow: mainline,
      todaySh: '2026-07-18',
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('TRIGGER_HIT');
  });

  it('fail-closes EXIT when entryDate is missing', () => {
    const card = deriveActionCard({
      symbol: 'CN:002821',
      gate: attackGate,
      trendok: {
        score: 70,
        trendOk: true,
        buyAction: 'wait',
        stopLossPrice: 100,
        stopLossParts: { atr14: 1 },
        values: { emIndustry: '化学制药' },
      },
      position: {
        symbol: 'CN:002821',
        positionPct: 7,
        costPrice: 110,
      },
      currentPrice: 95,
      mainlineAllow: mainline,
      todaySh: '2026-07-18',
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('ENTRY_DATE_MISSING');
  });

  it('blocks BUY when Entry_Trigger is at or below HardStop', () => {
    const card = deriveActionCard({
      symbol: 'CN:688192',
      gate: attackGate,
      trendok: {
        score: 90,
        trendOk: true,
        buyAction: 'buy',
        buyZoneHigh: 56.89,
        stopLossPrice: 57.6,
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:688192', positionPct: 0 },
      currentPrice: 60,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('ENTRY_BELOW_STOP');
    expect(card.entryTrigger).toBe(56.89);
    expect(card.hardStop).toBe(57.6);
  });

  it('computes held Dist% from Exit_Stop cushion', () => {
    const card = deriveActionCard({
      symbol: 'CN:600000',
      gate: attackGate,
      trendok: {
        score: 70,
        trendOk: true,
        buyAction: 'wait',
        stopLossPrice: 9,
        stopLossParts: { atr14: 0.3 },
        values: { emIndustry: '半导体' },
      },
      position: { symbol: 'CN:600000', positionPct: 5, costPrice: 10, entryDate: '2026-07-01' },
      currentPrice: 10,
      mainlineAllow: mainline,
    });
    expect(card.action).toBe('HOLD');
    expect(card.exitStop).toBe(9);
    expect(card.entryTrigger).toBeNull();
    expect(card.distPct).toBeCloseTo(((10 - 9) / 10) * 100, 6);
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 10.5, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 10.5, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 8, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 5, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 15, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 14.9, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11, entryDate: '2026-07-01' },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 20, maxPrice: 11, entryDate: '2026-07-01' },
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
      { industryName: '半导体', position: { symbol: 'CN:1', positionPct: 15, entryDate: '2026-07-01' } },
      { industryName: '半导体', position: { symbol: 'CN:2', positionPct: 15, entryDate: '2026-07-01' } },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11, entryDate: '2026-07-01' },
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
      { industryName: '半导体', position: { symbol: 'CN:1', costPrice: 10, entryDate: '2026-07-01' } },
      { industryName: '半导体', position: { symbol: 'CN:2', positionPct: 10, entryDate: '2026-07-01' } },
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11, entryDate: '2026-07-01' },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      sectorExposureByIndustry: new Map([['半导体', 40]]),
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('parses positionRangeHint upper bound', () => {
    expect(parsePositionRangeHintMaxPct('50%-60%')).toBe(60);
    expect(parsePositionRangeHintMaxPct('30%')).toBe(30);
    expect(parsePositionRangeHintMaxPct('0%-10%')).toBe(10);
    expect(parsePositionRangeHintMaxPct('80%-100%')).toBe(100);
    expect(parsePositionRangeHintMaxPct('—')).toBeNull();
    expect(parsePositionRangeHintMaxPct('')).toBeNull();
    expect(parsePositionRangeHintMaxPct(null)).toBeNull();
  });

  it('sums finite positive positionPct for sleeve exposure', () => {
    expect(
      buildSleeveExposurePct([
        { symbol: 'CN:1', positionPct: 20 },
        { symbol: 'CN:2', positionPct: 40 },
        { symbol: 'CN:3', costPrice: 10 },
        { symbol: 'CN:4', positionPct: 0 },
      ]),
    ).toBe(60);
  });

  it('blocks BUY when sleeve exposure >= hint max', () => {
    expect(isSleeveCapBlocked(60, '50%-60%')).toBe(true);
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
      sleeveExposurePct: 60,
    });
    expect(card.action).toBe('WATCH');
    expect(card.why).toBe('SLEEVE_CAP_BLOCK');
    expect(card.mainlineOk).toBe(true);
  });

  it('blocks ADD to HOLD on sleeve cap (not TRIM)', () => {
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11, entryDate: '2026-07-01' },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      sleeveExposurePct: 60,
    });
    expect(card.action).toBe('HOLD');
    expect(card.why).toBe('SLEEVE_CAP_BLOCK');
  });

  it('allows BUY when sleeve is under hint max', () => {
    expect(isSleeveCapBlocked(59.9, '50%-60%')).toBe(false);
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
      sleeveExposurePct: 59.9,
    });
    expect(card.action).toBe('BUY');
    expect(card.why).toBe('MAINLINE_5D_TOP3');
  });

  it('does not sleeve-block when hint is dash or sleeve omitted', () => {
    const dashGate: ExecutionGate = { ...attackGate, positionRangeHint: '—' };
    const cardDash = deriveActionCard({
      symbol: 'CN:600000',
      gate: dashGate,
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
      sleeveExposurePct: 90,
    });
    expect(cardDash.action).toBe('BUY');

    const cardOmit = deriveActionCard({
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
    expect(cardOmit.action).toBe('BUY');
  });

  it('EXIT ignores sleeve cap', () => {
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
      position: { symbol: 'CN:600000', costPrice: 10, positionPct: 10, maxPrice: 11, entryDate: '2026-07-01' },
      currentPrice: 10.5,
      mainlineAllow: mainline,
      sleeveExposurePct: 90,
    });
    expect(card.action).toBe('EXIT');
    expect(card.why).toBe('EXIT_NOW');
  });

  it('sector concentration wins over sleeve cap in entry order', () => {
    const entry = evaluateNewEntryGates({
      industryName: '半导体',
      mainlineAllow: mainline,
      sectorExposureByIndustry: new Map([['半导体', 35]]),
      sleeveExposurePct: 90,
      positionRangeHint: '50%-60%',
    });
    expect(entry).toEqual({ ok: false, tag: null, why: 'SECTOR_CONC_BLOCK' });
  });

  it('counts held names missing positionPct', () => {
    expect(
      countHeldMissingPositionPct([
        { symbol: 'CN:1', costPrice: 10 },
        { symbol: 'CN:2', costPrice: 10, positionPct: 15 },
        { symbol: 'CN:3' },
        { symbol: 'CN:4', positionPct: 0, costPrice: 8 },
      ]),
    ).toBe(2);
    expect(isHeldMissingPositionPct({ symbol: 'CN:1', costPrice: 10 })).toBe(true);
    expect(isHeldMissingPositionPct({ symbol: 'CN:2', costPrice: 10, positionPct: 15 })).toBe(false);
    expect(isHeldMissingPositionPct({ symbol: 'CN:3' })).toBe(false);
  });

  it('formats sleeve budget label with one decimal', () => {
    expect(formatSleeveBudgetLabel(45, '50%-60%')).toBe('Sleeve 45.0% / 60%');
    expect(formatSleeveBudgetLabel(0, '—')).toBe('Sleeve 0.0% / —');
    expect(formatSleeveBudgetLabel(59.9, '30%')).toBe('Sleeve 59.9% / 30%');
  });

  it('suggests fire size with 5% clip by default', () => {
    expect(
      suggestFireSizePct({
        positionPct: null,
        industryName: '半导体',
        sectorExposureByIndustry: new Map([['半导体', 10]]),
        sleeveExposurePct: 40,
        positionRangeHint: '50%-60%',
      }),
    ).toEqual({ addPct: 5, note: 'clip' });
  });

  it('binds fire size to sleeve room', () => {
    expect(
      suggestFireSizePct({
        positionPct: null,
        industryName: '半导体',
        sectorExposureByIndustry: new Map([['半导体', 10]]),
        sleeveExposurePct: 58,
        positionRangeHint: '50%-60%',
      }),
    ).toEqual({ addPct: 2, note: 'sleeve' });
  });

  it('binds fire size to single-name room on ADD', () => {
    expect(
      suggestFireSizePct({
        positionPct: 12,
        industryName: '半导体',
        sectorExposureByIndustry: new Map([['半导体', 12]]),
        sleeveExposurePct: 20,
        positionRangeHint: '50%-60%',
      }),
    ).toEqual({ addPct: 3, note: 'single' });
  });

  it('attaches suggestAddPct on BUY action card', () => {
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
      sleeveExposurePct: 40,
      sectorExposureByIndustry: new Map([['半导体', 5]]),
    });
    expect(card.action).toBe('BUY');
    expect(card.suggestAddPct).toBe(5);
    expect(card.suggestSizeNote).toBe('clip');
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
