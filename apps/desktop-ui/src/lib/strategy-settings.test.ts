import { afterEach, describe, expect, it } from 'vitest';

import {
  DEFAULT_STRATEGY_MODE,
  getStrategyMode,
  setStrategyMode,
} from './strategy-settings';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode');
});

describe('getStrategyMode', () => {
  it('defaults to twin_star when unset', () => {
    expect(DEFAULT_STRATEGY_MODE).toBe('twin_star');
    expect(getStrategyMode()).toBe('twin_star');
  });

  it('keeps an explicit single_track opt-out', () => {
    setStrategyMode('single_track');
    expect(getStrategyMode()).toBe('single_track');
  });

  it('ignores corrupt storage and falls back to twin_star', () => {
    window.localStorage.setItem('karios.strategyMode', 'not-json');
    expect(getStrategyMode()).toBe('twin_star');
  });
});
