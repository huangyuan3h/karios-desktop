import { afterEach, describe, expect, it } from 'vitest';

import { DEFAULT_STRATEGY_MODE, getStrategyMode, setStrategyMode } from './strategy-settings';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode');
});

describe('getStrategyMode', () => {
  it('defaults to harbor when unset', () => {
    expect(DEFAULT_STRATEGY_MODE).toBe('harbor');
    expect(getStrategyMode()).toBe('harbor');
  });

  it('migrates a stored twin_star value to harbor', () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('twin_star'));
    expect(getStrategyMode()).toBe('harbor');
    expect(window.localStorage.getItem('karios.strategyMode')).toBe('"harbor"');
  });

  it('migrates a stored single_track value to harbor', () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('single_track'));
    expect(getStrategyMode()).toBe('harbor');
  });

  it('ignores corrupt storage and falls back to harbor', () => {
    window.localStorage.setItem('karios.strategyMode', 'not-json');
    expect(getStrategyMode()).toBe('harbor');
  });

  it('keeps an explicit harbor value', () => {
    setStrategyMode('harbor');
    expect(getStrategyMode()).toBe('harbor');
  });
});
