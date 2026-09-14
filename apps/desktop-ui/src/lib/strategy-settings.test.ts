import { afterEach, describe, expect, it } from 'vitest';

import {
  DEFAULT_STRATEGY_MODE,
  STRATEGY_MODE_LABELS,
  getStrategyMode,
  setStrategyMode,
} from './strategy-settings';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode.v2');
  window.localStorage.removeItem('karios.strategyMode');
});

describe('getStrategyMode', () => {
  it('defaults to starport when unset', () => {
    expect(DEFAULT_STRATEGY_MODE).toBe('starport');
    expect(getStrategyMode()).toBe('starport');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"starport"');
  });

  it('migrates the legacy key to the new default and removes it', () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('harbor'));
    expect(getStrategyMode()).toBe('starport');
    expect(window.localStorage.getItem('karios.strategyMode')).toBeNull();
  });

  it('migrates a retired mode to starport', () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('twin_star'));
    expect(getStrategyMode()).toBe('starport');
  });

  it('ignores corrupt storage and falls back to starport', () => {
    window.localStorage.setItem('karios.strategyMode.v2', 'not-json');
    expect(getStrategyMode()).toBe('starport');
  });

  it('keeps explicit selections for every registered mode', () => {
    for (const mode of ['harbor', 'homeport', 'starport', 'starship'] as const) {
      setStrategyMode(mode);
      expect(getStrategyMode()).toBe(mode);
    }
  });

  it('labels all four family strategies', () => {
    expect(STRATEGY_MODE_LABELS).toEqual({
      harbor: '港湾',
      homeport: '母港',
      starport: '星港',
      starship: '星舰',
    });
  });
});
