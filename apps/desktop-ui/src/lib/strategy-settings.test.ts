import { afterEach, describe, expect, it, vi } from 'vitest';

const apiPutJson = vi.hoisted(() => vi.fn(async () => ({ ok: true })));
vi.mock('@/lib/api/client', () => ({ apiPutJson }));

import {
  DEFAULT_STRATEGY_MODE,
  STRATEGY_MODE_LABELS,
  getStrategyMode,
  pushStrategyModeToServer,
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
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('legacy_satellite'));
    expect(getStrategyMode()).toBe('starport');
  });

  it('keeps the twin_star parallel mode (restored 2026-09-17)', () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('twin_star'));
    expect(getStrategyMode()).toBe('twin_star');
  });

  it('ignores corrupt storage and falls back to starport', () => {
    window.localStorage.setItem('karios.strategyMode.v2', 'not-json');
    expect(getStrategyMode()).toBe('starport');
  });

  it('keeps explicit selections for every registered mode', () => {
    for (const mode of ['harbor', 'homeport', 'starport', 'starship', 'twin_star'] as const) {
      setStrategyMode(mode);
      expect(getStrategyMode()).toBe(mode);
    }
  });

  it('labels the three tiers plus the sunset baseline', () => {
    expect(STRATEGY_MODE_LABELS).toEqual({
      harbor: '港湾',
      homeport: '母港',
      starport: '星港',
      starship: '星舰',
      twin_star: '双子星',
    });
  });

  it('mirrors the selected mode to the backend (OPT-223)', async () => {
    setStrategyMode('starship');
    await vi.waitFor(() =>
      expect(apiPutJson).toHaveBeenCalledWith('/api/settings/strategy-mode', {
        mode: 'starship',
      }),
    );
  });

  it('pushStrategyModeToServer defaults to the stored mode', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('harbor'));
    apiPutJson.mockClear();
    pushStrategyModeToServer();
    await vi.waitFor(() =>
      expect(apiPutJson).toHaveBeenCalledWith('/api/settings/strategy-mode', { mode: 'harbor' }),
    );
  });
});
