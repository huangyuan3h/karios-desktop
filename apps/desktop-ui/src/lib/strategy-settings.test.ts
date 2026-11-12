import * as fs from 'node:fs';
import * as path from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';

const apiGetJson = vi.hoisted(() => vi.fn(async () => ({ ok: true, mode: 'homeport' })));
const apiPutJson = vi.hoisted(() => vi.fn(async () => ({ ok: true })));
vi.mock('@/lib/api/client', () => ({ apiGetJson, apiPutJson }));

import {
  DEFAULT_STRATEGY_MODE,
  STRATEGY_MODE_LABELS,
  getStrategyMode,
  pullStrategyModeFromServer,
  pushStrategyModeToServer,
  setStrategyMode,
} from './strategy-settings';

afterEach(() => {
  window.localStorage.removeItem('karios.strategyMode.v2');
  window.localStorage.removeItem('karios.strategyMode');
  apiGetJson.mockReset();
  apiGetJson.mockImplementation(async () => ({ ok: true, mode: 'homeport' }));
  apiPutJson.mockClear();
});

describe('getStrategyMode', () => {
  it('defaults to homeport (母港M30 baseline) when unset', () => {
    expect(DEFAULT_STRATEGY_MODE).toBe('homeport');
    expect(getStrategyMode()).toBe('homeport');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"homeport"');
  });

  it('migrates the legacy key to the new default and removes it', () => {
    window.localStorage.setItem('karios.strategyMode', JSON.stringify('harbor'));
    expect(getStrategyMode()).toBe('homeport');
    expect(window.localStorage.getItem('karios.strategyMode')).toBeNull();
  });

  it('migrates a retired mode to the current default', () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('legacy_satellite'));
    expect(getStrategyMode()).toBe('homeport');
  });

  it('keeps the twin_star parallel mode (restored 2026-09-17)', () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('twin_star'));
    expect(getStrategyMode()).toBe('twin_star');
  });

  it('ignores corrupt storage and falls back to the default', () => {
    window.localStorage.setItem('karios.strategyMode.v2', 'not-json');
    expect(getStrategyMode()).toBe('homeport');
  });

  it('keeps explicit selections for every registered mode', () => {
    for (const mode of [
      'harbor',
      'homeport',
      'starport',
      'starship',
      'starship_robust',
      'starship_b',
      'twin_star',
    ] as const) {
      setStrategyMode(mode);
      expect(getStrategyMode()).toBe(mode);
    }
  });

  it('labels the tiers plus the sunset baseline', () => {
    expect(STRATEGY_MODE_LABELS).toEqual({
      harbor: '港湾',
      homeport: '母港',
      starport: '星港',
      starship: '星舰',
      starship_robust: '稳健星舰 H2-a25',
      starship_b: '星舰 B',
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

describe('pullStrategyModeFromServer (backend = source of truth, 2026-11-12)', () => {
  it('adopts a different server mode into localStorage and never PUTs', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('starship_b'));
    apiGetJson.mockResolvedValueOnce({ ok: true, mode: 'homeport' });
    const changed = vi.fn();
    window.addEventListener('karios:strategy-mode', changed);

    await pullStrategyModeFromServer();

    window.removeEventListener('karios:strategy-mode', changed);
    expect(apiGetJson).toHaveBeenCalledWith('/api/settings/strategy-mode');
    expect(getStrategyMode()).toBe('homeport');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"homeport"');
    expect(changed).toHaveBeenCalledTimes(1);
    expect(apiPutJson).not.toHaveBeenCalled();
  });

  it('leaves localStorage untouched and does not PUT when GET fails', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('starship_b'));
    apiGetJson.mockRejectedValueOnce(new Error('backend down'));

    await pullStrategyModeFromServer();

    expect(getStrategyMode()).toBe('starship_b');
    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"starship_b"');
    expect(apiPutJson).not.toHaveBeenCalled();
  });

  it('ignores an invalid server mode without writing or PUTting', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('starport'));
    apiGetJson.mockResolvedValueOnce({ ok: true, mode: 'not_a_mode' });

    await pullStrategyModeFromServer();

    expect(getStrategyMode()).toBe('starport');
    expect(apiPutJson).not.toHaveBeenCalled();
  });

  it('is a no-op (no write, no PUT) when the server already matches', async () => {
    window.localStorage.setItem('karios.strategyMode.v2', JSON.stringify('homeport'));
    apiGetJson.mockResolvedValueOnce({ ok: true, mode: 'homeport' });

    await pullStrategyModeFromServer();

    expect(window.localStorage.getItem('karios.strategyMode.v2')).toBe('"homeport"');
    expect(apiPutJson).not.toHaveBeenCalled();
  });
});

describe('UI mount behaviour', () => {
  it('WatchlistPage no longer pushes the localStorage mode on mount', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '..', 'components', 'pages', 'WatchlistPage.tsx'),
      'utf-8',
    );
    expect(source).not.toContain('pushStrategyModeToServer');
  });
});
