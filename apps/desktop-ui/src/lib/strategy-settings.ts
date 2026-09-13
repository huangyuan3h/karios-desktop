'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'harbor';

const STORAGE_KEY = 'karios.strategyMode';

/** Live product default: 港湾 (S-3 stock core + idle-cash ETF parking). */
export const DEFAULT_STRATEGY_MODE: StrategyMode = 'harbor';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  harbor: '港湾',
};

/** Stored values from retired modes collapse to the single live baseline. */
export function getStrategyMode(): StrategyMode {
  const v = loadJson<unknown>(STORAGE_KEY, null);
  if (v !== 'harbor') saveJson(STORAGE_KEY, 'harbor');
  return DEFAULT_STRATEGY_MODE;
}

export function setStrategyMode(mode: StrategyMode): void {
  saveJson(STORAGE_KEY, mode);
  window.dispatchEvent(new Event('karios:strategy-mode'));
}

export function useStrategyMode(): StrategyMode {
  const [mode, setMode] = React.useState<StrategyMode>(() => getStrategyMode());
  React.useEffect(() => {
    const sync = () => setMode(getStrategyMode());
    window.addEventListener('karios:strategy-mode', sync);
    window.addEventListener('storage', sync);
    return () => {
      window.removeEventListener('karios:strategy-mode', sync);
      window.removeEventListener('storage', sync);
    };
  }, []);
  return mode;
}
