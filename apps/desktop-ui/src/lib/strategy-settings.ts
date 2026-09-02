'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'twin_star' | 'single_track';

const STORAGE_KEY = 'karios.strategyMode';

/** Live product default: opportunity twin-star v3.1 clip4 (4 × 12.5% NAV). */
export const DEFAULT_STRATEGY_MODE: StrategyMode = 'twin_star';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  twin_star: '机会双子星',
  single_track: '单轨择强',
};

export function getStrategyMode(): StrategyMode {
  const v = loadJson<StrategyMode | null>(STORAGE_KEY, null);
  if (v === 'single_track' || v === 'twin_star') return v;
  return DEFAULT_STRATEGY_MODE;
}

export function setStrategyMode(mode: StrategyMode): void {
  saveJson(STORAGE_KEY, mode);
  window.dispatchEvent(new Event('karios:strategy-mode'));
}

export function useStrategyMode(): [StrategyMode, (mode: StrategyMode) => void] {
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
  const set = React.useCallback((next: StrategyMode) => setStrategyMode(next), []);
  return [mode, set];
}
