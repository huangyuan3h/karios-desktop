'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'twin_star' | 'single_track';

const STORAGE_KEY = 'karios.strategyMode';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  twin_star: '双子星 (Twin-Star)',
  single_track: '单轨择强',
};

export function getStrategyMode(): StrategyMode {
  const v = loadJson<StrategyMode | null>(STORAGE_KEY, null);
  return v === 'single_track' ? v : 'twin_star';
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