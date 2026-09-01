'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'twin_star' | 'single_track';

const STORAGE_KEY = 'karios.strategyMode';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  twin_star: '机会双子星',
  single_track: '单轨择强',
};

export function getStrategyMode(): StrategyMode {
  const v = loadJson<StrategyMode | null>(STORAGE_KEY, null);
  // 2026-09-01 v3: executable twin-star wins walk-forward vs core, but live
  // default stays single_track (opt-in) — Sharpe/DD edge is small, not PS-G50.
  return v === 'twin_star' ? v : 'single_track';
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