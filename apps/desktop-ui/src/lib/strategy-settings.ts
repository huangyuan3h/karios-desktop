'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'harbor' | 'homeport' | 'starport' | 'starship';

/** v2 key: the legacy key only ever auto-held 'harbor' (pre-2026-09-14). */
const STORAGE_KEY = 'karios.strategyMode.v2';
const LEGACY_STORAGE_KEY = 'karios.strategyMode';

export const STRATEGY_MODES: readonly StrategyMode[] = [
  'harbor',
  'homeport',
  'starport',
  'starship',
];

/**
 * Data-collection default (user decision 2026-09-14): 星港 w=1/3.
 * Display only — Live orders stay 港湾 (S-3 + parking).
 */
export const DEFAULT_STRATEGY_MODE: StrategyMode = 'starport';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  harbor: '港湾',
  homeport: '母港',
  starport: '星港',
  starship: '星舰',
};

export const STRATEGY_MODE_TAGS: Record<StrategyMode, string> = {
  harbor: 'Live',
  homeport: '产品候选',
  starport: '默认',
  starship: '激进 · 未审计',
};

export const STRATEGY_MODE_DESCRIPTIONS: Record<StrategyMode, string> = {
  harbor: 'S-3 择强核心 + 闲置现金 ETF 停车场。实盘基线。',
  homeport: '港湾 × B3 风险预算 50/50（月初再平衡 5bp/边）。回撤减半、收益近半。',
  starport: '母港 × 卫星 1/3 曝露（H-B3-SAT PASS，K1 余量薄）。数据收集默认档。',
  starship: '卫星 100% standalone（long +463.6% / SR 3.50，未过执行审计）。激进档。',
};

function isStrategyMode(v: unknown): v is StrategyMode {
  return typeof v === 'string' && (STRATEGY_MODES as readonly string[]).includes(v);
}

/** Stored values from retired modes collapse to the current default. */
export function getStrategyMode(): StrategyMode {
  const v = loadJson<unknown>(STORAGE_KEY, null);
  if (isStrategyMode(v)) return v;
  try {
    window.localStorage.removeItem(LEGACY_STORAGE_KEY);
  } catch {
    // storage unavailable — fall through to the default
  }
  saveJson(STORAGE_KEY, DEFAULT_STRATEGY_MODE);
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
