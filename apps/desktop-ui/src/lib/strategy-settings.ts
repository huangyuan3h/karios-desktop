'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode = 'harbor' | 'homeport' | 'starport' | 'starship' | 'twin_star';

/** v2 key: the legacy key only ever auto-held 'harbor' (pre-2026-09-14). */
const STORAGE_KEY = 'karios.strategyMode.v2';
const LEGACY_STORAGE_KEY = 'karios.strategyMode';

export const STRATEGY_MODES: readonly StrategyMode[] = [
  'starship',
  'starport',
  'homeport',
  'harbor',
  'twin_star',
];

/**
 * Data-collection default (user decision 2026-09-16): 星港 w=0.2
 * (current-data K1-passing max; 1/3 frozen as audit reference).
 * Display only — Live orders stay 港湾 (S-3 + parking).
 */
export const DEFAULT_STRATEGY_MODE: StrategyMode = 'starport';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  harbor: '港湾',
  homeport: '母港',
  starport: '星港',
  starship: '星舰',
  twin_star: '双子星',
};

export const STRATEGY_MODE_TAGS: Record<StrategyMode, string> = {
  harbor: 'Live · 日落',
  homeport: '防守 · 产品候选',
  starport: '均衡 · 默认',
  starship: '进攻 · 前置未满',
  twin_star: '并行对照',
};

export const STRATEGY_MODE_DESCRIPTIONS: Record<StrategyMode, string> = {
  harbor: 'S-3 择强核心 + 闲置现金 ETF 停车场。实盘基线（日落模式：维持运行，不再开发）。',
  homeport: '港湾 70% × B3 风险预算 30%（M30 防守档，月初再平衡 5bp/边）。回撤浅、长期年化 21%。',
  starport: '母港 × 卫星 0.2 曝露（当前数据验收最大权重）。数据收集默认档。',
  starship:
    'v2 = 卫星 + 闲置现金停 ETF 停车场（H2 迟滞换仓；long +975.0% / MDD −30.5 / SR 2.15；15bps 仍 +921.4）。卫星执行审计 ✅（90bps +310%、容量 ≤5M）；进 Live 前置 = paper 3/20 + 用户风险授权。',
  twin_star: '港湾核心 × 卫星 50/50（无仓日 100% 港湾）。并行对照档，保留不退役。',
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
  pushStrategyModeToServer(mode);
  window.dispatchEvent(new Event('karios:strategy-mode'));
}

/**
 * OPT-223: mirror the selected mode to the backend so Bark/notification copy
 * is written for the strategy the user is watching. Fire-and-forget — the UI
 * stays usable when the backend is down (it will re-sync on the next change
 * or mount).
 */
export function pushStrategyModeToServer(mode: StrategyMode = getStrategyMode()): void {
  void import('@/lib/api/client')
    .then(({ apiPutJson }) => apiPutJson('/api/settings/strategy-mode', { mode }))
    .catch(() => {
      // best-effort mirror; the backend falls back to the UI default
    });
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
