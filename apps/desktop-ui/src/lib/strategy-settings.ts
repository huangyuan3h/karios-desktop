'use client';

import * as React from 'react';

import { loadJson, saveJson } from '@/lib/storage';

export type StrategyMode =
  | 'harbor'
  | 'homeport'
  | 'starport'
  | 'starship'
  | 'starship_robust'
  | 'starship_b'
  | 'twin_star';

/** v2 key: the legacy key only ever auto-held 'harbor' (pre-2026-09-14). */
const STORAGE_KEY = 'karios.strategyMode.v2';
const LEGACY_STORAGE_KEY = 'karios.strategyMode';

export const STRATEGY_MODES: readonly StrategyMode[] = [
  'starship_b',
  'starship_robust',
  'starship',
  'starport',
  'homeport',
  'harbor',
  'twin_star',
];

/**
 * Selected strategy (user decision 2026-09-24): 星舰 B (3-leg parking) is the
 * default — shallower drawdown, stronger 2022-23 stress, and a 3-ETF parking
 * leg that is easier to replicate by hand than H2-a25's H2+B3.
 * Display/push only — Live orders stay 港湾 (S-3 + parking); 星舰 B needs its
 * paper 3/20 + risk authorization before it can trade Live.
 */
export const DEFAULT_STRATEGY_MODE: StrategyMode = 'starship_b';

export const STRATEGY_MODE_LABELS: Record<StrategyMode, string> = {
  harbor: '港湾',
  homeport: '母港',
  starport: '星港',
  starship: '星舰',
  starship_robust: '稳健星舰 H2-a25',
  starship_b: '星舰 B',
  twin_star: '双子星',
};

export const STRATEGY_MODE_TAGS: Record<StrategyMode, string> = {
  harbor: 'Live · 日落',
  homeport: '防守 · 产品候选',
  starport: '均衡 · 数据档',
  starship: '进攻 · 前置未满',
  starship_robust: '现行 canonical · K3 风险',
  starship_b: '稳健备选 · 3 腿停放',
  twin_star: '并行 · 正式',
};

export const STRATEGY_MODE_DESCRIPTIONS: Record<StrategyMode, string> = {
  harbor: 'S-3 择强核心 + 闲置现金 ETF 停车场。实盘基线（日落模式：维持运行，不再开发）。',
  homeport: '港湾 70% × B3 风险预算 30%（M30 防守档，月初再平衡 5bp/边）。回撤浅、长期年化 21%。',
  starport: '母港 × 卫星 0.2 曝露（当前数据验收最大权重）。数据收集默认档。',
  starship:
    '激进对照 = 卫星 + 闲置现金 100% 停 H2 趋势 ETF（2pt 迟滞换仓；long +962.7% / MDD −30.5 / SR 2.14）。卫星执行审计 ✅（容量 ≤5M）；进 Live 前置 = paper 3/20 + 用户风险授权。',
  starship_robust:
    '稳健星舰 H2-a25 = 卫星 + 闲置现金 25% 停 H2 ETF / 75% 停 B3 风险预算（5 资产 inverse-vol 月调）。现行研究/展示 canonical；long +738.5% / MDD −8.4 / SR 3.50，K1/K2/K4/K5 PASS、K3 风险未过。前置 = paper 3/20 + 风险授权。',
  starship_b:
    '星舰 B = 卫星 + 闲置现金 100% 停 {国债+黄金+纳指} 逆波动率（3 腿月频，因果 T−1）。研究/展示/人工操作档；long +669.6% / MDD −5.5 / SR 3.90，2022–23 熊市显著强于 H2-a25（stress +124% / −7.4%）。停放腿 3 只 ETF，好复制；进 Live 前置 = paper 3/20 + 风险授权。',
  twin_star: '港湾核心 × 卫星 50/50（无仓日 100% 港湾）。五档正式并行档（展示口径，非 Live）；新成本 OOS2 +144.6/train +52.2/valid +24.5/long +299.8。',
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
