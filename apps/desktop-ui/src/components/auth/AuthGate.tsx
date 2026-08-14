'use client';

import * as React from 'react';

import { clearGatewayKey, getGatewayKey, setGatewayKey, UNAUTHORIZED_EVENT } from '@/lib/auth';

/**
 * Auth gate (Family Hub Phase 0 · 2026-08-14).
 *
 * Shows a full-screen password page until a gateway key is present; on any
 * API 401 (UNAUTHORIZED_EVENT) it clears the key and returns to the login
 * page. After a successful submit it reloads so every query re-runs with
 * the key attached.
 */
export function AuthGate({ children }: { children: React.ReactNode }) {
  // localStorage only exists on the client — resolve the initial state in an
  // effect so SSR HTML and the first client render are identical (empty),
  // avoiding the React hydration mismatch.
  const [ready, setReady] = React.useState(false);
  const [unlocked, setUnlocked] = React.useState(false);
  const [password, setPassword] = React.useState('');
  const [error, setError] = React.useState<string | null>(null);
  const [submitting, setSubmitting] = React.useState(false);

  React.useEffect(() => {
    setUnlocked(getGatewayKey() !== null);
    setReady(true);
  }, []);

  React.useEffect(() => {
    const onUnauthorized = () => {
      clearGatewayKey();
      setUnlocked(false);
      setError('密码错误或已过期，请重新输入');
    };
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, []);

  if (!ready) {
    return null;
  }

  if (unlocked) {
    return <>{children}</>;
  }

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!password.trim()) return;
    setSubmitting(true);
    setError(null);
    setGatewayKey(password.trim());
    // Reload so every cached query re-runs with the key attached.
    window.location.reload();
  };

  return (
    <div className="flex h-dvh w-full items-center justify-center bg-[var(--k-bg)] px-6 text-[var(--k-text)]">
      <form onSubmit={onSubmit} className="w-full max-w-sm space-y-4">
        <div className="text-center">
          <div className="text-2xl font-bold tracking-tight">Karios</div>
          <div className="mt-1 text-[12px] text-[var(--k-muted)]">家庭投资平台 · 数据说话</div>
        </div>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="访问密码"
          autoComplete="current-password"
          autoFocus
          className="w-full rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3 text-[14px] outline-none focus:border-emerald-500"
        />
        {error ? <div className="text-[12px] text-red-500">{error}</div> : null}
        <button
          type="submit"
          disabled={submitting || !password.trim()}
          className="w-full rounded-lg bg-emerald-600 py-3 text-[14px] font-semibold text-white disabled:opacity-40"
        >
          进入
        </button>
      </form>
    </div>
  );
}
