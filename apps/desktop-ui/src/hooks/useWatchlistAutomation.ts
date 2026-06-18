'use client';

import * as React from 'react';

import {
  applyAutomationRun,
  fetchAutomationPending,
  isAutomationPollWindow,
} from '@/lib/watchlist-automation';
import { syncRegistryToBackend, WATCHLIST_UPDATED_EVENT } from '@/lib/watchlist-storage';

const ACK_STORAGE_KEY = 'karios.watchlist.automation.ackedRunId';

function getAckedRunId(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    return window.localStorage.getItem(ACK_STORAGE_KEY);
  } catch {
    return null;
  }
}

function setAckedRunId(runId: string): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(ACK_STORAGE_KEY, runId);
  } catch {
    // ignore
  }
}

export function useWatchlistAutomation(): void {
  const applyingRef = React.useRef(false);

  React.useEffect(() => {
    void syncRegistryToBackend().catch(() => {
      // best-effort on mount
    });

    function onWatchlistUpdated() {
      void syncRegistryToBackend().catch(() => {
        // ignore
      });
    }

    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
  }, []);

  React.useEffect(() => {
    let cancelled = false;

    async function tick() {
      if (cancelled || applyingRef.current) return;
      if (!isAutomationPollWindow()) return;
      try {
        const pending = await fetchAutomationPending();
        if (!pending || pending.skipped) return;
        if (getAckedRunId() === pending.runId) return;
        applyingRef.current = true;
        await applyAutomationRun(pending, { silent: true });
        setAckedRunId(pending.runId);
      } catch {
        // silent scheduled apply
      } finally {
        applyingRef.current = false;
      }
    }

    void tick();
    const id = window.setInterval(() => void tick(), 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);
}
