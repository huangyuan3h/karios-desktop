'use client';

import * as React from 'react';
import { Bell, BellRing } from 'lucide-react';

import { cn } from '@/lib/utils';
import {
  openNotificationAnchor,
  useNotificationsQuery,
  type NotificationItem,
} from '@/lib/queries/notifications';
import { loadBuyReminders, BUY_REMINDERS_UPDATED_EVENT } from '@/lib/buy-reminders';

const SEEN_KEY = 'karios_notifications_seen';
const TOAST_MS = 6000;

function loadSeen(): Set<string> {
  if (typeof window === 'undefined') return new Set();
  try {
    const raw = window.localStorage.getItem(SEEN_KEY);
    return new Set(Array.isArray(JSON.parse(raw ?? '[]')) ? JSON.parse(raw ?? '[]') : []);
  } catch {
    return new Set();
  }
}

function saveSeen(ids: Set<string>) {
  try {
    window.localStorage.setItem(SEEN_KEY, JSON.stringify([...ids]));
  } catch {
    /* best effort */
  }
}

const SEVERITY_CLS: Record<string, string> = {
  high: 'border-red-500/40 bg-red-500/10 text-red-700 dark:text-red-300',
  medium: 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300',
  low: 'border-sky-500/40 bg-sky-500/10 text-sky-700 dark:text-sky-300',
};

const ANCHOR_LABEL: Record<string, string> = {
  holdings: '持仓/条件单',
  recon: '回测缺票',
  scheduler: '调度状态',
  backtest: '回测结论',
  reminders: '买入提醒',
};

function anchorLabel(anchor: string): string {
  return ANCHOR_LABEL[anchor] ?? anchor;
}

function NotificationRow({
  n,
  onClick,
}: {
  n: NotificationItem;
  onClick: (n: NotificationItem) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onClick(n)}
      className="flex w-full flex-col gap-0.5 rounded-md border border-transparent px-2 py-1.5 text-left hover:border-[var(--k-border)] hover:bg-[var(--k-surface-2)]"
    >
      <div className="flex items-center gap-1.5 text-[11px] font-medium">
        <span className={cn('rounded px-1 py-px text-[9px] font-semibold', SEVERITY_CLS[n.severity])}>
          {n.severity === 'high' ? '重要' : n.severity === 'medium' ? '提示' : '低'}
        </span>
        {n.title}
      </div>
      <div className="text-[10px] text-[var(--k-muted)]">{n.detail}</div>
      <div className="text-[9px] text-[var(--k-muted)]">→ watchlist · {anchorLabel(n.anchor)}</div>
    </button>
  );
}

/**
 * Global notification hub (2026-08-12): bell with unread badge in the app
 * header + toast popup on new items + panel listing all actionable
 * notifications. Clicking any row jumps to the watchlist page and scrolls
 * to the anchored block (holdings / recon / …). Local buy reminders are
 * merged in so "提醒我操作" is one place.
 */
export function NotificationHub() {
  const [open, setOpen] = React.useState(false);
  const [seen, setSeen] = React.useState<Set<string>>(loadSeen);
  const [toast, setToast] = React.useState<NotificationItem | null>(null);
  const [localReminders, setLocalReminders] = React.useState(loadBuyReminders());
  const toastTimer = React.useRef<number | null>(null);

  const q = useNotificationsQuery();
  const items = React.useMemo(() => q.data?.items ?? [], [q.data]);
  const prevIdsRef = React.useRef<Set<string> | null>(null);

  React.useEffect(() => {
    function onRemindersUpdate() {
      setLocalReminders(loadBuyReminders());
    }
    window.addEventListener(BUY_REMINDERS_UPDATED_EVENT, onRemindersUpdate);
    return () => window.removeEventListener(BUY_REMINDERS_UPDATED_EVENT, onRemindersUpdate);
  }, []);

  // Newly-appeared items → badge + one toast (only high/medium). An item
  // counts as new when its id was absent from the previous poll, so clearing
  // one toast never re-fires the rest.
  React.useEffect(() => {
    const prev = prevIdsRef.current;
    prevIdsRef.current = new Set(items.map((n) => n.id));
    if (prev === null) {
      setSeen((cur) => {
        if (cur.size === 0) return cur;
        const next = new Set(cur);
        let changed = false;
        for (const n of items) {
          if (next.has(n.id)) continue;
          next.add(n.id);
          changed = true;
        }
        if (changed) saveSeen(next);
        return changed ? next : cur;
      });
      return;
    }
    const fresh = items.filter((n) => !prev.has(n.id) && !seen.has(n.id));
    if (fresh.length === 0) return;
    const first = fresh[0];
    if (first.severity !== 'low') {
      setToast(first);
      if (toastTimer.current) window.clearTimeout(toastTimer.current);
      toastTimer.current = window.setTimeout(() => setToast(null), TOAST_MS);
    }
  }, [items, seen]);

  function handleClick(n: NotificationItem) {
    setSeen((prev) => {
      const next = new Set(prev).add(n.id);
      saveSeen(next);
      return next;
    });
    setOpen(false);
    setToast(null);
    if (toastTimer.current) {
      window.clearTimeout(toastTimer.current);
      toastTimer.current = null;
    }
    openNotificationAnchor(n.anchor);
  }

  function markAllSeen() {
    setSeen((prev) => {
      const next = new Set(prev);
      for (const n of items) next.add(n.id);
      saveSeen(next);
      return next;
    });
  }

  const localItems: NotificationItem[] = localReminders.map((r) => ({
    id: `reminder:${r.symbol}`,
    type: 'buy_reminder',
    severity: 'low' as const,
    title: `买入提醒 · ${r.name ?? r.symbol}`,
    detail:
      `${r.symbol}${r.targetPrice != null ? ` · 目标价 ${r.targetPrice}` : ''}${r.note ? ` · ${r.note}` : ''}`,
    anchor: 'reminders',
    createdAt: r.createdAt,
  }));

  const allItems = [...items, ...localItems];
  const unread = allItems.filter((n) => !seen.has(n.id)).length;

  React.useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false);
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open]);

  return (
    <>
      {/* Toast */}
      {toast && (
        <button
          type="button"
          onClick={() => handleClick(toast)}
          className="fixed right-4 top-14 z-[9998] w-80 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-3 text-left shadow-lg"
        >
          <div className="flex items-center gap-1.5 text-[11px] font-semibold">
            <span className={cn('rounded px-1 py-px text-[9px] font-semibold', SEVERITY_CLS[toast.severity])}>
              {toast.severity === 'high' ? '重要' : '提示'}
            </span>
            {toast.title}
          </div>
          <div className="mt-1 text-[10px] text-[var(--k-muted)]">{toast.detail}</div>
          <div className="mt-1 text-[9px] text-[var(--k-accent)]">
            点击查看 → watchlist · {anchorLabel(toast.anchor)}
          </div>
        </button>
      )}

      {/* Bell */}
      <div className="relative">
        <button
          type="button"
          onClick={() => {
            setOpen((v) => !v);
            if (!open) markAllSeen();
          }}
          className="grid h-9 w-9 place-items-center rounded-full text-[var(--k-muted)] hover:bg-[var(--k-surface-2)] hover:text-[var(--k-fg)]"
          title="提醒（点击查看，再点全部已读）"
        >
          {unread > 0 ? <BellRing className="h-4 w-4" /> : <Bell className="h-4 w-4" />}
          {unread > 0 && (
            <span className="absolute -right-0.5 -top-0.5 grid h-4 min-w-4 place-items-center rounded-full bg-red-500 px-1 text-[9px] font-bold text-white">
              {unread > 99 ? '99+' : unread}
            </span>
          )}
        </button>

        {open && (
          <div className="absolute right-0 top-10 z-[9997] w-80 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-2 shadow-lg">
            <div className="mb-1 flex items-center justify-between px-1 text-[10px] font-medium text-[var(--k-muted)]">
              <span>提醒（{allItems.length}）· 点击跳 watchlist</span>
              {unread > 0 && (
                <button type="button" onClick={markAllSeen} className="hover:text-[var(--k-fg)]">
                  全部已读
                </button>
              )}
            </div>
            <div className="flex max-h-80 flex-col gap-0.5 overflow-y-auto">
              {allItems.length === 0 ? (
                <div className="px-2 py-4 text-center text-[11px] text-[var(--k-muted)]">
                  暂无提醒（买入提醒/接近止损/回测缺票/cron 失败会出现在这里）
                </div>
              ) : (
                allItems.map((n) => <NotificationRow key={n.id} n={n} onClick={handleClick} />)
              )}
            </div>
          </div>
        )}
      </div>
    </>
  );
}
