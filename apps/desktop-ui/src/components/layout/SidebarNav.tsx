'use client';

import {
  BarChart3,
  BookOpen,
  LayoutDashboard,
  PieChart,
  Settings,
} from 'lucide-react';

import { cn } from '@/lib/utils';

const items = [
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'resources', label: 'Resources', icon: BookOpen },
  { id: 'portfolio', label: 'Portfolio', icon: PieChart },
  { id: 'analysis', label: 'Analysis', icon: BarChart3 },
  { id: 'settings', label: 'Settings', icon: Settings },
];

export function SidebarNav({
  activeId,
  onSelect,
}: {
  activeId: string;
  onSelect: (id: string) => void;
}) {
  return (
    <aside className="flex w-[240px] flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
      <div className="flex items-center gap-2 px-4 py-3">
        <div className="grid h-8 w-8 place-items-center rounded-lg bg-zinc-950 text-zinc-50 dark:bg-zinc-50 dark:text-zinc-950">
          K
        </div>
        <div className="text-sm font-semibold">Kairos</div>
      </div>

      <nav className="px-2 py-2">
        {items.map((it) => {
          const Icon = it.icon;
          const active = it.id === activeId;
          return (
            <button
              key={it.id}
              type="button"
              onClick={() => onSelect(it.id)}
              className={cn(
                'flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors',
                active
                  ? 'bg-zinc-100 text-zinc-950 dark:bg-zinc-900 dark:text-zinc-50'
                  : 'text-zinc-700 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-900',
              )}
            >
              <Icon className="h-4 w-4" />
              {it.label}
            </button>
          );
        })}
      </nav>

      <div className="mt-auto border-t border-zinc-200 px-4 py-3 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
        Local-first • SQLite-only (v0)
      </div>
    </aside>
  );
}


