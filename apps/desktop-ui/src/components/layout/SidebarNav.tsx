'use client';

import {
  BarChart3,
  BookOpen,
  LayoutDashboard,
  PieChart,
  Settings,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
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
    <aside className="flex w-[240px] flex-col border-r border-[var(--k-border)] bg-[var(--k-surface)]">
      <div className="flex items-center gap-2 px-4 py-3">
        <div className="grid h-8 w-8 place-items-center rounded-lg bg-[var(--k-text)] text-[var(--k-surface)]">
          K
        </div>
        <div className="text-sm font-semibold">Kairos</div>
      </div>

      <nav className="px-2 py-2">
        {items.map((it) => {
          const Icon = it.icon;
          const active = it.id === activeId;
          return (
            <Button
              key={it.id}
              onClick={() => onSelect(it.id)}
              variant="ghost"
              className={cn(
                'h-auto w-full justify-start gap-3 px-3 py-2 text-sm',
                active ? 'bg-[var(--k-surface-2)]' : 'text-[var(--k-muted)]',
              )}
            >
              <Icon className="h-4 w-4" />
              {it.label}
            </Button>
          );
        })}
      </nav>

      <div className="mt-auto border-t border-[var(--k-border)] px-4 py-3 text-xs text-[var(--k-muted)]">
        Local-first • SQLite-only (v0)
      </div>
    </aside>
  );
}


