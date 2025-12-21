'use client';

import {
  BarChart3,
  BookOpen,
  ChevronsLeft,
  ChevronsRight,
  CreditCard,
  LayoutDashboard,
  PieChart,
  Settings,
  Table2,
} from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

const items = [
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'market', label: 'Market', icon: Table2 },
  { id: 'broker', label: 'Broker', icon: CreditCard },
  { id: 'resources', label: 'Resources', icon: BookOpen },
  { id: 'portfolio', label: 'Portfolio', icon: PieChart },
  { id: 'analysis', label: 'Analysis', icon: BarChart3 },
  { id: 'screener', label: 'Screener', icon: Table2 },
  { id: 'settings', label: 'Settings', icon: Settings },
];

export function SidebarNav({
  activeId,
  onSelect,
  collapsed,
  onToggleCollapsed,
}: {
  activeId: string;
  onSelect: (id: string) => void;
  collapsed?: boolean;
  onToggleCollapsed?: () => void;
}) {
  const isCollapsed = Boolean(collapsed);
  return (
    <aside
      className={cn(
        'flex flex-col border-r border-[var(--k-border)] bg-[var(--k-surface)]',
        'transition-[width] duration-200 ease-out',
        isCollapsed ? 'w-[72px]' : 'w-[240px]',
      )}
    >
      <div
        className={cn(
          'flex items-center px-4 py-3',
          isCollapsed ? 'justify-between' : 'gap-2',
        )}
      >
        <div className="grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-[var(--k-text)] text-[var(--k-surface)]">
          K
        </div>
        {!isCollapsed ? <div className="text-sm font-semibold">Kairos</div> : null}
        {!isCollapsed ? <div className="flex-1" /> : null}
        <Button
          variant="ghost"
          size="icon"
          className={cn(
            'h-8 w-8 shrink-0',
            'text-[var(--k-muted)] hover:text-[var(--k-text)]',
            isCollapsed ? 'hidden' : '',
          )}
          onClick={() => onToggleCollapsed?.()}
          aria-label="Collapse sidebar"
          title="Collapse sidebar"
        >
          <ChevronsLeft className="h-4 w-4" />
        </Button>
        {isCollapsed ? (
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8 shrink-0 text-[var(--k-muted)] hover:text-[var(--k-text)]"
            onClick={() => onToggleCollapsed?.()}
            aria-label="Expand sidebar"
            title="Expand sidebar"
          >
            <ChevronsRight className="h-4 w-4" />
          </Button>
        ) : null}
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
                'h-auto w-full px-3 py-2 text-sm',
                isCollapsed ? 'justify-center' : 'justify-start gap-3',
                active ? 'bg-[var(--k-surface-2)]' : 'text-[var(--k-muted)]',
              )}
              title={isCollapsed ? it.label : undefined}
            >
              <Icon className="h-4 w-4" />
              {!isCollapsed ? it.label : null}
            </Button>
          );
        })}
      </nav>

      <div
        className={cn(
          'mt-auto border-t border-[var(--k-border)] px-4 py-3 text-xs text-[var(--k-muted)]',
          isCollapsed ? 'px-2 text-center' : '',
        )}
      >
        {!isCollapsed ? 'Local-first • SQLite-only (v0)' : 'v0'}
      </div>
    </aside>
  );
}


