import { ChatPanel } from '@/components/chat/ChatPanel';

export function AppShell() {
  return (
    <div className="flex h-screen w-screen bg-zinc-50 text-zinc-950 dark:bg-black dark:text-zinc-50">
      <aside className="flex w-[260px] flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
        <div className="flex items-center gap-2 px-4 py-3">
          <div className="h-6 w-6 rounded-md bg-zinc-950/10 dark:bg-zinc-50/10" />
          <div className="text-sm font-semibold">Karios</div>
        </div>
        <nav className="flex-1 px-2 py-2 text-sm text-zinc-700 dark:text-zinc-300">
          <div className="rounded-md px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-900">
            Dashboard (placeholder)
          </div>
          <div className="rounded-md px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-900">
            Imports (placeholder)
          </div>
          <div className="rounded-md px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-900">
            Portfolio (placeholder)
          </div>
          <div className="rounded-md px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-900">
            Context Collector (placeholder)
          </div>
          <div className="rounded-md px-3 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-900">
            Settings (placeholder)
          </div>
        </nav>
        <div className="border-t border-zinc-200 px-4 py-3 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
          Local-first • SQLite-only (v0)
        </div>
      </aside>

      <main className="flex flex-1 flex-col">
        <ChatPanel />
      </main>
    </div>
  );
}


