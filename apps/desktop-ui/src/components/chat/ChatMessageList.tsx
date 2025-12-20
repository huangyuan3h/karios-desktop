import { cn } from '@/lib/utils';

export type ChatMessage = {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
};

export function ChatMessageList({ messages }: { messages: ChatMessage[] }) {
  return (
    <div className="flex flex-col gap-3 p-4">
      {messages.map((m) => (
        <div
          key={m.id}
          className={cn(
            'max-w-[80%] rounded-lg border px-3 py-2 text-sm leading-6',
            m.role === 'user' &&
              'ml-auto border-zinc-200 bg-zinc-50 text-zinc-950 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-50',
            m.role === 'assistant' &&
              'mr-auto border-zinc-200 bg-white text-zinc-950 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-50',
            m.role === 'system' &&
              'mx-auto border-transparent bg-transparent text-zinc-500 dark:text-zinc-400',
          )}
        >
          {m.content}
        </div>
      ))}
    </div>
  );
}


