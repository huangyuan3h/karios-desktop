import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import type { ChatMessage } from '@/lib/chat/types';
import { cn } from '@/lib/utils';
import Image from 'next/image';

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
          {m.role === 'assistant' ? (
            <MarkdownMessage content={m.content} className="prose-sm" />
          ) : (
            <div className="whitespace-pre-wrap">{m.content}</div>
          )}

          {m.attachments?.length ? (
            <div className="mt-2 flex flex-wrap gap-2">
              {m.attachments.map((a) => (
                <Image
                  key={a.id}
                  src={a.dataUrl}
                  alt={a.name}
                  width={80}
                  height={80}
                  unoptimized
                  className={cn(
                    'h-20 w-20 rounded-md border object-cover',
                    'border-zinc-200 dark:border-zinc-800',
                  )}
                />
              ))}
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}


