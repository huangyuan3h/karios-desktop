'use client';

import * as React from 'react';
import { ImagePlus, X } from 'lucide-react';
import Image from 'next/image';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { newId } from '@/lib/id';
import type { ChatAttachment } from '@/lib/chat/types';

export function ChatComposer({
  onSend,
  disabled,
}: {
  onSend: (text: string, attachments: ChatAttachment[]) => void;
  disabled?: boolean;
}) {
  const [text, setText] = React.useState('');
  const [attachments, setAttachments] = React.useState<ChatAttachment[]>([]);
  const fileInputRef = React.useRef<HTMLInputElement | null>(null);

  function submit() {
    const trimmed = text.trim();
    if (!trimmed && attachments.length === 0) return;
    onSend(trimmed, attachments);
    setText('');
    setAttachments([]);
  }

  return (
    <div className="border-t border-zinc-200 p-3 dark:border-zinc-800">
      {attachments.length > 0 ? (
        <div className="mb-2 flex flex-wrap gap-2">
          {attachments.map((a) => (
            <div
              key={a.id}
              className="flex items-center gap-2 rounded-md border border-zinc-200 bg-white px-2 py-1 text-xs text-zinc-700 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-200"
            >
              <Image
                src={a.dataUrl}
                alt={a.name}
                width={24}
                height={24}
                className="h-6 w-6 rounded object-cover"
                unoptimized
              />
              <div className="max-w-[240px] truncate">{a.name}</div>
              <button
                type="button"
                className="text-zinc-500 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-zinc-50"
                onClick={() => setAttachments((prev) => prev.filter((x) => x.id !== a.id))}
                aria-label="Remove attachment"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          ))}
        </div>
      ) : null}

      <div className="flex gap-2">
        <input
          ref={fileInputRef}
          type="file"
          accept="image/*"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (!file) return;
            if (file.size > 2 * 1024 * 1024) {
              // 2MB soft limit for v0 localStorage-friendly attachments.
              e.currentTarget.value = '';
              return;
            }

            const reader = new FileReader();
            reader.onload = () => {
              const dataUrl = String(reader.result ?? '');
              setAttachments((prev) => [
                ...prev,
                {
                  id: newId(),
                  kind: 'image',
                  name: file.name,
                  mediaType: file.type || 'image/*',
                  dataUrl,
                  size: file.size,
                },
              ]);
            };
            reader.readAsDataURL(file);
            e.currentTarget.value = '';
          }}
        />

        <Button
          type="button"
          variant="secondary"
          size="sm"
          className="shrink-0"
          onClick={() => fileInputRef.current?.click()}
          disabled={disabled}
          title="Attach image"
        >
          <ImagePlus className="mr-2 h-4 w-4" />
          Attach
        </Button>

        <Textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Ask about your portfolio, imports, risk, or actions..."
          onKeyDown={(e) => {
            if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
              e.preventDefault();
              submit();
            }
          }}
          className="min-h-[44px]"
          disabled={disabled}
        />
        <Button
          onClick={submit}
          disabled={disabled || (!text.trim() && attachments.length === 0)}
          className="shrink-0"
        >
          Send
        </Button>
      </div>
    </div>
  );
}


