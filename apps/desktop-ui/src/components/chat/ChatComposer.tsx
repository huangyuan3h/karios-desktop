'use client';

import * as React from 'react';
import { ArrowUp, X } from 'lucide-react';
import Image from 'next/image';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { newId } from '@/lib/id';
import type { ChatAttachment, ChatReference } from '@/lib/chat/types';

export function ChatComposer({
  onSend,
  disabled,
  references,
  onRemoveReference,
  onClearReferences,
}: {
  onSend: (text: string, attachments: ChatAttachment[]) => void;
  disabled?: boolean;
  references?: ChatReference[];
  onRemoveReference?: (snapshotId: string) => void;
  onClearReferences?: () => void;
}) {
  const [text, setText] = React.useState('');
  const [attachments, setAttachments] = React.useState<ChatAttachment[]>([]);
  const [isDragging, setIsDragging] = React.useState(false);

  const addImageFiles = React.useCallback(async (files: File[]) => {
    for (const file of files) {
      if (!file.type.startsWith('image/')) continue;
      if (file.size > 2 * 1024 * 1024) continue; // 2MB soft limit for v0 localStorage-friendly attachments

      const dataUrl = await new Promise<string>((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result ?? ''));
        reader.readAsDataURL(file);
      });

      setAttachments((prev) => [
        ...prev,
        {
          id: newId(),
          kind: 'image',
          name: file.name || 'pasted-image',
          mediaType: file.type || 'image/*',
          dataUrl,
          size: file.size,
        },
      ]);
    }
  }, []);

  function submit() {
    const trimmed = text.trim();
    if (!trimmed && attachments.length === 0) return;
    onSend(trimmed, attachments);
    setText('');
    setAttachments([]);
  }

  return (
    <div className="border-t border-zinc-200 p-3 dark:border-zinc-800">
      <div
        className="relative"
        onDragEnter={(e) => {
          e.preventDefault();
          if (disabled) return;
          setIsDragging(true);
        }}
        onDragOver={(e) => {
          e.preventDefault();
          if (disabled) return;
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          if (disabled) return;
          const files = Array.from(e.dataTransfer.files ?? []);
          void addImageFiles(files);
        }}
      >
        <Textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Ask about your portfolio, imports, risk, or actions..."
          onKeyDown={(e) => {
            // Enter sends; Shift+Enter inserts newline.
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault();
              submit();
            }
          }}
          onPaste={(e) => {
            if (disabled) return;
            const items = Array.from(e.clipboardData?.items ?? []);
            const files: File[] = [];
            for (const it of items) {
              if (it.kind === 'file') {
                const f = it.getAsFile();
                if (f) files.push(f);
              }
            }
            if (files.length > 0) {
              e.preventDefault();
              void addImageFiles(files);
            }
          }}
          className="min-h-[64px] pr-12"
          disabled={disabled}
        />
        <Button
          onClick={submit}
          disabled={disabled || (!text.trim() && attachments.length === 0)}
          size="icon"
          className="absolute bottom-3 right-3 h-9 w-9 rounded-full"
          aria-label="Send"
        >
          <ArrowUp className="h-4 w-4" />
        </Button>

        {isDragging && !disabled ? (
          <div className="pointer-events-none absolute inset-0 rounded-md border-2 border-dashed border-[var(--k-accent)] bg-[var(--k-accent)]/5" />
        ) : null}
      </div>

      {references && references.length > 0 ? (
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <div className="text-xs text-[var(--k-muted)]">Referenced:</div>
          {references.map((r) => (
            <div
              key={r.refId}
              className="flex items-center gap-1 rounded-full border border-[var(--k-border)] bg-[var(--k-surface)] px-2 py-1 text-xs text-[var(--k-muted)]"
            >
              <span className="max-w-[220px] truncate">
                {r.kind === 'tv'
                  ? `${r.screenerName} @ ${new Date(r.capturedAt).toLocaleString()}`
                  : r.kind === 'stock'
                    ? `${r.ticker} ${r.name} (${r.barsDays}D) @ ${new Date(r.capturedAt).toLocaleString()}`
                    : r.kind === 'broker'
                      ? `${r.accountTitle} · ${r.snapshotKind} @ ${new Date(r.capturedAt).toLocaleString()}`
                      : r.kind === 'brokerState'
                        ? `${r.accountTitle} · account state @ ${new Date(r.capturedAt).toLocaleString()}`
                        : r.kind === 'strategyReport'
                          ? `${r.accountTitle} · strategy ${r.date}`
                          : 'Unknown reference'}
              </span>
              <Button
                variant="ghost"
                size="icon"
                className="h-5 w-5 rounded-full p-0"
                onClick={() => onRemoveReference?.(r.refId)}
                aria-label="Remove reference"
              >
                <X className="h-3 w-3" />
              </Button>
            </div>
          ))}
          {references.length > 1 ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-2 text-xs text-[var(--k-muted)]"
              onClick={() => onClearReferences?.()}
              disabled={disabled}
            >
              Clear
            </Button>
          ) : null}
        </div>
      ) : null}

      {attachments.length > 0 ? (
        <div className="mt-2 flex flex-wrap gap-2">
          {attachments.map((a) => (
            <div key={a.id} className="group relative">
              <Image
                src={a.dataUrl}
                alt={a.name}
                width={64}
                height={64}
                className="h-16 w-16 rounded-md border border-[var(--k-border)] object-cover"
                unoptimized
              />
              <Button
                variant="secondary"
                size="icon"
                className="absolute -right-2 -top-2 grid h-6 w-6 place-items-center rounded-full border border-[var(--k-border)] bg-[var(--k-surface)] text-[var(--k-muted)] shadow-sm hover:text-[var(--k-text)]"
                onClick={() => setAttachments((prev) => prev.filter((x) => x.id !== a.id))}
                aria-label="Remove image context"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}


