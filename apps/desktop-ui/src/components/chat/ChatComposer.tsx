'use client';

import * as React from 'react';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';

export function ChatComposer({
  onSend,
  disabled,
}: {
  onSend: (text: string) => void;
  disabled?: boolean;
}) {
  const [text, setText] = React.useState('');

  function submit() {
    const trimmed = text.trim();
    if (!trimmed) return;
    onSend(trimmed);
    setText('');
  }

  return (
    <div className="flex gap-2 border-t border-zinc-200 p-3 dark:border-zinc-800">
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
      <Button onClick={submit} disabled={disabled || !text.trim()} className="shrink-0">
        Send
      </Button>
    </div>
  );
}


