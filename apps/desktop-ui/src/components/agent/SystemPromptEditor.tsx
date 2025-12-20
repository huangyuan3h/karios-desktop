'use client';

import * as React from 'react';

import { Button } from '@/components/ui/button';
import { Textarea } from '@/components/ui/textarea';
import { useChatStore } from '@/lib/chat/store';

export function SystemPromptEditor() {
  const { state, setSystemPrompt } = useChatStore();
  const [draft, setDraft] = React.useState(state.settings.systemPrompt);
  const [saving, setSaving] = React.useState(false);

  React.useEffect(() => {
    setDraft(state.settings.systemPrompt);
  }, [state.settings.systemPrompt]);

  return (
    <div className="border-b border-[var(--k-border)] p-3">
      <div className="mb-2 flex items-center justify-between">
        <div className="text-xs font-semibold uppercase tracking-wide text-[var(--k-muted)]">
          System prompt
        </div>
        <Button
          size="sm"
          variant="secondary"
          disabled={saving || draft === state.settings.systemPrompt}
          onClick={async () => {
            setSaving(true);
            try {
              await setSystemPrompt(draft);
            } finally {
              setSaving(false);
            }
          }}
        >
          Save
        </Button>
      </div>
      <Textarea
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        placeholder="e.g., You are Kairos, an AI-first investment assistant. Be concise and cite evidence."
        className="min-h-[88px]"
      />
      <div className="mt-1 text-xs text-[var(--k-muted)]">
        Stored in local SQLite via quant-service.
      </div>
    </div>
  );
}


