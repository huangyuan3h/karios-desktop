'use client';

import * as React from 'react';

import { newId } from '@/lib/id';
import { loadJson, saveJson } from '@/lib/storage';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import type { AgentPanelState, AppSettings, ChatMessage, ChatSession } from '@/lib/chat/types';

const STORAGE_KEY = 'karios.chat.v0';

type PersistedState = {
  sessions: ChatSession[];
  activeSessionId: string | null;
  agent: AgentPanelState;
  settings: AppSettings;
};

const defaultState: PersistedState = {
  sessions: [],
  activeSessionId: null,
  agent: { visible: true, mode: 'docked', width: 420 },
  settings: { systemPrompt: '' },
};

function nowIso() {
  return new Date().toISOString();
}

export function useChatStore() {
  const ctx = React.useContext(ChatStoreContext);
  if (!ctx) {
    throw new Error('useChatStore must be used within <ChatStoreProvider>.');
  }
  return ctx;
}

export type ChatStoreApi = {
  state: PersistedState;
  activeSession: ChatSession | null;
  createSession: () => void;
  setActiveSession: (id: string) => void;
  renameSession: (id: string, title: string) => void;
  appendMessages: (sessionId: string, messages: ChatMessage[]) => void;
  updateMessageContent: (sessionId: string, messageId: string, content: string) => void;
  setAgent: (updater: (prev: AgentPanelState) => AgentPanelState) => void;
  setSystemPrompt: (value: string) => Promise<void>;
};

const ChatStoreContext = React.createContext<ChatStoreApi | null>(null);

export function ChatStoreProvider({ children }: { children: React.ReactNode }) {
  // IMPORTANT: Do not read localStorage during the initial render.
  // This prevents SSR/CSR mismatches (hydration errors). We load persisted state after mount.
  const [state, setState] = React.useState<PersistedState>(defaultState);

  React.useEffect(() => {
    const loaded = loadJson<PersistedState>(STORAGE_KEY, defaultState);
    setState(loaded);
  }, []);

  React.useEffect(() => {
    saveJson(STORAGE_KEY, state);
  }, [state]);

  const activeSession =
    state.sessions.find((s) => s.id === state.activeSessionId) ??
    (state.sessions[0] ?? null);

  React.useEffect(() => {
    if (!state.activeSessionId && state.sessions.length > 0) {
      setState((prev) => ({ ...prev, activeSessionId: prev.sessions[0]?.id ?? null }));
    }
  }, [state.activeSessionId, state.sessions.length]);

  const api = React.useMemo<ChatStoreApi>(() => {
    return {
      state,
      activeSession,
      createSession: () => {
        const id = newId();
        const session: ChatSession = {
          id,
          title: 'New chat',
          createdAt: nowIso(),
          updatedAt: nowIso(),
          messages: [
            {
              id: newId(),
              role: 'assistant',
              content:
                'Welcome to Karios Desktop. Paste context (links/text) or upload an image, then ask a question.',
              createdAt: nowIso(),
            },
          ],
        };
        setState((prev) => ({
          ...prev,
          sessions: [session, ...prev.sessions],
          activeSessionId: id,
        }));
      },
      setActiveSession: (id: string) => {
        setState((prev) => ({ ...prev, activeSessionId: id }));
      },
      renameSession: (id: string, title: string) => {
        setState((prev) => ({
          ...prev,
          sessions: prev.sessions.map((s) => (s.id === id ? { ...s, title } : s)),
        }));
      },
      appendMessages: (sessionId: string, messages: ChatMessage[]) => {
        setState((prev) => ({
          ...prev,
          sessions: prev.sessions.map((s) => {
            if (s.id !== sessionId) return s;
            return {
              ...s,
              updatedAt: nowIso(),
              messages: [...s.messages, ...messages],
            };
          }),
        }));
      },
      updateMessageContent: (sessionId: string, messageId: string, content: string) => {
        setState((prev) => ({
          ...prev,
          sessions: prev.sessions.map((s) => {
            if (s.id !== sessionId) return s;
            return {
              ...s,
              updatedAt: nowIso(),
              messages: s.messages.map((m) => (m.id === messageId ? { ...m, content } : m)),
            };
          }),
        }));
      },
      setAgent: (updater: (prev: AgentPanelState) => AgentPanelState) => {
        setState((prev) => ({ ...prev, agent: updater(prev.agent) }));
      },
      setSystemPrompt: async (value: string) => {
        setState((prev) => ({ ...prev, settings: { ...prev.settings, systemPrompt: value } }));
        try {
          await fetch(`${QUANT_BASE_URL}/settings/system-prompt`, {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ value }),
          });
        } catch {
          // Best-effort for v0 (backend may be offline).
        }
      },
    };
  }, [state, activeSession]);

  // Best-effort: load system prompt from the local DB when the backend is available.
  React.useEffect(() => {
    let cancelled = false;
    async function loadFromBackend() {
      try {
        const resp = await fetch(`${QUANT_BASE_URL}/settings/system-prompt`);
        if (!resp.ok) return;
        const data = (await resp.json()) as { value?: string };
        const value = typeof data.value === 'string' ? data.value : '';
        if (cancelled) return;
        setState((prev) => ({ ...prev, settings: { ...prev.settings, systemPrompt: value } }));
      } catch {
        // ignore
      }
    }
    loadFromBackend();
    return () => {
      cancelled = true;
    };
  }, []);

  return React.createElement(ChatStoreContext.Provider, { value: api }, children);
}

