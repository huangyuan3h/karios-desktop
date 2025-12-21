'use client';

import * as React from 'react';

import { newId } from '@/lib/id';
import { loadJson, saveJson } from '@/lib/storage';
import { QUANT_BASE_URL } from '@/lib/endpoints';
import type {
  AgentPanelState,
  AppSettings,
  ChatMessage,
  ChatReference,
  ChatSession,
} from '@/lib/chat/types';

const STORAGE_KEY = 'karios.chat.v0';

type PersistedState = {
  sessions: ChatSession[];
  activeSessionId: string | null;
  agent: AgentPanelState;
  settings: AppSettings;
  references: ChatReference[];
};

const defaultState: PersistedState = {
  sessions: [],
  activeSessionId: null,
  agent: { visible: true, mode: 'docked', width: 420, historyOpen: false },
  settings: { systemPrompt: '', systemPromptId: null, systemPromptTitle: 'Legacy' },
  references: [],
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
  createEmptySession: () => void;
  setActiveSession: (id: string) => void;
  renameSession: (id: string, title: string) => void;
  deleteSession: (id: string) => void;
  appendMessages: (sessionId: string, messages: ChatMessage[]) => void;
  updateMessageContent: (sessionId: string, messageId: string, content: string) => void;
  setAgent: (updater: (prev: AgentPanelState) => AgentPanelState) => void;
  setSystemPrompt: (value: string) => Promise<void>;
  setSystemPromptLocal: (next: { id: string | null; title: string; content: string }) => void;
  addReference: (ref: ChatReference) => void;
  removeReference: (snapshotId: string) => void;
  clearReferences: () => void;
};

const ChatStoreContext = React.createContext<ChatStoreApi | null>(null);

export function ChatStoreProvider({ children }: { children: React.ReactNode }) {
  // IMPORTANT: Do not read localStorage during the initial render.
  // This prevents SSR/CSR mismatches (hydration errors). We load persisted state after mount.
  const [state, setState] = React.useState<PersistedState>(defaultState);

  React.useEffect(() => {
    const loaded = loadJson<PersistedState>(STORAGE_KEY, defaultState);
    setState({
      ...defaultState,
      ...loaded,
      references: Array.isArray((loaded as Partial<PersistedState>).references)
        ? (loaded as Partial<PersistedState>).references!
        : [],
    });
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
      createEmptySession: () => {
        const id = newId();
        const session: ChatSession = {
          id,
          title: 'New chat',
          createdAt: nowIso(),
          updatedAt: nowIso(),
          messages: [],
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
      deleteSession: (id: string) => {
        setState((prev) => {
          const nextSessions = prev.sessions.filter((s) => s.id !== id);
          const nextActive =
            prev.activeSessionId === id ? (nextSessions[0]?.id ?? null) : prev.activeSessionId;
          return { ...prev, sessions: nextSessions, activeSessionId: nextActive };
        });
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
        setState((prev) => ({
          ...prev,
          settings: { ...prev.settings, systemPrompt: value },
        }));
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
      setSystemPromptLocal: (next: { id: string | null; title: string; content: string }) => {
        setState((prev) => ({
          ...prev,
          settings: {
            ...prev.settings,
            systemPromptId: next.id,
            systemPromptTitle: next.title,
            systemPrompt: next.content,
          },
        }));
      },
      addReference: (ref: ChatReference) => {
        setState((prev) => {
          if (prev.references.some((r) => r.snapshotId === ref.snapshotId)) return prev;
          return { ...prev, references: [ref, ...prev.references] };
        });
      },
      removeReference: (snapshotId: string) => {
        setState((prev) => ({
          ...prev,
          references: prev.references.filter((r) => r.snapshotId !== snapshotId),
        }));
      },
      clearReferences: () => {
        setState((prev) => ({ ...prev, references: [] }));
      },
    };
  }, [state, activeSession]);

  // Best-effort: load system prompt from the local DB when the backend is available.
  React.useEffect(() => {
    let cancelled = false;
    async function loadFromBackend() {
      try {
        // Prefer presets API (v0.2+). Fall back to legacy single-value API.
        const resp = await fetch(`${QUANT_BASE_URL}/system-prompts/active`);
        if (resp.ok) {
          const data = (await resp.json()) as { id?: string | null; title?: string; content?: string };
          const content = typeof data.content === 'string' ? data.content : '';
          const title = typeof data.title === 'string' ? data.title : 'Legacy';
          const id = data.id === null || typeof data.id === 'string' ? (data.id ?? null) : null;
          if (cancelled) return;
          setState((prev) => ({
            ...prev,
            settings: { ...prev.settings, systemPromptId: id, systemPromptTitle: title, systemPrompt: content },
          }));
          return;
        }

        const legacy = await fetch(`${QUANT_BASE_URL}/settings/system-prompt`);
        if (!legacy.ok) return;
        const legacyData = (await legacy.json()) as { value?: string };
        const value = typeof legacyData.value === 'string' ? legacyData.value : '';
        if (cancelled) return;
        setState((prev) => ({
          ...prev,
          settings: { ...prev.settings, systemPromptId: null, systemPromptTitle: 'Legacy', systemPrompt: value },
        }));
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

