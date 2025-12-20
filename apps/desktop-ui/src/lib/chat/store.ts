'use client';

import * as React from 'react';

import { newId } from '@/lib/id';
import { loadJson, saveJson } from '@/lib/storage';
import type { AgentPanelState, ChatMessage, ChatSession } from '@/lib/chat/types';

const STORAGE_KEY = 'karios.chat.v0';

type PersistedState = {
  sessions: ChatSession[];
  activeSessionId: string | null;
  agent: AgentPanelState;
};

const defaultState: PersistedState = {
  sessions: [],
  activeSessionId: null,
  agent: { visible: true, mode: 'docked' },
};

function nowIso() {
  return new Date().toISOString();
}

export function useChatStore() {
  const [state, setState] = React.useState<PersistedState>(() =>
    loadJson<PersistedState>(STORAGE_KEY, defaultState),
  );

  React.useEffect(() => {
    saveJson(STORAGE_KEY, state);
  }, [state]);

  const activeSession =
    state.sessions.find((s) => s.id === state.activeSessionId) ??
    (state.sessions[0] ?? null);

  const api = React.useMemo(() => {
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
      setAgent: (updater: (prev: AgentPanelState) => AgentPanelState) => {
        setState((prev) => ({ ...prev, agent: updater(prev.agent) }));
      },
    };
  }, [state, activeSession]);

  React.useEffect(() => {
    if (!state.activeSessionId && state.sessions.length > 0) {
      setState((prev) => ({ ...prev, activeSessionId: prev.sessions[0]?.id ?? null }));
    }
  }, [state.activeSessionId, state.sessions.length]);

  return api;
}


