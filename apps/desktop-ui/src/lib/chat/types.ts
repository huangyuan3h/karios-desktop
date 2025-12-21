export type ChatAttachment = {
  id: string;
  kind: 'image';
  name: string;
  mediaType: string;
  dataUrl: string;
  size: number;
};

export type ChatMessage = {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  createdAt: string;
  attachments?: ChatAttachment[];
  references?: Array<{ id: string; label: string }>;
};

export type ChatSession = {
  id: string;
  title: string;
  createdAt: string;
  updatedAt: string;
  messages: ChatMessage[];
};

export type ChatReference =
  | {
      kind: 'tv';
      refId: string; // snapshotId
      snapshotId: string;
      screenerId: string;
      screenerName: string;
      capturedAt: string;
    }
  | {
      kind: 'stock';
      refId: string; // symbol
      symbol: string;
      market: string;
      ticker: string;
      name: string;
      barsDays: number;
      chipsDays: number;
      fundFlowDays: number;
      capturedAt: string;
    };

export type AppSettings = {
  systemPrompt: string;
  systemPromptId: string | null;
  systemPromptTitle: string;
};

export type AgentPanelState = {
  visible: boolean;
  mode: 'docked' | 'maximized';
  width: number;
  historyOpen: boolean;
};


