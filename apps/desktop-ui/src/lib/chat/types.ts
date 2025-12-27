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
    }
  | {
      kind: 'broker';
      refId: string; // snapshotId
      snapshotId: string;
      broker: 'pingan' | 'xueqiu' | 'unknown';
      accountId: string | null;
      accountTitle: string;
      snapshotKind: string; // positions | account_overview | conditional_orders | ...
      capturedAt: string;
    }
  | {
      kind: 'brokerState';
      refId: string; // accountId
      broker: 'pingan' | 'xueqiu' | 'unknown';
      accountId: string;
      accountTitle: string;
      capturedAt: string;
    }
  | {
      kind: 'strategyReport';
      refId: string; // reportId
      reportId: string;
      accountId: string;
      accountTitle: string;
      date: string; // YYYY-MM-DD
      createdAt: string;
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


