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
      kind: 'watchlistStock';
      refId: string; // stable key: `${symbol}:${capturedAt}`
      symbol: string;
      name?: string | null;
      capturedAt: string;
      asOfDate?: string | null;
      close?: number | null;
      trendOk?: boolean | null;
      score?: number | null;
      stopLossPrice?: number | null;
      buyMode?: string | null;
      buyAction?: string | null;
      buyZoneLow?: number | null;
      buyZoneHigh?: number | null;
      buyWhy?: string | null;
    }
  | {
      kind: 'watchlistTable';
      refId: string; // stable key: `${capturedAt}:${count}`
      capturedAt: string;
      total: number;
      items: Array<{
        symbol: string;
        name?: string | null;
        asOfDate?: string | null;
        close?: number | null;
        trendOk?: boolean | null;
        score?: number | null;
        stopLossPrice?: number | null;
        buyMode?: string | null;
        buyAction?: string | null;
        buyZoneLow?: number | null;
        buyZoneHigh?: number | null;
      }>;
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
      kind: 'industryFundFlow';
      refId: string; // stable key: `${asOfDate}:${days}:${topN}`
      asOfDate: string; // YYYY-MM-DD
      days: number; // typically 10
      topN: number; // typically 10
      // Optional view configuration for referencing a specific widget/card.
      metric?: 'netInflow' | 'sum';
      windowDays?: number; // e.g. 1/5/10 (used with metric='sum' or for labeling)
      direction?: 'in' | 'out'; // in=top positive, out=top negative
      view?: 'rankedList' | 'dailyTopByDate' | 'matrixValues';
      title?: string;
      createdAt: string;
    }
  | {
      kind: 'leaderStocks';
      refId: string; // stable key: `leaderStocks:${days}:${ts}`
      days: number; // typically 10
      createdAt: string;
    }
  | {
      kind: 'marketSentiment';
      refId: string; // stable key: `${asOfDate}:${days}`
      asOfDate: string; // YYYY-MM-DD
      days: number; // typically 5
      title?: string;
      createdAt: string;
    }
  | {
      kind: 'rankList';
      refId: string; // stable key: `rankList:${ts}`
      asOfDate: string; // YYYY-MM-DD
      limit: number; // typically 30
      createdAt: string;
    }
  | {
      kind: 'intradayRankList';
      refId: string; // stable key: `intradayRankList:${ts}`
      asOfTs: string; // ISO timestamp
      slot: string; // 0930_1030 | 1030_1130 | 1300_1400 | 1400_1445
      limit: number; // typically 30
      createdAt: string;
    }
  | {
      kind: 'journal';
      refId: string; // stable key: `journal:${journalId}:${timestamp}`
      journalId: string;
      title: string;
      content: string; // markdown content
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
