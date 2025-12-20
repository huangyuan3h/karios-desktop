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

export type AgentPanelState = {
  visible: boolean;
  mode: 'docked' | 'maximized';
};


