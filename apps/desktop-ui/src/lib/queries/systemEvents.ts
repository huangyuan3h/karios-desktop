'use client';

const BASE = process.env.NEXT_PUBLIC_DATA_SYNC_BASE_URL ?? 'http://localhost:4330';

export type SystemEvent = {
  id: number;
  eventType: string;
  severity: 'high' | 'low';
  title: string;
  detail: string;
  payload: Record<string, unknown>;
  dedupeKey: string;
  resolved: boolean;
  createdAt: string;
};

export async function fetchSystemEvents(limit = 50): Promise<SystemEvent[]> {
  const r = await fetch(`${BASE}/api/health/system-events?limit=${limit}`, { cache: 'no-store' });
  if (!r.ok) return [];
  const j = await r.json();
  return (j.events ?? []) as SystemEvent[];
}

export async function resolveSystemEvent(id: number): Promise<boolean> {
  const r = await fetch(`${BASE}/api/health/system-events/${id}/resolve`, { method: 'POST' });
  return r.ok;
}
