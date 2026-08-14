/**
 * Service endpoints. Local dev defaults to 127.0.0.1; when served through
 * the Cloudflare tunnel (karios.it-t.xyz — Family Hub Phase 0), everything
 * stays on ONE host so the Basic-Auth credential entered once covers UI +
 * API + AI (the local caddy gate splits paths: /api /v1 -> 4330, /ai -> 4310).
 */
const isTunnelOrigin =
  typeof window !== 'undefined' && window.location.hostname.endsWith('it-t.xyz');

export const AI_BASE_URL = isTunnelOrigin
  ? 'https://karios.it-t.xyz/ai'
  : process.env.NEXT_PUBLIC_AI_BASE_URL ?? 'http://127.0.0.1:4310';
export const DATA_SYNC_BASE_URL = isTunnelOrigin
  ? 'https://karios.it-t.xyz'
  : process.env.NEXT_PUBLIC_DATA_SYNC_BASE_URL ?? 'http://127.0.0.1:4330';


