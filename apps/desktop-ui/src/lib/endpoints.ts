/**
 * Service endpoints. Local dev defaults to 127.0.0.1; when served through
 * the Cloudflare tunnel (karios.it-t.xyz — Family Hub Phase 0), the same
 * frontend talks to the public API/AI subdomains so a phone anywhere can
 * use it without rebuilding with injected env vars.
 */
const isTunnelOrigin =
  typeof window !== 'undefined' && window.location.hostname.endsWith('it-t.xyz');

export const AI_BASE_URL = isTunnelOrigin
  ? 'https://ai-karios.it-t.xyz'
  : process.env.NEXT_PUBLIC_AI_BASE_URL ?? 'http://127.0.0.1:4310';
export const DATA_SYNC_BASE_URL = isTunnelOrigin
  ? 'https://api-karios.it-t.xyz'
  : process.env.NEXT_PUBLIC_DATA_SYNC_BASE_URL ?? 'http://127.0.0.1:4330';


