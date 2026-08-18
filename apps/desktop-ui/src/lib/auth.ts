'use client';

/**
 * Gateway auth (Family Hub Phase 0 · 2026-08-14).
 *
 * The caddy gate protects every API path with the `X-Karios-Key` header
 * (no native browser auth dialog — iOS PWA standalone keeps re-prompting
 * Basic Auth). The UI shell is public; once the user enters the password
 * it is stored in localStorage and EVERY fetch gets the header.
 */

const KEY_STORAGE = 'karios.gateway-key';
export const UNAUTHORIZED_EVENT = 'karios:unauthorized';

export function getGatewayKey(): string | null {
  try {
    return localStorage.getItem(KEY_STORAGE);
  } catch {
    return null;
  }
}

export function setGatewayKey(key: string): void {
  localStorage.setItem(KEY_STORAGE, key);
}

export function clearGatewayKey(): void {
  localStorage.removeItem(KEY_STORAGE);
}

/** True when the request targets a Karios API (needs the key header). */
function isKariosApi(url: string): boolean {
  if (url.startsWith('http://127.0.0.1:') || url.includes('127.0.0.1:4330')) return true;
  if (url.includes('it-t.xyz')) return true;
  return false;
}

/**
 * Wrap window.fetch: attach X-Karios-Key to every Karios API request and
 * broadcast UNAUTHORIZED_EVENT on 401 so the AuthGate can show the login
 * page. Called once at app entry.
 */
export function installFetchAuth(): void {
  if (typeof window === 'undefined') return;
  const orig = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
    const key = getGatewayKey();
    if (isKariosApi(url) && key) {
      const headers = new Headers(init?.headers);
      headers.set('X-Karios-Key', key);
      init = { ...init, headers };
    }
    return orig(input, init).then((resp) => {
      if (resp.status === 401 && isKariosApi(url)) {
        clearGatewayKey();
        window.dispatchEvent(new CustomEvent(UNAUTHORIZED_EVENT));
      }
      return resp;
    });
  };
}
