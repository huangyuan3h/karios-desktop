import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

export type ApiClientOptions = {
  baseUrl?: string;
  cache?: RequestCache;
  signal?: AbortSignal | null;
  timeoutMs?: number;
};

export const DEFAULT_API_TIMEOUT_MS = 30_000;

type ApiFetchInit = RequestInit & ApiClientOptions;

function resolveRequestSignal(options?: ApiClientOptions): AbortSignal | undefined {
  if (options?.signal) return options.signal;
  const timeoutMs = options?.timeoutMs ?? DEFAULT_API_TIMEOUT_MS;
  if (timeoutMs > 0) return AbortSignal.timeout(timeoutMs);
  return undefined;
}

function resolveUrl(path: string, options?: ApiClientOptions): string {
  const base = options?.baseUrl ?? DATA_SYNC_BASE_URL;
  return `${base}${path}`;
}

async function readJsonResponse<T>(res: Response): Promise<T> {
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return txt ? (JSON.parse(txt) as T) : ({} as T);
}

export async function apiFetchJson<T>(path: string, init?: ApiFetchInit): Promise<T> {
  const { baseUrl, cache = 'no-store', timeoutMs, signal: _signal, ...requestInit } = init ?? {};
  const signal = resolveRequestSignal({ timeoutMs, signal: _signal });
  const res = await fetch(resolveUrl(path, { baseUrl, cache }), {
    cache,
    ...requestInit,
    ...(signal ? { signal } : {}),
  });
  return readJsonResponse<T>(res);
}

export function apiGetJson<T>(path: string, options?: ApiClientOptions): Promise<T> {
  return apiFetchJson<T>(path, { method: 'GET', ...options });
}

export function apiPostJson<T>(
  path: string,
  body?: unknown,
  options?: ApiClientOptions,
): Promise<T> {
  return apiFetchJson<T>(path, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    ...options,
  });
}

export function apiPutJson<T>(
  path: string,
  body?: unknown,
  options?: ApiClientOptions,
): Promise<T> {
  return apiFetchJson<T>(path, {
    method: 'PUT',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    ...options,
  });
}

export function apiPatchJson<T>(
  path: string,
  body?: unknown,
  options?: ApiClientOptions,
): Promise<T> {
  return apiFetchJson<T>(path, {
    method: 'PATCH',
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    ...options,
  });
}

export function apiDeleteJson<T>(path: string, options?: ApiClientOptions): Promise<T> {
  return apiFetchJson<T>(path, { method: 'DELETE', ...options });
}
