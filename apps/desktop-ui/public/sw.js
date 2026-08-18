/* Karios PWA service worker — v2 (2026-08-14)
 * Only precached public assets (manifest/icons) are cache-first; every other
 * request goes straight to the network so dev-mode JS bundles and API
 * responses are NEVER stale. This prevents the "old bundle points at a dead
 * subdomain" failure seen on phones after the single-host gateway change.
 */
const CACHE = 'karios-v2';
const PRECACHE = ['/manifest.webmanifest', '/icon-192.png', '/icon-512.png', '/apple-touch-icon.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) => cache.addAll(PRECACHE)).then(() => self.skipWaiting()),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  // Never intercept API calls.
  if (url.port === '4330' || url.pathname.startsWith('/api/') || url.pathname.startsWith('/v1/')) return;

  // Cache-first ONLY for the precached public assets; everything else
  // (pages, JS bundles, AI calls) goes to the network.
  if (PRECACHE.includes(url.pathname)) {
    event.respondWith(
      caches.match(event.request).then(
        (hit) => hit || fetch(event.request),
      ),
    );
    return;
  }
});
