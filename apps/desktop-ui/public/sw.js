/* Karios PWA service worker — cache-first for static assets, network-first
 * for navigations (the app is a static export; dynamic data comes from the
 * data-sync API which must stay fresh).
 */
const CACHE = 'karios-v1';
const PRECACHE = ['/', '/manifest.webmanifest', '/icon-192.png', '/icon-512.png', '/apple-touch-icon.png'];

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
  if (url.port === '4330' || url.pathname.startsWith('/api/')) return;

  if (event.request.mode === 'navigate') {
    // Network-first for pages (so the app always loads fresh when online).
    event.respondWith(
      fetch(event.request)
        .then((resp) => {
          const copy = resp.clone();
          caches.open(CACHE).then((cache) => cache.put('/', copy));
          return resp;
        })
        .catch(() => caches.match('/')),
    );
    return;
  }

  // Cache-first for static assets.
  event.respondWith(
    caches.match(event.request).then(
      (hit) =>
        hit ||
        fetch(event.request).then((resp) => {
          const copy = resp.clone();
          caches.open(CACHE).then((cache) => cache.put(event.request, copy));
          return resp;
        }),
    ),
  );
});
