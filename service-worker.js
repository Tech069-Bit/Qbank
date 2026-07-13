/* ============================================================
   QBank Center — service worker
   ------------------------------------------------------------
   DESIGN GOAL: you should be able to replace index.html on its own
   (e.g. push a new version to GitHub) WITHOUT ever having to touch
   this file, manifest.webmanifest, or the icon-*.png files again.

   How that works:
   - Every navigation / HTML request (index.html, "/", etc.) is
     NETWORK-FIRST: the browser always tries to fetch the latest
     copy from the server first, and only falls back to the cached
     copy if there's no connection. That means a fresh index.html
     you deploy is picked up on the very next load — no cache
     version bump required here.
   - Static assets that rarely change (manifest, icons) use
     stale-while-revalidate: instant from cache, refreshed quietly
     in the background, so the app still works offline.
   - CACHE_NAME only needs bumping if you deliberately want to force
     every asset to be re-downloaded (rare) — it does NOT need to
     change just because index.html changed.
   ============================================================ */
const CACHE_NAME = 'qbank-center-shell-v1';
const PRECACHE_URLS = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icon-192.png',
  './icon-512.png',
  './favicon-64.png',
  './apple-touch-icon.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(PRECACHE_URLS))
      .catch(() => {}) // don't fail install if e.g. an icon 404s — the app should still work
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)));
      await self.clients.claim(); // start controlling already-open tabs immediately
    })()
  );
});

// Lets index.html's "App update" button (Settings → App update) swap in a waiting
// update right away instead of waiting for every tab to close.
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const acceptsHtml = (req.headers.get('accept') || '').includes('text/html');
  const isNavigation = req.mode === 'navigate' || acceptsHtml;

  if (isNavigation) {
    // Network-first for the app shell HTML — see the file header comment above.
    event.respondWith(
      fetch(req)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(req, copy)).catch(() => {});
          return res;
        })
        .catch(() =>
          caches.match(req).then((cached) => cached || caches.match('./index.html'))
        )
    );
    return;
  }

  // Everything else (manifest, icons, fonts, etc.): stale-while-revalidate.
  event.respondWith(
    caches.match(req).then((cached) => {
      const network = fetch(req)
        .then((res) => {
          if (res && res.ok) {
            const copy = res.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(req, copy)).catch(() => {});
          }
          return res;
        })
        .catch(() => cached);
      return cached || network;
    })
  );
});
