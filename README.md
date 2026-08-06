# QBank Center

A self-contained PWA (single-file app + manifest + service worker + icons).

## Deploy on GitHub Pages

1. Create a new GitHub repository (public, unless you have GitHub Pro/Enterprise for private Pages).
2. Upload all the files in this zip to the **root** of the repo (no subfolders):
   - `index.html`
   - `manifest.webmanifest`
   - `service-worker.js`
   - `icon-192.png`
   - `icon-512.png`
   - `icon-192-maskable.png`
   - `icon-512-maskable.png`
3. Go to **Settings → Pages**.
4. Under **Build and deployment**, set **Source** to `Deploy from a branch`.
5. Pick the branch (usually `main`) and folder `/ (root)`, then **Save**.
6. Wait a minute or two, then your app will be live at:
   `https://<your-username>.github.io/<repo-name>/`

## Notes

- Everything lives at the same level (no subfolders) because the manifest, service worker,
  and icons must be same-origin, sibling files next to `index.html` for the browser to
  treat the app as installable (Android "Add to Home screen" → real app, not just a shortcut).
- The service worker caches the app shell for offline use and auto-updates itself
  whenever you push new files and the app checks for updates (or the SW's own update check runs).
- If you rename the repo or move it, no changes are needed — all paths in `index.html`,
  `manifest.webmanifest`, and `service-worker.js` are relative.
- If you ever change `index.html` (or any cached file) and don't see updates reflected,
  bump `CACHE_NAME` in `service-worker.js` (e.g. `qbank-center-v2`) — this forces
  the browser to treat it as a new service worker and refresh the cache.
