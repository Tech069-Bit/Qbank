# QBank Center

A self-contained web app, packaged as an installable PWA (Progressive Web App). No build step, no npm install — just static files.

## Files in this repo (all at root, no subfolders)
- `index.html` — the app itself (HTML/CSS/JS all inline)
- `manifest.json` — PWA manifest (app name, icons, colors, display mode)
- `sw.js` — service worker (caches the app so it works offline and can be "installed")
- `icon-192.png` / `icon-512.png` — app icons used for the install prompt and home-screen icon
- `README.md` — this file

## 1. Push to GitHub
1. Create a new repo.
2. Add all 5 files above to the repo **root** (no subfolders).
3. `git add . && git commit -m "Initial commit" && git push`

## 2. Turn on GitHub Pages
1. In the repo, go to **Settings → Pages**.
2. Under "Build and deployment", set **Source** to `Deploy from a branch`.
3. Set **Branch** to `main` (or `master`), folder `/ (root)`.
4. Save. Your app will be live at:
   `https://<your-username>.github.io/<repo-name>/`

**Important:** PWA install only works over **HTTPS** (which GitHub Pages provides automatically) — it will not work if you just double-click `index.html` locally, since service workers require a real web server origin.

## 3. Install it as an app
Once the GitHub Pages URL is live:

- **Android (Chrome):** open the URL → tap the **⋮** menu → **"Install app"** (or you'll see an automatic "Add to Home screen" banner).
- **iPhone/iPad (Safari):** open the URL → tap the **Share** icon → **"Add to Home Screen"**.
- **Desktop (Chrome/Edge):** open the URL → click the **install icon** (⊕ or a small monitor icon) in the address bar → **Install**.

Once installed, it opens in its own window/icon, no browser chrome, and (thanks to `sw.js`) keeps working even with no internet connection.

## Updating the app later
If you edit `index.html`, bump the version string in `sw.js` (`CACHE_VERSION`, e.g. `qbank-cache-v2`) so installed devices pick up the new version instead of serving a stale cached copy.
