QBank Center — single-file PWA workflow
=========================================

WHAT'S IN THIS ZIP
-------------------
  index.html          the ENTIRE app: your code + PWA manifest + icons,
                       all embedded inline. This is the only file you
                       ever need to edit/replace.
  service-worker.js   tiny, generic, filename-agnostic. Add it once to
                       your repo root and you never need to touch it
                       again, even as you keep updating index.html.
  LICENSE.txt
  README.txt          this file

WHY TWO FILES INSTEAD OF ONE
------------------------------
Everything that CAN live inside index.html already does (the PWA
manifest and every icon are embedded as data URIs right in the
<head>, so there's no separate manifest.json or icon .png files to
keep in sync anymore).

service-worker.js has to stay a separate physical .js file — this is
a hard browser/spec rule, not a workaround limitation: browsers only
allow navigator.serviceWorker.register() to load a real same-origin
.js URL, never an inline/data/blob script. But since it doesn't
reference index.html by name, you install it once and can forget
about it — updating your app is just "replace index.html".

YOUR NEW WORKFLOW
-------------------
1. First time: push both index.html and service-worker.js to your
   GitHub repo (root, no subfolders).
2. Every time you update your app: just overwrite index.html in the
   repo with your new version. Nothing else needs to change.
3. Because the service worker fetches index.html network-first,
   anyone who already installed the PWA will automatically get your
   latest index.html the next time they open the app while online —
   no reinstall needed.

INSTALLING AS AN APP (recommended, ~30 seconds)
--------------------------------------------------
Open index.html (via your GitHub Pages URL or by double-clicking it)
in Chrome:
  - Android/Windows Chrome: menu (three dots) -> "Add to Home screen" / "Install app"
  - iOS Safari: Share icon -> "Add to Home Screen"
It installs like a normal app -- its own icon, opens full-screen with
no browser address bar.

NOTES
-----
- Fonts and icon glyphs (Google Fonts, Font Awesome) still load from
  public CDNs, so the very first load needs internet. After that, the
  service worker caches them and the app keeps working offline.
- Your data (progress, resources, history) is stored locally in your
  browser via IndexedDB -- it stays on-device unless you export it.
- If someone already has the app installed from the older
  multi-file version (manifest.json + separate icon files), they
  should clear that site's data once (or uninstall/reinstall) so the
  browser drops the old service worker and picks up this new
  single-file setup cleanly.
