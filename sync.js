/* QBank Sync — QR-paired, serverless device-to-device sync (WebRTC data channel).
   Pair once by showing/scanning two QR codes; data then flows directly between devices.
   Merge model: per-item last-write-wins with delete tombstones, so work done on either
   device (answers, sessions, resume-state, decks, settings…) is combined, not overwritten. */
(function(){
'use strict';
if(window.__qbSync) return; window.__qbSync = true;

const ICE = [{urls:'stun:stun.l.google.com:19302'},{urls:'stun:global.stun.twilio.com:3478'}];
const IDB_KEYS = {resources:'id', history:'qid', sessions:'id', media:'id'};
const enc = new TextEncoder(), dec = new TextDecoder();
let KEYS = null;
const isSyncKey = k => (KEYS || (KEYS = new Set(typeof BACKUP_LS_KEYS !== 'undefined' ? BACKUP_LS_KEYS : []))).has(k);

/* ---------- change tracking (own tiny IndexedDB, so no localStorage quota is used) ---------- */
let meta = {t:{}, d:{}}, clock = 0, flushT = null;
const now = () => (clock = Math.max(Date.now(), clock + 1));
const metaDB = new Promise(res => {
  try{ const r = indexedDB.open('qbank_sync_meta', 1);
    r.onupgradeneeded = () => r.result.createObjectStore('kv');
    r.onsuccess = () => res(r.result); r.onerror = () => res(null);
  }catch(e){ res(null); }
});
metaDB.then(db => { if(!db) return;
  const g = db.transaction('kv').objectStore('kv').get('meta');
  g.onsuccess = () => { const o = g.result; if(o){ meta.t = Object.assign(o.t||{}, meta.t); meta.d = Object.assign(o.d||{}, meta.d); for(const k in meta.t) clock = Math.max(clock, meta.t[k]); } };
});
function flushMeta(){ clearTimeout(flushT); return metaDB.then(db => db && new Promise(r => { const tx = db.transaction('kv','readwrite'); tx.objectStore('kv').put(meta,'meta'); tx.oncomplete = tx.onerror = () => r(); })); }
function touch(id, del, ts){ meta.t[id] = ts || now(); if(del) meta.d[id] = 1; else delete meta.d[id]; clearTimeout(flushT); flushT = setTimeout(flushMeta, 2000); }

const origSet = Storage.prototype.setItem, origRem = Storage.prototype.removeItem;
Storage.prototype.setItem = function(k,v){ origSet.call(this,k,v); if(this === localStorage && isSyncKey(k)) touch('ls|'+k, 0); };
Storage.prototype.removeItem = function(k){ origRem.call(this,k); if(this === localStorage && isSyncKey(k)) touch('ls|'+k, 1); };

const idbPut0 = window.idbPut, idbDel0 = window.idbDelete, idbDelIdx0 = window.idbDeleteByIndex;
window.idbPut = async function(store, val){ const r = await idbPut0(store, val); if(r && IDB_KEYS[store] && val) touch('idb|'+store+'|'+val[IDB_KEYS[store]], 0); return r; };
window.idbDelete = async function(store, key){ const r = await idbDel0(store, key); if(r && IDB_KEYS[store]) touch('idb|'+store+'|'+key, 1); return r; };
window.idbDeleteByIndex = async function(store, index, value){
  let ids = []; if(IDB_KEYS[store]){ try{ ids = (await idbGetAll(store)).filter(x => x && x[index] === value).map(x => x[IDB_KEYS[store]]); }catch(e){} }
  const r = await idbDelIdx0(store, index, value); if(r) ids.forEach(id => touch('idb|'+store+'|'+id, 1)); return r;
};
addEventListener('pagehide', () => { if(flushT) flushMeta(); });

/* ---------- manifest / merge planning ---------- */
async function buildManifest(){
  const m = {}, recs = {};
  for(const k of BACKUP_LS_KEYS) if(localStorage.getItem(k) !== null) m['ls|'+k] = [meta.t['ls|'+k]||0, 0];
  for(const s in IDB_KEYS) for(const r of await idbGetAll(s)){ const id = 'idb|'+s+'|'+r[IDB_KEYS[s]]; m[id] = [meta.t[id]||0, 0]; recs[id] = r; }
  for(const id in meta.d) if(!(id in m)) m[id] = [meta.t[id]||0, 1];
  return {m, recs};
}
function plan(local, remote){
  const out = [];
  for(const id in local){ const l = local[id], r = remote[id];
    if(r ? l[0] > r[0] : !l[1]) out.push(id); }
  return out;
}

/* ---------- framing over the data channel ---------- */
async function sendBundle(dc, head, json, blob){
  const h = enc.encode(JSON.stringify(head));
  const j = json == null ? new Uint8Array(0) : enc.encode(json);
  const b = blob ? new Uint8Array(await blob.arrayBuffer()) : new Uint8Array(0);
  const buf = new Uint8Array(8 + h.length + j.length + b.length), dv = new DataView(buf.buffer);
  dv.setUint32(0, h.length); dv.setUint32(4, j.length); buf.set(h,8); buf.set(j,8+h.length); buf.set(b,8+h.length+j.length);
  dc.send(JSON.stringify({n:buf.length}));
  for(let o = 0; o < buf.length; o += 16384){
    if(dc.bufferedAmount > (1<<20)) await new Promise(r => { dc.bufferedAmountLowThreshold = 262144; dc.onbufferedamountlow = () => { dc.onbufferedamountlow = null; r(); }; });
    dc.send(buf.subarray(o, Math.min(buf.length, o+16384)));
  }
}
function makeReceiver(onBundle){
  let need = 0, got = 0, parts = [];
  return ev => {
    if(typeof ev.data === 'string'){ need = JSON.parse(ev.data).n; got = 0; parts = []; return; }
    parts.push(new Uint8Array(ev.data)); got += ev.data.byteLength;
    if(got >= need){
      const buf = new Uint8Array(need); let o = 0; parts.forEach(p => { buf.set(p,o); o += p.length; });
      const dv = new DataView(buf.buffer), hl = dv.getUint32(0), jl = dv.getUint32(4);
      onBundle(JSON.parse(dec.decode(buf.subarray(8,8+hl))), buf.subarray(8+hl, 8+hl+jl), buf.subarray(8+hl+jl));
      parts = []; need = got = 0;
    }
  };
}

/* ---------- applying remote items (validated; remote data is never trusted for keys/stores) ---------- */
async function applyItem(h, json, blob){
  const id = h.id, ts = +h.ts || 0; if(typeof id !== 'string') return false;
  clock = Math.max(clock, ts);
  if(id.startsWith('ls|')){
    const k = id.slice(3); if(!isSyncKey(k)) return false;
    if(h.del) origRem.call(localStorage, k); else origSet.call(localStorage, k, dec.decode(json));
  }else if(id.startsWith('idb|')){
    const rest = id.slice(4), i = rest.indexOf('|'), store = rest.slice(0,i), key = rest.slice(i+1);
    if(!IDB_KEYS[store]) return false;
    if(h.del){ await idbDel0(store, key); if(/^\d+$/.test(key)) await idbDel0(store, +key); }
    else{ const rec = JSON.parse(dec.decode(json)); if(h.blob) rec.blob = new Blob([blob], {type: h.mime||''}); if(!await idbPut0(store, rec)) return false; }
  }else return false;
  touch(id, !!h.del, ts); return true;
}

/* ---------- one full two-way sync over an open data channel ---------- */
function runSync(dc, progress){
  return new Promise(async (resolve, reject) => {
    const st = {recv:0, sent:0, expect:null, sentDone:false};
    let chain = Promise.resolve(), local;
    const ready = buildManifest().then(x => { local = x; return sendBundle(dc, {k:'man'}, JSON.stringify(x.m)); }).catch(reject);
    const check = () => { if(st.sentDone && st.expect !== null && st.recv >= st.expect) resolve(st); };
    dc.onmessage = makeReceiver((h, json, blob) => {
      chain = chain.then(async () => {
        await ready;
        if(h.k === 'man'){
          const ids = plan(local.m, JSON.parse(dec.decode(json)));
          for(const id of ids){
            const head = {k:'item', id, ts: local.m[id][0]}; let body = null, bl = null;
            if(local.m[id][1]) head.del = 1;
            else if(id.startsWith('ls|')) body = localStorage.getItem(id.slice(3));
            else{ const rec = Object.assign({}, local.recs[id]); if(rec.blob instanceof Blob){ bl = rec.blob; head.blob = 1; head.mime = bl.type; } delete rec.blob; body = JSON.stringify(rec); }
            await sendBundle(dc, head, body, bl); st.sent++; progress(st);
          }
          await sendBundle(dc, {k:'done', n: ids.length}); st.sentDone = true;
        }else if(h.k === 'item'){ if(await applyItem(h, json, blob)) st.recv++; progress(st); }
        else if(h.k === 'done') st.expect = h.n;
        check();
      }).catch(reject);
    });
  });
}

/* ---------- signalling codes (compressed SDP → base64url, small enough for one QR) ---------- */
const b64 = u => { let s = ''; for(let i = 0; i < u.length; i += 8192) s += String.fromCharCode.apply(null, u.subarray(i,i+8192)); return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); };
const unb64 = s => Uint8Array.from(atob(s.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0));
const pipe = async (u, S) => new Uint8Array(await new Response(new Blob([u]).stream().pipeThrough(new S('deflate-raw'))).arrayBuffer());
async function packCode(desc){
  const sdp = desc.sdp.split('\r\n').filter(l => !(/^a=candidate/.test(l) && / tcp /i.test(l))).join('\r\n');
  const raw = enc.encode(sdp), z = window.CompressionStream;
  return 'QB1' + (z ? 'z' : 'r') + (desc.type === 'offer' ? 'o' : 'a') + '.' + b64(z ? await pipe(raw, CompressionStream) : raw);
}
async function unpackCode(code){
  const m = /^QB1([zr])([oa])\.([\w-]+)$/.exec(code.trim()); if(!m) throw new Error('That is not a QBank pairing code.');
  let u = unb64(m[3]);
  if(m[1] === 'z'){ if(!window.DecompressionStream) throw new Error('This browser is too old for QR sync — please update it.'); u = await pipe(u, DecompressionStream); }
  return {type: m[2] === 'o' ? 'offer' : 'answer', sdp: dec.decode(u)};
}
const gathered = pc => new Promise(res => {
  if(pc.iceGatheringState === 'complete') return res();
  const t = setTimeout(res, 3500);
  pc.addEventListener('icegatheringstatechange', () => { if(pc.iceGatheringState === 'complete'){ clearTimeout(t); res(); } });
});

/* ---------- QR scan (jsQR, works on iOS Safari too) ---------- */
async function startScan(video, onCode){
  const stream = await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}, width:{ideal:1280}}});
  video.srcObject = stream; video.setAttribute('playsinline',''); await video.play();
  const cv = document.createElement('canvas'), cx = cv.getContext('2d', {willReadFrequently:true}); let stop = false;
  (async () => { while(!stop){
    await new Promise(r => setTimeout(r, 120));
    if(video.readyState < 2 || !video.videoWidth) continue;
    const w = Math.min(video.videoWidth, 800), h = Math.round(w * video.videoHeight / video.videoWidth);
    cv.width = w; cv.height = h; cx.drawImage(video, 0, 0, w, h);
    const q = jsQR(cx.getImageData(0,0,w,h).data, w, h, {inversionAttempts:'dontInvert'});
    if(q && /^QB1/.test(q.data)){ stop = true; onCode(q.data); }
  } })();
  return () => { stop = true; stream.getTracks().forEach(t => t.stop()); video.srcObject = null; };
}
function qrSvg(text){ const q = qrcode(0,'L'); q.addData(text,'Byte'); q.make(); return q.createSvgTag({scalable:true, margin:3}); }

/* ---------- UI ---------- */
function openSync(){
  if(document.getElementById('qs-ov')) return;
  let pc = null, stopScan = null;
  const ov = document.createElement('div'); ov.id = 'qs-ov';
  ov.style.cssText = 'position:fixed;inset:0;z-index:100000;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;padding:14px;';
  ov.innerHTML = '<div id="qs-box" style="background:var(--card-bg,#fff);color:var(--text,#111);border:1px solid var(--card-border,#ddd);border-radius:var(--radius,14px);width:min(440px,100%);max-height:92vh;overflow:auto;padding:16px;box-shadow:var(--shadow-md);"></div>';
  document.body.appendChild(ov);
  const box = ov.firstChild, B = 'btn';
  const btn = (id, label, ghost) => `<button id="${id}" class="${B}${ghost?' btn-ghost':''}" style="width:100%;margin-top:8px;">${label}</button>`;
  const $ = id => box.querySelector('#'+id);
  const close = () => { if(stopScan) stopScan(); if(pc) try{ pc.close(); }catch(e){} ov.remove(); };
  const show = (title, body) => { if(stopScan){ stopScan(); stopScan = null; } box.innerHTML = `<div style="font-weight:800;font-size:1.05rem;margin-bottom:8px;">${title}</div>${body}${btn('qs-x','Close',true)}`; $('qs-x').onclick = close; };
  const fail = e => { console.warn('sync', e); show('Sync problem', `<p class="muted" style="font-size:.85rem;">${String(e && e.message || e)}</p><p class="muted" style="font-size:.78rem;">Tip: put both devices on the same Wi-Fi and try again. Strict mobile-data networks can block direct connections — use Backup &amp; Restore as a fallback.</p>` + btn('qs-r','Start over')); $('qs-r').onclick = home; };
  const pasteBox = (cb, label) => `<details style="margin-top:10px;font-size:.8rem;"><summary class="muted">No camera? Paste the code instead</summary><textarea id="qs-ta" rows="3" style="width:100%;margin-top:6px;font-size:.7rem;"></textarea>${btn('qs-go', label)}</details>`;
  const wirePaste = cb => { const g = $('qs-go'); if(g) g.onclick = () => cb($('qs-ta').value); };
  const copyBtn = code => { $('qs-cp').onclick = () => { navigator.clipboard && navigator.clipboard.writeText(code); $('qs-cp').textContent = 'Copied ✓'; }; };
  const scanner = async (cb) => { const v = $('qs-v'); try{ stopScan = await startScan(v, c => cb(c)); }catch(e){ v.outerHTML = '<div class="muted" style="font-size:.82rem;">Camera unavailable — allow camera access, or paste the code below.</div>'; } };

  function wire(p, onUp){
    p.onconnectionstatechange = () => { if(p.connectionState === 'failed') fail('Could not connect directly between the two devices.'); };
    return dc => {
      dc.binaryType = 'arraybuffer';
      dc.onopen = async () => {
        show('Syncing…', '<div id="qs-p" class="muted" style="font-size:.9rem;">Comparing data…</div>');
        try{
          const st = await runSync(dc, s => { const e = $('qs-p'); if(e) e.textContent = `Received ${s.recv} · sent ${s.sent}`; });
          await flushMeta();
          while(dc.bufferedAmount) await new Promise(r => setTimeout(r, 100));
          await new Promise(r => setTimeout(r, 800));
          show('Synced ✓', `<p style="font-size:.9rem;">Received <b>${st.recv}</b> change${st.recv===1?'':'s'}, sent <b>${st.sent}</b>.</p>` + (st.recv ? '<p class="muted" style="font-size:.8rem;">Reloading to show the new data…</p>' : ''));
          try{ pc.close(); }catch(e){}
          if(st.recv) setTimeout(() => location.reload(), 1400);
        }catch(e){ fail(e); }
      };
    };
  }

  async function host(){ // Device A: show offer, then read the answer
    show('Step 1 · Show this code', '<div class="muted" style="font-size:.85rem;">Preparing…</div>');
    try{
      pc = new RTCPeerConnection({iceServers: ICE});
      const adopt = wire(pc); const dc = pc.createDataChannel('qb'); adopt(dc);
      await pc.setLocalDescription(await pc.createOffer()); await gathered(pc);
      const code = await packCode(pc.localDescription);
      show('Step 1 · Show this code', `<div style="background:#fff;padding:6px;border-radius:10px;">${qrSvg(code)}</div><p class="muted" style="font-size:.8rem;">On the other device: Settings → Sync devices → <b>Scan a code</b>. It will then show a reply code.</p>` + btn('qs-cp','Copy code instead',true) + btn('qs-n','Next: scan its reply'));
      copyBtn(code);
      $('qs-n').onclick = () => {
        show('Step 2 · Scan the reply', '<video id="qs-v" muted style="width:100%;border-radius:10px;background:#000;"></video>' + pasteBox(null, 'Connect'));
        const finish = async c => { try{ show('Connecting…', '<div class="muted">Linking devices…</div>'); await pc.setRemoteDescription(await unpackCode(c)); }catch(e){ fail(e); } };
        scanner(finish); wirePaste(finish);
      };
    }catch(e){ fail(e); }
  }
  function join(){ // Device B: scan offer, show answer
    show('Scan the code', '<video id="qs-v" muted style="width:100%;border-radius:10px;background:#000;"></video>' + pasteBox(null, 'Continue'));
    const got = async c => {
      try{
        show('Preparing reply…', '<div class="muted">One moment…</div>');
        pc = new RTCPeerConnection({iceServers: ICE}); const adopt = wire(pc); pc.ondatachannel = e => adopt(e.channel);
        await pc.setRemoteDescription(await unpackCode(c)); await pc.setLocalDescription(await pc.createAnswer()); await gathered(pc);
        const code = await packCode(pc.localDescription);
        show('Show this reply code', `<div style="background:#fff;padding:6px;border-radius:10px;">${qrSvg(code)}</div><p class="muted" style="font-size:.8rem;">Point it at the first device's camera (Step 2). Syncing starts automatically.</p>` + btn('qs-cp','Copy code instead',true));
        copyBtn(code);
      }catch(e){ fail(e); }
    };
    scanner(got); wirePaste(got);
  }
  function home(){
    show('Sync devices', '<p class="muted" style="font-size:.85rem;margin:0 0 4px;">Directly links two devices (phone ↔ PC ↔ iPad) with a QR code and merges everything: question progress, resume-able tests, flashcards, planner, settings and decks. Nothing goes through a server. Repeat any time — only changes move.</p>' + btn('qs-h','Show a code (start here)') + btn('qs-j','Scan a code from another device', true));
    $('qs-h').onclick = host; $('qs-j').onclick = join;
  }
  home();
}
window.openQBankSync = openSync;
})();
