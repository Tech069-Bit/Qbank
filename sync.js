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
  const m = {}, recs = {}; const all = {};
  for(const st in IDB_KEYS) all[st] = await idbGetAll(st);
  // A brand-new device only holds auto-created defaults. Mark those as "oldest" so they never overwrite real data on the other device.
  const fresh = !all.resources.length && !all.history.length && !all.sessions.length;
  for(const k of BACKUP_LS_KEYS) if(localStorage.getItem(k) !== null) m['ls|'+k] = [fresh ? 0 : (meta.t['ls|'+k]||0), 0];
  for(const s in IDB_KEYS) for(const r of all[s]){ const id = 'idb|'+s+'|'+r[IDB_KEYS[s]]; m[id] = [meta.t[id]||0, 0]; recs[id] = r; }
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
    const mine = enabledKeys(), st = {recv:0, sent:0, expect:null, sentDone:false, recvIds:[], sentIds:[], skipped:[]};
    let eff = new Set(mine);
    let chain = Promise.resolve(), local;
    const ready = buildManifest().then(x => { local = x; return sendBundle(dc, {k:'man'}, JSON.stringify({m: x.m, cats: mine})); }).catch(reject);
    const check = () => { if(st.sentDone && st.expect !== null && st.recv >= st.expect) resolve(st); };
    dc.onmessage = makeReceiver((h, json, blob) => {
      chain = chain.then(async () => {
        await ready;
        if(h.k === 'man'){
          const rem = JSON.parse(dec.decode(json));
          eff = new Set(mine.filter(k => !rem.cats || rem.cats.includes(k)));
          const G = getGroups(); st.skipped = G.filter(g => !eff.has(g.key)).map(g => g.label);
          const ids = plan(local.m, rem.m).filter(id => { const k = catOf(id).key; return k === 'other' || eff.has(k); });
          await sendBundle(dc, {k:'plan', n: ids.length}); st.planned = ids.length; progress(st);
          for(const id of ids){
            const head = {k:'item', id, ts: local.m[id][0]}; let body = null, bl = null;
            if(local.m[id][1]) head.del = 1;
            else if(id.startsWith('ls|')) body = localStorage.getItem(id.slice(3));
            else{ const rec = Object.assign({}, local.recs[id]); if(rec.blob instanceof Blob){ bl = rec.blob; head.blob = 1; head.mime = bl.type; } delete rec.blob; body = JSON.stringify(rec); }
            await sendBundle(dc, head, body, bl); st.sent++; st.sentIds.push([id, head.del?1:0]); progress(st);
          }
          st.sentDone = true;
        }else if(h.k === 'item'){ const ck = catOf(h.id||'').key; if((ck === 'other' || eff.has(ck)) && await applyItem(h, json, blob)){ st.recv++; st.recvIds.push([h.id, h.del?1:0]); } progress(st); }
        else if(h.k === 'plan') { st.expect = h.n; progress(st); }
        check();
      }).catch(reject);
    });
  });
}


/* ---------- data categories (shared by the chooser, the sync filter and the summary) ---------- */
function getGroups(){
  const L = {
    LS_ACTIVE_SESSION: typeof LS_ACTIVE_SESSION!=='undefined'?LS_ACTIVE_SESSION:null, LS_INCOMPLETE_SESSIONS: typeof LS_INCOMPLETE_SESSIONS!=='undefined'?LS_INCOMPLETE_SESSIONS:null,
    LS_FLASHCARD_SRS: typeof LS_FLASHCARD_SRS!=='undefined'?LS_FLASHCARD_SRS:null, LS_FLASHCARD_SESSIONS: typeof LS_FLASHCARD_SESSIONS!=='undefined'?LS_FLASHCARD_SESSIONS:null,
    LS_FLASHCARD_DAILY_COUNTS: typeof LS_FLASHCARD_DAILY_COUNTS!=='undefined'?LS_FLASHCARD_DAILY_COUNTS:null, LS_ASRS_SETTINGS: typeof LS_ASRS_SETTINGS!=='undefined'?LS_ASRS_SETTINGS:null,
    LS_BOOKMARK_LISTS: typeof LS_BOOKMARK_LISTS!=='undefined'?LS_BOOKMARK_LISTS:null,
    LS_STREAK: typeof LS_STREAK!=='undefined'?LS_STREAK:null, LS_LEVEL: typeof LS_LEVEL!=='undefined'?LS_LEVEL:null, LS_GARDEN: typeof LS_GARDEN!=='undefined'?LS_GARDEN:null,
    LS_ACHIEVEMENTS: typeof LS_ACHIEVEMENTS!=='undefined'?LS_ACHIEVEMENTS:null, LS_DAILY: typeof LS_DAILY!=='undefined'?LS_DAILY:null,
    LS_GOAL_PROGRESS: typeof LS_GOAL_PROGRESS!=='undefined'?LS_GOAL_PROGRESS:null, LS_GOAL_DAYS_HIT: typeof LS_GOAL_DAYS_HIT!=='undefined'?LS_GOAL_DAYS_HIT:null,
    SP_LS_KEY: typeof SP_LS_KEY!=='undefined'?SP_LS_KEY:null, LS_SP_TASK_DONE: typeof LS_SP_TASK_DONE!=='undefined'?LS_SP_TASK_DONE:null,
    LS_TOPIC_PROGRESS: typeof LS_TOPIC_PROGRESS!=='undefined'?LS_TOPIC_PROGRESS:null, LS_TOPIC_TRACKER: typeof LS_TOPIC_TRACKER!=='undefined'?LS_TOPIC_TRACKER:null,
    LS_EXAM_COUNTDOWNS: typeof LS_EXAM_COUNTDOWNS!=='undefined'?LS_EXAM_COUNTDOWNS:null, LS_STUDY_SESSIONS: typeof LS_STUDY_SESSIONS!=='undefined'?LS_STUDY_SESSIONS:null,
    LS_STUDY_EXTRA_TASKS: typeof LS_STUDY_EXTRA_TASKS!=='undefined'?LS_STUDY_EXTRA_TASKS:null, LS_STUDY_WATER: typeof LS_STUDY_WATER!=='undefined'?LS_STUDY_WATER:null,
    LS_RESOURCE_FOLDERS: typeof LS_RESOURCE_FOLDERS!=='undefined'?LS_RESOURCE_FOLDERS:null, LS_QBANK_RESOURCE_ORDER: typeof LS_QBANK_RESOURCE_ORDER!=='undefined'?LS_QBANK_RESOURCE_ORDER:null,
    LS_RECYCLE_BIN: typeof LS_RECYCLE_BIN!=='undefined'?LS_RECYCLE_BIN:null,
    LS_USER_PROFILE: typeof LS_USER_PROFILE!=='undefined'?LS_USER_PROFILE:null, LS_SETTINGS: typeof LS_SETTINGS!=='undefined'?LS_SETTINGS:null
  };
  const ls = (...n) => n.map(x => L[x]).filter(Boolean);
  return [
    {key:'banks',  label:'Question banks & decks',   idb:['resources'], ls:[]},
    {key:'media',  label:'Images & audio',           idb:['media'],     ls:[]},
    {key:'history',label:'Answer history',           idb:['history'],   ls:[]},
    {key:'sessions',label:'Saved quiz sessions',     idb:['sessions'],  ls:[]},
    {key:'resume', label:'Unfinished tests (resume)',idb:[], ls:ls('LS_ACTIVE_SESSION','LS_INCOMPLETE_SESSIONS')},
    {key:'cards',  label:'Flashcards & revision',    idb:[], ls:ls('LS_FLASHCARD_SRS','LS_FLASHCARD_SESSIONS','LS_FLASHCARD_DAILY_COUNTS','LS_ASRS_SETTINGS')},
    {key:'marks',  label:'Bookmarks',                idb:[], ls:ls('LS_BOOKMARK_LISTS')},
    {key:'xp',     label:'Streak, XP & achievements',idb:[], ls:ls('LS_STREAK','LS_LEVEL','LS_GARDEN','LS_ACHIEVEMENTS','LS_DAILY','LS_GOAL_PROGRESS','LS_GOAL_DAYS_HIT')},
    {key:'plan',   label:'Study planner & topics',   idb:[], ls:ls('SP_LS_KEY','LS_SP_TASK_DONE','LS_TOPIC_PROGRESS','LS_TOPIC_TRACKER','LS_EXAM_COUNTDOWNS','LS_STUDY_SESSIONS','LS_STUDY_EXTRA_TASKS','LS_STUDY_WATER')},
    {key:'folders',label:'Folders & recycle bin',    idb:[], ls:ls('LS_RESOURCE_FOLDERS','LS_QBANK_RESOURCE_ORDER','LS_RECYCLE_BIN')},
    {key:'profile',label:'Profile',                  idb:[], ls:ls('LS_USER_PROFILE')},
    {key:'settings',label:'Settings',                idb:[], ls:ls('LS_SETTINGS')}
  ];
}
function catOf(id){
  const G = getGroups();
  for(const g of G){
    if(id.startsWith('ls|') && g.ls.includes(id.slice(3))) return g;
    if(id.startsWith('idb|') && g.idb.some(s => id.startsWith('idb|'+s+'|'))) return g;
  }
  return {key:'other', label:'Other app data'};
}
const CHOICE_KEY = 'qb_sync_choice'; // device-local preference (not itself synced)
function enabledKeys(){
  const all = getGroups().map(g => g.key);
  try{ const raw = localStorage.getItem(CHOICE_KEY); if(raw){ const a = JSON.parse(raw); if(Array.isArray(a)) return all.filter(k => a.includes(k)); } }catch(e){}
  return all;
}
function saveChoice(keys){ try{ origSet.call(localStorage, CHOICE_KEY, JSON.stringify(keys)); }catch(e){} }

/* ---------- human-readable summary of what moved ---------- */
function categorize(list){
  const out = {};
  list.forEach(([id, del]) => { const l = catOf(id).label; const o = out[l] || (out[l] = {n:0, del:0}); o.n++; if(del) o.del++; });
  return out;
}
function summaryHtml(title, list){
  if(!list.length) return '';
  const c = categorize(list);
  return `<div style="text-align:left;margin-top:10px;"><div class="muted" style="font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px;">${title}</div>` +
    Object.keys(c).map(l => `<div style="display:flex;justify-content:space-between;gap:8px;font-size:.84rem;padding:4px 0;border-bottom:1px solid var(--card-border,#eee);"><span><i class="fa-solid fa-check" style="color:var(--green-status,#16a34a);margin-right:6px;"></i>${l}</span><span class="muted">${c[l].n - c[l].del ? (c[l].n - c[l].del) + (c[l].del ? ' · ' + c[l].del + ' removed' : '') : c[l].del + ' removed'}</span></div>`).join('') + '</div>';
}

/* ---------- compact SDP codes (internal only — users never see these) ---------- */
const b64 = u => { let s = ''; for(let i = 0; i < u.length; i += 8192) s += String.fromCharCode.apply(null, u.subarray(i,i+8192)); return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); };
const unb64 = s => Uint8Array.from(atob(s.replace(/-/g,'+').replace(/_/g,'/')), c => c.charCodeAt(0));
const pipe = async (u, S) => new Uint8Array(await new Response(new Blob([u]).stream().pipeThrough(new S('deflate-raw'))).arrayBuffer());
async function packCode(desc){
  const sdp = desc.sdp.split('\r\n').filter(l => !(/^a=candidate/.test(l) && / tcp /i.test(l))).join('\r\n');
  const raw = enc.encode(sdp), z = window.CompressionStream;
  return 'QB1' + (z ? 'z' : 'r') + (desc.type === 'offer' ? 'o' : 'a') + '.' + b64(z ? await pipe(raw, CompressionStream) : raw);
}
async function unpackCode(code){
  const m = /^QB1([zr])([oa])\.([\w-]+)$/.exec(code.trim()); if(!m) throw new Error('Invalid pairing data.');
  let u = unb64(m[3]);
  if(m[1] === 'z'){ if(!window.DecompressionStream) throw new Error('This browser is too old for QR sync — please update it.'); u = await pipe(u, DecompressionStream); }
  return {type: m[2] === 'o' ? 'offer' : 'answer', sdp: dec.decode(u)};
}
const gathered = pc => new Promise(res => {
  if(pc.iceGatheringState === 'complete') return res();
  const t = setTimeout(res, 3500);
  pc.addEventListener('icegatheringstatechange', () => { if(pc.iceGatheringState === 'complete'){ clearTimeout(t); res(); } });
});

/* ---------- tiny signalling relay: the QR holds only a short room id.
   The two devices swap their (data-free) connection details through a public pub/sub topic
   (ntfy.sh), then talk directly. So only ONE device has to scan. Override with window.QB_SIGNAL. ---------- */
const SIGNAL = () => (window.QB_SIGNAL || 'https://ntfy.sh').replace(/\/$/, '');
const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const newRoom = () => Array.from(crypto.getRandomValues(new Uint8Array(12)), x => ALPHA[x % ALPHA.length]).join('');
const topicUrl = room => SIGNAL() + '/qbsync-' + room;
function parseRoom(text){
  let t = String(text || '').toUpperCase().replace(/[\s-]/g, '');
  if(t.length === 15 && t.startsWith('QB2')) t = t.slice(3);
  return /^[A-Z0-9]{12}$/.test(t) ? t : null;
}
const prettyRoom = r => r.replace(/(.{4})(?=.)/g, '$1-');
async function publish(room, msg){
  const r = await fetch(topicUrl(room), {method:'POST', body: msg});
  if(!r.ok) throw new Error('Could not reach the sync relay (' + r.status + ').');
}
async function fetchOffer(room){
  const r = await fetch(topicUrl(room) + '/json?poll=1&since=all'); if(!r.ok) throw new Error('Could not reach the sync relay (' + r.status + ').');
  let found = null;
  (await r.text()).split('\n').forEach(l => { try{ const d = JSON.parse(l); if(d.event === 'message' && /^O:/.test(d.message)) found = d.message.slice(2); }catch(e){} });
  return found;
}
function listenAnswer(room, cb){
  const es = new EventSource(topicUrl(room) + '/sse');
  es.onmessage = e => { try{ const d = JSON.parse(e.data); if(d.event === 'message' && /^A:/.test(d.message)) cb(d.message.slice(2)); }catch(x){} };
  return es;
}

/* ---------- QR scan (jsQR, works on iOS Safari too) ---------- */
async function startScan(video, onRoom){
  const stream = await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}, width:{ideal:1280}}});
  video.srcObject = stream; video.setAttribute('playsinline',''); await video.play();
  const cv = document.createElement('canvas'), cx = cv.getContext('2d', {willReadFrequently:true}); let stop = false;
  (async () => { while(!stop){
    await new Promise(r => setTimeout(r, 110));
    if(video.readyState < 2 || !video.videoWidth) continue;
    const w = Math.min(video.videoWidth, 720), h = Math.round(w * video.videoHeight / video.videoWidth);
    cv.width = w; cv.height = h; cx.drawImage(video, 0, 0, w, h);
    const q = jsQR(cx.getImageData(0,0,w,h).data, w, h, {inversionAttempts:'dontInvert'});
    const room = q && /^QB2/.test(q.data) ? parseRoom(q.data) : null;
    if(room){ stop = true; onRoom(room); }
  } })();
  return () => { stop = true; stream.getTracks().forEach(t => t.stop()); video.srcObject = null; };
}
function qrSvg(text){ const q = qrcode(0,'M'); q.addData(text,'Byte'); q.make(); return q.createSvgTag({scalable:true, margin:2}); }

/* ---------- UI ---------- */
function openSync(){
  if(document.getElementById('qs-ov')) return;
  let pc = null, es = null, stopScan = null, timers = [], busy = false;
  const ov = document.createElement('div'); ov.id = 'qs-ov';
  ov.style.cssText = 'position:fixed;inset:0;z-index:100000;background:rgba(0,0,0,.55);display:flex;align-items:center;justify-content:center;padding:14px;';
  ov.innerHTML = '<div id="qs-box" style="background:var(--card-bg,#fff);color:var(--text,#111);border:1px solid var(--card-border,#ddd);border-radius:var(--radius,14px);width:min(340px,100%);max-height:92vh;overflow:auto;padding:16px;box-shadow:var(--shadow-md);text-align:center;"></div>';
  document.body.appendChild(ov);
  const box = ov.firstChild, $ = id => box.querySelector('#'+id);
  const btn = (id, label, ghost) => `<button id="${id}" class="btn${ghost?' btn-ghost':''}" style="width:100%;margin-top:8px;">${label}</button>`;
  const later = (fn, ms) => { const t = setTimeout(fn, ms); timers.push(t); return t; };
  const cleanup = () => { timers.forEach(clearTimeout); timers = []; if(es){ es.close(); es = null; } if(stopScan){ stopScan(); stopScan = null; } };
  const dropPc = () => { if(pc && !busy){ try{ pc.close(); }catch(e){} pc = null; } };
  const close = () => { busy = false; cleanup(); if(pc) try{ pc.close(); }catch(e){} ov.remove(); };
  const show = (title, body, noClose) => { cleanup(); box.innerHTML = `<div style="font-weight:800;font-size:1.05rem;margin-bottom:8px;">${title}</div>${body}${noClose ? '' : btn('qs-x','Close',true)}`; const x = $('qs-x'); if(x) x.onclick = close; };
  const fail = e => { console.warn('sync', e); busy = false; dropPc(); show('Couldn’t sync', `<p class="muted" style="font-size:.85rem;margin:4px 0;">${String(e && e.message || e)}</p><p class="muted" style="font-size:.76rem;margin:4px 0;">Check both devices are online (same Wi-Fi works best), then try again.</p>` + btn('qs-r','Try again')); $('qs-r').onclick = host; };

  const chooserHtml = () => { const on = new Set(enabledKeys()); const G = getGroups();
    return `<details id="qs-ch" style="margin-top:8px;text-align:left;font-size:.8rem;"><summary class="muted" style="cursor:pointer;text-align:center;"><i class="fa-solid fa-sliders"></i> Choose what to sync <span id="qs-chn">(${on.size}/${G.length})</span></summary>
      <div style="margin-top:6px;">` + G.map(g => `<label style="display:flex;align-items:center;gap:8px;padding:5px 2px;cursor:pointer;"><input type="checkbox" data-k="${g.key}" ${on.has(g.key)?'checked':''}> ${g.label}</label>`).join('') +
      `<div style="display:flex;gap:8px;margin-top:6px;"><button type="button" class="btn btn-ghost" id="qs-all" style="flex:1;font-size:.75rem;">All</button><button type="button" class="btn btn-ghost" id="qs-none" style="flex:1;font-size:.75rem;">None</button></div>
      <div class="muted" style="font-size:.7rem;margin-top:6px;">Only items ticked on <b>both</b> devices are synced.</div></div></details>`; };
  const wireChooser = () => { const d = $('qs-ch'); if(!d) return;
    const boxes = () => Array.from(d.querySelectorAll('input[data-k]'));
    const save = () => { const k = boxes().filter(b => b.checked).map(b => b.dataset.k); saveChoice(k); $('qs-chn').textContent = `(${k.length}/${boxes().length})`; };
    boxes().forEach(b => b.onchange = save);
    $('qs-all').onclick = () => { boxes().forEach(b => b.checked = true); save(); };
    $('qs-none').onclick = () => { boxes().forEach(b => b.checked = false); save(); }; };
  const spinner = t => `<div class="muted" style="font-size:.88rem;padding:10px 0;"><i class="fa-solid fa-spinner fa-spin"></i> ${t}</div>`;

  function wire(p){
    p.onconnectionstatechange = () => { if(!busy && (p.connectionState === 'failed')) fail('Could not connect directly between the two devices.'); };
    return dc => {
      dc.binaryType = 'arraybuffer';
      let finished = false;
      dc.onopen = async () => {
        busy = true; timers.forEach(clearTimeout); timers = []; if(es){ es.close(); es = null; }
        show('Syncing…', '<div style="height:8px;border-radius:6px;background:var(--track,#eee);overflow:hidden;margin:10px 0 8px;"><div id="qs-bar" style="height:100%;width:6%;background:var(--accent,#0f766e);transition:width .2s;"></div></div><div id="qs-p" class="muted" style="font-size:.85rem;">Comparing data…</div>', true);
        try{
          const st = await runSync(dc, s => {
            const e = $('qs-p'), b = $('qs-bar'); if(!e) return;
            if(s.expect === null){ e.textContent = 'Comparing data…'; return; }
            e.textContent = s.expect ? `Receiving ${Math.min(s.recv, s.expect)} of ${s.expect}…` : 'Nothing new to receive…';
            if(b) b.style.width = Math.max(8, Math.round(100 * (s.expect ? s.recv / s.expect : 1))) + '%';
          });
          finished = true;
          await flushMeta();
          while(dc.bufferedAmount) await new Promise(r => setTimeout(r, 100));
          await new Promise(r => setTimeout(r, 700));
          try{ pc.close(); }catch(e){}
          busy = false;
          const head = st.recv ? `Updated ${st.recv} item${st.recv===1?'':'s'} on this device` : 'Already up to date';
          show('Synced ✓', `<div style="font-size:2rem;color:var(--green-status,#16a34a);line-height:1.1;">✓</div><p style="font-size:.9rem;margin:6px 0 0;font-weight:700;">${head}</p>`
            + summaryHtml('Received on this device', st.recvIds) + summaryHtml('Sent to the other device', st.sentIds)
            + (!st.recv && !st.sent ? '<p class="muted" style="font-size:.8rem;margin:6px 0 0;">Nothing to move — the selected data already matches.</p>' : '')
            + (st.skipped.length ? `<p class="muted" style="font-size:.74rem;margin:8px 0 0;text-align:left;">Not synced (turned off on a device): ${st.skipped.join(', ')}</p>` : '')
            + btn('qs-done', st.recv ? 'Done — refresh app' : 'Done'), true);
          $('qs-done').onclick = () => { if(st.recv) location.reload(); else close(); };
        }catch(e){ fail(e); }
      };
      dc.onclose = () => { if(busy && !finished && $('qs-bar')) fail('The connection dropped mid-sync. Nothing was lost — just try again.'); };
    };
  }

  /* default screen: this device shows a small QR; the OTHER device scans it (one scan connects both ways) */
  async function host(){
    busy = false; dropPc(); show('Sync devices', spinner('Getting ready…'));
    const room = newRoom();
    try{
      pc = new RTCPeerConnection({iceServers: ICE}); const my = pc;
      const adopt = wire(pc); adopt(pc.createDataChannel('qb'));
      await pc.setLocalDescription(await pc.createOffer()); await gathered(pc);
      await publish(room, 'O:' + await packCode(pc.localDescription));
      if(pc !== my || !document.getElementById('qs-ov')) return;
      show('Sync devices', `<p class="muted" style="font-size:.8rem;margin:0 0 8px;">On your other device open <b>Sync</b> and tap <b>Scan</b>, then point it here.</p>
        <div style="width:168px;height:168px;margin:0 auto;background:#fff;padding:4px;border-radius:10px;box-sizing:border-box;">${qrSvg('QB2' + room)}</div>
        <div style="font-family:monospace;letter-spacing:.06em;font-size:.82rem;margin:8px 0 2px;">${prettyRoom(room)}</div>
        <div id="qs-st" class="muted" style="font-size:.78rem;"><i class="fa-solid fa-circle-notch fa-spin"></i> Waiting for the other device…</div>` + chooserHtml() + btn('qs-s','<i class="fa-solid fa-camera"></i> Scan the other device instead', true));
      wireChooser(); $('qs-s').onclick = scan;
      let answered = false;
      es = listenAnswer(room, async a => {
        if(answered) return; answered = true; es && es.close(); es = null;
        const st = $('qs-st'); if(st) st.innerHTML = '<i class="fa-solid fa-circle-notch fa-spin"></i> Connecting…';
        try{ await my.setRemoteDescription(await unpackCode(a)); later(() => { if(!busy) fail('Connection timed out.'); }, 30000); }catch(e){ fail(e); }
      });
      later(host, 4.5 * 60 * 1000); // keep the QR fresh
    }catch(e){ fail(e.name === 'TypeError' ? 'No connection to the sync relay. Are you online?' : e); }
  }

  function scan(){
    dropPc();
    show('Scan the other device', '<video id="qs-v" muted style="width:100%;aspect-ratio:1;object-fit:cover;border-radius:10px;background:#000;"></video><div id="qs-hint" class="muted" style="font-size:.78rem;margin-top:6px;">Show Sync on the other device and point at its QR.</div>'
      + '<details style="margin-top:8px;font-size:.78rem;text-align:left;"><summary class="muted">No camera? Type the code</summary><input id="qs-in" placeholder="XXXX-XXXX-XXXX" autocapitalize="characters" autocomplete="off" style="width:100%;margin-top:6px;padding:8px;font-family:monospace;box-sizing:border-box;">' + btn('qs-go','Connect') + '</details>' + chooserHtml() + btn('qs-b','Back', true));
    wireChooser(); $('qs-b').onclick = host;
    $('qs-go').onclick = () => { const r = parseRoom($('qs-in').value); r ? join(r) : ($('qs-hint').textContent = 'That code doesn’t look right.'); };
    startScan($('qs-v'), join).then(stop => { if(document.getElementById('qs-v')) stopScan = stop; else stop(); }).catch(() => { const v = $('qs-v'); if(v) v.outerHTML = '<div class="muted" style="font-size:.82rem;padding:14px 0;">Camera unavailable — allow camera access, or type the code below.</div>'; });
  }

  async function join(room){
    show('Connecting…', spinner('Found it — linking devices…'), true);
    try{
      const offer = await fetchOffer(room);
      if(!offer) throw new Error('That code has expired. Open Sync again on the other device and rescan.');
      pc = new RTCPeerConnection({iceServers: ICE}); const adopt = wire(pc); pc.ondatachannel = e => adopt(e.channel);
      await pc.setRemoteDescription(await unpackCode(offer));
      await pc.setLocalDescription(await pc.createAnswer()); await gathered(pc);
      await publish(room, 'A:' + await packCode(pc.localDescription));
      later(() => { if(!busy) fail('Connection timed out.'); }, 30000);
    }catch(e){ fail(e.name === 'TypeError' ? 'No connection to the sync relay. Are you online?' : e); }
  }

  host();
}
window.openQBankSync = openSync;
})();
