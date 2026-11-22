/* ═══════════════════════════════════════════════════════════════════════════
   STREAM WORKER — turns a drive into something the browser can range-read.

   A <video src> cannot be fed from a WebRTC data channel, and a blob: URL only
   exists once every byte has already arrived. So the page hands out a URL that
   points here instead, and this worker answers it like a real HTTP server:
   Accept-Ranges, 206 responses, Content-Range. The bytes themselves come from
   the page over a MessagePort and are piped straight into the response body —
   nothing is ever held whole.

     <video src="…/__p2pstream?t=…">
            │  fetch (+ Range: bytes=…)
            ▼
        this worker ──postMessage──▶ page ──▶ drive (P2P host, or this device)
            ▲                          │
            └──── chunks back ─────────┘

   A ranged request is answered with a bounded span (the page decides how big),
   as an honest 206, so the cost of watching a film does not depend on its
   length. The page ramps the span up while playback is sequential and drops it
   on a seek.

   ── What this worker does to stay fast and stay up ──────────────────────────

   • Back-pressure that cannot wedge. A span is braked when its queue passes a
     high-water mark and released when the queue falls below half of *its own*
     mark. The release is re-evaluated every time a byte leaves the queue, so a
     drained queue always resumes the source — it never waits for one more chunk
     to arrive and notice.

   • Look-ahead that does not fight the span it is ahead of. The next span is
     opened only once the current one is half delivered, so the first span of a
     video gets the whole link and the picture starts sooner, and the one after
     it is usually already waiting when the player asks.

   • Look-ahead that is bounded. A warm span holds at its own water mark until
     it is promoted, rather than racing to the end of its range.

   • Kept alive for as long as a body is being delivered. A worker is stopped
     when it has no events; a stream the player is still reading must not count
     as idle. Each fetch holds the worker open until its body ends, with a
     ceiling so a lost page cannot pin it forever, and the page sends a
     heartbeat while any stream is live.

   • Cleans up after itself. A viewer that closes tells the worker to drop that
     token's warm spans, so the host stops sending them at once instead of when
     a timer notices.
   ═══════════════════════════════════════════════════════════════════════════ */
'use strict';

const ROUTE = '__p2pstream';

const HOLD_AT        = 96 * 1024 * 1024;   // brake a span whose queue reaches this
const RESUME_AT      = 48 * 1024 * 1024;   // ...and let it go below this (never above half its own mark)
const BODY_WATER     = 16 * 1024 * 1024;   // what the response body buffers ahead of its reader
const AHEAD_WATER    = 16 * 1024 * 1024;   // a warm span holds here until promoted
const MAX_SPAN_CHUNK = 32 * 1024 * 1024;   // longest look-ahead range asked for
const AHEAD_AT       = 0.5;                // open the look-ahead when this much of a span has arrived
const AHEAD_TTL      = 60000;              // a warm span nobody asks for is dropped after this
const AHEAD_MAX      = 4;                  // warm spans kept across all tokens
const HEAD_TIMEOUT   = 60000;              // the page must answer a span within this, or it is a 504
const KEEPALIVE_MAX  = 10 * 60 * 1000;     // longest one fetch pins the worker open

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil((async () => {
  // Navigation preload would start a network request for every page load that
  // this worker never uses.
  try{ if(self.registration.navigationPreload) await self.registration.navigationPreload.disable(); }catch{}
  await self.clients.claim();
})()));

/* Responses whose body is still being delivered. A fetch holds the worker open
   until its body is done; one that is answered whole has nothing left to wait
   for and lets go as soon as it is returned. */
const streaming = new WeakSet();

self.addEventListener('fetch', e => {
  let url;
  try{ url = new URL(e.request.url); }catch{ return; }
  if(url.origin !== self.location.origin) return;
  if(!url.pathname.endsWith('/' + ROUTE) && !url.pathname.endsWith(ROUTE)) return;

  let release;
  const alive = new Promise(res => { release = res; });
  const cap = setTimeout(release, KEEPALIVE_MAX);
  alive.then(() => clearTimeout(cap));
  e.waitUntil(alive);

  e.respondWith(serve(e, release).then(
    r => { if(!streaming.has(r)) release(); return r; },
    err => { release(); throw err; }
  ));
});

/* Messages from the page. A message is itself an event, which is what makes the
   heartbeat keep the worker alive; the others are housekeeping. */
self.addEventListener('message', e => {
  const d = e.data || {};
  if(d.type === 'skip-waiting') self.skipWaiting();
  else if(d.type === 'forget' && d.token) forgetWarm(String(d.token));
  else if(d.type === 'forget-all') for(const t of [...warm.keys()]) forgetWarm(t);
});

function rangeStartOf(h){
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(h || '').trim());
  if(!m || m[1] === '') return null;
  // parseInt yields a double, exact up to 2^53 — far past any file.
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

function Span(client, token, range, water){
  const mc = new MessageChannel();
  const sp = {
    token, range,
    port: mc.port1,
    head: null,
    qHead: null, qTail: null, queued: 0,
    sent: 0,
    want: 0,
    water: water || HOLD_AT,
    ended: false, failed: null, dead: false, held: false,
    onIdle: null, onProgress: null,
    idleFired: false,
    waker: null
  };

  let settle = null;
  sp.head = new Promise(res => { settle = res; });
  let settled = false;
  const timer = setTimeout(() => {
    finishHead({ ok:false, status:504, msg:'The page did not answer in time.' });
  }, HEAD_TIMEOUT);

  function finishHead(h){
    if(settled) return;
    settled = true;
    clearTimeout(timer);
    settle(h);
  }

  const wake = () => {
    const w = sp.waker;
    if(w){ sp.waker = null; w(); }
  };

  const goneIdle = () => {
    if(sp.idleFired || sp.dead || !sp.onIdle) return;
    sp.idleFired = true;
    try{ sp.onIdle(); }catch{}
  };
  sp.checkIdle = () => { if(sp.held || sp.ended) goneIdle(); };

  /* The brake. Resume is half of this span's own mark, capped at RESUME_AT:
     a resume level above the hold level releases the span on the very next
     chunk, so a short-marked span would never hold at all. */
  const brake = () => {
    if(sp.dead) return;
    if(!sp.held && sp.queued >= sp.water){
      sp.held = true;
      try{ sp.port.postMessage({ type:'hold' }); }catch{}
    } else if(sp.held && sp.queued <= Math.min(RESUME_AT, sp.water / 2)){
      sp.held = false;
      try{ sp.port.postMessage({ type:'go' }); }catch{}
    }
  };

  sp.setWater = n => {
    if(!(n > sp.water)) return;
    sp.water = n;
    brake();
  };

  mc.port1.onmessage = ev => {
    const d = ev.data || {};
    if(d.type === 'head'){
      if(d.ok) sp.want = Number(d.length) || 0;
      finishHead(d);
      return;
    }
    if(d.type === 'chunk'){
      if(sp.dead) return;
      const u8 = d.off ? new Uint8Array(d.buf, d.off) : new Uint8Array(d.buf);
      if(!u8.length) return;

      const node = { c: u8, next: null };
      if(sp.qTail) sp.qTail.next = node;
      else sp.qHead = node;
      sp.qTail = node;

      sp.queued += u8.length;
      sp.sent += u8.length;
      brake();
      if(sp.onProgress) sp.onProgress();
      sp.checkIdle();
      wake();
      return;
    }
    if(d.type === 'end'){
      sp.ended = true;
      try{ sp.port.close(); }catch{}
      sp.checkIdle();
      wake();
      return;
    }
    if(d.type === 'error'){
      sp.failed = new Error(d.msg || 'stream failed');
      sp.ended = true;
      finishHead({ ok:false, status:502, msg: d.msg || 'The drive could not send that file.' });
      wake();
      return;
    }
  };

  try{
    client.postMessage({ type:'p2p-open', token, range: range || '' }, [mc.port2]);
  }catch{
    finishHead({ ok:false, notmine:true });
  }

  /* Every byte that leaves the queue goes through here, so the brake is
     re-evaluated at the only moment it can have become releasable. */
  sp.take = () => {
    const node = sp.qHead;
    if(!node) return null;
    sp.qHead = node.next;
    if(!sp.qHead) sp.qTail = null;
    sp.queued -= node.c.length;
    brake();
    return node.c;
  };

  sp.next = async () => {
    while(!sp.qHead){
      if(sp.failed) throw sp.failed;
      if(sp.ended || sp.dead) return null;
      await new Promise(res => { sp.waker = res; });
    }
    return sp.take();
  };

  sp.cancel = () => {
    if(sp.dead) return;
    sp.dead = true;
    sp.qHead = null; sp.qTail = null; sp.queued = 0;
    try{ sp.port.postMessage({ type:'cancel' }); }catch{}
    try{ sp.port.close(); }catch{}
    finishHead({ ok:false, status:499, msg:'cancelled' });
    wake();
  };

  sp.release = () => {
    sp.dead = true;
    sp.qHead = null; sp.qTail = null; sp.queued = 0;
    try{ sp.port.close(); }catch{}
    wake();
  };
  return sp;
}

/* Spans opened ahead of the player, keyed by token. */
const warm = new Map();

function forgetWarm(token){
  const s = warm.get(token);
  if(!s) return;
  warm.delete(token);
  clearTimeout(s.timer);
  try{ s.sp.cancel(); }catch{}
}

function keepWarm(slot){
  forgetWarm(slot.token);
  warm.set(slot.token, slot);
  while(warm.size > AHEAD_MAX) forgetWarm(warm.keys().next().value);
}

async function serve(e, alive){
  const url = new URL(e.request.url);
  const token = url.searchParams.get('t') || '';
  const range = e.request.headers.get('range') || '';
  const start = rangeStartOf(range);

  const slot = warm.get(token);
  if(slot){
    if(start !== null && slot.at === start && !slot.sp.dead){
      warm.delete(token);
      clearTimeout(slot.timer);
      slot.sp.setWater(HOLD_AT);
      const r = await respond(slot.sp, slot.client, token, alive);
      if(r) return r;
    } else {
      forgetWarm(token);
    }
  }

  const order = [];
  try{ if(e.clientId){ const c = await self.clients.get(e.clientId); if(c) order.push(c); } }catch{}
  try{
    const all = await self.clients.matchAll({ type:'window', includeUncontrolled:true });
    for(const c of all) if(!order.some(x => x.id === c.id)) order.push(c);
  }catch{}
  if(!order.length) return new Response('The page serving this file is no longer open.', { status:503 });

  for(const client of order){
    const r = await respond(Span(client, token, range), client, token, alive);
    if(r) return r;
  }
  return new Response('That stream is no longer available.', { status:404 });
}

async function respond(sp, client, token, alive){
  const head = await sp.head;

  if(head && head.notmine){ sp.release(); return null; }

  // A span the page answered with a Blob (a file on this device): the browser
  // streams it out of its own store, nothing passes through here.
  if(head && head.ok && head.blob){
    sp.release();
    const h = new Headers();
    h.set('Content-Type', head.mime || 'application/octet-stream');
    h.set('Accept-Ranges', 'bytes');
    h.set('Cache-Control', 'no-store');
    h.set('X-Content-Type-Options', 'nosniff');
    h.set('Content-Length', String(Number(head.length)));
    if(head.partial){
      h.set('Content-Range', `bytes ${head.start}-${head.end}/${Number(head.total)}`);
      return new Response(head.blob, { status:206, statusText:'Partial Content', headers:h });
    }
    return new Response(head.blob, { status:200, headers:h });
  }

  if(!head || !head.ok){
    sp.release();
    if(head && head.status === 416){
      return new Response(null, { status:416, statusText:'Range Not Satisfiable',
        headers:{ 'Content-Range': `bytes */${Number(head.total) || 0}`, 'Accept-Ranges':'bytes' } });
    }
    return new Response(head && head.msg ? head.msg : 'Not found', { status: (head && head.status) || 404 });
  }

  const armAhead = () => {
    if(sp.dead || warm.has(token)) return;
    if(!head.partial) return;

    const total = Number(head.total) || 0;
    const next  = Number(head.end) + 1;
    if(!total || !Number.isFinite(next) || next <= 0 || next >= total) return;

    const nextEnd = Math.min(next + MAX_SPAN_CHUNK - 1, total - 1);
    const sp2 = Span(client, token, `bytes=${next}-${nextEnd}`, AHEAD_WATER);

    const slot = { token, at: next, sp: sp2, client, timer: 0 };
    slot.timer = setTimeout(() => { if(warm.get(token) === slot) forgetWarm(token); }, AHEAD_TTL);
    keepWarm(slot);
    const bad = () => { if(warm.get(token) === slot) forgetWarm(token); };
    sp2.head.then(h => { if(!h || !h.ok) bad(); }, bad);
  };

  const promoteAhead = () => {
    const s = warm.get(token);
    if(s && s.sp !== sp) try{ s.sp.setWater(HOLD_AT); }catch{}
  };

  const want = Number(head.length) || 0;
  let delivered = 0, armed = false, over = false;

  // The look-ahead opens once this span is half here — see the header.
  const arm = () => { if(armed) return; armed = true; armAhead(); };
  sp.onProgress = () => { if(want && sp.sent >= want * AHEAD_AT) arm(); };
  sp.onIdle = () => { arm(); promoteAhead(); };
  sp.checkIdle();
  sp.onProgress();

  const finish = () => {
    if(over) return;
    over = true;
    try{ alive && alive(); }catch{}
  };

  const body = new ReadableStream({
    async pull(c){
      try{
        const chunk = await sp.next();
        if(chunk === null){
          if(want && delivered < want) c.error(new Error('short read'));
          else c.close();
          promoteAhead();
          finish();
          return;
        }

        delivered += chunk.length;
        c.enqueue(chunk);

        // Top the body up while its reader has room, so one pull moves many
        // chunks instead of one per scheduling round trip. `take` re-evaluates
        // the brake for each, so a queue that drains here releases the source.
        while(sp.qHead && c.desiredSize > 0){
          const more = sp.take();
          delivered += more.length;
          c.enqueue(more);
        }
      }catch(err){
        try{ c.error(err); }catch{}
        // The source is still sending into a body that has failed.
        sp.cancel();
        finish();
      }
    },
    cancel(){ sp.cancel(); finish(); }
  }, new ByteLengthQueuingStrategy({ highWaterMark: BODY_WATER }));

  const h = new Headers();
  h.set('Content-Type', head.mime || 'application/octet-stream');
  h.set('Accept-Ranges', 'bytes');
  h.set('Cache-Control', 'no-store');
  h.set('X-Content-Type-Options', 'nosniff');
  h.set('Content-Length', String(Number(head.length)));
  let res;
  if(head.partial){
    h.set('Content-Range', `bytes ${head.start}-${head.end}/${Number(head.total)}`);
    res = new Response(body, { status:206, statusText:'Partial Content', headers:h });
  } else {
    res = new Response(body, { status:200, headers:h });
  }
  streaming.add(res);
  return res;
}
