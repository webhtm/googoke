/* ═══════════════════════════════════════════════════════════════════════════
   STREAM WORKER — turns a drive into something the browser can range-read.

   A <video src> cannot be fed from a WebRTC data channel, and a blob: URL only
   exists once every byte has already arrived. So the page hands out a URL that
   points here instead, and this worker answers it like a real HTTP server:
   Accept-Ranges, 206 responses, Content-Range. The bytes themselves come from
   the page over a MessagePort, one chunk at a time, and are piped straight into
   the response body — nothing is ever held whole.

     <video src="…/__p2pstream?t=…">
            │  fetch (+ Range: bytes=…)
            ▼
        this worker ──postMessage──▶ page ──▶ drive (P2P host, or this device)
            ▲                          │
            └──── chunks back ─────────┘

   ── Why a request is answered with LESS than it asked for ──────────────────

   A media element opens with `Range: bytes=0-` and means it: answer that
   literally on a 900 MB film and the browser will pull all 900 MB into its own
   media buffer as fast as the link delivers it, because the loader reads ahead
   of the player and the backpressure of a service-worker stream does not reach
   back through it. That is where a gigabyte of resident memory came from, why
   playback stuttered, and why seeking made the picture jump: the buffer filled,
   the browser evicted from it, and the evicted ranges had to be fetched again
   from a stream that was still busy delivering the rest of the file.

   So a ranged request is answered with a bounded span — a real 206 whose
   Content-Range says exactly which bytes these are — and the player comes back
   for the next one when it wants it. This is what every real media server does,
   it is what keeps the cost of watching a film independent of its length, and
   it is the difference between playing on a TV box and killing the tab.

   The page decides the span (it knows the source); this worker only insists
   that what arrives matches what the headers promised.

   ── Read-ahead: why this worker fetches bytes nobody has asked for ─────────

   Bounding the span fixed the memory and left a hole in its place. A media
   element does not pipeline: it reads one 206 to the end, and only *then* asks
   for the next range. So at every span boundary the whole pipe went quiet —
   fetch dispatch, postMessage to the page, a request across the data channel,
   the host opening the file and decrypting its first block — and not one byte
   was in flight for any of it. Playback was a sawtooth: a burst of data, a
   stall, a burst, a stall. Bigger spans made the stalls rarer without making
   them shorter, and on a fast link the stall was most of the wall clock.

   That is the difference between this and a real streaming player, and it has
   nothing to do with bandwidth. YouTube is never waiting to be asked. It has
   the next piece before the player wants it, so there is no boundary to stall
   at — the bytes are already here.

   So this worker keeps a span warm. As soon as a span's headers are known, the
   *next* one is opened speculatively, on the guess that a player walking
   forward through a film will ask for the bytes that come after the ones it is
   reading. When the request for them arrives it is barely a request at all: the
   drive is already working, the headers are already known, and the bytes are
   already on their way.

   The timing is the whole trick, and getting it wrong is why an obvious version
   of this does nothing. Arm on the current span *finishing* and you have armed
   at the exact moment the player asks — the setup cost is still paid in full,
   just moved a few milliseconds earlier. It has to be armed early, while the
   current span still has most of its data to deliver.

   Which raises the obvious objection: a second span opened early competes for a
   link the player is already waiting on. It does not, because the expensive
   part of a span is not its bytes — it is the round trip, the file lookup and
   the first block decrypt, none of which use bandwidth. So a warm span is
   allowed to get itself started and buffer a token amount (AHEAD_WATER), then
   told to hold. It sits there having already paid the setup, costing nothing,
   until either the current span stops using the link — at which point it is
   promoted to a full buffer and allowed to run — or the player asks for it.

   Three things keep the guess honest:

     · It costs almost nothing to be wrong. A warm span holds AHEAD_WATER and
       no more, so a bad guess wastes that much transfer at worst — and the
       request that disagrees with it cancels it at once, which reaches the
       drive and stops it mid-span.
     · It never competes for bandwidth the player is waiting on, because it is
       parked the moment it has started itself.
     · One per file, a few files at most. More than that is a download wearing
       a costume.

   ═══════════════════════════════════════════════════════════════════════════ */
'use strict';

const ROUTE = '__p2pstream';

/* How much one span may buffer here before the drive is told to hold.

   This is the shock absorber between a drive that delivers in bursts and a
   player that consumes steadily, and it is also the cap on what a wrong
   read-ahead guess can cost. Every byte parked here is resident memory on a
   machine that may not have much, and the worst case is two spans — the one
   being read and the one being kept warm — so the real figure is twice this. */
const HOLD_AT = 8 * 1024 * 1024;
// Released at half, not at empty: a brake that lifts only when the buffer has
// run dry has already let the player starve for one round trip.
const RESUME_AT = HOLD_AT / 2;
// The response's own queue, on top of the span's. Small: it exists so a read
// does not have to wait a microtask for each chunk, not to hold anything.
const BODY_WATER = 1024 * 1024;
/* What a speculative span may buffer before it is told to hold.

   Deliberately small. Its job is to *have started* — request sent, file opened,
   first block decrypted, first bytes moving — not to get ahead. Everything past
   that point is bandwidth taken from the span the player is actually waiting
   on, and it is also the entire cost of guessing wrong. */
const AHEAD_WATER = 1024 * 1024;
// How long a warm span may sit unclaimed. A paused film, or a viewer closed
// without the player saying so, must not hold a drive open indefinitely.
const AHEAD_TTL = 30000;
// Warm spans are per file, because two videos playing at once must not keep
// evicting each other's guess. A handful is plenty; past that it is a download.
const AHEAD_MAX = 3;
const HEAD_TIMEOUT = 25000;

// Take over as soon as possible: a worker that only controls the *next* page
// load is useless to a viewer the user is opening right now.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));

self.addEventListener('fetch', e => {
  let url;
  try{ url = new URL(e.request.url); }catch{ return; }
  if(url.origin !== self.location.origin) return;
  if(!url.pathname.endsWith('/' + ROUTE) && !url.pathname.endsWith(ROUTE)) return;
  e.respondWith(serve(e));
});

/* Where a Range header starts, or null when that cannot be known here.

   Only used to decide whether a warm span is the one being asked for, so it is
   deliberately strict: a suffix range (`bytes=-500`) has no start without the
   file's length, and a malformed one has no start at all. Both answer null,
   which means "no match" — never a wrong match. The page does the real
   parsing, against the length it actually knows. */
function rangeStartOf(h){
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(h || '').trim());
  if(!m || m[1] === '') return null;
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

/* ── one span, in flight ───────────────────────────────────────────────────

   Owns the MessagePort to the page, buffers what arrives, and works the brake.
   It is deliberately separate from the Response it will eventually feed,
   because a speculative span exists before there is any request to attach it
   to — and has to behave identically once there is. */
function Span(client, token, range, water){
  const mc = new MessageChannel();
  const sp = {
    token, range,
    port: mc.port1,
    head: null,          // promise, replaced below
    queue: [], queued: 0,
    sent: 0,
    want: 0,             // what the head promised, once it has
    water: water || HOLD_AT,
    ended: false, failed: null, dead: false, held: false,
    onIdle: null,        // set by whoever adopts this span
    idleFired: false
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

  const wakers = [];
  const wake = () => { while(wakers.length) wakers.pop()(); };

  /* This span has stopped needing the link to itself — the drive was told to
     hold, or it ran out of bytes. Either way the warm span behind it can stop
     being polite and fill up. Fires at most once, and does nothing until
     somebody has adopted this span and supplied the hook. */
  const goneIdle = () => {
    if(sp.idleFired || sp.dead || !sp.onIdle) return;
    sp.idleFired = true;
    try{ sp.onIdle(); }catch(e){ /* a failed guess must never break the read */ }
  };
  sp.checkIdle = () => { if(sp.held || sp.ended) goneIdle(); };

  const brake = () => {
    if(sp.dead) return;
    if(!sp.held && sp.queued >= sp.water){
      sp.held = true;
      try{ sp.port.postMessage({ type:'hold' }); }catch{}
    } else if(sp.held && sp.queued <= sp.water / 2){
      sp.held = false;
      try{ sp.port.postMessage({ type:'go' }); }catch{}
    }
  };
  /* Raising the allowance is how a warm span is let off its leash — when the
     span in front of it goes quiet, or when the player claims it. Lowering it
     is never done: a span that has already buffered more than a new, smaller
     allowance would simply stop, and the bytes are paid for either way. */
  sp.setWater = n => {
    if(!(n > sp.water)) return;
    sp.water = n;
    brake();
  };

  mc.port1.onmessage = ev => {
    const d = ev.data || {};
    if(d.type === 'head'){
      // Known even on a span nobody has claimed yet, so the moment one is
      // adopted it can already say how far through itself it is.
      if(d.ok) sp.want = Number(d.length) || 0;
      finishHead(d);
      return;
    }
    if(d.type === 'chunk'){
      if(sp.dead) return;
      /* `off` lets the page hand over the buffer it received rather than a copy
         of the interesting part of it. Frames off a data channel carry a
         four-byte request id in front of the payload, and stripping it with
         slice() copied every byte of every film through a second allocation on
         the page's heap. A view onto the transferred buffer says the same thing
         and costs nothing. */
      const u8 = d.off ? new Uint8Array(d.buf, d.off) : new Uint8Array(d.buf);
      if(!u8.length) return;
      sp.queue.push(u8);
      sp.queued += u8.length;
      sp.sent += u8.length;
      brake();
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
  }catch(err){
    // A window that has gone away since matchAll() listed it. Not an error —
    // the next one in the list may well own this token.
    finishHead({ ok:false, notmine:true });
  }

  // The next buffered chunk, waiting for one if the drive has not sent it yet.
  // null means the span is complete; a throw means it failed part-way.
  sp.next = async () => {
    for(;;){
      if(sp.queue.length){
        const c = sp.queue.shift();
        sp.queued -= c.length;
        brake();
        return c;
      }
      if(sp.failed) throw sp.failed;
      if(sp.ended || sp.dead) return null;
      await new Promise(res => { wakers.push(res); });
    }
  };
  // Stop the drive mid-span. The page turns this into a real abort on the wire,
  // so an abandoned span stops costing bandwidth almost immediately.
  sp.cancel = () => {
    if(sp.dead) return;
    sp.dead = true;
    sp.queue = []; sp.queued = 0;
    try{ sp.port.postMessage({ type:'cancel' }); }catch{}
    try{ sp.port.close(); }catch{}
    finishHead({ ok:false, status:499, msg:'cancelled' });
    wake();
  };
  // Finished with, but nothing to stop — the blob path, or an error the page
  // has already reported. Closing without a cancel keeps the log honest.
  sp.release = () => {
    sp.dead = true;
    sp.queue = []; sp.queued = 0;
    try{ sp.port.close(); }catch{}
    wake();
  };
  return sp;
}

/* The warm spans: one per file, a few files at most.

   Keyed by token, and the offset has to match too before one is used — the same
   offset in a different file is not a hit. A single global slot would have done
   for one video, but two playing at once would then spend the whole time
   evicting each other's guess and getting the benefit of neither. */
const warm = new Map();   // token -> { token, at, sp, client, timer }

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
  // Oldest out first — Map iterates in insertion order.
  while(warm.size > AHEAD_MAX) forgetWarm(warm.keys().next().value);
}

/* ── which page owns this token ────────────────────────────────────────────

   A media element's request often carries no clientId, and one worker serves
   every tab on the origin. Asking whichever window happened to be first in the
   list is a coin toss: tokens are minted by the page that opened the file and
   live only there, so with two tabs open the wrong one answered "no such
   stream" and the video failed with a 404 that had nothing to do with the file.

   So the request is offered to each window in turn — the one the browser named
   first, if it named one. A page that does not hold the token says so (`notmine`)
   and costs one postMessage; the page that does answers with the head and
   serves the body. Only when every window has disowned it is it really gone. */
async function serve(e){
  const url = new URL(e.request.url);
  const token = url.searchParams.get('t') || '';
  const range = e.request.headers.get('range') || '';
  const start = rangeStartOf(range);

  /* Already here? Then the expensive part of this request has been paid: the
     drive is running, the headers are known, and the first bytes are buffered.
     The response begins in this tick with no round trip anywhere. */
  const slot = warm.get(token);
  if(slot){
    if(start !== null && slot.at === start && !slot.sp.dead){
      warm.delete(token);
      clearTimeout(slot.timer);
      // Claimed: it may stop rationing itself now.
      slot.sp.setWater(HOLD_AT);
      const r = await respond(slot.sp, slot.client, token);
      if(r) return r;
      // Unservable after all. Fall through and ask properly rather than
      // failing on the strength of a guess.
    } else {
      // A seek, or a range whose start cannot be known here. The guess is
      // wrong and the drive is still working on it.
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
    const r = await respond(Span(client, token, range), client, token);
    if(r) return r;                       // null = that page does not own it
  }
  return new Response('That stream is no longer available.', { status:404 });
}

// Build the Response for a span. Resolves to null when the page disowns the
// token, so the caller can try the next window.
async function respond(sp, client, token){
  const head = await sp.head;

  // This page has never heard of the token. Say nothing and let another window
  // answer; only the caller knows whether one is left to ask.
  if(head && head.notmine){ sp.release(); return null; }

  /* The page handed over the bytes themselves, as a Blob.

     This is the fast path, and it is enormously faster: a file that already
     lives in this browser needs no pipe at all. Chunking it through a
     MessagePort meant a postMessage, a queue push and a backpressure decision
     for every few hundred kilobytes — all of it JavaScript standing between the
     player and bytes the browser already had. Handing over the Blob instead
     lets the response stream straight out of the browser's own store at native
     speed, with no per-chunk work anywhere and nothing on the heap: a Blob
     slice is a lazy view, not a copy. Nothing to read ahead of, either. */
  if(head && head.ok && head.blob){
    sp.release();
    const h = new Headers();
    h.set('Content-Type', head.mime || 'application/octet-stream');
    h.set('Accept-Ranges', 'bytes');
    h.set('Cache-Control', 'no-store');
    h.set('Content-Length', String(head.length));
    if(head.partial){
      h.set('Content-Range', `bytes ${head.start}-${head.end}/${head.total}`);
      return new Response(head.blob, { status:206, statusText:'Partial Content', headers:h });
    }
    return new Response(head.blob, { status:200, headers:h });
  }

  if(!head || !head.ok){
    sp.release();
    // A range that starts past the end of the file has its own status, and the
    // player needs the real length back or it cannot correct itself.
    if(head && head.status === 416){
      return new Response(null, { status:416, statusText:'Range Not Satisfiable',
        headers:{ 'Content-Range': `bytes */${Number(head.total) || 0}`, 'Accept-Ranges':'bytes' } });
    }
    return new Response(head && head.msg ? head.msg : 'Not found', { status: (head && head.status) || 404 });
  }

  /* Open the span that comes after this one, on the guess that the player is
     walking forward through the file.

     Done now, as soon as this span's headers are known, rather than when this
     span finishes — see the read-ahead note at the top. Waiting until the end
     arms it at the same instant the player asks, which hides nothing at all.
     It is opened with a small allowance so that getting itself started is all
     it does until the link is free. */
  const armAhead = () => {
    if(sp.dead || warm.has(token)) return;
    if(!head.partial) return;                       // whole file: nothing follows
    const total = Number(head.total) || 0;
    const next  = Number(head.end) + 1;
    if(!total || !Number.isFinite(next) || next <= 0 || next >= total) return;
    const sp2 = Span(client, token, 'bytes=' + next + '-', AHEAD_WATER);
    const slot = { token, at: next, sp: sp2, client, timer: 0 };
    slot.timer = setTimeout(() => { if(warm.get(token) === slot) forgetWarm(token); }, AHEAD_TTL);
    keepWarm(slot);
    /* A guess the page cannot serve is worse than no guess: left in the map it
       would make the real request wait on it and then fail. Drop it the moment
       the answer is known to be unusable. */
    const bad = () => { if(warm.get(token) === slot) forgetWarm(token); };
    sp2.head.then(h => { if(!h || !h.ok) bad(); }, bad);
  };
  /* This span has gone quiet — held, or done. Whatever is warm behind it may
     now use the link properly instead of sitting on its token allowance. */
  const promoteAhead = () => {
    const s = warm.get(token);
    if(s && s.sp !== sp) try{ s.sp.setWater(HOLD_AT); }catch{}
  };
  armAhead();
  sp.onIdle = promoteAhead;
  sp.checkIdle();          // it may already be held, or already finished

  const want = Number(head.length) || 0;
  let delivered = 0;
  const body = new ReadableStream({
    async pull(c){
      try{
        const chunk = await sp.next();
        if(chunk === null){
          /* A body that closes short of its Content-Length is worse than an
             error: the browser takes the truncated bytes as the whole answer,
             the player decodes garbage at the join, and the only visible
             symptom is a picture that jumps. Say it failed instead — the player
             then re-requests the range, which is recoverable. */
          if(want && delivered < want) c.error(new Error('short read'));
          else c.close();
          promoteAhead();          // this span is finished with the link
          return;
        }
        delivered += chunk.length;
        c.enqueue(chunk);
      }catch(err){
        try{ c.error(err); }catch{}
      }
    },
    /* The player seeking, or the viewer closing, cancels the body — tell the
       page so it can stop the drive mid-span instead of paying for the rest.

       The warm span is deliberately left alone. Chrome routinely cancels a
       response it has read enough of and comes back for the next range moments
       later, so throwing the read-ahead away here would give up the benefit in
       exactly the case it was built for. A guess that really is stale is
       discarded by the next request that disagrees with it. */
    cancel(){ sp.cancel(); }
  }, new ByteLengthQueuingStrategy({ highWaterMark: BODY_WATER }));

  const h = new Headers();
  h.set('Content-Type', head.mime || 'application/octet-stream');
  h.set('Accept-Ranges', 'bytes');
  // Nothing is cached anywhere: the bytes are only ever a span the player asked
  // for, and holding them would defeat the point of bounding the span.
  h.set('Cache-Control', 'no-store');
  h.set('Content-Length', String(head.length));
  if(head.partial){
    h.set('Content-Range', `bytes ${head.start}-${head.end}/${head.total}`);
    return new Response(body, { status:206, statusText:'Partial Content', headers:h });
  }
  return new Response(body, { status:200, headers:h });
}
