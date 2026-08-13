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
   ═══════════════════════════════════════════════════════════════════════════ */
'use strict';

const ROUTE = '__p2pstream';

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

  const order = [];
  try{ if(e.clientId){ const c = await self.clients.get(e.clientId); if(c) order.push(c); } }catch{}
  try{
    const all = await self.clients.matchAll({ type:'window', includeUncontrolled:true });
    for(const c of all) if(!order.some(x => x.id === c.id)) order.push(c);
  }catch{}
  if(!order.length) return new Response('The page serving this file is no longer open.', { status:503 });

  for(const client of order){
    const r = await attempt(client, token, e.request);
    if(r) return r;                       // null = that page does not own it
  }
  return new Response('That stream is no longer available.', { status:404 });
}

// One offer to one page. Resolves to a Response, or to null when the page
// disowns the token so the caller can try the next window.
async function attempt(client, token, request){
  const mc = new MessageChannel();
  let controller = null, finished = false, held = false;
  let want = 0, sent = 0;               // what the headers promised, what arrived
  /* Byte-counted queue with a real high-water mark, and a brake wired to it.

     Without this the worker enqueues everything the drive sends regardless of
     whether anything is reading, so a large span streamed to a slow consumer
     piles up here in full. `desiredSize` going negative is the signal to tell
     the page — and through it the drive — to hold off.

     Kept bounded deliberately. This queue buys nothing but smoothness: the span
     itself is the read-ahead, and every byte parked here is resident memory on
     a machine that may not have much.

     8 MB rather than 4: the round trip that lifts a hold is worker → page →
     drive → back, and on anything but a LAN that is long enough for the queue
     to run dry before the first byte returns. Twice the buffer is one more
     quarter-second of playback to cover it with, and it is still a fixed cost
     that does not grow with the length of the film. */
  const HOLD_AT = 8 * 1024 * 1024;
  const check = () => {
    const room = controller ? controller.desiredSize : 0;
    if(!held && room <= 0){ held = true; try{ mc.port1.postMessage({ type:'hold' }); }catch{} }
    else if(held && room > HOLD_AT / 2){ held = false; try{ mc.port1.postMessage({ type:'go' }); }catch{} }
  };
  const body = new ReadableStream({
    start(c){ controller = c; },
    pull(){ check(); },
    // The player seeking, or the viewer closing, cancels the body — tell the
    // page so it can stop the drive mid-span instead of paying for the rest.
    cancel(){
      finished = true;
      try{ mc.port1.postMessage({ type:'cancel' }); }catch{}
      try{ mc.port1.close(); }catch{}
    }
  }, new ByteLengthQueuingStrategy({ highWaterMark: HOLD_AT }));

  const head = await new Promise(resolve => {
    const timer = setTimeout(() => resolve({ ok:false, status:504, msg:'The page did not answer in time.' }), 25000);
    mc.port1.onmessage = ev => {
      const d = ev.data || {};
      if(d.type === 'head'){ clearTimeout(timer); if(d.ok) want = Number(d.length) || 0; resolve(d); return; }
      if(d.type === 'chunk'){
        if(!finished && controller){
          /* `off` lets the page hand over the buffer it received rather than a
             copy of the interesting part of it.

             Frames off a data channel carry a four-byte request id in front of
             the payload, and stripping it with slice() copied every byte of
             every film through a second allocation on the page's heap before
             it was transferred here. A view onto the transferred buffer costs
             nothing and says the same thing. */
          const u8 = d.off ? new Uint8Array(d.buf, d.off) : new Uint8Array(d.buf);
          sent += u8.length;
          try{ controller.enqueue(u8); }catch{}
          check();
        }
        return;
      }
      if(d.type === 'end'){
        /* A body that closes short of its Content-Length is worse than an
           error: the browser takes the truncated bytes as the whole answer,
           the player decodes garbage at the join, and the only visible symptom
           is a picture that jumps. Say it failed instead — the player then
           re-requests the range, which is recoverable. */
        if(!finished && controller){
          if(want && sent < want) try{ controller.error(new Error('short read')); }catch{}
          else try{ controller.close(); }catch{}
        }
        finished = true;
        try{ mc.port1.close(); }catch{}
        return;
      }
      if(d.type === 'error'){
        clearTimeout(timer);
        if(!finished && controller) try{ controller.error(new Error(d.msg || 'stream failed')); }catch{}
        finished = true;
        resolve({ ok:false, status:502, msg:d.msg || 'The drive could not send that file.' });
      }
    };
    try{
      client.postMessage({ type:'p2p-open', token, range: request.headers.get('range') || '' }, [mc.port2]);
    }catch(err){
      // A window that has gone away since matchAll() listed it. Not an error —
      // the next one in the list may well own this token.
      clearTimeout(timer);
      resolve({ ok:false, notmine:true });
    }
  });

  // This page has never heard of the token. Say nothing and let another window
  // answer; only the caller knows whether one is left to ask.
  if(head && head.notmine){ try{ mc.port1.close(); }catch{} return null; }

  /* The page handed over the bytes themselves, as a Blob.

     This is the fast path, and it is enormously faster: a file that already
     lives in this browser needs no pipe at all. Chunking it through a
     MessagePort meant a postMessage, a queue push and a backpressure decision
     for every few hundred kilobytes — all of it JavaScript standing between the
     player and bytes the browser already had. Handing over the Blob instead
     lets the response stream straight out of the browser's own store at native
     speed, with no per-chunk work anywhere and nothing on the heap: a Blob
     slice is a lazy view, not a copy. */
  if(head && head.ok && head.blob){
    try{ mc.port1.close(); }catch{}
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
    try{ mc.port1.close(); }catch{}
    // A range that starts past the end of the file has its own status, and the
    // player needs the real length back or it cannot correct itself.
    if(head && head.status === 416){
      return new Response(null, { status:416, statusText:'Range Not Satisfiable',
        headers:{ 'Content-Range': `bytes */${Number(head.total) || 0}`, 'Accept-Ranges':'bytes' } });
    }
    return new Response(head && head.msg ? head.msg : 'Not found', { status: (head && head.status) || 404 });
  }

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
