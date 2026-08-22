/* ═══════════════════════════════════════════════════════════════════════════
   STREAM WORKER — Multi-Gigabyte Hardened Pipeline
   ═══════════════════════════════════════════════════════════════════════════ */
'use strict';

const ROUTE = '__p2pstream';

const HOLD_AT = 96 * 1024 * 1024;      
const RESUME_AT = 48 * 1024 * 1024;    
const BODY_WATER = 16 * 1024 * 1024;   
const AHEAD_WATER = 16 * 1024 * 1024;  
const MAX_SPAN_CHUNK = 32 * 1024 * 1024; 

// FIXED: Increased TTL and timeouts for massive files and deep seeks in P2P swarms
const AHEAD_TTL = 60000;               
const AHEAD_MAX = 4;
// Increased from 25000 to 60000 to prevent 504 errors on deep seeks
const HEAD_TIMEOUT = 60000;

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));

self.addEventListener('fetch', e => {
  let url;
  try{ url = new URL(e.request.url); }catch{ return; }
  if(url.origin !== self.location.origin) return;
  if(!url.pathname.endsWith('/' + ROUTE) && !url.pathname.endsWith(ROUTE)) return;
  e.respondWith(serve(e));
});

function rangeStartOf(h){
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(h || '').trim());
  if(!m || m[1] === '') return null;
  // FIXED: parseInt returns a double-precision float, safe up to 9 Petabytes.
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
    onIdle: null,
    idleFired: false,
    waker: null 
  };

  let settle = null;
  sp.head = new Promise(res => { settle = res; });
  let settled = false;
  const timer = setTimeout(() => {
    // This now waits 60 seconds before triggering a 504 Gateway Timeout
    finishHead({ ok:false, status:504, msg:'The page did not answer in time.' });
  }, HEAD_TIMEOUT);

  function finishHead(h){
    if(settled) return;
    settled = true;
    clearTimeout(timer);
    settle(h);
  }

  const wake = () => { 
    if (sp.waker) {
      sp.waker();
      sp.waker = null;
    }
  };

  const goneIdle = () => {
    if(sp.idleFired || sp.dead || !sp.onIdle) return;
    sp.idleFired = true;
    try{ sp.onIdle(); }catch(e){}
  };
  sp.checkIdle = () => { if(sp.held || sp.ended) goneIdle(); };

  const brake = () => {
    if(sp.dead) return;
    if(!sp.held && sp.queued >= sp.water){
      sp.held = true;
      try{ sp.port.postMessage({ type:'hold' }); }catch{}
    } else if(sp.held && sp.queued <= RESUME_AT){
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
      // Number() explicitly casts to float, avoiding bitwise 32-bit traps
      if(d.ok) sp.want = Number(d.length) || 0;
      finishHead(d);
      return;
    }
    if(d.type === 'chunk'){
      if(sp.dead) return;
      const u8 = d.off ? new Uint8Array(d.buf, d.off) : new Uint8Array(d.buf);
      if(!u8.length) return;
      
      const node = { c: u8, next: null };
      if (sp.qTail) sp.qTail.next = node;
      else sp.qHead = node;
      sp.qTail = node;
      
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
    finishHead({ ok:false, notmine:true });
  }

  sp.next = async () => {
    while(!sp.qHead){
      if(sp.failed) throw sp.failed;
      if(sp.ended || sp.dead) return null;
      await new Promise(res => { sp.waker = res; });
    }
    
    const node = sp.qHead;
    sp.qHead = node.next;
    if (!sp.qHead) sp.qTail = null;
    
    sp.queued -= node.c.length;
    return node.c;
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

async function serve(e){
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
      const r = await respond(slot.sp, slot.client, token);
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
    const r = await respond(Span(client, token, range), client, token);
    if(r) return r;
  }
  return new Response('That stream is no longer available.', { status:404 });
}

async function respond(sp, client, token){
  const head = await sp.head;

  if(head && head.notmine){ sp.release(); return null; }

  if(head && head.ok && head.blob){
    sp.release();
    const h = new Headers();
    h.set('Content-Type', head.mime || 'application/octet-stream');
    h.set('Accept-Ranges', 'bytes');
    h.set('Cache-Control', 'no-store');
    // Number explicitly handles files > 2.14 GB safely
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

  armAhead();
  sp.onIdle = promoteAhead;
  sp.checkIdle();

  const want = Number(head.length) || 0;
  let delivered = 0;
  
  const body = new ReadableStream({
    async pull(c){
      try{
        const chunk = await sp.next();
        if(chunk === null){
          if(want && delivered < want) c.error(new Error('short read'));
          else c.close();
          promoteAhead();
          return;
        }
        
        delivered += chunk.length;
        c.enqueue(chunk);

        while(sp.qHead && c.desiredSize > 0) {
          const node = sp.qHead;
          sp.qHead = node.next;
          if (!sp.qHead) sp.qTail = null;
          
          sp.queued -= node.c.length;
          delivered += node.c.length;
          c.enqueue(node.c);
        }
        
        if (!sp.held && sp.queued <= RESUME_AT) {
          try{ sp.port.postMessage({ type:'go' }); }catch{}
        }
        
      }catch(err){
        try{ c.error(err); }catch{}
      }
    },
    cancel(){ sp.cancel(); }
  }, new ByteLengthQueuingStrategy({ highWaterMark: BODY_WATER }));

  const h = new Headers();
  h.set('Content-Type', head.mime || 'application/octet-stream');
  h.set('Accept-Ranges', 'bytes');
  h.set('Cache-Control', 'no-store');
  h.set('Content-Length', String(Number(head.length)));
  if(head.partial){
    h.set('Content-Range', `bytes ${head.start}-${head.end}/${Number(head.total)}`);
    return new Response(body, { status:206, statusText:'Partial Content', headers:h });
  }
  return new Response(body, { status:200, headers:h });
}
