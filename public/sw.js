// Service worker PWA. HTML SIEMPRE fresco (red, no-store) y auto-recarga las
// pestañas cuando se activa una versión nueva. CSS/JS/img: network-first.
// Siempre responde con un Response válido (nunca undefined) y deja pasar terceros.
const CACHE = 'cobrapro-v5';
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => { e.waitUntil((async () => {
  const keys = await caches.keys();
  await Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)));
  await self.clients.claim();
  const cs = await self.clients.matchAll({ type: 'window' });
  for (const c of cs) { try { c.navigate(c.url); } catch (e) {} }
})()); });
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  let url; try { url = new URL(req.url); } catch (err) { return; }
  if (url.origin !== self.location.origin) return;   // terceros (beacons, CDNs): los maneja el navegador
  if (url.pathname.startsWith('/api/')) return;       // API nunca por el SW
  const isHTML = req.mode === 'navigate' || (req.headers.get('accept') || '').includes('text/html');
  if (isHTML) {
    e.respondWith((async () => {
      try { return await fetch(req, { cache: 'no-store' }); }
      catch (err) { return (await caches.match(req)) || new Response('<h1>Sin conexión</h1>', { status: 503, headers: { 'Content-Type': 'text/html; charset=utf-8' } }); }
    })());
    return;
  }
  e.respondWith((async () => {
    try {
      const res = await fetch(req);
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(req, copy)).catch(() => {});
      return res;
    } catch (err) {
      return (await caches.match(req)) || new Response('', { status: 504 });
    }
  })());
});
