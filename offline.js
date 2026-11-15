const tag = 'pinefull-web-3';
const shell = ['./','index.html','config.js','links.js','pineapple.svg','island.css'];
self.addEventListener('install', event => {
  event.waitUntil(caches.open(tag).then(cache => cache.addAll(shell)));
  self.skipWaiting();
});
self.addEventListener('activate', event => {
  event.waitUntil(caches.keys().then(keys => Promise.all(keys.filter(key => key.startsWith('pinefull-web-') && key !== tag).map(key => caches.delete(key)))).then(() => self.clients.claim()));
});
self.addEventListener('message', event => {
  const files = event.data?.assets;
  if (!Array.isArray(files)) return;
  const allowed = files.filter(file => typeof file === 'string' && file.startsWith('Build/') && /^[a-zA-Z0-9_. -]+$/.test(file.slice(6)));
  event.waitUntil(caches.open(tag).then(cache => cache.addAll(allowed)));
});
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  if (event.request.method !== 'GET' || url.origin !== location.origin || url.pathname.endsWith('/signal')) return;
  event.respondWith(fetch(event.request).catch(async () => (await caches.match(event.request, { ignoreSearch: true })) ||
    (event.request.mode === 'navigate' ? await caches.match('./') : null) ||
    new Response('Offline', { status: 503 })));
});

