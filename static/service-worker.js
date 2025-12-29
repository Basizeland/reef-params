self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open("reef-metrics-v1").then((cache) => cache.addAll([
      "/",
      "/static/style.css",
      "/static/logo.png",
      "/static/manifest.json"
    ]))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }
  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request))
  );
});
