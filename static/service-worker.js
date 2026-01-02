self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open("reef-metrics-v2").then((cache) => cache.addAll([
      "/",
      "/static/style.css",
      "/static/logo.png",
      "/static/logo-light.png",
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

self.addEventListener("push", (event) => {
  if (!event.data) {
    return;
  }
  let payload = {};
  try {
    payload = event.data.json();
  } catch (err) {
    payload = { title: "Reef Metrics", body: event.data.text() };
  }
  const title = payload.title || "Reef Metrics";
  const options = {
    body: payload.body || "",
    data: { url: payload.url || "/" },
    tag: payload.tag || undefined
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = event.notification?.data?.url || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes(url) && "focus" in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(url);
      }
      return undefined;
    })
  );
});
