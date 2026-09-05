// Magic Vibes Service Worker — handles Web Push notifications.
// Served at /sw.js (Vite copies public/ to dist root).

self.addEventListener("push", (event) => {
  if (!event.data) return;
  let data = {};
  try { data = event.data.json(); } catch { data = { title: "Magic Vibes", body: event.data.text() }; }

  const title = data.title || "Magic Vibes";
  const options = {
    body:    data.body  || "",
    icon:    data.icon  || "/icon-192.png",
    badge:   data.badge || "/badge-72.png",
    data:    { url: data.url || "/" },
    vibrate: [100, 50, 100],
    tag:     "magicvibes-promo",
    renotify: true,
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = event.notification.data?.url || "/";
  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((windowClients) => {
      // Focus existing tab if open
      for (const client of windowClients) {
        if (client.url.includes("magicvibes.ru") && "focus" in client) {
          client.navigate(url);
          return client.focus();
        }
      }
      return clients.openWindow(url);
    })
  );
});
