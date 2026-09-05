import { useState, useEffect, useCallback } from "react";

const SHOP_API = import.meta.env.VITE_SHOP_API_BASE || "";

async function getVapidKey(): Promise<string> {
  const res = await fetch(`${SHOP_API}/api/shop/push/vapid-key`);
  if (!res.ok) throw new Error("Push not configured");
  const data = await res.json();
  return data.key as string;
}

function urlBase64ToUint8Array(base64String: string): ArrayBuffer {
  const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(base64);
  const arr = Uint8Array.from([...raw].map((c) => c.charCodeAt(0)));
  return arr.buffer as ArrayBuffer;
}

async function subscribeSW(vapidKey: string): Promise<PushSubscription> {
  const reg = await navigator.serviceWorker.ready;
  return reg.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: urlBase64ToUint8Array(vapidKey),
  });
}

async function sendSubToServer(sub: PushSubscription, token: string | null) {
  const json = sub.toJSON();
  await fetch(`${SHOP_API}/api/shop/push/subscribe`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({ endpoint: json.endpoint, keys: json.keys }),
  });
}

async function removeSubFromServer(sub: PushSubscription) {
  const json = sub.toJSON();
  await fetch(`${SHOP_API}/api/shop/push/subscribe`, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ endpoint: json.endpoint }),
  });
}

export type PushPermission = "default" | "granted" | "denied" | "unsupported";

export interface UsePushReturn {
  isSupported: boolean;
  permission: PushPermission;
  isSubscribed: boolean;
  isLoading: boolean;
  subscribe: (token?: string | null) => Promise<void>;
  unsubscribe: () => Promise<void>;
}

export function usePush(): UsePushReturn {
  const isSupported =
    typeof window !== "undefined" &&
    "Notification" in window &&
    "serviceWorker" in navigator &&
    "PushManager" in window;

  const [permission, setPermission] = useState<PushPermission>(
    isSupported ? (Notification.permission as PushPermission) : "unsupported"
  );
  const [isSubscribed, setIsSubscribed] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  // Check existing subscription on mount
  useEffect(() => {
    if (!isSupported) return;
    let cancelled = false;
    navigator.serviceWorker.ready.then((reg) =>
      reg.pushManager.getSubscription()
    ).then((sub) => {
      if (!cancelled) setIsSubscribed(Boolean(sub));
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [isSupported]);

  // Register service worker once
  useEffect(() => {
    if (!isSupported) return;
    navigator.serviceWorker.register("/sw.js", { scope: "/" }).catch(() => {});
  }, [isSupported]);

  const subscribe = useCallback(async (token?: string | null) => {
    if (!isSupported || isLoading) return;
    setIsLoading(true);
    try {
      // Request permission
      const result = await Notification.requestPermission();
      setPermission(result as PushPermission);
      if (result !== "granted") return;

      const vapidKey = await getVapidKey();
      const sub = await subscribeSW(vapidKey);
      await sendSubToServer(sub, token ?? null);
      setIsSubscribed(true);
    } catch (err) {
      console.warn("[push] subscribe failed:", err);
    } finally {
      setIsLoading(false);
    }
  }, [isSupported, isLoading]);

  const unsubscribe = useCallback(async () => {
    if (!isSupported || isLoading) return;
    setIsLoading(true);
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.getSubscription();
      if (sub) {
        await removeSubFromServer(sub);
        await sub.unsubscribe();
      }
      setIsSubscribed(false);
    } catch (err) {
      console.warn("[push] unsubscribe failed:", err);
    } finally {
      setIsLoading(false);
    }
  }, [isSupported, isLoading]);

  return { isSupported, permission, isSubscribed, isLoading, subscribe, unsubscribe };
}
