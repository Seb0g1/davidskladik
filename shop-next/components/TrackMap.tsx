"use client";
// Карта отслеживания: магазин, дом покупателя и курьер (плавно едет между обновлениями). Схема — наша (lib/mapStyle).
import "leaflet/dist/leaflet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import "@maplibre/maplibre-gl-leaflet";
import { useEffect, useRef } from "react";
import L from "leaflet";
import { DELIVERY_MAP_STYLE } from "@/lib/mapStyle";

type Pt = { lat: number; lng: number } | null | undefined;

const icon = (html: string, w: number, h: number, ax = w / 2, ay = h / 2) => L.divIcon({ className: "", html, iconSize: [w, h], iconAnchor: [ax, ay] });
const SHOP = icon(`<div style="width:34px;height:34px;border-radius:12px;background:#121212;color:#d9f84a;font:800 11px/34px Onest,sans-serif;text-align:center;box-shadow:0 4px 12px rgba(0,0,0,.3);border:2px solid #fff">MV</div>`, 34, 34);
const HOME = icon(`<svg width="38" height="50" viewBox="0 0 38 50"><path d="M19 1C9 1 1 9 1 19c0 13 18 30 18 30s18-17 18-30C37 9 29 1 19 1z" fill="#121212"/><path d="M11 20l8-7 8 7v8h-5v-5h-6v5h-5z" fill="#d9f84a"/></svg>`, 38, 50, 19, 49);
const courierIcon = (photo?: string | null) => icon(
  `<div style="position:relative;width:52px;height:52px"><div style="position:absolute;inset:0;border-radius:50%;background:rgba(75,60,255,.22);animation:mvpulse 1.8s ease-out infinite"></div>
   <div style="position:absolute;inset:6px;border-radius:50%;border:3px solid #fff;box-shadow:0 4px 14px rgba(0,0,0,.35);background:#4b3cff ${photo ? `url('${photo.replace(/'/g, "")}') center/cover` : ""};display:flex;align-items:center;justify-content:center">
   ${photo ? "" : `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><circle cx="5.5" cy="17.5" r="3.5"/><circle cx="18.5" cy="17.5" r="3.5"/><path d="M15 6h3l3 8M5.5 17.5L9 10h6l3.5 7.5"/></svg>`}</div></div>`, 52, 52);

export default function TrackMap({ origin, destination, courier, courierPhoto }: { origin: Pt; destination: Pt; courier: Pt; courierPhoto?: string | null }) {
  const div = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const mk = useRef<{ o?: L.Marker; d?: L.Marker; c?: L.Marker }>({});
  const userMoved = useRef(false);
  const anim = useRef<number | null>(null);

  useEffect(() => {
    if (!div.current || mapRef.current) return;
    const map = L.map(div.current, { center: [55.751, 37.618], zoom: 12, zoomControl: false, attributionControl: false });
    (L as unknown as { maplibreGL: (o: { style: unknown }) => L.Layer }).maplibreGL({ style: DELIVERY_MAP_STYLE }).addTo(map);
    L.control.zoom({ position: "topright" }).addTo(map);
    map.on("dragstart zoomstart", (e) => { if ((e as L.LeafletEvent & { originalEvent?: Event }).originalEvent) userMoved.current = true; });
    mapRef.current = map;
    const ro = new ResizeObserver(() => map.invalidateSize());
    ro.observe(div.current);
    return () => { ro.disconnect(); if (anim.current) cancelAnimationFrame(anim.current); map.remove(); mapRef.current = null; mk.current = {}; };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const put = (key: "o" | "d", p: Pt, ic: L.DivIcon) => {
      if (!p) { mk.current[key]?.remove(); mk.current[key] = undefined; return; }
      if (mk.current[key]) mk.current[key]!.setLatLng([p.lat, p.lng]);
      else mk.current[key] = L.marker([p.lat, p.lng], { icon: ic, zIndexOffset: key === "d" ? 500 : 0 }).addTo(map);
    };
    put("o", origin, SHOP);
    put("d", destination, HOME);
    if (courier) {
      const to = L.latLng(courier.lat, courier.lng);
      if (!mk.current.c) mk.current.c = L.marker(to, { icon: courierIcon(courierPhoto), zIndexOffset: 1000 }).addTo(map);
      else {
        // плавно довозим метку до новой точки за 1,2 с
        const from = mk.current.c.getLatLng();
        const t0 = performance.now();
        if (anim.current) cancelAnimationFrame(anim.current);
        const step = (t: number) => {
          const k = Math.min(1, (t - t0) / 1200), e = k * (2 - k);
          mk.current.c?.setLatLng([from.lat + (to.lat - from.lat) * e, from.lng + (to.lng - from.lng) * e]);
          if (k < 1) anim.current = requestAnimationFrame(step);
        };
        anim.current = requestAnimationFrame(step);
      }
    } else { mk.current.c?.remove(); mk.current.c = undefined; }
    if (!userMoved.current) {
      const pts = [courier, destination, courier ? null : origin].filter(Boolean) as { lat: number; lng: number }[];
      if (pts.length > 1) map.fitBounds(L.latLngBounds(pts.map((p) => [p.lat, p.lng] as [number, number])), { padding: [60, 60], maxZoom: 16 });
      else if (pts.length === 1) map.setView([pts[0].lat, pts[0].lng], 15);
    }
  }, [origin?.lat, origin?.lng, destination?.lat, destination?.lng, courier?.lat, courier?.lng, courierPhoto]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <>
      <style>{`@keyframes mvpulse{0%{transform:scale(.6);opacity:.9}100%{transform:scale(1.5);opacity:0}}`}</style>
      <div ref={div} style={{ position: "absolute", inset: 0 }} />
    </>
  );
}
