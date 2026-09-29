"use client";
import { useState } from "react";
import dynamic from "next/dynamic";
import { MapPin } from "lucide-react";

const DeliveryPicker = dynamic(() => import("@/components/DeliveryPicker"), { ssr: false });

// «Доставка и оплата»: общая карта пунктов выдачи всех служб — посмотреть, что рядом, до заказа
export default function DeliveryMapButton() {
  const [open, setOpen] = useState(false);
  // the map bundle (~1 MB: Leaflet + MapLibre) is loaded only once the buyer asks for it
  const [mounted, setMounted] = useState(false);
  return (
    <>
      <button type="button" onClick={() => { setMounted(true); setOpen(true); }} className="mv-dl-big" style={{ marginTop: 16 }}>
        <span className="mv-dl-big-ic"><MapPin size={22} /></span>
        <span className="mv-dl-big-t"><b>Пункты выдачи на карте</b><span>СДЭК и Яндекс Доставка рядом с вами — адреса и часы работы</span></span>
      </button>
      {mounted && <DeliveryPicker open={open} onClose={() => setOpen(false)} items={[]} goodsRub={0} browse />}
    </>
  );
}
