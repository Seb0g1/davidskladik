"use client";
import { useState, useEffect, useRef } from "react";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Check, AlertCircle, MapPin, ChevronLeft, Loader2, Lock, Tag, X, ChevronRight, Truck } from "lucide-react";
import { useCart } from "@/components/CartContext";
import { useAuth } from "@/components/AuthContext";
import type { ShopOrderPayload } from "@/lib/types";
import type { DeliverySelection, PickOption } from "@/components/DeliveryPicker";
import { CARRIER_LOGO, daysText, slotText } from "@/lib/delivery";
import { OzonPayLogo } from "@/components/OzonPay";
import { productImg } from "@/lib/img";
import ConsentCheck from "@/components/ConsentCheck";
import { CONSENT_VERSION } from "@/lib/legal";
import { ymGoal } from "@/lib/metrika";
import { loadAddresses, rememberAddress, rememberContact, forgetAddress, prettyAddress, type SavedAddress } from "@/lib/addresses";

const DeliveryPicker = dynamic(() => import("@/components/DeliveryPicker"), { ssr: false });

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";

const CARRIER_MARK: Record<string, string> = { cdek: "СДЭК", yandex: "Яндекс Доставка", dostavista: "Достависта", ozon: "Ozon" };

function Field({
  label, required, textarea, ...props
}: { label: string; required?: boolean; textarea?: boolean } & React.InputHTMLAttributes<HTMLInputElement | HTMLTextAreaElement>) {
  return (
    <label className="mv-co-field">
      <span className="mv-co-label">{label}{required && <i> *</i>}</span>
      {textarea
        ? <textarea {...(props as React.TextareaHTMLAttributes<HTMLTextAreaElement>)} rows={3} className="mv-co-input" />
        : <input {...(props as React.InputHTMLAttributes<HTMLInputElement>)} required={required} className="mv-co-input" />}
    </label>
  );
}

function Section({ n, title, children }: { n: string; title: string; children: React.ReactNode }) {
  return (
    <section className="mv-co-card">
      <h2 className="mv-co-h"><span className="mv-co-n">{n}</span>{title}</h2>
      {children}
    </section>
  );
}

export default function CheckoutPage() {
  const { items, totalRub, clear, hydrated } = useCart();
  const { customer, token } = useAuth();
  const router = useRouter();

  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [city, setCity] = useState(""); // только для доставки Ozon (цена по городу)
  const [comment, setComment] = useState("");
  const [pdConsent, setPdConsent] = useState(false);
  const [adsConsent, setAdsConsent] = useState(false);
  const paymentMethod = "ozon_pay" as const;
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [repriced, setRepriced] = useState<{ url: string; totalRub: number; was: number } | null>(null);
  // выбранная доставка: пункт или дом на карте (СДЭК / Яндекс / Достависта) либо Ozon (адрес — на странице оплаты)
  const [sel, setSel] = useState<DeliverySelection | null>(null);
  const [ozonChosen, setOzonChosen] = useState(false);
  const [picker, setPicker] = useState<null | "point" | "address">(null);
  // the map bundle (~1 MB) is fetched in the background after the page settles and mounted on first use
  const [pickerMounted, setPickerMounted] = useState(false);
  useEffect(() => { if (picker) setPickerMounted(true); }, [picker]);
  useEffect(() => { const t = setTimeout(() => { void import("@/components/DeliveryPicker"); }, 3000); return () => clearTimeout(t); }, []);
  const [savedBusy, setSavedBusy] = useState<string | null>(null);

  const [promoInput, setPromoInput] = useState("");
  const [promoValidating, setPromoValidating] = useState(false);
  const [promoResult, setPromoResult] = useState<{ valid: boolean; discountPct?: number; code?: string } | null>(null);
  const promoRef = useRef<HTMLInputElement>(null);

  // Saved addresses & contact (orders history + this browser), like Ozon / WB / Market
  const [saved, setSaved] = useState<SavedAddress[]>([]);
  const [showAllSaved, setShowAllSaved] = useState(false);

  // Delivery mode + tariff (davidsklad → Магазин → Настройки). With Ozon Delivery the buyer picks
  // the pickup point / courier address once, on the Ozon Pay form — the site doesn't ask for it.
  // Price comes from the server (davidsklad → Магазин → Настройки → «Доставка: правила»): flat, or by
  // the Ozon tariffs for the buyer's city — so with Ozon Delivery the site asks for the city only.
  const [dm, setDm] = useState<{ ozonDelivery: boolean; deliveryRub: number; needCity: boolean; cluster: string | null; freeDeliveryFrom: number } | null>(null);
  const [quoting, setQuoting] = useState(false);
  const ozonDelivery = dm?.ozonDelivery === true;
  useEffect(() => {
    let alive = true;
    loadAddresses(token).then(({ addresses, contact }) => {
      if (!alive) return;
      setSaved(addresses);
      if (contact) {
        setFirstName((v) => v || contact.firstName || "");
        setLastName((v) => v || contact.lastName || "");
        setPhone((v) => v || contact.phone || "");
        setEmail((v) => v || contact.email || "");
      }
      // последний адрес со службой доставки — сразу выбран (цена пересчитается)
      const last = addresses.find((a) => a.carrier && a.method);
      if (last) void applySaved(last);
    });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, hydrated]);

  // сохранённый пункт / адрес: пересчитываем цену заново (тарифы меняются), выбираем тот же способ
  async function applySaved(a: SavedAddress) {
    if (!a.carrier || !items.length) return;
    setSavedBusy(a.id);
    try {
      const body = a.type === "pickup" && a.pvzId
        ? { type: "point", carrier: a.carrier, id: a.pvzId }
        : { type: "address", city: a.city || a.region || "", region: a.region || "", full: [a.city, a.address].filter(Boolean).join(", "), lat: a.lat, lng: a.lng };
      const d = await (await fetch(`${BASE}/api/shop/checkout/quote`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity })), goodsRub: goods, target: body }),
      })).json();
      const list: PickOption[] = d.options || [];
      const o = list.find((x) => x.id === a.method) || list.find((x) => x.speed !== "slot") || null;
      if (!o) return;
      setOzonChosen(false);
      if (body.type === "point" && d.point) setSel({ mode: "point", option: o, point: d.point });
      else if (body.type === "address") setSel({
        mode: "address", option: o,
        place: { kind: "house", lat: a.lat || 0, lng: a.lng || 0, city: a.city || "", region: a.region || "", street: "", house: "", postcode: a.postalCode || "", title: a.address, subtitle: a.city || "", full: [a.city, a.address].filter(Boolean).join(", ") },
        details: { flat: a.flat || "", entrance: a.entrance || "", floor: a.floor || "", intercom: a.intercom || "", comment: a.addrComment || "" },
      });
    } catch { /* выберет на карте */ }
    finally { setSavedBusy(null); }
  }
  const activeSavedId = sel?.mode === "point" ? `pvz:${sel.point.id}` : sel?.mode === "address" ? `courier:${sel.place.title.trim().toLowerCase()}` : "";

  // Pre-fill from auth
  useEffect(() => {
    if (customer) {
      if (customer.firstName) setFirstName(customer.firstName);
      if (customer.lastName) setLastName(customer.lastName ?? "");
      if (customer.email) setEmail(customer.email);
      if (customer.phone) setPhone(customer.phone ?? "");
    }
  }, [customer]);

  // wait for the saved cart, and don't bounce to /cart after a successful order clears it
  const submittedRef = useRef(false);
  useEffect(() => {
    if (hydrated && items.length === 0 && !submittedRef.current) router.replace("/cart");
  }, [hydrated, items, router]);

  // referral code from a ?ref= link: the discount is shown only once the server confirmed the code
  // (a stale or mistyped code would otherwise promise 7% the order won't get and hide the promo field)
  const [refCode, setRefCode] = useState<string | undefined>(undefined);
  useEffect(() => {
    let stored = "";
    try { stored = localStorage.getItem("shopRefCode") || ""; } catch { /* private mode */ }
    if (!stored) return;
    let alive = true;
    fetch(`${BASE}/api/shop/referral/validate`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ code: stored }) })
      .then((r) => r.json())
      .then((d) => {
        if (!alive) return;
        if (d?.valid) setRefCode(stored.toUpperCase());
        else if (d?.ok) { try { localStorage.removeItem("shopRefCode"); } catch { /* ignore */ } }
      })
      .catch(() => {});
    return () => { alive = false; };
  }, []);
  // same rounding as the server: goods = round(sum × (1 − discount))
  const discountPct = refCode ? 7 : promoResult?.valid ? promoResult.discountPct ?? 0 : 0;
  const goods = Math.round(totalRub * (1 - discountPct / 100));
  const refDiscount = refCode ? totalRub - goods : 0;
  const promoDiscount = !refCode && promoResult?.valid ? totalRub - goods : 0;
  // same rule as the server (shopDeliveryRub): free from the threshold, counted on goods after discounts
  const quoteCity = sel?.mode === "point" ? sel.point.city : sel?.mode === "address" ? sel.place.city : city;
  const quoteKey = `${items.map((i) => `${i.product.offerId}:${i.quantity}`).join(",")}|${quoteCity.trim().toLowerCase()}|${goods}`;
  useEffect(() => {
    if (!items.length) return;
    let alive = true;
    setQuoting(true);
    const t = setTimeout(() => {
      fetch(`${BASE}/api/shop/checkout/delivery-mode`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity })), city: quoteCity, goodsRub: goods }),
      }).then((r) => r.json()).then((d) => { if (alive) setDm(d); })
        .catch(() => { if (alive) setDm((v) => v || { ozonDelivery: false, deliveryRub: 0, needCity: false, cluster: null, freeDeliveryFrom: 0 }); })
        .finally(() => { if (alive) setQuoting(false); });
    }, 350); // typing the city → one request
    return () => { alive = false; clearTimeout(t); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [quoteKey]);
  // the chosen point / address was priced for the cart at that moment: after a quantity change or a
  // promo code (free delivery threshold, insurance, weight) ask the server again, so the total shown
  // here is the one the order and the Ozon Pay form will have
  const [requoting, setRequoting] = useState(false);
  const cartKey = `${items.map((i) => `${i.product.offerId}:${i.quantity}`).join(",")}|${goods}`;
  const pricedKey = useRef("");
  const pricedSel = useRef("");
  const selKey = sel ? `${sel.option.id}|${sel.mode === "point" ? sel.point.id : sel.place.full}` : "";
  useEffect(() => {
    if (!sel) { pricedKey.current = ""; pricedSel.current = ""; return; }
    // a newly picked point / address was just priced for this very cart
    if (pricedSel.current !== selKey) { pricedSel.current = selKey; pricedKey.current = cartKey; return; }
    if (pricedKey.current === cartKey || !items.length) return;
    pricedKey.current = cartKey;
    let alive = true;
    setRequoting(true);
    const target = sel.mode === "point"
      ? { type: "point", carrier: sel.point.carrier, id: sel.point.id }
      : { type: "address", city: sel.place.city || sel.place.region, region: sel.place.region, full: sel.place.full, lat: sel.place.lat, lng: sel.place.lng };
    fetch(`${BASE}/api/shop/checkout/quote`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity })), goodsRub: goods, target }),
    }).then((r) => r.json()).then((d) => {
      if (!alive) return;
      const o: PickOption | undefined = (d.options || []).find((x: PickOption) => x.id === sel.option.id);
      if (o) setSel((s) => (s ? { ...s, option: o } as DeliverySelection : s));
      else { setSel(null); setError("Выбранная доставка для этой корзины недоступна — выберите способ ещё раз"); }
    }).catch(() => {}).finally(() => { if (alive) setRequoting(false); });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cartKey, selKey]);
  const deliveryRub = sel ? sel.option.priceRub : ozonChosen ? dm?.deliveryRub ?? 0 : 0;
  const toFree = dm && deliveryRub > 0 && dm.freeDeliveryFrom > 0 ? dm.freeDeliveryFrom - goods : 0;
  const total = goods + deliveryRub;

  async function applyPromo() {
    const code = promoInput.trim().toUpperCase();
    if (!code) return;
    setPromoValidating(true);
    try {
      const r = await fetch(`${BASE}/api/shop/promo/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code }),
      });
      const data = await r.json();
      setPromoResult(data.valid ? { valid: true, discountPct: data.discountPct, code: data.code } : { valid: false });
    } catch {
      setPromoResult({ valid: false });
    } finally {
      setPromoValidating(false);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    // Enter in a field submits the form even while the button is disabled
    if (loading || !valid) return;
    setLoading(true);
    setError(null);
    try {
      const useOzon = !sel && ozonChosen;
      const pt = sel?.mode === "point" ? sel.point : null;
      const pl = sel?.mode === "address" ? sel : null;
      const payload: ShopOrderPayload = {
        items: items.map(i => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
        delivery: {
          type: useOzon ? "ozon_pay" : pt ? "pickup" : "courier", firstName, lastName, phone, email,
          city: pt ? pt.city : pl ? (pl.place.city || pl.place.region) : city,
          ...(pt ? { region: pt.region, address: pt.address, pvzId: pt.id, pvzName: pt.kind === "postamat" ? "Постамат" : "Пункт выдачи" } : {}),
          ...(pl ? {
            region: pl.place.region, address: pl.place.title, postalCode: pl.place.postcode, lat: pl.place.lat, lng: pl.place.lng,
            flat: pl.details.flat, entrance: pl.details.entrance, floor: pl.details.floor, intercom: pl.details.intercom, addrComment: pl.details.comment,
            ...(pl.slot ? { dvFrom: pl.slot.from, dvTo: pl.slot.to } : {}),
          } : {}),
          ...(sel ? { carrier: sel.option.carrier, method: sel.option.id } : {}),
        },
        comment: comment || undefined,
        paymentMethod,
        promoCode: promoResult?.valid ? promoResult.code : undefined,
        refCode,
        consents: { pd: pdConsent, ads: adsConsent, version: CONSENT_VERSION },
      };
      const res = await fetch(`${BASE}/api/shop/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error ?? `Ошибка ${res.status}`);
      }
      const data = await res.json();
      if (pt && sel) rememberAddress({ type: "pickup", pvzId: pt.id, name: `${CARRIER_MARK[pt.carrier]} · ${pt.kind === "postamat" ? "постамат" : "пункт выдачи"}`, address: pt.address, city: pt.city, region: pt.region, carrier: pt.carrier, method: sel.option.id, lat: pt.lat, lng: pt.lng });
      if (pl && sel) rememberAddress({ type: "courier", address: pl.place.title, city: pl.place.city, region: pl.place.region, postalCode: pl.place.postcode, lat: pl.place.lat, lng: pl.place.lng, carrier: sel.option.carrier, method: sel.option.id, ...pl.details, addrComment: pl.details.comment });
      rememberContact({ firstName, lastName, phone, email });
      submittedRef.current = true;
      ymGoal("order", { order_price: data.totalRub ?? total, currency: "RUB" });
      clear();
      // prices are always recounted on the server: if the sum differs from the one on the button
      // (a price changed while the page was open), show it instead of silently charging another amount
      if (data.paymentUrl && typeof data.totalRub === "number" && Math.abs(data.totalRub - total) >= 1) {
        setRepriced({ url: data.paymentUrl, totalRub: data.totalRub, was: total });
        return;
      }
      if (data.paymentUrl) {
        window.location.href = data.paymentUrl;
      } else {
        router.push(`/order-success?id=${data.id ?? data.orderId ?? ""}`);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Не удалось оформить заказ");
    } finally {
      setLoading(false);
    }
  }

  if (repriced) {
    const fmt = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;
    return (
      <div className="mv-co">
        <div className="mv-co-wrap" style={{ maxWidth: 560 }}>
          <h1 className="mv-co-title">Сумма изменилась</h1>
          <section className="mv-co-card" style={{ marginTop: 24 }}>
            <p style={{ margin: 0, lineHeight: 1.6 }}>
              Пока вы оформляли заказ, изменилась цена товара или доставки. Заказ создан, к оплате — <b>{fmt(repriced.totalRub)}</b> вместо {fmt(repriced.was)}.
            </p>
            <a href={repriced.url} className="mv-co-paybtn" style={{ marginTop: 20, textDecoration: "none" }}>Оплатить {fmt(repriced.totalRub)}</a>
            <p style={{ margin: "14px 0 0", fontSize: 13, opacity: 0.6 }}>Если сумма не подходит, просто не оплачивайте: деньги не спишутся.</p>
          </section>
        </div>
      </div>
    );
  }

  if (!hydrated || items.length === 0) return <div className="mv-co" />;

  // tell the buyer why "Оплатить" is disabled instead of just fading it
  const missing = [
    !firstName.trim() && "имя",
    phone.replace(/\D/g, "").length < 10 && (phone ? "телефон полностью" : "телефон"),
    !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim()) && (email ? "email без ошибок" : "email"),
    !pdConsent && "согласие на обработку данных",
    !sel && !ozonChosen ? "способ доставки" : ozonChosen && !sel && dm?.needCity && !city.trim() ? "город" : false,
    (requoting || (ozonChosen && !sel && quoting)) && "расчёт доставки",
  ].filter(Boolean) as string[];
  const valid = missing.length === 0;
  const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

  return (
    <div className="mv-co">
      {pickerMounted && <DeliveryPicker
        open={!!picker}
        initialMode={picker || "point"}
        onClose={() => setPicker(null)}
        items={items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity }))}
        goodsRub={goods}
        start={sel?.mode === "address" && sel.place.lat ? { lat: sel.place.lat, lng: sel.place.lng } : sel?.mode === "point" ? { lat: sel.point.lat, lng: sel.point.lng } : null}
        onSelect={(s) => { setSel(s); setOzonChosen(false); setPicker(null); }}
      />}
      <div className="mv-co-wrap">
        <Link href="/cart" className="mv-co-back"><ChevronLeft size={16} /> Назад в корзину</Link>
        <h1 className="mv-co-title">Оформление</h1>
        <div className="mv-co-script">ещё пара шагов — и аромат ваш</div>

        <form onSubmit={handleSubmit} className="mv-co-grid">
          <div className="mv-co-main">
            <Section n="01" title="Контакты">
              <div className="mv-co-2col">
                <Field label="Имя" required value={firstName} onChange={e => setFirstName((e.target as HTMLInputElement).value)} placeholder="Иван" autoComplete="given-name" />
                <Field label="Фамилия" value={lastName} onChange={e => setLastName((e.target as HTMLInputElement).value)} placeholder="Иванов" autoComplete="family-name" />
                <Field label="Телефон" required type="tel" inputMode="tel" value={phone} onChange={e => setPhone((e.target as HTMLInputElement).value)} placeholder="+7 900 000-00-00" autoComplete="tel" />
                <Field label="Email" required type="email" inputMode="email" value={email} onChange={e => setEmail((e.target as HTMLInputElement).value)} placeholder="ivan@mail.ru" autoComplete="email" />
              </div>
            </Section>

            <Section n="02" title="Доставка">
              {sel ? (
                <div className="mv-dl-sel">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img className="mv-dl-logo" src={CARRIER_LOGO[sel.option.carrier]} alt={CARRIER_MARK[sel.option.carrier]} />
                  <div className="mv-dl-sel-t">
                    <b>{sel.mode === "point" ? `${sel.point.kind === "postamat" ? "Постамат" : "Пункт выдачи"} · ${sel.point.address}` : `${sel.place.title}${sel.details.flat ? `, кв. ${sel.details.flat}` : ""}`}</b>
                    <span>
                      {sel.mode === "point" ? [sel.point.city, sel.point.schedule].filter(Boolean).join(" · ")
                        : [sel.place.city, sel.details.entrance && `подъезд ${sel.details.entrance}`, sel.details.floor && `этаж ${sel.details.floor}`, sel.details.intercom && `домофон ${sel.details.intercom}`].filter(Boolean).join(", ")}
                    </span>
                    <span className="mv-dl-when">{sel.mode === "address" && sel.slot ? slotText(sel.slot) : daysText(sel.option)}</span>
                  </div>
                  <div className="mv-dl-sel-p">{sel.option.priceRub > 0 ? rub(sel.option.priceRub) : "Бесплатно"}</div>
                  <button type="button" className="mv-co-link mv-dl-change" onClick={() => setPicker(sel.mode === "point" ? "point" : "address")}>Изменить</button>
                </div>
              ) : ozonChosen ? (
                <div className="mv-dl-sel">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img className="mv-dl-logo" src={CARRIER_LOGO.ozon} alt="Ozon" />
                  <div className="mv-dl-sel-t">
                    <b>Ozon — пункт выдачи или курьер</b>
                    <span>Адрес выберете на странице оплаты Ozon Pay · 1–5 дн.</span>
                    {dm?.needCity && <input className="mv-co-input" style={{ marginTop: 8 }} value={city} onChange={(e) => setCity(e.target.value)} placeholder="Ваш город — для расчёта цены" aria-label="Город" />}
                  </div>
                  <div className="mv-dl-sel-p">{quoting ? "…" : dm && dm.deliveryRub > 0 ? rub(dm.deliveryRub) : "Бесплатно"}</div>
                  <button type="button" className="mv-co-link mv-dl-change" onClick={() => setOzonChosen(false)}>Изменить</button>
                </div>
              ) : (
                <div className="mv-dl-choose">
                  <button type="button" className="mv-dl-big" onClick={() => setPicker("point")}>
                    <span className="mv-dl-big-ic"><MapPin size={22} /></span>
                    <span className="mv-dl-big-t"><b>Пункт выдачи или постамат</b><span>Выберите на карте рядом с домом</span></span>
                    <span className="mv-dl-big-logos">
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img src={CARRIER_LOGO.cdek} alt="СДЭК" />{/* eslint-disable-next-line @next/next/no-img-element */}<img src={CARRIER_LOGO.yandex} alt="Яндекс Доставка" />
                    </span>
                    <ChevronRight size={20} className="mv-dl-big-go" />
                  </button>
                  <button type="button" className="mv-dl-big" onClick={() => setPicker("address")}>
                    <span className="mv-dl-big-ic"><Truck size={22} /></span>
                    <span className="mv-dl-big-t"><b>Курьером до двери</b><span>Отметьте дом на карте, по Москве — сегодня</span></span>
                    <span className="mv-dl-big-logos">
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img src={CARRIER_LOGO.dostavista} alt="Достависта" />{/* eslint-disable-next-line @next/next/no-img-element */}<img src={CARRIER_LOGO.cdek} alt="СДЭК" />{/* eslint-disable-next-line @next/next/no-img-element */}<img src={CARRIER_LOGO.yandex} alt="Яндекс Доставка" />
                    </span>
                    <ChevronRight size={20} className="mv-dl-big-go" />
                  </button>
                  {dm?.ozonDelivery && (
                    <button type="button" className="mv-dl-big" onClick={() => { setOzonChosen(true); setSel(null); }}>
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <span className="mv-dl-big-ic"><img src={CARRIER_LOGO.ozon} alt="" style={{ width: 30 }} /></span>
                      <span className="mv-dl-big-t"><b>Доставка Ozon</b><span>Пункт выдачи или курьер — выберете на странице оплаты</span></span>
                      <ChevronRight size={20} className="mv-dl-big-go" />
                    </button>
                  )}
                </div>
              )}

              {saved.filter((a) => a.carrier).length > 0 && (
                <div className="mv-co-saved" style={{ marginTop: 14 }}>
                  <div className="mv-co-label">Недавние адреса</div>
                  <div className="mv-co-saved-list">
                    {(showAllSaved ? saved : saved.slice(0, 3)).filter((a) => a.carrier).map((a) => (
                      <div key={a.id} className={`mv-co-saved-item${a.id === activeSavedId ? " on" : ""}`}>
                        <button type="button" onClick={() => applySaved(a)} disabled={savedBusy === a.id}>
                          <span className="mv-co-saved-ic">{savedBusy === a.id ? <Loader2 size={15} className="mv-spin" /> : a.type === "pickup" ? <MapPin size={15} /> : <Truck size={15} />}</span>
                          <span className="mv-co-saved-txt">
                            <b>{a.type === "pickup" ? (a.name || "Пункт выдачи") : `${CARRIER_MARK[a.carrier || ""] || ""} · курьер`}</b>
                            <span>{prettyAddress(a)}{a.flat ? `, кв. ${a.flat}` : ""}</span>
                          </span>
                          {a.id === activeSavedId && <Check size={16} className="mv-co-saved-check" />}
                        </button>
                        <button type="button" className="mv-co-saved-x" aria-label="Удалить адрес" onClick={() => { forgetAddress(a); setSaved((l) => l.filter((x) => x.id !== a.id)); }}><X size={14} /></button>
                      </div>
                    ))}
                  </div>
                  {saved.filter((a) => a.carrier).length > 3 && (
                    <button type="button" className="mv-co-link" onClick={() => setShowAllSaved((v) => !v)}>
                      {showAllSaved ? "Свернуть" : `Все адреса (${saved.filter((a) => a.carrier).length})`}
                    </button>
                  )}
                </div>
              )}
            </Section>

            <Section n="03" title="Оплата">
              <div className="mv-co-pay">
                <OzonPayLogo height={28} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div className="mv-co-pvz-t">Ozon Pay</div>
                  <div className="mv-co-pvz-s">Карта любого банка, СБП, Ozon Карта или Рассрочка</div>
                </div>
                <span className="mv-co-radio" aria-hidden><Check size={13} /></span>
              </div>
            </Section>

            <Section n="04" title="Комментарий">
              <Field label="Пожелания к заказу" textarea value={comment} onChange={e => setComment((e.target as HTMLTextAreaElement).value)} placeholder="Необязательно" />
            </Section>
          </div>

          <aside className="mv-co-card mv-co-sum">
            <h2 className="mv-co-h">Ваш заказ <span className="mv-co-count">{items.reduce((n, i) => n + i.quantity, 0)}</span></h2>
            <ul className="mv-co-items">
              {items.map(({ product, quantity }) => (
                <li key={product.offerId}>
                  <span className="mv-co-thumb">
                    {product.images?.[0]
                      // eslint-disable-next-line @next/next/no-img-element
                      ? <img src={productImg(product.images[0], 160)} alt="" loading="lazy" />
                      : <b>{(product.brand || product.name || "?")[0]}</b>}
                    {quantity > 1 && <em>{quantity}</em>}
                  </span>
                  <span className="mv-co-iname">{product.name}</span>
                  <span className="mv-co-iprice">{rub(product.priceRub * quantity)}</span>
                </li>
              ))}
            </ul>

            {!refCode && (
              <div className="mv-co-promo">
                <div className={`mv-co-promo-row${promoResult === null ? "" : promoResult.valid ? " ok" : " bad"}`}>
                  <Tag size={15} />
                  <input
                    ref={promoRef}
                    type="text"
                    value={promoInput}
                    onChange={e => { setPromoInput(e.target.value.toUpperCase()); setPromoResult(null); }}
                    onKeyDown={e => e.key === "Enter" && (e.preventDefault(), applyPromo())}
                    placeholder="Промокод"
                    maxLength={32}
                    disabled={promoResult?.valid}
                    aria-label="Промокод"
                  />
                  {promoResult?.valid
                    ? <button type="button" className="mv-co-promo-x" aria-label="Убрать промокод" onClick={() => { setPromoResult(null); setPromoInput(""); }}><X size={14} /></button>
                    : <button type="button" className="mv-co-promo-btn" onClick={applyPromo} disabled={!promoInput.trim() || promoValidating}>
                        {promoValidating ? <Loader2 size={14} className="mv-spin" /> : "Применить"}
                      </button>}
                </div>
                {promoResult?.valid && <div className="mv-co-msg ok"><Check size={13} /> {promoResult.code} — скидка {promoResult.discountPct}%</div>}
                {promoResult !== null && !promoResult.valid && <div className="mv-co-msg bad">Промокод не найден</div>}
              </div>
            )}

            <div className="mv-co-rows">
              <div><span>Товары</span><span>{rub(totalRub)}</span></div>
              <div><span>Доставка{sel ? ` ${CARRIER_MARK[sel.option.carrier]}` : ozonChosen ? " Ozon" : ""}</span>{!sel && !ozonChosen ? <span>выберите способ</span> : ozonChosen && (!dm || quoting) ? <span>…</span> : deliveryRub > 0 ? <span>{rub(deliveryRub)}</span> : <span className="mv-co-free">Бесплатно</span>}</div>
              {refDiscount > 0 && <div><span>Реферальная скидка −7%</span><span className="mv-co-free">−{rub(refDiscount)}</span></div>}
              {promoDiscount > 0 && <div><span>Промокод −{promoResult?.discountPct}%</span><span className="mv-co-free">−{rub(promoDiscount)}</span></div>}
            </div>
            <div className="mv-co-total"><span>К оплате</span><strong>{rub(total)}</strong></div>
            {toFree > 0 && <div className="mv-co-msg ok"><Truck size={13} /> Добавьте товаров ещё на {rub(toFree)} — доставка станет бесплатной</div>}

            <div style={{ display: "flex", flexDirection: "column", gap: 10, margin: "14px 0 4px" }}>
              <ConsentCheck kind="pd" checked={pdConsent} onChange={setPdConsent} />
              <ConsentCheck kind="ads" checked={adsConsent} onChange={setAdsConsent} />
            </div>

            {error && <div className="mv-co-error" role="alert"><AlertCircle size={15} />{error}</div>}
            {!valid && <div className="mv-co-need">Осталось указать: {missing.join(", ")}</div>}

            <button type="submit" disabled={!valid || loading} className="mv-co-paybtn">
              {loading
                ? <><Loader2 size={18} className="mv-spin" /> Оформляем…</>
                : <>Оплатить {rub(total)}<span className="mv-co-paylogo"><OzonPayLogo height={16} variant="white" /></span></>}
            </button>
            <div className="mv-co-secure"><Lock size={12} /> Оплата на защищённой странице Ozon Pay — данные карты не передаются магазину</div>
            <div className="mv-co-secure"><span>Нажимая «Оплатить», вы принимаете <Link href="/terms" style={{ color: "var(--accent)" }}>условия покупки (оферту)</Link></span></div>
          </aside>
        </form>
      </div>
    </div>
  );
}
