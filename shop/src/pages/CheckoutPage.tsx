import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { ChevronLeft, Loader2, Shield, Lock, MapPin, ChevronRight } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import clsx from "clsx";
import OzonPickupMap, { type PvzPoint } from "../components/OzonPickupMap";

interface FormData {
  firstName: string;
  lastName: string;
  phone: string;
  email: string;
  address: string;
  city: string;
  postalCode: string;
  comment: string;
}

const INITIAL: FormData = { firstName: "", lastName: "", phone: "", email: "", address: "", city: "", postalCode: "", comment: "" };

function Field({ label, required, ...props }: { label: string; required?: boolean } & React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div>
      <label className="text-xs font-semibold text-apple-gray uppercase tracking-wider block mb-1.5">
        {label}{required && " *"}
      </label>
      <input
        {...props}
        required={required}
        className={clsx(
          "w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm text-apple-black focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all placeholder:text-apple-gray/60",
          props.className
        )}
      />
    </div>
  );
}

export default function CheckoutPage() {
  const { items, totalRub, clear } = useCart();
  const { customer, token } = useAuth();
  const navigate = useNavigate();
  const [pvzOpen, setPvzOpen] = useState(false);
  const [selectedPvz, setSelectedPvz] = useState<PvzPoint | null>(null);
  const [form, setForm] = useState<FormData>({
    ...INITIAL,
    firstName: customer?.firstName || "",
    lastName: customer?.lastName || "",
    email: customer?.email || "",
    phone: customer?.phone || "",
  });

  const deliveryCost = 0;
  const pickupAddress = selectedPvz ? selectedPvz.address : "";
  const total = totalRub + deliveryCost;

  const mutation = useMutation({
    mutationFn: () => api.createOrder({
      items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
      delivery: {
        type: "pickup",
        firstName: form.firstName,
        lastName: form.lastName,
        phone: form.phone,
        email: form.email,
        city: selectedPvz?.city || form.city || "",
        address: pickupAddress,
        pvzId: selectedPvz?.id,
      },
      comment: form.comment || undefined,
    }, token ?? undefined),
    onSuccess: (order) => {
      clear();
      if (order.paymentUrl) {
        window.location.href = order.paymentUrl;
      } else {
        navigate(`/order-success?id=${order.id}`);
      }
    },
  });

  function set(key: keyof FormData) {
    return (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      setForm((f) => ({ ...f, [key]: e.target.value }));
  }

  const valid = form.firstName && form.phone && form.email && !!selectedPvz;

  if (!items.length) {
    return (
      <div className="bg-apple-gray-bg min-h-screen flex items-center justify-center">
        <div className="text-center py-16">
          <p className="text-apple-gray mb-4 text-sm">Корзина пуста</p>
          <Link to="/catalog" className="text-violet-600 hover:text-violet-800 font-medium text-sm">В каталог</Link>
        </div>
      </div>
    );
  }

  return (
    <>
    <OzonPickupMap
      open={pvzOpen}
      onClose={() => setPvzOpen(false)}
      onSelect={pvz => { setSelectedPvz(pvz); setPvzOpen(false); }}
      defaultCity={form.city}
    />
    <div className="bg-apple-gray-bg min-h-screen">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8">
        <Link to="/cart" className="inline-flex items-center gap-1.5 text-sm text-apple-gray hover:text-apple-black mb-6 group transition-colors">
          <ChevronLeft size={15} className="group-hover:-translate-x-0.5 transition-transform" />
          Назад в корзину
        </Link>
        <h1 className="text-2xl font-bold text-apple-black tracking-tight mb-8">Оформление заказа</h1>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
          <div className="lg:col-span-2 space-y-4">
            {/* Contact */}
            <div className="bg-white rounded-2xl p-6" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
              <h3 className="font-bold text-apple-black mb-5 text-[15px]">Контактные данные</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Имя" required value={form.firstName} onChange={set("firstName")} placeholder="Иван" />
                <Field label="Фамилия" value={form.lastName} onChange={set("lastName")} placeholder="Иванов" />
                <Field label="Телефон" required type="tel" value={form.phone} onChange={set("phone")} placeholder="+7 900 000-00-00" />
                <Field label="Email" required type="email" value={form.email} onChange={set("email")} placeholder="ivan@mail.ru" />
              </div>
            </div>

            {/* Delivery — pickup only */}
            <div className="bg-white rounded-2xl p-6" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
              <h3 className="font-bold text-apple-black mb-4 text-[15px]">Пункт выдачи Ozon</h3>
              <button
                type="button"
                onClick={() => setPvzOpen(true)}
                className={clsx(
                  "w-full flex items-center gap-3 p-4 rounded-xl border-2 text-left transition-all",
                  selectedPvz
                    ? "border-emerald-400 bg-emerald-50"
                    : "border-dashed border-gray-200 hover:border-violet-400 bg-gray-50 hover:bg-violet-50"
                )}
              >
                <div className={clsx(
                  "w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0",
                  selectedPvz ? "bg-emerald-100" : "bg-orange-100"
                )}>
                  <MapPin size={16} className={selectedPvz ? "text-emerald-600" : "text-orange-500"} />
                </div>
                <div className="flex-1 min-w-0">
                  {selectedPvz ? (
                    <>
                      <p className="text-[12px] font-semibold text-emerald-700 mb-0.5">Пункт выдачи выбран ✓</p>
                      <p className="text-[12px] text-apple-gray truncate">{selectedPvz.address}</p>
                    </>
                  ) : (
                    <>
                      <p className="text-[13px] font-semibold text-apple-black">Выбрать пункт выдачи Ozon</p>
                      <p className="text-[11px] text-apple-gray">Бесплатно — откроется карта с ПВЗ</p>
                    </>
                  )}
                </div>
                <ChevronRight size={15} className="text-apple-gray flex-shrink-0" />
              </button>
            </div>

            {/* Comment */}
            <div className="bg-white rounded-2xl p-6" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
              <h3 className="font-bold text-apple-black mb-3 text-[15px]">Комментарий</h3>
              <textarea
                value={form.comment}
                onChange={set("comment")}
                rows={3}
                placeholder="Пожелания к заказу..."
                className="w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm text-apple-black focus:outline-none focus:ring-2 focus:ring-violet-400 resize-none transition-all placeholder:text-apple-gray/60"
              />
            </div>
          </div>

          {/* Summary */}
          <div>
            <div className="bg-white rounded-2xl p-5 sticky top-20" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
              <h3 className="font-bold text-apple-black mb-4 text-[15px]">Ваш заказ</h3>
              <div className="space-y-2 mb-4 max-h-48 overflow-y-auto">
                {items.map(({ product, quantity }) => (
                  <div key={product.offerId} className="flex gap-2 text-xs">
                    <span className="flex-1 text-apple-gray line-clamp-2">{product.name}</span>
                    <span className="text-apple-gray flex-shrink-0">×{quantity}</span>
                    <span className="font-semibold text-apple-black flex-shrink-0">{(product.priceRub * quantity).toLocaleString("ru-RU")} ₽</span>
                  </div>
                ))}
              </div>

              <div className="border-t border-gray-100 pt-3 space-y-2 text-sm mb-5">
                <div className="flex justify-between text-apple-gray">
                  <span>Товары</span><span className="text-apple-black font-medium">{totalRub.toLocaleString("ru-RU")} ₽</span>
                </div>
                <div className="flex justify-between text-apple-gray">
                  <span>Доставка</span>
                  {deliveryCost === 0
                    ? <span className="text-emerald-600 font-medium">Бесплатно</span>
                    : <span className="text-apple-black font-medium">{deliveryCost} ₽</span>}
                </div>
                <div className="flex justify-between font-bold text-[16px] text-apple-black pt-1 border-t border-gray-100">
                  <span>Итого</span><span>{total.toLocaleString("ru-RU")} ₽</span>
                </div>
              </div>

              {mutation.error && (
                <div className="bg-red-50 text-red-600 text-xs px-4 py-3 rounded-xl mb-4">
                  {(mutation.error as Error).message}
                </div>
              )}

              <button
                type="button"
                onClick={() => mutation.mutate()}
                disabled={!valid || mutation.isPending}
                className="w-full flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 disabled:opacity-50 disabled:cursor-not-allowed text-white py-3.5 rounded-2xl font-semibold text-[15px] transition-all shadow-lg shadow-violet-200 hover:-translate-y-0.5"
              >
                {mutation.isPending ? (
                  <><Loader2 size={16} className="animate-spin" /> Оформляем...</>
                ) : (
                  <><Lock size={14} /> Оформить · {total.toLocaleString("ru-RU")} ₽</>
                )}
              </button>

              <div className="mt-3 flex items-center justify-center gap-1.5 text-[11px] text-apple-gray">
                <Shield size={11} />
                <span>Безопасное оформление заказа</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    </>
  );
}
