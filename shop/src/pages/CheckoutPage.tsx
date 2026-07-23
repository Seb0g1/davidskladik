import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { ChevronLeft, Loader2, Shield } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import clsx from "clsx";

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

const INITIAL: FormData = {
  firstName: "", lastName: "", phone: "", email: "",
  address: "", city: "", postalCode: "", comment: "",
};

function Field({ label, ...props }: { label: string } & React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <input
        {...props}
        className={clsx(
          "w-full px-4 py-2.5 border rounded-xl text-sm focus:outline-none focus:border-violet-400 focus:ring-2 focus:ring-violet-100 transition-all",
          props.className ?? "border-gray-200"
        )}
      />
    </div>
  );
}

export default function CheckoutPage() {
  const { items, totalRub, clear } = useCart();
  const navigate = useNavigate();
  const [form, setForm] = useState<FormData>(INITIAL);

  const delivery = totalRub >= 3000 ? 0 : 350;
  const total = totalRub + delivery;

  const mutation = useMutation({
    mutationFn: () => api.createOrder({
      items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
      delivery: {
        firstName: form.firstName,
        lastName: form.lastName,
        phone: form.phone,
        email: form.email,
        address: form.address,
        city: form.city,
        postalCode: form.postalCode,
      },
      comment: form.comment || undefined,
    }),
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

  const valid = form.firstName && form.lastName && form.phone && form.email && form.city;

  if (!items.length) {
    return (
      <div className="max-w-2xl mx-auto px-4 py-16 text-center">
        <p className="text-gray-500 mb-4">Корзина пуста</p>
        <Link to="/catalog" className="text-violet-600 hover:underline">В каталог</Link>
      </div>
    );
  }

  return (
    <div className="max-w-5xl mx-auto px-4 py-6">
      <Link to="/cart" className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-violet-600 mb-6 group">
        <ChevronLeft size={16} className="group-hover:-translate-x-0.5 transition-transform" /> Назад в корзину
      </Link>
      <h1 className="text-2xl font-display font-bold text-gray-900 mb-6">Оформление заказа</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Form */}
        <div className="lg:col-span-2 space-y-5">
          {/* Contact */}
          <div className="bg-white rounded-2xl p-5 shadow-card">
            <h3 className="font-semibold text-gray-800 mb-4">Контактные данные</h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <Field label="Имя *" value={form.firstName} onChange={set("firstName")} placeholder="Иван" required />
              <Field label="Фамилия *" value={form.lastName} onChange={set("lastName")} placeholder="Иванов" required />
              <Field label="Телефон *" value={form.phone} onChange={set("phone")} placeholder="+7 900 000-00-00" type="tel" required />
              <Field label="Email *" value={form.email} onChange={set("email")} placeholder="ivan@mail.ru" type="email" required />
            </div>
          </div>

          {/* Delivery */}
          <div className="bg-white rounded-2xl p-5 shadow-card">
            <h3 className="font-semibold text-gray-800 mb-1">Доставка Ozon</h3>
            <p className="text-xs text-gray-400 mb-4">Доставка осуществляется через сеть Ozon. Вы получите трек-номер на email.</p>
            <div className="space-y-3">
              <Field label="Город *" value={form.city} onChange={set("city")} placeholder="Москва" required />
              <Field label="Адрес (улица, дом, кв.)" value={form.address} onChange={set("address")} placeholder="ул. Ленина, д. 1, кв. 1" />
              <Field label="Индекс" value={form.postalCode} onChange={set("postalCode")} placeholder="123456" />
            </div>
          </div>

          {/* Comment */}
          <div className="bg-white rounded-2xl p-5 shadow-card">
            <h3 className="font-semibold text-gray-800 mb-3">Комментарий к заказу</h3>
            <textarea
              value={form.comment}
              onChange={set("comment")}
              rows={3}
              placeholder="Пожелания к заказу..."
              className="w-full px-4 py-2.5 border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 focus:ring-2 focus:ring-violet-100 resize-none transition-all"
            />
          </div>
        </div>

        {/* Summary */}
        <div>
          <div className="bg-white rounded-2xl p-5 shadow-card sticky top-24">
            <h3 className="font-semibold text-gray-800 mb-4">Ваш заказ</h3>
            <div className="space-y-2 mb-4">
              {items.map(({ product, quantity }) => (
                <div key={product.offerId} className="flex gap-2 text-xs">
                  <div className="flex-1 text-gray-700 line-clamp-2">{product.name}</div>
                  <div className="text-gray-500 flex-shrink-0">×{quantity}</div>
                  <div className="font-medium flex-shrink-0">{(product.priceRub * quantity).toLocaleString("ru-RU")} ₽</div>
                </div>
              ))}
            </div>

            <div className="border-t border-gray-100 pt-3 space-y-1.5 text-sm mb-4">
              <div className="flex justify-between text-gray-600">
                <span>Товары</span>
                <span>{totalRub.toLocaleString("ru-RU")} ₽</span>
              </div>
              <div className="flex justify-between text-gray-600">
                <span>Доставка</span>
                <span className={delivery === 0 ? "text-green-600 font-medium" : ""}>
                  {delivery === 0 ? "Бесплатно" : `${delivery} ₽`}
                </span>
              </div>
              <div className="flex justify-between font-bold text-base pt-1 border-t border-gray-100">
                <span>Итого</span>
                <span>{total.toLocaleString("ru-RU")} ₽</span>
              </div>
            </div>

            {mutation.error && (
              <div className="bg-rose-50 text-rose-600 text-xs p-3 rounded-lg mb-3">
                {(mutation.error as Error).message}
              </div>
            )}

            <button
              onClick={() => mutation.mutate()}
              disabled={!valid || mutation.isPending}
              className="w-full flex items-center justify-center gap-2 bg-violet-600 text-white py-3.5 rounded-xl font-semibold hover:bg-violet-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {mutation.isPending ? (
                <><Loader2 size={18} className="animate-spin" /> Оформляем...</>
              ) : (
                <>Оплатить {total.toLocaleString("ru-RU")} ₽</>
              )}
            </button>

            <div className="mt-3 flex items-center justify-center gap-1.5 text-xs text-gray-400">
              <Shield size={12} />
              <span>Оплата через Ozon Pay · Безопасно</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
