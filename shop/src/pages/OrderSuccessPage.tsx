import { useSearchParams, Link } from "react-router-dom";
import { CheckCircle, ShoppingBag, Package } from "lucide-react";

export default function OrderSuccessPage() {
  const [params] = useSearchParams();
  const orderId = params.get("id");

  return (
    <div className="bg-apple-gray-bg min-h-screen flex items-center justify-center">
      <div className="text-center py-16 max-w-md mx-auto px-4">
        <div className="w-20 h-20 bg-emerald-100 rounded-3xl flex items-center justify-center mx-auto mb-6">
          <CheckCircle size={40} className="text-emerald-500" />
        </div>
        <h1 className="text-2xl font-bold text-apple-black tracking-tight mb-2">Заказ оформлен!</h1>
        {orderId && (
          <p className="text-sm text-apple-gray mb-1">
            Номер заказа: <span className="font-mono font-bold text-apple-black">{orderId}</span>
          </p>
        )}
        <p className="text-sm text-apple-gray mb-10 leading-relaxed">
          Подтверждение придёт на вашу почту.<br />
          Доставка через Ozon, 1–5 рабочих дней.
        </p>
        <div className="flex flex-col sm:flex-row gap-3 justify-center">
          <Link
            to="/catalog"
            className="inline-flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 text-white px-7 py-3.5 rounded-2xl font-semibold text-sm transition-all shadow-lg shadow-violet-200"
          >
            <ShoppingBag size={16} /> Продолжить покупки
          </Link>
          <Link
            to="/orders"
            className="inline-flex items-center justify-center gap-2 bg-white hover:bg-gray-50 border border-gray-200 text-apple-black px-7 py-3.5 rounded-2xl font-semibold text-sm transition-all"
          >
            <Package size={16} /> Мои заказы
          </Link>
        </div>
      </div>
    </div>
  );
}
