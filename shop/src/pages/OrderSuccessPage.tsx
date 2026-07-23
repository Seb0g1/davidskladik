import { useSearchParams, Link } from "react-router-dom";
import { CheckCircle, ShoppingBag } from "lucide-react";

export default function OrderSuccessPage() {
  const [params] = useSearchParams();
  const orderId = params.get("id");

  return (
    <div className="max-w-lg mx-auto px-4 py-20 text-center">
      <CheckCircle size={72} className="text-green-500 mx-auto mb-5" />
      <h1 className="text-2xl font-display font-bold text-gray-900 mb-3">Заказ оформлен!</h1>
      {orderId && <p className="text-sm text-gray-500 mb-2">Номер заказа: <span className="font-mono font-semibold text-gray-700">{orderId}</span></p>}
      <p className="text-gray-500 mb-8">
        Мы отправим подтверждение на вашу почту. Доставка будет осуществлена через Ozon.
      </p>
      <Link
        to="/catalog"
        className="inline-flex items-center gap-2 bg-violet-600 text-white px-8 py-3 rounded-xl font-semibold hover:bg-violet-700 transition-colors"
      >
        <ShoppingBag size={18} /> Продолжить покупки
      </Link>
    </div>
  );
}
