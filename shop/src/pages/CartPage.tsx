import { Link } from "react-router-dom";
import { Trash2, Plus, Minus, ShoppingBag, ArrowRight } from "lucide-react";
import { useCart } from "../CartContext";

export default function CartPage() {
  const { items, totalRub, remove, setQty } = useCart();

  if (!items.length) {
    return (
      <div className="max-w-2xl mx-auto px-4 py-20 text-center">
        <ShoppingBag size={64} className="text-gray-200 mx-auto mb-4" />
        <h2 className="text-2xl font-display font-bold text-gray-700 mb-2">Корзина пуста</h2>
        <p className="text-gray-500 mb-8">Добавьте товары из каталога, чтобы оформить заказ</p>
        <Link
          to="/catalog"
          className="inline-flex items-center gap-2 bg-violet-600 text-white px-8 py-3 rounded-xl font-semibold hover:bg-violet-700 transition-colors"
        >
          Перейти в каталог <ArrowRight size={18} />
        </Link>
      </div>
    );
  }

  const delivery = totalRub >= 3000 ? 0 : 350;

  return (
    <div className="max-w-5xl mx-auto px-4 py-6">
      <h1 className="text-2xl font-display font-bold text-gray-900 mb-6">Корзина</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Items */}
        <div className="lg:col-span-2 space-y-3">
          {items.map(({ product, quantity }) => (
            <div key={product.offerId} className="bg-white rounded-2xl p-4 flex gap-4 shadow-card">
              <Link to={`/product/${encodeURIComponent(product.offerId)}`} className="flex-shrink-0 w-20 h-20 bg-gray-50 rounded-xl overflow-hidden">
                {product.images[0] ? (
                  <img src={product.images[0]} alt={product.name} className="w-full h-full object-contain p-1" />
                ) : (
                  <div className="w-full h-full flex items-center justify-center text-gray-300 text-2xl font-display">
                    {product.brand?.[0] ?? "?"}
                  </div>
                )}
              </Link>

              <div className="flex-1 min-w-0">
                <div className="text-xs text-violet-600 font-semibold mb-0.5">{product.brand}</div>
                <Link
                  to={`/product/${encodeURIComponent(product.offerId)}`}
                  className="text-sm font-medium text-gray-800 line-clamp-2 hover:text-violet-600 transition-colors"
                >
                  {product.name}
                </Link>
                {product.volume && <div className="text-xs text-gray-400 mt-0.5">{product.volume}</div>}

                <div className="flex items-center justify-between mt-3">
                  <div className="flex items-center border border-gray-200 rounded-lg overflow-hidden">
                    <button
                      onClick={() => setQty(product.offerId, quantity - 1)}
                      className="px-2 py-1.5 hover:bg-gray-50 transition-colors"
                    >
                      <Minus size={14} />
                    </button>
                    <span className="px-3 text-sm font-medium">{quantity}</span>
                    <button
                      onClick={() => setQty(product.offerId, Math.min(product.stockQty, quantity + 1))}
                      className="px-2 py-1.5 hover:bg-gray-50 transition-colors"
                    >
                      <Plus size={14} />
                    </button>
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="font-bold text-gray-900">
                      {(product.priceRub * quantity).toLocaleString("ru-RU")} ₽
                    </div>
                    <button
                      onClick={() => remove(product.offerId)}
                      className="text-gray-300 hover:text-rose-500 transition-colors"
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Summary */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-2xl p-5 shadow-card sticky top-24">
            <h3 className="font-semibold text-gray-800 mb-4">Итого</h3>

            <div className="space-y-2 text-sm mb-4">
              <div className="flex justify-between text-gray-600">
                <span>Товары ({items.reduce((s, i) => s + i.quantity, 0)} шт.)</span>
                <span>{totalRub.toLocaleString("ru-RU")} ₽</span>
              </div>
              <div className="flex justify-between text-gray-600">
                <span>Доставка</span>
                <span>{delivery === 0 ? <span className="text-green-600 font-medium">Бесплатно</span> : `${delivery} ₽`}</span>
              </div>
              {delivery > 0 && (
                <div className="text-xs text-gray-400">
                  До бесплатной доставки: {(3000 - totalRub).toLocaleString("ru-RU")} ₽
                </div>
              )}
            </div>

            <div className="border-t border-gray-100 pt-3 mb-5">
              <div className="flex justify-between font-bold text-lg">
                <span>К оплате</span>
                <span>{(totalRub + delivery).toLocaleString("ru-RU")} ₽</span>
              </div>
            </div>

            <Link
              to="/checkout"
              className="w-full flex items-center justify-center gap-2 bg-violet-600 text-white py-3.5 rounded-xl font-semibold hover:bg-violet-700 transition-colors"
            >
              Оформить заказ <ArrowRight size={18} />
            </Link>

            <div className="mt-3 flex items-center justify-center gap-2 text-xs text-gray-400">
              <div className="bg-blue-500 text-white text-[10px] font-bold px-1.5 py-0.5 rounded">Ozon</div>
              Оплата через Ozon Pay
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
