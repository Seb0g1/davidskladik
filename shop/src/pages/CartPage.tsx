import { Link } from "react-router-dom";
import { Trash2, Plus, Minus, ShoppingBag, ArrowRight, Package } from "lucide-react";
import { useCart } from "../CartContext";

export default function CartPage() {
  const { items, totalRub, remove, setQty } = useCart();

  if (!items.length) {
    return (
      <div className="bg-apple-gray-bg min-h-screen flex items-center justify-center">
        <div className="text-center py-20 max-w-sm mx-auto">
          <div className="w-20 h-20 bg-white rounded-3xl flex items-center justify-center mx-auto mb-6 shadow-card">
            <ShoppingBag size={36} className="text-gray-300" />
          </div>
          <h2 className="text-2xl font-bold text-apple-black tracking-tight mb-2">Корзина пуста</h2>
          <p className="text-apple-gray mb-8 text-sm">Добавьте товары из каталога</p>
          <Link
            to="/catalog"
            className="inline-flex items-center gap-2 bg-violet-600 hover:bg-violet-700 text-white px-8 py-3.5 rounded-2xl font-semibold text-sm transition-all shadow-lg shadow-violet-200"
          >
            Перейти в каталог <ArrowRight size={16} />
          </Link>
        </div>
      </div>
    );
  }

  const delivery = totalRub >= 3000 ? 0 : 350;
  const total = totalRub + delivery;

  return (
    <div className="bg-apple-gray-bg min-h-screen">
      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8">
        <h1 className="text-2xl font-bold text-apple-black tracking-tight mb-8">
          Корзина <span className="text-apple-gray font-normal text-lg">· {items.reduce((s, i) => s + i.quantity, 0)} товара</span>
        </h1>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
          {/* Items */}
          <div className="lg:col-span-2 space-y-3">
            {items.map(({ product, quantity }) => (
              <div key={product.offerId} className="bg-white rounded-2xl p-4 flex gap-4" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
                <Link to={`/product/${encodeURIComponent(product.offerId)}`} className="flex-shrink-0 w-20 h-20 bg-apple-gray-bg rounded-xl overflow-hidden">
                  {product.images[0] ? (
                    <img src={product.images[0]} alt={product.name} className="w-full h-full object-contain p-1.5" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-gray-200 text-2xl font-bold">
                      {product.brand?.[0] ?? "?"}
                    </div>
                  )}
                </Link>

                <div className="flex-1 min-w-0">
                  <div className="text-[10px] font-bold uppercase tracking-wider text-violet-500 mb-0.5">{product.brand}</div>
                  <Link to={`/product/${encodeURIComponent(product.offerId)}`}
                    className="text-sm font-medium text-apple-black line-clamp-2 hover:text-violet-600 transition-colors leading-snug">
                    {product.name}
                  </Link>
                  {product.volume && <div className="text-[11px] text-apple-gray mt-0.5">{product.volume}</div>}

                  <div className="flex items-center justify-between mt-3">
                    <div className="flex items-center bg-apple-gray-bg rounded-xl overflow-hidden">
                      <button onClick={() => setQty(product.offerId, quantity - 1)} className="px-2.5 py-1.5 hover:bg-gray-200 transition-colors">
                        <Minus size={13} />
                      </button>
                      <span className="px-3 text-sm font-semibold">{quantity}</span>
                      <button onClick={() => setQty(product.offerId, Math.min(product.stockQty || 99, quantity + 1))} className="px-2.5 py-1.5 hover:bg-gray-200 transition-colors">
                        <Plus size={13} />
                      </button>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="font-bold text-apple-black text-[15px]">
                        {(product.priceRub * quantity).toLocaleString("ru-RU")} ₽
                      </span>
                      <button onClick={() => remove(product.offerId)} className="p-1.5 text-gray-300 hover:text-rose-400 transition-colors rounded-lg hover:bg-rose-50">
                        <Trash2 size={15} />
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Summary */}
          <div>
            <div className="bg-white rounded-2xl p-5 sticky top-20" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
              <h3 className="font-bold text-apple-black mb-5 text-[15px]">Итого</h3>

              <div className="space-y-2 text-sm mb-4">
                <div className="flex justify-between text-apple-gray">
                  <span>Товары · {items.reduce((s, i) => s + i.quantity, 0)} шт.</span>
                  <span className="text-apple-black font-medium">{totalRub.toLocaleString("ru-RU")} ₽</span>
                </div>
                <div className="flex justify-between text-apple-gray">
                  <span>Доставка Ozon</span>
                  {delivery === 0
                    ? <span className="text-emerald-600 font-medium">Бесплатно</span>
                    : <span className="text-apple-black font-medium">{delivery} ₽</span>
                  }
                </div>
                {delivery > 0 && (
                  <p className="text-[11px] text-apple-gray bg-amber-50 rounded-lg px-3 py-2">
                    До бесплатной: {(3000 - totalRub).toLocaleString("ru-RU")} ₽
                  </p>
                )}
              </div>

              <div className="border-t border-gray-100 pt-4 mb-5">
                <div className="flex justify-between font-bold text-[17px] text-apple-black">
                  <span>К оплате</span>
                  <span>{total.toLocaleString("ru-RU")} ₽</span>
                </div>
              </div>

              <Link to="/checkout"
                className="w-full flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 text-white py-3.5 rounded-2xl font-semibold text-[15px] transition-all shadow-lg shadow-violet-200 hover:-translate-y-0.5">
                Оформить заказ <ArrowRight size={16} />
              </Link>

              <div className="mt-4 flex items-center justify-center gap-1.5 text-[11px] text-apple-gray">
                <Package size={11} />
                <span>Доставка через Ozon · Оригинальная продукция</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
