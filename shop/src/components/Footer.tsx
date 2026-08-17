import { Link } from "react-router-dom";
import { MessageCircle } from "lucide-react";

export default function Footer() {
  return (
    <footer className="bg-[#0D0A1A] text-white/60 mt-16">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 pb-10 border-b border-white/10">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-8 h-8 rounded-xl bg-violet-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white text-xs font-bold">MV</span>
              </div>
              <span className="font-bold text-white text-[15px]">Magic Vibes</span>
            </div>
            <p className="text-sm leading-relaxed">
              Оригинальная парфюмерия<br />с доставкой по всей России
            </p>
            <div className="mt-4 text-xs">
              <a href="mailto:info@magicvibes.ru" className="hover:text-white transition-colors">info@magicvibes.ru</a>
            </div>
          </div>

          {/* Catalog */}
          <div>
            <h4 className="text-white font-semibold text-sm mb-4">Каталог</h4>
            <ul className="space-y-2.5 text-sm">
              {[
                { label: "Все товары", to: "/catalog" },
                { label: "Новинки", to: "/catalog?sort=price_desc" },
                { label: "Акции", to: "/catalog/sale" },
                { label: "Бренды", to: "/catalog" },
              ].map((l) => (
                <li key={l.to}><Link to={l.to} className="hover:text-white transition-colors">{l.label}</Link></li>
              ))}
            </ul>
          </div>

          {/* Info */}
          <div>
            <h4 className="text-white font-semibold text-sm mb-4">Информация</h4>
            <ul className="space-y-2.5 text-sm">
              {["О магазине", "Доставка и оплата", "Возврат товара", "Гарантия оригинала"].map((item) => (
                <li key={item}><a href="#" className="hover:text-white transition-colors">{item}</a></li>
              ))}
            </ul>
          </div>

          {/* Delivery */}
          <div>
            <h4 className="text-white font-semibold text-sm mb-4">Доставка</h4>
            <ul className="space-y-2.5 text-sm">
              <li>Доставка Ozon по России</li>
              <li>Срок 1–5 дней</li>
              <li className="pt-2">
                <div className="inline-flex items-center gap-2 bg-white/10 rounded-lg px-3 py-1.5 text-xs">
                  <span className="text-blue-400 font-bold">Ozon</span>
                  <span>Pay · Доставка</span>
                </div>
              </li>
            </ul>
          </div>
        </div>

        <div className="pt-6 flex flex-col sm:flex-row justify-between items-center gap-4">
          <div className="text-xs text-white/30 flex flex-col sm:flex-row gap-2 sm:gap-6 items-center">
            <span>© {new Date().getFullYear()} Magic Vibes. Все права защищены.</span>
            <span>100% оригинальная продукция</span>
          </div>
          <a
            href="https://davidsklad.ru"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 text-xs text-white/50 hover:text-white transition-colors"
          >
            <MessageCircle size={13} />
            Техподдержка
          </a>
        </div>
      </div>
    </footer>
  );
}
