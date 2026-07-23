import { Link } from "react-router-dom";
import { Sparkles, Phone, Mail, MapPin, Instagram } from "lucide-react";

export default function Footer() {
  return (
    <footer className="bg-brand-dark text-gray-300 mt-16">
      <div className="max-w-7xl mx-auto px-4 py-12 grid grid-cols-1 md:grid-cols-4 gap-8">
        {/* Brand */}
        <div className="md:col-span-1">
          <div className="flex items-center gap-2 mb-3">
            <Sparkles className="text-violet-400" size={22} />
            <div>
              <div className="font-display font-bold text-lg text-white leading-none">Magic Vibes</div>
              <div className="text-[10px] text-gray-500 uppercase tracking-widest">Парфюмерия</div>
            </div>
          </div>
          <p className="text-sm leading-relaxed text-gray-400 mb-4">
            Оригинальная парфюмерия и косметика. Быстрая доставка по всей России через Ozon.
          </p>
          <a href="https://instagram.com" className="inline-flex items-center gap-2 text-sm hover:text-violet-400 transition-colors">
            <Instagram size={16} /> @magicvibes.ru
          </a>
        </div>

        {/* Catalog */}
        <div>
          <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">Каталог</h4>
          <ul className="space-y-2 text-sm">
            {["Парфюмерия", "Уход за кожей", "Для волос", "Для мужчин", "Подарки", "Акции"].map((item) => (
              <li key={item}>
                <Link to="/catalog" className="hover:text-violet-400 transition-colors">{item}</Link>
              </li>
            ))}
          </ul>
        </div>

        {/* Info */}
        <div>
          <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">Информация</h4>
          <ul className="space-y-2 text-sm">
            {["О магазине", "Доставка и оплата", "Возврат товара", "Гарантия подлинности", "Контакты"].map((item) => (
              <li key={item}>
                <Link to="/" className="hover:text-violet-400 transition-colors">{item}</Link>
              </li>
            ))}
          </ul>
        </div>

        {/* Contacts */}
        <div>
          <h4 className="text-white font-semibold mb-4 text-sm uppercase tracking-wider">Контакты</h4>
          <ul className="space-y-3 text-sm">
            <li className="flex items-start gap-2">
              <Phone size={15} className="text-violet-400 mt-0.5 flex-shrink-0" />
              <span>+7 (800) 123-45-67</span>
            </li>
            <li className="flex items-start gap-2">
              <Mail size={15} className="text-violet-400 mt-0.5 flex-shrink-0" />
              <span>info@magicvibes.ru</span>
            </li>
            <li className="flex items-start gap-2">
              <MapPin size={15} className="text-violet-400 mt-0.5 flex-shrink-0" />
              <span>Доставка по всей России</span>
            </li>
          </ul>
          <div className="mt-4 p-3 bg-white/5 rounded-lg">
            <div className="text-xs text-gray-400 mb-1">Доставка и оплата</div>
            <div className="flex gap-2 items-center">
              <div className="bg-blue-500 text-white text-xs font-bold px-2 py-0.5 rounded">Ozon</div>
              <div className="text-xs text-gray-300">Pay · Доставка</div>
            </div>
          </div>
        </div>
      </div>

      <div className="border-t border-white/10">
        <div className="max-w-7xl mx-auto px-4 py-4 flex flex-col sm:flex-row justify-between items-center gap-2 text-xs text-gray-500">
          <span>© {new Date().getFullYear()} Magic Vibes. Все права защищены.</span>
          <span>ИП Иванов И.И. · ИНН 1234567890</span>
        </div>
      </div>
    </footer>
  );
}
