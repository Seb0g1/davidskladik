import { Link } from "react-router-dom";
import { Sparkles, Droplets, Wind, User, Gift, Zap } from "lucide-react";
import type { ShopCategory } from "../types";

const DEFAULT_CATEGORIES: ShopCategory[] = [
  { id: "c1", name: "Женская парфюмерия",   slug: "women",   order: 0 },
  { id: "c2", name: "Мужская парфюмерия",    slug: "men",     order: 1 },
  { id: "c3", name: "Унисекс",              slug: "unisex",  order: 2 },
  { id: "c4", name: "Уход за кожей",        slug: "care",    order: 3 },
  { id: "c5", name: "Подарочные наборы",    slug: "gifts",   order: 4 },
  { id: "c6", name: "Акции",               slug: "sale",    order: 5 },
];

const ICONS = [Sparkles, User, Wind, Droplets, Gift, Zap];
const COLORS = [
  "bg-rose-50 text-rose-600",
  "bg-blue-50 text-blue-600",
  "bg-amber-50 text-amber-600",
  "bg-violet-50 text-violet-600",
  "bg-pink-50 text-pink-600",
  "bg-red-50 text-red-600",
];

interface Props { categories?: ShopCategory[]; }

export default function CategoryGrid({ categories }: Props) {
  const items = categories?.length ? categories : DEFAULT_CATEGORIES;

  return (
    <section>
      <h2 className="text-xl font-bold text-apple-black tracking-tight mb-4">Категории</h2>
      <div className="grid grid-cols-3 sm:grid-cols-6 gap-3">
        {items.map((cat, i) => {
          const Icon = ICONS[i % ICONS.length];
          const color = COLORS[i % COLORS.length];
          return (
            <Link
              key={cat.id}
              to={`/catalog/${cat.slug}`}
              className="group flex flex-col items-center gap-2.5 p-4 bg-white rounded-2xl hover:shadow-card-hover transition-all duration-200 hover:-translate-y-0.5 text-center"
              style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04)" }}
            >
              <div className={`w-12 h-12 rounded-xl ${color} flex items-center justify-center group-hover:scale-105 transition-transform duration-200`}>
                <Icon size={20} />
              </div>
              <span className="text-[11px] font-semibold text-apple-black leading-tight">{cat.name}</span>
            </Link>
          );
        })}
      </div>
    </section>
  );
}
