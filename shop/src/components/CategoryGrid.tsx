import { Link } from "react-router-dom";
import { Sparkles, Droplets, Wind, User, Gift, Zap } from "lucide-react";
import type { ShopCategory } from "../types";

const DEFAULT_CATEGORIES: ShopCategory[] = [
  { id: "c1", name: "Парфюмерия", slug: "parfumery", order: 0 },
  { id: "c2", name: "Уход за кожей", slug: "care", order: 1 },
  { id: "c3", name: "Для волос", slug: "hair", order: 2 },
  { id: "c4", name: "Для мужчин", slug: "men", order: 3 },
  { id: "c5", name: "Подарки", slug: "gifts", order: 4 },
  { id: "c6", name: "Акции", slug: "sale", order: 5 },
];

const ICONS = [Sparkles, Droplets, Wind, User, Gift, Zap];
const COLORS = [
  "from-violet-500 to-purple-600",
  "from-pink-400 to-rose-500",
  "from-blue-400 to-cyan-500",
  "from-slate-500 to-gray-700",
  "from-amber-400 to-orange-500",
  "from-red-500 to-rose-600",
];

interface Props {
  categories?: ShopCategory[];
}

export default function CategoryGrid({ categories }: Props) {
  const items = categories?.length ? categories : DEFAULT_CATEGORIES;

  return (
    <section>
      <h2 className="text-2xl font-display font-bold text-gray-900 mb-5">Категории</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
        {items.map((cat, i) => {
          const Icon = ICONS[i % ICONS.length];
          const gradient = COLORS[i % COLORS.length];
          return (
            <Link
              key={cat.id}
              to={`/catalog/${cat.slug}`}
              className="group flex flex-col items-center gap-3 p-4 rounded-2xl bg-white shadow-card hover:shadow-card-hover transition-all duration-300 hover:-translate-y-0.5"
            >
              {cat.imageUrl ? (
                <img src={cat.imageUrl} alt={cat.name} className="w-14 h-14 object-cover rounded-xl" />
              ) : (
                <div className={`w-14 h-14 rounded-xl bg-gradient-to-br ${gradient} flex items-center justify-center text-white shadow-sm group-hover:scale-110 transition-transform duration-300`}>
                  <Icon size={24} />
                </div>
              )}
              <span className="text-xs font-semibold text-gray-700 text-center leading-tight">{cat.name}</span>
            </Link>
          );
        })}
      </div>
    </section>
  );
}
