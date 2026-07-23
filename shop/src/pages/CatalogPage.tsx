import { useState } from "react";
import { useSearchParams, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { SlidersHorizontal, X, ChevronDown } from "lucide-react";
import { api } from "../api";
import ProductCard from "../components/ProductCard";
import clsx from "clsx";

const SORT_OPTIONS = [
  { value: "name", label: "По названию" },
  { value: "price_asc", label: "Сначала дешевле" },
  { value: "price_desc", label: "Сначала дороже" },
];

const PAGE_SIZE = 24;

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden shadow-card animate-pulse">
      <div className="aspect-square bg-gray-200" />
      <div className="p-4 space-y-2">
        <div className="h-3 bg-gray-200 rounded w-1/3" />
        <div className="h-4 bg-gray-200 rounded" />
        <div className="h-4 bg-gray-200 rounded w-3/4" />
        <div className="h-6 bg-gray-200 rounded w-1/2 mt-2" />
      </div>
    </div>
  );
}

export default function CatalogPage() {
  const { category } = useParams<{ category?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const [filtersOpen, setFiltersOpen] = useState(false);

  const q = searchParams.get("q") ?? "";
  const brand = searchParams.get("brand") ?? "";
  const sort = (searchParams.get("sort") ?? "name") as "name" | "price_asc" | "price_desc";
  const page = Number(searchParams.get("page") ?? 1);
  const inStock = searchParams.get("inStock") === "true";

  const { data, isLoading } = useQuery({
    queryKey: ["shop-catalog", { category, q, brand, sort, page, inStock }],
    queryFn: () => api.catalog({ category, q, brand, sort, page, pageSize: PAGE_SIZE, inStock }),
  });

  function setParam(key: string, value: string | null) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (value === null || value === "") {
        next.delete(key);
      } else {
        next.set(key, value);
      }
      if (key !== "page") next.delete("page");
      return next;
    });
  }

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;

  const title = category
    ? category.charAt(0).toUpperCase() + category.slice(1)
    : q ? `Поиск: «${q}»` : "Весь каталог";

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-display font-bold text-gray-900">{title}</h1>
          {data && <p className="text-sm text-gray-500 mt-1">{data.total} товаров</p>}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setFiltersOpen((v) => !v)}
            className="flex items-center gap-2 px-4 py-2 border border-gray-200 rounded-xl text-sm font-medium hover:border-violet-400 hover:text-violet-700 transition-colors lg:hidden"
          >
            <SlidersHorizontal size={16} /> Фильтры
          </button>
          <select
            value={sort}
            onChange={(e) => setParam("sort", e.target.value)}
            className="px-4 py-2 border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 bg-white"
          >
            {SORT_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="flex gap-6">
        {/* Filters sidebar */}
        <aside className={clsx(
          "flex-shrink-0 w-60 space-y-6",
          "hidden lg:block",
          filtersOpen ? "!block fixed inset-0 z-40 bg-white p-6 overflow-y-auto lg:static lg:bg-transparent lg:p-0 lg:z-auto" : ""
        )}>
          {filtersOpen && (
            <button onClick={() => setFiltersOpen(false)} className="lg:hidden flex items-center gap-2 text-sm text-gray-500 mb-4">
              <X size={16} /> Закрыть
            </button>
          )}

          {/* Search */}
          <div>
            <h3 className="font-semibold text-sm text-gray-700 mb-2">Поиск</h3>
            <input
              type="text"
              value={q}
              onChange={(e) => setParam("q", e.target.value)}
              placeholder="Название, бренд..."
              className="w-full px-3 py-2 border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400"
            />
          </div>

          {/* Brands */}
          {data?.brands && data.brands.length > 0 && (
            <div>
              <h3 className="font-semibold text-sm text-gray-700 mb-2 flex items-center gap-1">
                Бренд <ChevronDown size={14} />
              </h3>
              <div className="space-y-1 max-h-48 overflow-y-auto">
                <label className="flex items-center gap-2 cursor-pointer py-1">
                  <input
                    type="radio"
                    name="brand"
                    checked={!brand}
                    onChange={() => setParam("brand", null)}
                    className="accent-violet-600"
                  />
                  <span className="text-sm text-gray-700">Все бренды</span>
                </label>
                {data.brands.map((b) => (
                  <label key={b} className="flex items-center gap-2 cursor-pointer py-1">
                    <input
                      type="radio"
                      name="brand"
                      checked={brand === b}
                      onChange={() => setParam("brand", b)}
                      className="accent-violet-600"
                    />
                    <span className="text-sm text-gray-700">{b}</span>
                  </label>
                ))}
              </div>
            </div>
          )}

          {/* In stock only */}
          <div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={inStock}
                onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)}
                className="accent-violet-600 w-4 h-4"
              />
              <span className="text-sm font-medium text-gray-700">Только в наличии</span>
            </label>
          </div>

          {/* Reset */}
          {(brand || q || inStock) && (
            <button
              onClick={() => setSearchParams({})}
              className="w-full text-sm text-violet-600 hover:text-violet-800 font-medium"
            >
              Сбросить фильтры
            </button>
          )}
        </aside>

        {/* Product grid */}
        <div className="flex-1 min-w-0">
          {isLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-4 gap-4">
              {Array.from({ length: PAGE_SIZE }).map((_, i) => <ProductSkeleton key={i} />)}
            </div>
          ) : data?.products.length ? (
            <>
              <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-4 gap-4">
                {data.products.map((p) => (
                  <ProductCard key={p.offerId} product={p} />
                ))}
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex justify-center items-center gap-2 mt-8">
                  {Array.from({ length: totalPages }, (_, i) => i + 1).map((p) => (
                    <button
                      key={p}
                      onClick={() => setParam("page", String(p))}
                      className={clsx(
                        "w-9 h-9 rounded-lg text-sm font-medium transition-colors",
                        p === page
                          ? "bg-violet-600 text-white"
                          : "bg-white border border-gray-200 text-gray-700 hover:border-violet-400"
                      )}
                    >
                      {p}
                    </button>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div className="text-center py-24 text-gray-400">
              <div className="text-4xl mb-3">🔍</div>
              <p className="text-lg font-medium">Товары не найдены</p>
              <p className="text-sm mt-1">Попробуйте изменить фильтры или поисковый запрос</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
