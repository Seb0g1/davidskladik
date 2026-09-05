import { useState, useEffect, useRef } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Search, ChevronRight, ShoppingBag } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import type { ShopProduct } from "../types";

const PACKAGING = [
  { id: "ribbon", label: "Атласная лента",    desc: "Золотая лента + фирменный пакет Magic Vibes",                          emoji: "🎀", price: 0 },
  { id: "box",    label: "Подарочная коробка", desc: "Крафт-коробка + шёлковая бумага + наполнитель",                       emoji: "📦", price: 150 },
  { id: "deluxe", label: "Deluxe-упаковка",    desc: "Фирменная коробка + открытка + сухие цветы + атласная лента",         emoji: "✨", price: 350 },
];

const STEPS = ["Аромат", "Упаковка", "Открытка", "Итог"];

const PAGE_STYLE = `
  .gift-product-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 12px;
  }
  @media (min-width: 640px) {
    .gift-product-grid {
      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
    }
  }
  .gift-product-card {
    background: #111;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 2px;
    cursor: pointer;
    transition: border-color 0.25s ease, transform 0.25s ease;
    overflow: hidden;
  }
  .gift-product-card:hover {
    border-color: rgba(201,162,94,0.4);
    transform: translateY(-2px);
  }
  .gift-product-card.selected {
    border-color: #c9a25e;
    background: rgba(201,162,94,0.06);
  }
  .gift-pack-card {
    background: #111;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 2px;
    padding: 20px 24px;
    min-height: 120px;
    cursor: pointer;
    transition: border-color 0.25s ease, background 0.25s ease;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .gift-pack-card:hover {
    border-color: rgba(201,162,94,0.4);
  }
  .gift-pack-card.selected {
    border-color: #c9a25e;
    background: rgba(201,162,94,0.07);
  }
  .gift-skip-link {
    font-size: 12px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: rgba(245,244,240,0.28);
    background: none;
    border: none;
    cursor: pointer;
    padding: 4px 0;
    font-family: inherit;
    transition: color 0.2s;
  }
  .gift-skip-link:hover {
    color: rgba(245,244,240,0.6);
  }
`;

function StepIndicator({ step }: { step: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 0, justifyContent: "center", marginBottom: "clamp(28px,4vw,48px)" }}>
      {STEPS.map((label, i) => (
        <div key={label} style={{ display: "flex", alignItems: "center" }}>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
            <div style={{
              width: 28, height: 28, borderRadius: "50%",
              border: `1px solid ${i === step ? "#c9a25e" : i < step ? "rgba(201,162,94,0.4)" : "rgba(255,255,255,0.12)"}`,
              background: i === step ? "rgba(201,162,94,0.15)" : i < step ? "rgba(201,162,94,0.08)" : "transparent",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 11, fontWeight: 500,
              color: i === step ? "#c9a25e" : i < step ? "rgba(201,162,94,0.6)" : "rgba(245,244,240,0.2)",
              transition: "all 0.3s ease",
            }}>
              {i < step ? "✓" : i + 1}
            </div>
            <span style={{ fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", color: i === step ? "#c9a25e" : i < step ? "rgba(201,162,94,0.5)" : "rgba(245,244,240,0.2)", whiteSpace: "nowrap" }}>
              {label}
            </span>
          </div>
          {i < STEPS.length - 1 && (
            <div style={{ width: "clamp(20px,5vw,60px)", height: 1, background: i < step ? "rgba(201,162,94,0.35)" : "rgba(255,255,255,0.08)", margin: "0 8px", marginBottom: 22, transition: "background 0.3s" }} />
          )}
        </div>
      ))}
    </div>
  );
}

function ProductCardMini({ product, selected, onClick }: { product: ShopProduct; selected: boolean; onClick: () => void }) {
  const img = product.images?.[0];
  return (
    <div className={`gift-product-card${selected ? " selected" : ""}`} onClick={onClick}>
      <div style={{ aspectRatio: "4/5", overflow: "hidden", background: "#0d0d0d", position: "relative" }}>
        {img
          ? <img src={img} alt={product.name} loading="lazy" style={{ width: "100%", height: "100%", objectFit: "cover" }} />
          : <div style={{ width: "100%", height: "100%", background: "#181818" }} />
        }
        {selected && (
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(201,162,94,0.15)" }}>
            <div style={{ width: 28, height: 28, borderRadius: "50%", background: "#c9a25e", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <span style={{ fontSize: 14, color: "#0b0b0b", fontWeight: 700 }}>✓</span>
            </div>
          </div>
        )}
      </div>
      <div style={{ padding: "10px 12px 12px" }}>
        <p style={{ margin: "0 0 3px", fontSize: 9, letterSpacing: "0.14em", textTransform: "uppercase", color: "#6f6c66" }}>{product.brand}</p>
        <p style={{ margin: "0 0 5px", fontSize: 12.5, color: "#d8d5cc", lineHeight: 1.35, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{product.name}</p>
        <p style={{ margin: 0, fontSize: 13, fontWeight: 500, color: "#c9a25e" }}>{product.priceRub.toLocaleString("ru-RU")} ₽</p>
      </div>
    </div>
  );
}

function MiniSkeleton() {
  return (
    <div style={{ background: "#111", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 2, overflow: "hidden" }}>
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div style={{ padding: "10px 12px 12px", display: "flex", flexDirection: "column", gap: 6 }}>
        <div className="skeleton" style={{ height: 7, width: "40%" }} />
        <div className="skeleton" style={{ height: 11, width: "85%" }} />
        <div className="skeleton" style={{ height: 11, width: "50%", marginTop: 3 }} />
      </div>
    </div>
  );
}

export default function GiftPage() {
  const navigate = useNavigate();
  const { add } = useCart();

  const [step, setStep] = useState(0);
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [selectedProduct, setSelectedProduct] = useState<ShopProduct | null>(null);
  const [selectedPackaging, setSelectedPackaging] = useState<string | null>("ribbon");
  const [message, setMessage] = useState("");

  const searchInputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Debounce search input
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setDebouncedQ(searchQuery), 300);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [searchQuery]);

  const { data, isLoading } = useQuery({
    queryKey: ["gift-catalog", debouncedQ],
    queryFn: () => api.catalog({ q: debouncedQ || undefined, pageSize: 9, inStock: true }),
    staleTime: 2 * 60_000,
  });

  const packaging = PACKAGING.find((p) => p.id === selectedPackaging) ?? PACKAGING[0];
  const totalPrice = (selectedProduct?.priceRub ?? 0) + packaging.price;

  function handleAddToCart() {
    if (!selectedProduct) return;
    add(selectedProduct, 1);
    navigate("/cart");
  }

  return (
    <div style={{ background: "#0b0b0b", minHeight: "100vh", color: "#f5f4f0" }}>
      <style>{PAGE_STYLE}</style>

      <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(32px,5vw,64px) clamp(18px,4vw,56px)" }}>

        {/* Eyebrow */}
        <p style={{ fontSize: 10, letterSpacing: "0.26em", textTransform: "uppercase", color: "#c9a25e", margin: "0 0 12px", textAlign: "center" }}>
          Собери подарок
        </p>

        {/* Title */}
        <h1 className="serif" style={{ fontSize: "clamp(32px,5vw,54px)", fontStyle: "italic", fontWeight: 300, color: "#f5f4f0", margin: "0 0 clamp(28px,4vw,48px)", lineHeight: 1.1, textAlign: "center" }}>
          Подарочный конфигуратор
        </h1>

        {/* Step indicator */}
        <StepIndicator step={step} />

        {/* ── STEP 0: Choose fragrance ── */}
        {step === 0 && (
          <div>
            <h2 style={{ fontSize: "clamp(18px,2.4vw,26px)", fontWeight: 400, color: "#d8d5cc", margin: "0 0 20px", letterSpacing: "0.02em" }}>
              Выберите аромат
            </h2>

            {/* Search */}
            <div style={{ position: "relative", marginBottom: 24 }}>
              <Search size={15} style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", color: "rgba(245,244,240,0.28)", pointerEvents: "none" }} />
              <input
                ref={searchInputRef}
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Поиск по названию, бренду..."
                className="input-base"
                style={{ paddingLeft: 42, fontSize: 14 }}
              />
            </div>

            {/* Results grid */}
            {isLoading ? (
              <div className="gift-product-grid">
                {Array.from({ length: 9 }).map((_, i) => <MiniSkeleton key={i} />)}
              </div>
            ) : data?.products.length ? (
              <div className="gift-product-grid">
                {data.products.map((p) => (
                  <ProductCardMini
                    key={p.offerId}
                    product={p}
                    selected={selectedProduct?.offerId === p.offerId}
                    onClick={() => setSelectedProduct(p)}
                  />
                ))}
              </div>
            ) : (
              <div style={{ padding: "40px 0", textAlign: "center", color: "#7d7a73", fontSize: 14 }}>
                Ничего не найдено — попробуйте другой запрос
              </div>
            )}

            {/* Next */}
            <div style={{ marginTop: 28, display: "flex", justifyContent: "flex-end" }}>
              <button
                disabled={!selectedProduct}
                onClick={() => setStep(1)}
                style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "12px 28px", borderRadius: 2,
                  background: selectedProduct ? "#c9a25e" : "rgba(255,255,255,0.05)",
                  border: `1px solid ${selectedProduct ? "#c9a25e" : "rgba(255,255,255,0.08)"}`,
                  color: selectedProduct ? "#0b0b0b" : "rgba(245,244,240,0.28)",
                  fontSize: 13, fontWeight: 500, letterSpacing: "0.08em",
                  cursor: selectedProduct ? "pointer" : "not-allowed",
                  transition: "all 0.25s ease",
                  fontFamily: "inherit",
                }}
                onMouseEnter={e => { if (selectedProduct) e.currentTarget.style.background = "#d4ae6f"; }}
                onMouseLeave={e => { if (selectedProduct) e.currentTarget.style.background = "#c9a25e"; }}
              >
                Далее <ChevronRight size={16} />
              </button>
            </div>
          </div>
        )}

        {/* ── STEP 1: Choose packaging ── */}
        {step === 1 && (
          <div>
            <h2 style={{ fontSize: "clamp(18px,2.4vw,26px)", fontWeight: 400, color: "#d8d5cc", margin: "0 0 20px", letterSpacing: "0.02em" }}>
              Стиль упаковки
            </h2>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 14, marginBottom: 28 }}>
              {PACKAGING.map((pkg) => (
                <div
                  key={pkg.id}
                  className={`gift-pack-card${selectedPackaging === pkg.id ? " selected" : ""}`}
                  onClick={() => setSelectedPackaging(pkg.id)}
                >
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                    <span style={{ fontSize: 32, lineHeight: 1 }}>{pkg.emoji}</span>
                    <span style={{
                      fontSize: 11, letterSpacing: "0.08em", padding: "3px 10px",
                      borderRadius: 2, flexShrink: 0, marginTop: 4,
                      border: `1px solid ${pkg.price === 0 ? "rgba(93,216,118,0.3)" : "rgba(201,162,94,0.3)"}`,
                      background: pkg.price === 0 ? "rgba(93,216,118,0.06)" : "rgba(201,162,94,0.07)",
                      color: pkg.price === 0 ? "#5dd876" : "#c9a25e",
                    }}>
                      {pkg.price === 0 ? "Бесплатно" : `+${pkg.price.toLocaleString("ru-RU")} ₽`}
                    </span>
                  </div>
                  <div>
                    <p style={{ margin: "0 0 4px", fontSize: 15, color: selectedPackaging === pkg.id ? "#f5f4f0" : "#d8d5cc", fontWeight: 400 }}>{pkg.label}</p>
                    <p style={{ margin: 0, fontSize: 12.5, color: "#7d7a73", lineHeight: 1.55 }}>{pkg.desc}</p>
                  </div>
                </div>
              ))}
            </div>

            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
              <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
                <button onClick={() => setStep(0)} className="gift-skip-link">← Назад</button>
                <button onClick={() => { setSelectedPackaging(null); setStep(2); }} className="gift-skip-link">Пропустить</button>
              </div>
              <button
                onClick={() => setStep(2)}
                style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "12px 28px", borderRadius: 2,
                  background: "#c9a25e", border: "1px solid #c9a25e",
                  color: "#0b0b0b", fontSize: 13, fontWeight: 500, letterSpacing: "0.08em",
                  cursor: "pointer", transition: "background 0.2s", fontFamily: "inherit",
                }}
                onMouseEnter={e => { e.currentTarget.style.background = "#d4ae6f"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "#c9a25e"; }}
              >
                Далее <ChevronRight size={16} />
              </button>
            </div>
          </div>
        )}

        {/* ── STEP 2: Add message ── */}
        {step === 2 && (
          <div>
            <h2 style={{ fontSize: "clamp(18px,2.4vw,26px)", fontWeight: 400, color: "#d8d5cc", margin: "0 0 8px", letterSpacing: "0.02em" }}>
              Личное послание
            </h2>
            <p style={{ margin: "0 0 24px", fontSize: 13.5, color: "#7d7a73", lineHeight: 1.6 }}>
              Напишите тёплые слова — они будут напечатаны на вложенной открытке.
            </p>

            <div style={{ position: "relative" }}>
              <textarea
                value={message}
                onChange={(e) => setMessage(e.target.value.slice(0, 200))}
                placeholder="Напишите тёплые слова..."
                rows={5}
                style={{
                  width: "100%", boxSizing: "border-box",
                  padding: "16px 18px",
                  background: "#0e0e0e",
                  border: "1px solid rgba(255,255,255,0.09)",
                  borderRadius: 2,
                  color: "#f5f4f0", fontSize: 14, lineHeight: 1.7,
                  outline: "none", resize: "vertical",
                  fontFamily: "inherit",
                  transition: "border-color 0.25s",
                }}
                onFocus={e => (e.target.style.borderColor = "rgba(201,162,94,0.5)")}
                onBlur={e => (e.target.style.borderColor = "rgba(255,255,255,0.09)")}
              />
              <span style={{
                position: "absolute", bottom: 12, right: 14,
                fontSize: 11, color: message.length >= 180 ? "#f87171" : "#6f6c66", letterSpacing: "0.04em",
              }}>
                {message.length}/200
              </span>
            </div>

            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap", marginTop: 22 }}>
              <div style={{ display: "flex", gap: 16, alignItems: "center" }}>
                <button onClick={() => setStep(1)} className="gift-skip-link">← Назад</button>
                <button onClick={() => setStep(3)} className="gift-skip-link">Пропустить</button>
              </div>
              <button
                onClick={() => setStep(3)}
                style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "12px 28px", borderRadius: 2,
                  background: "#c9a25e", border: "1px solid #c9a25e",
                  color: "#0b0b0b", fontSize: 13, fontWeight: 500, letterSpacing: "0.08em",
                  cursor: "pointer", transition: "background 0.2s", fontFamily: "inherit",
                }}
                onMouseEnter={e => { e.currentTarget.style.background = "#d4ae6f"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "#c9a25e"; }}
              >
                Далее <ChevronRight size={16} />
              </button>
            </div>
          </div>
        )}

        {/* ── STEP 3: Summary ── */}
        {step === 3 && selectedProduct && (
          <div>
            <h2 style={{ fontSize: "clamp(18px,2.4vw,26px)", fontWeight: 400, color: "#d8d5cc", margin: "0 0 24px", letterSpacing: "0.02em" }}>
              Ваш подарочный набор
            </h2>

            {/* Summary card */}
            <div style={{ border: "1px solid rgba(255,255,255,0.08)", borderRadius: 2, background: "#0e0e0e", overflow: "hidden", marginBottom: 20 }}>

              {/* Product row */}
              <div style={{ display: "flex", gap: 18, padding: "20px 22px", borderBottom: "1px solid rgba(255,255,255,0.06)", alignItems: "center" }}>
                <div style={{ width: 72, height: 90, flexShrink: 0, background: "#141414", borderRadius: 2, overflow: "hidden" }}>
                  {selectedProduct.images?.[0]
                    ? <img src={selectedProduct.images[0]} alt={selectedProduct.name} style={{ width: "100%", height: "100%", objectFit: "cover" }} />
                    : <div style={{ width: "100%", height: "100%", background: "#181818" }} />
                  }
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <p style={{ margin: "0 0 3px", fontSize: 9.5, letterSpacing: "0.14em", textTransform: "uppercase", color: "#6f6c66" }}>{selectedProduct.brand}</p>
                  <p style={{ margin: "0 0 6px", fontSize: 15, color: "#f5f4f0", lineHeight: 1.35 }}>{selectedProduct.name}</p>
                  {selectedProduct.volume && <p style={{ margin: 0, fontSize: 12, color: "#7d7a73" }}>{selectedProduct.volume}</p>}
                </div>
                <p style={{ margin: 0, fontSize: 16, fontWeight: 500, color: "#f5f4f0", flexShrink: 0 }}>
                  {selectedProduct.priceRub.toLocaleString("ru-RU")} ₽
                </p>
              </div>

              {/* Packaging row */}
              {selectedPackaging && (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 22px", borderBottom: "1px solid rgba(255,255,255,0.06)", gap: 12 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <span style={{ fontSize: 20 }}>{packaging.emoji}</span>
                    <div>
                      <p style={{ margin: 0, fontSize: 13.5, color: "#d8d5cc" }}>{packaging.label}</p>
                      <p style={{ margin: "2px 0 0", fontSize: 12, color: "#7d7a73" }}>{packaging.desc}</p>
                    </div>
                  </div>
                  <p style={{ margin: 0, fontSize: 14, color: packaging.price === 0 ? "#5dd876" : "#c9a25e", flexShrink: 0 }}>
                    {packaging.price === 0 ? "Бесплатно" : `+${packaging.price.toLocaleString("ru-RU")} ₽`}
                  </p>
                </div>
              )}

              {/* Message row */}
              {message.trim() && (
                <div style={{ padding: "14px 22px", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
                  <p style={{ margin: "0 0 6px", fontSize: 9.5, letterSpacing: "0.14em", textTransform: "uppercase", color: "#6f6c66" }}>Личное послание</p>
                  <p style={{ margin: 0, fontSize: 13.5, color: "#8b8880", lineHeight: 1.65, fontStyle: "italic" }}>
                    «{message}»
                  </p>
                </div>
              )}

              {/* Total */}
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "16px 22px" }}>
                <p style={{ margin: 0, fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "#6f6c66" }}>Итого</p>
                <p style={{ margin: 0, fontSize: 20, fontWeight: 500, color: "#f5f4f0" }}>
                  {totalPrice.toLocaleString("ru-RU")} ₽
                </p>
              </div>
            </div>

            {/* CTA */}
            <button
              onClick={handleAddToCart}
              style={{
                width: "100%", display: "flex", alignItems: "center", justifyContent: "center", gap: 10,
                padding: "16px 28px", borderRadius: 2,
                background: "#c9a25e", border: "1px solid #c9a25e",
                color: "#0b0b0b", fontSize: 14, fontWeight: 600, letterSpacing: "0.08em",
                cursor: "pointer", transition: "background 0.2s", fontFamily: "inherit",
              }}
              onMouseEnter={e => { e.currentTarget.style.background = "#d4ae6f"; }}
              onMouseLeave={e => { e.currentTarget.style.background = "#c9a25e"; }}
            >
              <ShoppingBag size={18} />
              Добавить в корзину как подарок
            </button>

            <p style={{ margin: "14px 0 20px", fontSize: 12, color: "#6f6c66", lineHeight: 1.65, textAlign: "center" }}>
              Специальная упаковка и открытка будут вложены в заказ. Укажите пожелания в комментарии к заказу.
            </p>

            <div style={{ display: "flex", justifyContent: "center" }}>
              <button onClick={() => setStep(2)} className="gift-skip-link">← Изменить послание</button>
            </div>
          </div>
        )}

        {/* Fallback if step 3 but no product (shouldn't happen normally) */}
        {step === 3 && !selectedProduct && (
          <div style={{ textAlign: "center", padding: "40px 0" }}>
            <p style={{ color: "#7d7a73", marginBottom: 16 }}>Аромат не выбран</p>
            <button onClick={() => setStep(0)} className="gift-skip-link">← Выбрать аромат</button>
          </div>
        )}

        {/* Bottom nav links */}
        {step < 3 && (
          <div style={{ marginTop: 40, paddingTop: 20, borderTop: "1px solid rgba(255,255,255,0.06)", display: "flex", justifyContent: "center", gap: 24 }}>
            <Link to="/catalog" style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "rgba(245,244,240,0.3)", textDecoration: "none", transition: "color 0.2s" }}
              onMouseEnter={e => ((e.currentTarget as HTMLElement).style.color = "rgba(245,244,240,0.6)")}
              onMouseLeave={e => ((e.currentTarget as HTMLElement).style.color = "rgba(245,244,240,0.3)")}
            >
              Весь каталог
            </Link>
            <Link to="/catalog?category=sets" style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "rgba(245,244,240,0.3)", textDecoration: "none", transition: "color 0.2s" }}
              onMouseEnter={e => ((e.currentTarget as HTMLElement).style.color = "rgba(245,244,240,0.6)")}
              onMouseLeave={e => ((e.currentTarget as HTMLElement).style.color = "rgba(245,244,240,0.3)")}
            >
              Готовые наборы
            </Link>
          </div>
        )}
      </div>
    </div>
  );
}
