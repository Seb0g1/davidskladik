import Link from "next/link";
import { getFeatures } from "@/lib/features";
import { Send, Mail, Clock, ArrowUpRight } from "lucide-react";
import { OzonPayLogo } from "@/components/OzonPay";
import Logo from "./Logo";
import YandexRatingBadge from "./YandexRatingBadge";
import { SELLER } from "@/lib/legal";

const FOOTER_CSS = `
.mv-foot a.fl { font-size: 15px; color: rgba(245,242,236,0.62); text-decoration: none; transition: color 0.2s; line-height: 1.5; display: inline-block; }
.mv-foot a.fl:hover { color: #d9f84a; }
.mv-foot .mv-yrating { display: block; border: 1px solid rgba(245,242,236,0.2); border-radius: 10px; overflow: hidden; transition: border-color 0.2s; }
.mv-foot .mv-yrating:hover { border-color: rgba(245,242,236,0.45); }
.mv-foot .mv-yrating iframe { display: block; border: 0; filter: invert(0.9) hue-rotate(180deg) contrast(1.25); }
.mv-foot .mv-yrating-link { font-size: 13px; font-weight: 600; color: rgba(245,242,236,0.7); text-decoration: none; border: 1px solid rgba(245,242,236,0.2); border-radius: 10px; padding: 8px 12px; }
.mv-foot .mv-yrating-link:hover { color: #d9f84a; border-color: rgba(245,242,236,0.45); }
.mv-foot .mv-legal-links { display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; }
.mv-foot .mv-legal-links a, .mv-foot .mv-legal-links button { color: rgba(245,242,236,0.55); text-decoration: none; background: none; border: 0; padding: 0; font: inherit; cursor: pointer; }
.mv-foot .mv-legal-links a:hover, .mv-foot .mv-legal-links button:hover { color: #d9f84a; }
.mv-foot .ft { font-size: 12px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; color: #ff3d7f; margin: 0 0 16px; }
`;

const COLUMNS = [
  { title: "Каталог", links: [
    { label: "Все товары", href: "/catalog" },
    { label: "Бренды", href: "/brands" },
    { label: "Новинки", href: "/new" },
    { label: "Женская парфюмерия", href: "/collections/zhenskaya" },
    { label: "Мужская парфюмерия", href: "/collections/muzhskaya" },
    { label: "Пробники и отливанты", href: "/collections/probniki" },
    { label: "Подарочные наборы", href: "/collections/nabory" },
  ]},
  { title: "Сервисы", links: [
    { label: "✦ AI-подбор аромата", href: "/find" },
    { label: "Конструктор подарка", href: "/gift", feature: "giftBuilder" },
    { label: "Карта ароматов мира", href: "/world" },
    { label: "Гарантия оригинала", href: "/warranty" },
  ]},
  { title: "Помощь", links: [
    { label: "Как сделать заказ", href: "/faq" },
    { label: "Вопросы и ответы", href: "/faq" },
    { label: "Оплата через Ozon Pay", href: "/delivery#payment" },
    { label: "Доставка", href: "/delivery" },
    { label: "Возврат товара", href: "/terms#return" },
    { label: "Условия покупки", href: "/terms" },
  ]},
  { title: "Magic Vibes", links: [
    { label: "Блог и статьи", href: "/blog" },
    { label: "Новости", href: "/news" },
    { label: "Женские ароматы — гид", href: "/guide/women" },
    { label: "Мужские ароматы — гид", href: "/guide/men" },
    { label: "Парфюм в подарок", href: "/guide/gift" },
    { label: "Ароматы для офиса", href: "/guide/office" },
  ]},
];

export default async function Footer() {
  const features = await getFeatures();
  return (
    <>
      <style>{FOOTER_CSS}</style>
      <footer className="mv-foot" style={{ background: "#121212", color: "#f5f2ec", marginTop: "clamp(60px,8vw,120px)", overflow: "hidden", borderRadius: "clamp(24px,4vw,48px) clamp(24px,4vw,48px) 0 0" }}>
        <div style={{ maxWidth: 1360, margin: "0 auto", padding: "clamp(48px,6vw,80px) clamp(20px,4vw,56px) 0" }}>

          {/* CTA row */}
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", flexWrap: "wrap", gap: 28, paddingBottom: "clamp(36px,5vw,56px)", borderBottom: "1px solid rgba(245,242,236,0.12)" }}>
            <div style={{ maxWidth: 560 }}>
              <Logo tone="paper" height={34} />
              <p className="h-section" style={{ color: "#f5f2ec", marginTop: 28, fontSize: "clamp(26px,3.4vw,46px)" }}>
                Оригинальные ароматы
                <span className="h-script" style={{ color: "#d9f84a" }}>с доставкой по всей России</span>
              </p>
            </div>
            <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="btn-primary on-dark" style={{ gap: 12 }}>
              <Send size={18} /> Наш Telegram <ArrowUpRight size={18} />
            </a>
          </div>

          {/* columns */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: "clamp(28px,4vw,48px)", padding: "clamp(36px,5vw,56px) 0" }}>
            {COLUMNS.map((c) => (
              <div key={c.title}>
                <p className="ft">{c.title}</p>
                <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 9 }}>
                  {c.links.filter((l) => !("feature" in l) || features[l.feature as keyof typeof features]).map((l) => <li key={l.href + l.label}><Link href={l.href} className="fl">{l.label}</Link></li>)}
                </ul>
              </div>
            ))}
            <div>
              <p className="ft">Контакты</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 11 }}>
                <li><a href="mailto:noreply@magicvibes.ru" className="fl" style={{ display: "flex", alignItems: "center", gap: 8 }}><Mail size={15} color="#d9f84a" />noreply@magicvibes.ru</a></li>
                <li><a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="fl" style={{ display: "flex", alignItems: "center", gap: 8 }}><Send size={15} color="#d9f84a" />Telegram</a></li>
                <li style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 15, color: "rgba(245,242,236,0.62)" }}><Clock size={15} color="#d9f84a" />Работаем круглосуточно</li>
              </ul>
            </div>
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "12px 24px", padding: "20px 0", borderTop: "1px solid rgba(245,242,236,0.12)" }}>
            <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: "12px 20px" }}>
              <div style={{ fontSize: 13, color: "rgba(245,242,236,0.5)" }}>© {new Date().getFullYear()} Magic Vibes — 100% оригинальная продукция</div>
              <YandexRatingBadge />
            </div>
            <Link href="/delivery#payment" aria-label="Оплата через Ozon Pay" style={{ display: "flex", alignItems: "center", gap: 8, textDecoration: "none" }}>
              <OzonPayLogo height={20} />
              {["МИР", "СБП"].map(p => <span key={p} style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.06em", color: "rgba(245,242,236,0.7)", border: "1px solid rgba(245,242,236,0.2)", borderRadius: 8, padding: "4px 8px" }}>{p}</span>)}
            </Link>
          </div>

          {/* seller requisites (ЗоЗПП ст. 9, ПП РФ № 2463) + legal documents */}
          <div style={{ display: "flex", flexDirection: "column", gap: 10, paddingBottom: 20 }}>
            <nav className="mv-legal-links" aria-label="Правовая информация">
              <Link prefetch={false} href="/terms">Условия покупки (оферта)</Link>
              <Link prefetch={false} href="/privacy">Политика обработки персональных данных</Link>
              <Link prefetch={false} href="/consent">Согласие на обработку данных</Link>
              <Link prefetch={false} href="/consent-ads">Согласие на рассылку</Link>
              <Link prefetch={false} href="/cookies">Файлы cookie</Link>
            </nav>
            <div style={{ fontSize: 12, color: "rgba(245,242,236,0.4)", lineHeight: 1.5 }}>
              {SELLER.name} · ОГРНИП {SELLER.ogrnip} · ИНН {SELLER.inn}
            </div>
          </div>
        </div>

        {/* giant wordmark */}
        <p aria-hidden="true" className="mv-giant" style={{ color: "#ff3d7f", textAlign: "center", fontSize: "clamp(44px, 11.6vw, 230px)", transform: "translateY(16%)" }}>
          Magic Vibes
        </p>
      </footer>
    </>
  );
}
