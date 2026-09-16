"use client";
import Link from "next/link";
import { Send, Mail, Clock } from "lucide-react";

const FOOTER_CSS = `
.footer-link { font-size: 13px; color: rgba(245,244,240,0.46); text-decoration: none; letter-spacing: 0.02em; transition: color 0.25s; line-height: 1.5; display: inline-block; }
.footer-link:hover { color: #f5f4f0; }
.footer-col-title { font-size: 9.5px; font-weight: 400; letter-spacing: 0.28em; text-transform: uppercase; color: rgba(201,162,94,0.7); margin: 0 0 18px; }
`;

const CATALOG_LINKS = [
  { label: "Все товары", href: "/catalog" },
  { label: "Бренды", href: "/brands" },
  { label: "Новинки", href: "/new" },
  { label: "Женская парфюмерия", href: "/catalog?q=женская" },
  { label: "Мужская парфюмерия", href: "/catalog?q=мужская" },
  { label: "Пробники и отливанты", href: "/catalog?category=testers" },
  { label: "Подарочные наборы", href: "/catalog?category=sets" },
  { label: "✦ AI-подбор аромата", href: "/find" },
];

const COMPANY_LINKS = [
  { label: "Блог и статьи", href: "/blog" },
  { label: "Новости", href: "/news" },
  { label: "Женские ароматы", href: "/guide/women" },
  { label: "Мужские ароматы", href: "/guide/men" },
  { label: "Парфюм в подарок", href: "/guide/gift" },
  { label: "Ароматы для офиса", href: "/guide/office" },
];

const SERVICE_LINKS = [
  { label: "Как сделать заказ", href: "/faq" },
  { label: "Вопросы и ответы", href: "/faq" },
  { label: "Способы оплаты", href: "/delivery" },
  { label: "Способы доставки", href: "/delivery" },
  { label: "Возврат товара", href: "/delivery" },
  { label: "Гарантия оригинала", href: "/warranty" },
];

export default function Footer() {
  return (
    <>
      <style>{FOOTER_CSS}</style>
      <footer style={{ background: "#080808", borderTop: "1px solid rgba(255,255,255,0.05)", marginTop: 80 }}>
        <div style={{ maxWidth: 1240, margin: "0 auto", padding: "clamp(48px,6vw,72px) clamp(18px,4vw,56px) 0" }}>

          {/* Brand row */}
          <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 24, paddingBottom: 40, borderBottom: "1px solid rgba(255,255,255,0.05)", marginBottom: 48 }}>
            <div style={{ maxWidth: 300 }}>
              <Link href="/" style={{ textDecoration: "none" }}>
                <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: 26, color: "#f5f4f0" }}>Magic Vibes</span>
              </Link>
              <p style={{ fontSize: 13, lineHeight: 1.7, color: "#6b6760", marginTop: 12, fontWeight: 300 }}>Оригинальная парфюмерия мировых брендов. Доставка по всей России.</p>
            </div>
            <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" style={{ width: 36, height: 36, borderRadius: "50%", border: "1px solid rgba(255,255,255,0.1)", display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(245,244,240,0.4)", textDecoration: "none", transition: "border-color 0.3s,color 0.3s" }}
              onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,162,94,0.5)"; el.style.color = "#c9a25e"; }}
              onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "rgba(245,244,240,0.4)"; }}>
              <Send size={14} strokeWidth={1.5} />
            </a>
          </div>

          {/* 4-col grid */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "clamp(28px,4vw,48px)", paddingBottom: "clamp(40px,5vw,60px)" }}>
            <div>
              <p className="footer-col-title">Сервис и помощь</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {SERVICE_LINKS.map(l => <li key={l.href + l.label}><Link href={l.href} className="footer-link">{l.label}</Link></li>)}
              </ul>
            </div>
            <div>
              <p className="footer-col-title">О компании</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {COMPANY_LINKS.map(l => <li key={l.href + l.label}><Link href={l.href} className="footer-link">{l.label}</Link></li>)}
              </ul>
            </div>
            <div>
              <p className="footer-col-title">Каталог</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {CATALOG_LINKS.map(l => <li key={l.href + l.label}><Link href={l.href} className="footer-link">{l.label}</Link></li>)}
              </ul>
            </div>
            <div>
              <p className="footer-col-title">Контакты</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                <li><a href="mailto:info@magicvibes.ru" className="footer-link" style={{ display: "flex", alignItems: "center", gap: 8 }}><Mail size={12} strokeWidth={1.5} style={{ color: "rgba(201,162,94,0.5)" }} />info@magicvibes.ru</a></li>
                <li><a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="footer-link" style={{ display: "flex", alignItems: "center", gap: 8 }}><Send size={12} strokeWidth={1.5} style={{ color: "rgba(201,162,94,0.5)" }} />Telegram</a></li>
                <li style={{ display: "flex", alignItems: "flex-start", gap: 8, color: "rgba(245,244,240,0.32)", fontSize: 12.5 }}><Clock size={12} strokeWidth={1.5} style={{ flexShrink: 0, marginTop: 3, color: "rgba(201,162,94,0.4)" }} /><span>Работаем круглосуточно</span></li>
              </ul>
            </div>
          </div>
        </div>

        <div style={{ borderTop: "1px solid rgba(255,255,255,0.05)" }}>
          <div style={{ maxWidth: 1240, margin: "0 auto", padding: "18px clamp(18px,4vw,56px)", display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "10px 24px" }}>
            <div style={{ fontSize: 11.5, color: "#3d3b39", letterSpacing: "0.04em" }}>© {new Date().getFullYear()} Magic Vibes — 100% оригинальная продукция</div>
            <div style={{ display: "flex", gap: 8 }}>
              {["VISA", "Mastercard", "МИР", "СБП"].map(p => <span key={p} style={{ fontSize: 9.5, fontWeight: 600, letterSpacing: "0.08em", color: "rgba(245,244,240,0.2)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, padding: "3px 7px" }}>{p}</span>)}
            </div>
            <div style={{ fontSize: 11.5, color: "#3d3b39" }}>Доставка по всей России</div>
          </div>
        </div>
      </footer>
    </>
  );
}
