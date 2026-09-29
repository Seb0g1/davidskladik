import Link from "next/link";
import { SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import { SELLER, LEGAL_REVISION } from "@/lib/legal";

export type LegalSection = { id: string; title: string; items: React.ReactNode[] };

// Same layout as /terms: numbered cards + seller requisites
export default function LegalDoc({ path, title, script, sections, email, phone, intro }: {
  path: string; title: string; script: string; sections: LegalSection[];
  email: string; phone?: string; intro?: React.ReactNode;
}) {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: title, url: path }]);
  return (
    <div className="mv-legal">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <div className="mv-legal-wrap">
        <nav className="mv-legal-crumbs" aria-label="Навигация"><Link href="/">Главная</Link> / <span>{title}</span></nav>
        <h1 className={`mv-legal-title${title.length > 24 ? " long" : ""}`}>{title}</h1>
        <div className="mv-legal-script">{script}</div>
        <p className="mv-legal-rev">Редакция от {LEGAL_REVISION}</p>
        {intro && <p className="mv-legal-rev" style={{ fontSize: 15, lineHeight: 1.6 }}>{intro}</p>}

        {sections.length > 3 && (
          <nav className="mv-legal-toc" aria-label="Содержание">
            {sections.map((s, i) => <a key={s.id} href={`#${s.id}`}><span>{String(i + 1).padStart(2, "0")}</span>{s.title}</a>)}
          </nav>
        )}

        {sections.map((s, i) => (
          <section key={s.id} id={s.id} className="mv-legal-card">
            <h2><span>{String(i + 1).padStart(2, "0")}</span>{s.title}</h2>
            <ol>
              {s.items.map((it, j) => <li key={j}><b>{i + 1}.{j + 1}.</b> <span>{it}</span></li>)}
            </ol>
          </section>
        ))}

        <section id="operator" className="mv-legal-card ink">
          <h2><span>{String(sections.length + 1).padStart(2, "0")}</span>Оператор и продавец</h2>
          <dl className="mv-legal-req">
            <div><dt>Оператор</dt><dd>{SELLER.name}</dd></div>
            <div><dt>ОГРНИП</dt><dd>{SELLER.ogrnip}</dd></div>
            <div><dt>ИНН</dt><dd>{SELLER.inn}</dd></div>
            <div><dt>Email</dt><dd><a href={`mailto:${email}`}>{email}</a></dd></div>
            {phone && <div><dt>Телефон</dt><dd><a href={`tel:${phone.replace(/[^\d+]/g, "")}`}>{phone}</a></dd></div>}
            <div><dt>Сайт</dt><dd><a href={SITE_URL}>{SELLER.site}</a></dd></div>
          </dl>
        </section>

        <p className="mv-legal-foot">
          См. также: <Link prefetch={false} href="/privacy">Политика обработки персональных данных</Link> · <Link prefetch={false} href="/consent">Согласие на обработку данных</Link> · <Link prefetch={false} href="/consent-ads">Согласие на рассылку</Link> · <Link prefetch={false} href="/cookies">Файлы cookie</Link> · <Link prefetch={false} href="/terms">Условия покупки</Link>
        </p>
      </div>
    </div>
  );
}

export async function legalContacts() {
  const { fetchSettings } = await import("@/lib/api");
  let email = "noreply@magicvibes.ru", phone = "";
  try {
    const s = await fetchSettings();
    email = s.contactEmail || email;
    phone = s.contactPhone || "";
  } catch { /* defaults */ }
  return { email, phone };
}
