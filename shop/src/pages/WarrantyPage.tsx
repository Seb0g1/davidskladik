import { ShieldCheck, Award, FileText, Phone } from "lucide-react";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent2: "#D9BF8F",
};

function Section({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <div style={{ background: S.surface, borderRadius: 20, padding: "28px 28px 24px", border: `1px solid ${S.border}`, marginBottom: 16 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
        <div style={{ width: 40, height: 40, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(201,169,110,0.1)", border: `1px solid rgba(201,169,110,0.2)`, flexShrink: 0 }}>
          {icon}
        </div>
        <h2 style={{ fontSize: 17, fontWeight: 600, color: S.text, margin: 0 }}>{title}</h2>
      </div>
      <div style={{ fontSize: 14, color: S.muted, lineHeight: 1.8 }}>
        {children}
      </div>
    </div>
  );
}

const guarantees = [
  {
    icon: "🔬",
    title: "Прямые поставки",
    desc: "Мы работаем напрямую с официальными дистрибьюторами и авторизованными импортёрами. Каждая позиция проходит проверку подлинности до попадания на склад.",
  },
  {
    icon: "📦",
    title: "Оригинальная упаковка",
    desc: "Товары хранятся и доставляются в оригинальных фабричных упаковках без вскрытия. Все защитные пломбы и наклейки сохранены.",
  },
  {
    icon: "🧾",
    title: "Документы",
    desc: "По запросу предоставляем подтверждающие документы о происхождении товара (сертификат соответствия, декларация ЕАЭС).",
  },
  {
    icon: "💯",
    title: "100% возврат за подделку",
    desc: "Если вы докажете, что полученный товар является подделкой, мы вернём полную стоимость заказа без каких-либо условий.",
  },
];

const rights = [
  {
    article: "Ст. 18 ЗоЗПП",
    title: "Товар ненадлежащего качества",
    desc: "Вы вправе потребовать замены, устранения недостатков, соразмерного снижения цены или возврата уплаченной суммы.",
  },
  {
    article: "Ст. 25 ЗоЗПП",
    title: "Обмен товара надлежащего качества",
    desc: "Непродовольственный товар надлежащего качества можно обменять в течение 14 дней, если он не был в употреблении и сохранены потребительские свойства.",
  },
  {
    article: "Ст. 26.1 ЗоЗПП",
    title: "Дистанционная продажа",
    desc: "При покупке через интернет вы вправе отказаться от товара в любое время до его получения или в течение 7 дней после получения без объяснения причин.",
  },
  {
    article: "Ст. 22 ЗоЗПП",
    title: "Срок возврата денег",
    desc: "Требование о возврате денежных средств подлежит удовлетворению в течение 10 дней со дня предъявления соответствующего требования.",
  },
];

export default function WarrantyPage() {
  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 800, margin: "0 auto", padding: "64px 24px 96px" }}>

        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 56 }}>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 10, marginBottom: 20, padding: "6px 16px", borderRadius: 999, border: `1px solid ${S.borderMd}`, background: "rgba(201,169,110,0.06)" }}>
            <ShieldCheck size={13} style={{ color: S.accent }} />
            <span style={{ fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", color: S.accent }}>Гарантии</span>
          </div>
          <h1 className="serif" style={{ fontSize: "clamp(26px,5vw,40px)", fontStyle: "italic", fontWeight: 600, color: S.text, margin: "0 0 16px" }}>
            Гарантия оригинала
          </h1>
          <div style={{ width: 48, height: 1, background: S.accent, margin: "0 auto 16px" }} />
          <p style={{ fontSize: 15, color: S.muted, lineHeight: 1.7, maxWidth: 520, margin: "0 auto" }}>
            Мы несём полную ответственность за подлинность каждого товара и соблюдаем все права потребителей по российскому законодательству
          </p>
        </div>

        {/* Authenticity guarantee */}
        <Section icon={<Award size={18} style={{ color: S.accent }} />} title="Гарантия подлинности">
          <div style={{ marginBottom: 20 }}>
            Magic Vibes продаёт <strong style={{ color: S.text }}>исключительно оригинальную парфюмерию</strong>.
            Мы категорически против контрафакта и несём личную ответственность за каждый товар на нашей платформе.
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))", gap: 12 }}>
            {guarantees.map(g => (
              <div key={g.title} style={{ padding: "16px 18px", borderRadius: 14, background: S.surface2, border: `1px solid ${S.border}` }}>
                <div style={{ fontSize: 22, marginBottom: 10 }}>{g.icon}</div>
                <div style={{ fontSize: 13, fontWeight: 600, color: S.accent2, marginBottom: 6 }}>{g.title}</div>
                <div style={{ fontSize: 13, lineHeight: 1.7 }}>{g.desc}</div>
              </div>
            ))}
          </div>
        </Section>

        {/* Consumer rights */}
        <Section icon={<FileText size={18} style={{ color: S.accent }} />} title="Ваши права как потребителя">
          <div style={{ marginBottom: 20 }}>
            Закон РФ «О защите прав потребителей» № 2300-1 гарантирует вам следующие права при покупке в Magic Vibes:
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {rights.map(r => (
              <div key={r.article} style={{ display: "flex", gap: 14, padding: "14px 16px", borderRadius: 12, background: S.surface2, border: `1px solid ${S.border}` }}>
                <div style={{ flexShrink: 0, paddingTop: 2 }}>
                  <span style={{ fontSize: 11, fontWeight: 700, color: S.accent, fontFamily: "monospace", whiteSpace: "nowrap" }}>{r.article}</span>
                </div>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 600, color: S.text, marginBottom: 4 }}>{r.title}</div>
                  <div style={{ fontSize: 13, lineHeight: 1.7 }}>{r.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </Section>

        {/* How to claim */}
        <Section icon={<Phone size={18} style={{ color: S.accent }} />} title="Как обратиться с претензией">
          <div style={{ marginBottom: 20 }}>
            Если у вас возникли вопросы по качеству товара или вы хотите воспользоваться своими правами:
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {[
              { step: "1", text: "Напишите на info@magicvibes.ru с темой «Претензия» и укажите номер заказа" },
              { step: "2", text: "Приложите фотографии товара и упаковки (при необходимости)" },
              { step: "3", text: "Мы рассмотрим обращение в течение 1 рабочего дня и предложим решение" },
              { step: "4", text: "Возврат средств производится в течение 10 рабочих дней после подтверждения" },
            ].map(s => (
              <div key={s.step} style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
                <div style={{ width: 28, height: 28, borderRadius: "50%", background: "rgba(201,169,110,0.12)", border: `1px solid rgba(201,169,110,0.25)`, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, fontSize: 12, fontWeight: 700, color: S.accent }}>
                  {s.step}
                </div>
                <div style={{ fontSize: 14, paddingTop: 5 }}>{s.text}</div>
              </div>
            ))}
          </div>
          <div style={{ marginTop: 20, padding: "14px 16px", borderRadius: 12, background: "rgba(201,169,110,0.06)", border: `1px solid rgba(201,169,110,0.15)` }}>
            <span style={{ fontSize: 13 }}>
              Email:{" "}
              <a href="mailto:info@magicvibes.ru" style={{ color: S.accent, textDecoration: "none", fontWeight: 500 }}>info@magicvibes.ru</a>
              {" "}· Telegram:{" "}
              <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" style={{ color: S.accent, textDecoration: "none", fontWeight: 500 }}>@magicvibes_ru</a>
            </span>
          </div>
        </Section>

        {/* Legal details */}
        <div style={{ background: S.surface, borderRadius: 20, padding: "24px 28px", border: `1px solid ${S.border}` }}>
          <h2 style={{ fontSize: 14, fontWeight: 700, color: S.muted, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 16 }}>Реквизиты продавца</h2>
          <div style={{ fontSize: 13, color: S.muted, lineHeight: 2 }}>
            <div><span style={{ color: S.subtle }}>Продавец: </span><span style={{ color: S.text }}>ИП Шальнев Давид Алиевич</span></div>
            <div><span style={{ color: S.subtle }}>ОГРНИП: </span><span style={{ color: S.text, fontFamily: "monospace" }}>323861700065205</span></div>
            <div><span style={{ color: S.subtle }}>ИНН: </span><span style={{ color: S.text, fontFamily: "monospace" }}>860203590860</span></div>
            <div><span style={{ color: S.subtle }}>Email: </span><a href="mailto:info@magicvibes.ru" style={{ color: S.accent, textDecoration: "none" }}>info@magicvibes.ru</a></div>
          </div>
          <div style={{ marginTop: 16, fontSize: 12, color: S.subtle, lineHeight: 1.7, borderTop: `1px solid ${S.border}`, paddingTop: 14 }}>
            Информация актуальна в соответствии с Законом РФ «О защите прав потребителей» № 2300-1
            и Федеральным законом № 149-ФЗ «Об информации, информационных технологиях и о защите информации».
            ИП Шальнев Давид Алиевич несёт ответственность за достоверность предоставляемой информации о товарах.
          </div>
        </div>
      </div>
    </div>
  );
}
