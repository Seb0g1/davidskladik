"use client";
import { useState, useEffect } from "react";

export default function ContestSection() {
  const [capitalizedMonth, setCapitalizedMonth] = useState("");
  useEffect(() => {
    const m = new Intl.DateTimeFormat("ru-RU", { month: "long" }).format(new Date());
    setCapitalizedMonth(m.charAt(0).toUpperCase() + m.slice(1));
  }, []);

  return (
    <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
      <div style={{
        background: "linear-gradient(135deg, rgba(14,12,8,0.9) 0%, rgba(18,14,10,0.8) 100%)",
        border: "1px solid rgba(201,162,94,0.18)",
        borderRadius: 4, padding: "clamp(28px,4vw,52px)",
        display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: "clamp(24px,4vw,48px)",
        alignItems: "center", position: "relative", overflow: "hidden",
      }}>
        <div style={{ position: "absolute", top: 0, right: 0, width: 120, height: 120, background: "radial-gradient(circle at 100% 0%, rgba(201,162,94,0.08) 0%, transparent 65%)", pointerEvents: "none" }} />

        <div>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "rgba(201,162,94,0.07)", border: "1px solid rgba(201,162,94,0.2)", borderRadius: 2, padding: "4px 12px", marginBottom: 18 }}>
            <span style={{ fontSize: 14 }}>🏅</span>
            <span style={{ fontSize: 10, letterSpacing: "0.2em", textTransform: "uppercase", color: "#c9a25e" }}>Конкурс {capitalizedMonth}</span>
          </div>
          <h2 className="serif reveal-heading" style={{ margin: "0 0 14px", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(26px,3.5vw,44px)", lineHeight: 1.05, color: "#f5f4f0" }}>
            #МойАромат
          </h2>
          <p style={{ fontSize: "clamp(13px,1.4vw,15px)", color: "rgba(242,237,230,0.55)", lineHeight: 1.8, maxWidth: "38ch", margin: "0 0 24px" }}>
            Опубликуйте фото с вашим ароматом в Instagram или Telegram с тегом <span style={{ color: "#c9a25e" }}>#МойАромат</span>. Победитель с наибольшим числом реакций получит флакон на выбор.
          </p>
          <a
            href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer"
            style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "10px 22px", borderRadius: 2, background: "rgba(201,162,94,0.1)", border: "1px solid rgba(201,162,94,0.3)", color: "#c9a25e", fontSize: 13, fontWeight: 600, textDecoration: "none", letterSpacing: "0.04em", transition: "all 0.2s" }}
            onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(201,162,94,0.2)"; }}
            onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "rgba(201,162,94,0.1)"; }}
          >
            Участвовать в Telegram
          </a>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          {[
            { num: "01", title: "Сфотографируйтесь", desc: "с любым ароматом из нашего каталога" },
            { num: "02", title: "Опубликуйте", desc: "фото с тегом #МойАромат в Instagram или Telegram" },
            { num: "03", title: "Победитель", desc: "получает флакон на выбор — объявляем в конце месяца" },
          ].map(step => (
            <div key={step.num} style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
              <span style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontSize: 26, color: "rgba(201,162,94,0.3)", lineHeight: 1, flexShrink: 0, minWidth: 28 }}>{step.num}</span>
              <div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#f2ede6", marginBottom: 2 }}>{step.title}</div>
                <div style={{ fontSize: 12, color: "rgba(242,237,230,0.45)", lineHeight: 1.55 }}>{step.desc}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
