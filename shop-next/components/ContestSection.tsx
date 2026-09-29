/* eslint-disable @next/next/no-img-element */
import type { ShopContest } from "@/lib/types";

const fmtDate = (d: string) => new Date(`${d}T00:00:00`).toLocaleDateString("ru-RU", { day: "numeric", month: "long" });

/** Contests are managed in the davidsklad.ru admin (Магазин → Конкурсы); nothing renders when none is live. */
export default function ContestSection({ contests }: { contests: ShopContest[] }) {
  if (!contests.length) return null;
  return (
    <>
      {contests.map((c) => <ContestCard key={c.id} contest={c} />)}
    </>
  );
}

function ContestCard({ contest: c }: { contest: ShopContest }) {
  const steps = (c.steps || []).filter((s) => s.title || s.desc);
  return (
    <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
      <div style={{
        background: "linear-gradient(135deg, rgba(255,255,255,0.9) 0%, rgba(255,255,255,0.8) 100%)",
        border: "1px solid rgba(var(--accent-rgb),0.18)",
        borderRadius: 14, padding: "clamp(28px,4vw,52px)",
        display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: "clamp(24px,4vw,48px)",
        alignItems: "center", position: "relative", overflow: "hidden",
      }}>
        <div style={{ position: "absolute", top: 0, right: 0, width: 120, height: 120, background: "radial-gradient(circle at 100% 0%, rgba(var(--accent-rgb),0.08) 0%, transparent 65%)", pointerEvents: "none" }} />

        <div>
          {c.badge && (
            <div style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "rgba(var(--accent-rgb),0.07)", border: "1px solid rgba(var(--accent-rgb),0.2)", borderRadius: 10, padding: "4px 12px", marginBottom: 18 }}>
              <span style={{ fontSize: 14 }}>🏅</span>
              <span style={{ fontSize: 10, letterSpacing: "0.2em", textTransform: "uppercase", color: "var(--accent)" }}>{c.badge}</span>
            </div>
          )}
          <h2 className="serif reveal-heading" style={{ margin: "0 0 14px", fontWeight: 300, fontSize: "clamp(26px,3.5vw,44px)", lineHeight: 1.05, color: "var(--ink)" }}>
            {c.title}
          </h2>
          {c.description && (
            <p style={{ fontSize: "clamp(13px,1.4vw,15px)", color: "rgba(var(--ink-rgb),0.63)", lineHeight: 1.8, maxWidth: "42ch", margin: "0 0 16px", whiteSpace: "pre-line" }}>
              {c.description}
            </p>
          )}
          {(c.prize || c.endDate) && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "6px 18px", fontSize: 12.5, color: "rgba(var(--ink-rgb),0.69)", marginBottom: 24 }}>
              {c.prize && <span>Приз: <b style={{ color: "var(--accent)", fontWeight: 600 }}>{c.prize}</b></span>}
              {c.endDate && <span>Итоги: {fmtDate(c.endDate)}</span>}
            </div>
          )}
          <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 16 }}>
            {c.linkUrl && (
              <a
                href={c.linkUrl} target="_blank" rel="noopener noreferrer" className="contest-cta"
                style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "10px 22px", borderRadius: 10, background: "rgba(var(--accent-rgb),0.1)", border: "1px solid rgba(var(--accent-rgb),0.3)", color: "var(--accent)", fontSize: 13, fontWeight: 600, textDecoration: "none", letterSpacing: "0.04em" }}
              >
                {c.linkText || "Участвовать"}
              </a>
            )}
            {c.rulesUrl && (
              <a href={c.rulesUrl} target="_blank" rel="noopener noreferrer" style={{ fontSize: 12, color: "rgba(var(--ink-rgb),0.52)" }}>Правила конкурса</a>
            )}
          </div>
        </div>

        {c.imageUrl ? (
          <img src={c.imageUrl} alt={c.title} style={{ width: "100%", height: "auto", borderRadius: 14, display: "block" }} />
        ) : steps.length > 0 ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            {steps.map((step, i) => (
              <div key={i} style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
                <span style={{ fontFamily: "var(--font-display)", fontSize: 26, color: "rgba(var(--accent-rgb),0.3)", lineHeight: 1, flexShrink: 0, minWidth: 28 }}>{String(i + 1).padStart(2, "0")}</span>
                <div>
                  {step.title && <div style={{ fontSize: 13, fontWeight: 600, color: "var(--ink)", marginBottom: 2 }}>{step.title}</div>}
                  {step.desc && <div style={{ fontSize: 12, color: "rgba(var(--ink-rgb),0.52)", lineHeight: 1.55 }}>{step.desc}</div>}
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </div>
      <style>{`.contest-cta{transition:background .2s}.contest-cta:hover{background:rgba(var(--accent-rgb),0.2)!important}`}</style>
    </section>
  );
}
