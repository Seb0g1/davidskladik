"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { X, ArrowUpRight, ChevronDown } from "lucide-react";
import { GROUPS, EXTRA_LINKS, SERVICE_LINKS } from "@/lib/nav";
import Logo from "./Logo";

/** Full-screen navigation (LV-style drawer): big group list left, subcategories + visual right. */
export default function MenuDrawer({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [active, setActive] = useState(GROUPS[0].id);
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => { document.body.style.overflow = prev; window.removeEventListener("keydown", onKey); };
  }, [open, onClose]);

  if (!open) return null;
  const group = GROUPS.find((g) => g.id === active) ?? GROUPS[0];

  return (
    <>
      <div className="mv-drawer-backdrop" onClick={onClose} />
      <div className="mv-drawer" role="dialog" aria-modal="true" aria-label="Меню">
        {/* left: groups */}
        <div style={{ display: "flex", flexDirection: "column", minHeight: 0, overflowY: "auto", padding: "22px clamp(20px,4vw,48px) 32px" }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "clamp(24px,5vh,48px)" }}>
            <button onClick={onClose} className="mv-hbtn" style={{ background: "var(--ink)", color: "var(--paper)" }}>
              <X size={18} /> Закрыть
            </button>
            <Link href="/" onClick={onClose} aria-label="На главную"><Logo height={28} /></Link>
          </div>

          <nav>
            {GROUPS.map((g, i) => (
              <div key={g.id} style={{ borderBottom: "1px solid var(--border)" }}>
                {/* desktop: hover switches the right panel */}
                <Link
                  href={g.href}
                  onClick={onClose}
                  onMouseEnter={() => setActive(g.id)}
                  onFocus={() => setActive(g.id)}
                  className={`mv-drawer-link mv-lg-only${active === g.id ? " active" : ""}`}
                  style={{ animation: `mv-rise 0.5s var(--ease-out) ${0.05 + i * 0.04}s both` }}
                >
                  {g.label}
                  <ArrowUpRight size={26} strokeWidth={2} />
                </Link>
                {/* mobile: accordion */}
                <button
                  className={`mv-drawer-link mv-lg-hide${expanded === g.id ? " active" : ""}`}
                  onClick={() => setExpanded(expanded === g.id ? null : g.id)}
                  aria-expanded={expanded === g.id}
                  style={{ animation: `mv-rise 0.5s var(--ease-out) ${0.05 + i * 0.04}s both` }}
                >
                  {g.label}
                  <ChevronDown size={24} style={{ transition: "transform 0.3s", transform: expanded === g.id ? "rotate(180deg)" : "none" }} />
                </button>
                {expanded === g.id && (
                  <div className="lg:hidden anim-fade-in" style={{ padding: "4px 0 18px" }}>
                    <Link href={g.href} onClick={onClose} className="mv-drawer-sub" style={{ color: "var(--accent)", fontWeight: 600 }}>Смотреть всё →</Link>
                    {g.columns.map((c) => (
                      <div key={c.title} style={{ marginTop: 12 }}>
                        <p style={{ margin: "0 0 4px", fontSize: 11, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--pink)" }}>{c.title}</p>
                        {c.items.map((it) => <Link key={it.href + it.label} href={it.href} onClick={onClose} className="mv-drawer-sub">{it.label}</Link>)}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </nav>

          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 28 }}>
            {EXTRA_LINKS.map((l) => <Link key={l.href} href={l.href} onClick={onClose} className="chip">{l.label}</Link>)}
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "6px 18px", marginTop: "auto", paddingTop: 28 }}>
            {SERVICE_LINKS.map((l) => <Link key={l.href} href={l.href} onClick={onClose} style={{ fontSize: 14, color: "var(--muted)" }}>{l.label}</Link>)}
          </div>
        </div>

        {/* right: subcategories + visual (desktop) */}
        <div className="hidden lg:grid" style={{ gridTemplateRows: "1fr auto", background: "var(--surface)", minHeight: 0 }}>
          <div key={group.id} className="anim-fade-in" style={{ position: "relative", overflow: "hidden" }}>
            <Image src={group.shot} alt="" fill sizes="56vw" style={{ objectFit: "cover" }} priority />
            <div style={{ position: "absolute", inset: 0, background: "linear-gradient(180deg, rgba(18,18,18,0) 40%, rgba(18,18,18,0.55) 100%)" }} />
            <div style={{ position: "absolute", left: 40, bottom: 34, right: 40, color: "#fff" }}>
              <p className="h-section" style={{ color: "#fff" }}>{group.label}<span className="h-script" style={{ color: "var(--lime)" }}>магия в каждом флаконе</span></p>
            </div>
          </div>
          <div key={group.id + "-cols"} className="anim-fade-in" style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(group.columns.length, 4)}, 1fr)`, gap: "20px 32px", padding: "28px 40px 34px" }}>
            {group.columns.map((c) => (
              <div key={c.title}>
                <p style={{ margin: "0 0 10px", fontSize: 11, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--pink)" }}>{c.title}</p>
                {c.items.map((it) => <Link key={it.href + it.label} href={it.href} onClick={onClose} className="mv-drawer-sub" style={{ padding: "4px 0" }}>{it.label}</Link>)}
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}
