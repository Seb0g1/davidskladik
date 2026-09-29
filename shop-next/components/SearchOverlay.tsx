"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { X, ArrowRight, Sparkles } from "lucide-react";
import { POPULAR_QUERIES, GROUPS } from "@/lib/nav";

export default function SearchOverlay({ open, onClose }: { open: boolean; onClose: () => void }) {
  const router = useRouter();
  const [q, setQ] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const t = setTimeout(() => inputRef.current?.focus(), 60);
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => { clearTimeout(t); document.body.style.overflow = prev; window.removeEventListener("keydown", onKey); };
  }, [open, onClose]);

  if (!open) return null;

  const go = (term: string) => {
    const t = term.trim();
    if (!t) return;
    router.push(`/catalog?q=${encodeURIComponent(t)}`);
    setQ("");
    onClose();
  };

  return (
    <div className="mv-search" role="dialog" aria-modal="true" aria-label="Поиск">
      <div style={{ maxWidth: 1180, margin: "0 auto", padding: "28px clamp(20px,4vw,48px) 60px" }}>
        <div style={{ display: "flex", justifyContent: "flex-end" }}>
          <button onClick={onClose} className="mv-hbtn" style={{ background: "var(--ink)", color: "var(--paper)" }}><X size={18} /> Закрыть</button>
        </div>
        <form onSubmit={(e) => { e.preventDefault(); go(q); }} style={{ display: "flex", alignItems: "flex-end", gap: 16, marginTop: "clamp(24px,8vh,80px)" }}>
          <input ref={inputRef} value={q} onChange={(e) => setQ(e.target.value)} className="mv-search-input" placeholder="Бренд, аромат, нота…" aria-label="Поиск по каталогу" />
          <button type="submit" aria-label="Найти" style={{ flexShrink: 0, width: 64, height: 64, borderRadius: "50%", border: "none", background: "var(--ink)", color: "var(--lime)", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <ArrowRight size={28} />
          </button>
        </form>

        <p style={{ margin: "36px 0 12px", fontSize: 12, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--muted)" }}>Часто ищут</p>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {POPULAR_QUERIES.map((p) => <button key={p} onClick={() => go(p)} className="chip">{p}</button>)}
        </div>

        <Link href="/find" onClick={onClose} style={{ marginTop: 32, display: "flex", alignItems: "center", gap: 16, padding: "22px 24px", borderRadius: "var(--r-lg)", background: "var(--accent)", color: "#fff", textDecoration: "none" }}>
          <Sparkles size={26} color="var(--lime)" />
          <span style={{ flex: 1 }}>
            <span style={{ display: "block", fontFamily: "var(--font-display)", fontWeight: 700, fontSize: "clamp(18px,2.4vw,26px)", textTransform: "uppercase", letterSpacing: "-0.02em" }}>Не знаете, что искать?</span>
            <span style={{ fontFamily: "var(--font-script)", fontSize: 22, color: "var(--lime)" }}>AI подберёт аромат за минуту</span>
          </span>
          <ArrowRight size={24} />
        </Link>

        <p style={{ margin: "36px 0 12px", fontSize: 12, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--muted)" }}>Разделы</p>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          {GROUPS.map((g) => <Link key={g.id} href={g.href} onClick={onClose} className="chip">{g.label}</Link>)}
        </div>
      </div>
    </div>
  );
}
