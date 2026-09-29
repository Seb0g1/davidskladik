"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { motion, useScroll, useTransform, useReducedMotion } from "motion/react";
import { ArrowUpRight } from "lucide-react";

const ITEMS = [
  { id: 1, color: "#ff3d7f", label: "Для неё",        script: "нежно и смело",      href: "/collections/zhenskaya",         image: "/brand/shots/her.jpg?v=3" },
  { id: 2, color: "#d9f84a", label: "Для него",       script: "дерево и характер",  href: "/collections/muzhskaya",         image: "/brand/shots/him.jpg?v=3" },
  { id: 3, color: "#9d8cff", label: "Нишевая",        script: "редкие ноты",        href: "/collections/nishevaya",         image: "/brand/shots/niche.jpg?v=3" },
  { id: 4, color: "#ff7a0f", label: "Арабская",       script: "амбра, уд, пряности", href: "/collections/arabskaya",       image: "/brand/shots/oriental.jpg?v=3" },
  { id: 5, color: "#2fb4ff", label: "Свежие",         script: "море и воздух",      href: "/collections/svezhie",          image: "/brand/shots/fresh.jpg?v=3" },
  { id: 6, color: "#ffc800", label: "Цитрусовые",     script: "игристый цитрус",    href: "/collections/tsitrusovye",      image: "/brand/shots/citrus.jpg?v=3" },
  { id: 7, color: "#ff2f72", label: "Подарочные наборы", script: "с любовью",        href: "/collections/nabory",     image: "/brand/shots/gift.jpg?v=3" },
];

/** Sticky horizontal gallery: vertical scroll drives the track sideways. */
export default function CollectionsGallery() {
  const containerRef = useRef<HTMLDivElement>(null);
  const trackRef = useRef<HTMLDivElement>(null);
  const [distance, setDistance] = useState(0);
  // useReducedMotion() is already true on the client's first render but false on the server →
  // hydration mismatch (React #418) for visitors with «reduce motion» on; switch only after mount
  const prefersReduced = useReducedMotion();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  const reduced = mounted && prefersReduced === true;
  const { scrollYProgress } = useScroll({ target: containerRef, offset: ["start start", "end end"] });
  const x = useTransform(scrollYProgress, (v) => -v * distance);
  const progress = useTransform(scrollYProgress, [0, 1], ["0%", "100%"]);

  useEffect(() => {
    const track = trackRef.current;
    if (!track) return;
    const measure = () => setDistance(Math.max(0, track.scrollWidth - window.innerWidth + 48));
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(track);
    window.addEventListener("resize", measure);
    return () => { ro.disconnect(); window.removeEventListener("resize", measure); };
  }, []);

  const cards = ITEMS.map((it) => (
    <Link key={it.id} href={it.href} className="mv-gcard" style={{ "--item-color": it.color } as React.CSSProperties}>
      <Image src={it.image} alt={`${it.label} — Magic Vibes`} fill sizes="(max-width: 600px) 280px, 420px" style={{ objectFit: "cover" }} />
      <span className="mv-gcard-shade" />
      <span className="mv-gcard-arrow"><ArrowUpRight size={20} /></span>
      <span className="mv-gcard-content">
        <span className="mv-gcard-num">0{it.id}</span>
        <span className="mv-gcard-title">{it.label}</span>
        <span className="mv-gcard-script">{it.script}</span>
      </span>
    </Link>
  ));

  return (
    <section aria-labelledby="collections-title" style={{ background: "var(--paper)" }}>
      <style>{GALLERY_CSS}</style>
      <div style={{ maxWidth: 1360, margin: "0 auto", padding: "clamp(80px,10vw,140px) clamp(18px,4vw,56px) 0" }}>
        <span className="eyebrow">Коллекции</span>
        <h2 id="collections-title" className="h-mega" style={{ marginTop: 18 }}>
          Выбери свою<span className="h-script">магию</span>
        </h2>
      </div>

      {reduced ? (
        <div className="scroll-x" style={{ display: "flex", gap: 20, padding: "40px clamp(18px,4vw,56px) 20px" }}>{cards}</div>
      ) : (
        <div ref={containerRef} style={{ height: `calc(100vh + ${distance}px)`, position: "relative" }}>
          <div style={{ position: "sticky", top: 0, height: "100vh", display: "flex", flexDirection: "column", justifyContent: "center", overflow: "hidden" }}>
            <motion.div ref={trackRef} className="mv-gtrack" style={{ x }}>{cards}</motion.div>
            <div style={{ margin: "34px clamp(18px,4vw,56px) 0", height: 3, background: "rgba(18,18,18,0.1)", borderRadius: 3, overflow: "hidden", maxWidth: 1360 }}>
              <motion.div style={{ width: progress, height: "100%", background: "var(--ink)" }} />
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

const GALLERY_CSS = `
.mv-gtrack { display: flex; gap: 28px; padding: 0 clamp(18px,4vw,56px); width: max-content; will-change: transform; }
.mv-gcard {
  position: relative; flex-shrink: 0; width: 420px; height: min(540px, 66vh);
  border-radius: 28px; overflow: hidden; text-decoration: none; color: #fff;
  transition: transform 0.6s var(--ease-out);
}
.mv-gcard:hover { transform: translateY(-8px) rotate(-0.6deg); color: #fff; }
.mv-gcard img { transition: transform 1s var(--ease-out); }
.mv-gcard:hover img { transform: scale(1.06); }
.mv-gcard-shade { position: absolute; inset: 0; background: linear-gradient(to bottom, transparent 45%, var(--item-color)); mix-blend-mode: multiply; }
.mv-gcard::after { content: ""; position: absolute; inset: 0; background: linear-gradient(to bottom, transparent 55%, rgba(18,18,18,0.55)); }
.mv-gcard-arrow { position: absolute; top: 18px; right: 18px; z-index: 2; width: 46px; height: 46px; border-radius: 50%; background: #fff; color: #121212; display: flex; align-items: center; justify-content: center; transition: transform 0.4s var(--ease-spring); }
.mv-gcard:hover .mv-gcard-arrow { transform: rotate(45deg) scale(1.08); }
.mv-gcard-content { position: absolute; left: 28px; right: 28px; bottom: 26px; z-index: 2; display: flex; flex-direction: column; }
.mv-gcard-num { font-family: ui-monospace, "Azeret Mono", monospace; font-size: 14px; color: var(--item-color); margin-bottom: 8px; filter: brightness(1.3); }
.mv-gcard-title { font-family: var(--font-display); font-weight: 700; text-transform: uppercase; font-size: 30px; letter-spacing: -0.02em; line-height: 1; }
.mv-gcard-script { font-family: var(--font-script); font-size: 24px; color: #fff; opacity: 0.92; margin-top: 6px; }
@media (max-width: 600px) {
  .mv-gtrack { gap: 14px; }
  .mv-gcard { width: 280px; height: 380px; border-radius: 22px; }
  .mv-gcard-title { font-size: 22px; }
}
`;
