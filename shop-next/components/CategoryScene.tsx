"use client";
import { useEffect, useRef } from "react";

/* Category hero with its own 3D scent props (citrus slices for citrus, petals for floral…).
   The canvas never takes pointer events, and it lives only in the hero band — the product grid stays clean. */

interface Theme { bg: string; props: string[]; word: string; script: string }

const THEMES: { match: RegExp; theme: Theme }[] = [
  { match: /цитрус/,               theme: { bg: "#fff3a6", props: ["orange", "lemon", "lime", "leaves", "fruit"], word: "цитрус", script: "игристая свежесть" } },
  { match: /цвет|женск/,           theme: { bg: "#ffd3e3", props: ["petals", "petalsWhite", "leaves"],           word: "цветы",  script: "роза, пион, жасмин" } },
  { match: /древес|мужск|бород|брить/, theme: { bg: "#eadcc0", props: ["sticks", "leaves", "vanilla"],           word: "дерево", script: "кедр, сандал, ветивер" } },
  { match: /восточ|араб/,          theme: { bg: "#ffc796", props: ["orbs", "vanilla", "sparks"],                 word: "восток", script: "амбра, уд, ваниль" } },
  { match: /свеж/,                 theme: { bg: "#cdebff", props: ["drops", "leaves", "lime"],                   word: "бриз",   script: "море и воздух" } },
  { match: /мускус/,               theme: { bg: "#efe9ff", props: ["cotton", "sparks"],                          word: "мускус", script: "чистая кожа" } },
  { match: /фужер/,                theme: { bg: "#d7f0c8", props: ["fern", "leaves", "drops"],                   word: "фужер",  script: "лаванда и мох" } },
  { match: /шипр/,                 theme: { bg: "#e3ecc9", props: ["fern", "orbs", "leaves"],                    word: "шипр",   script: "бергамот и мох" } },
  { match: /элит|люкс|премиум/,    theme: { bg: "#f1e6cf", props: ["sparks", "orbs", "petalsWhite"],             word: "люкс",   script: "статусная классика" } },
  { match: /нишев/,                theme: { bg: "#dcd6ff", props: ["sparks", "orbs", "drops"],                   word: "ниша",   script: "редкие ноты" } },
  { match: /унисекс/,              theme: { bg: "#e6f99b", props: ["sparks", "drops", "lime"],                   word: "унисекс", script: "для всех" } },
  { match: /детск/,                theme: { bg: "#ffe0f0", props: ["sparks", "petalsWhite", "fruit"],            word: "детям",  script: "мягко и весело" } },
  { match: /^sets$|набор|подар/,   theme: { bg: "#ffd0df", props: ["sparks", "petals", "orbs"],                  word: "подарок", script: "с любовью" } },
  { match: /^testers$|миниатюр/,   theme: { bg: "#e6f99b", props: ["drops", "sparks"],                           word: "пробник", script: "попробуй сначала" } },
  { match: /^home$|свеч|дом/,      theme: { bg: "#ffe2c4", props: ["orbs", "vanilla", "cotton"],                 word: "дом",    script: "уют и тепло" } },
  { match: /^(parfum|edp|edt|edc)$/, theme: { bg: "#e9e2ff", props: ["drops", "sparks", "petals"],              word: "парфюм", script: "концентрация чувств" } },
  { match: /макияж|помад|тушь|тени|тональ|пудр/, theme: { bg: "#ffd9d0", props: ["petals", "sparks"],          word: "образ",  script: "яркий акцент" } },
  { match: /уход|крем|сыворот|маск|шампун|волос|^body$|^deo$|гель/, theme: { bg: "#d9f1ec", props: ["drops", "cotton", "leaves"], word: "уход", script: "мягкость и сияние" } },
];
const DEFAULT_THEME: Theme = { bg: "#e6f99b", props: ["sparks", "petals", "drops", "orange"], word: "магия", script: "22 000 ароматов" };

export function themeFor(q: string, category: string): Theme {
  const key = `${category} ${q}`.toLowerCase().trim();
  if (!key) return DEFAULT_THEME;
  for (const t of THEMES) {
    if (t.match.test(category.toLowerCase()) || t.match.test(q.toLowerCase())) return t.theme;
  }
  return DEFAULT_THEME;
}

export default function CategoryScene({ q, category, title, count }: { q: string; category: string; title: string; count?: number }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const theme = themeFor(q, category);
  const propsKey = theme.props.join(",");

  useEffect(() => {
    const canvasEl = canvasRef.current;
    if (!canvasEl) return;
    const canvas: HTMLCanvasElement = canvasEl;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    let disposed = false;
    let cleanup = () => {};

    // like FlaconStory: no three.js on the critical path — the props start on first interaction
    const START_EVENTS = ["scroll", "pointerdown", "pointermove", "touchstart", "keydown", "wheel"] as const;
    let started = false;
    const start = () => {
      if (started || disposed) return;
      started = true;
      START_EVENTS.forEach((e) => window.removeEventListener(e, start));
      boot().catch(() => {});
    };
    START_EVENTS.forEach((e) => window.addEventListener(e, start, { passive: true, once: true }));
    if (window.scrollY > 0) start();

    async function boot() {
      const { PROP_KINDS, setupStudio, THREE } = await import("@/lib/flacon");
      if (disposed) return;
      const mobile = window.matchMedia("(max-width: 767px)").matches;
      let renderer: InstanceType<typeof THREE.WebGLRenderer>;
      try { renderer = new THREE.WebGLRenderer({ canvas, antialias: !mobile, alpha: true }); } catch { return; }
      renderer.setPixelRatio(Math.min(window.devicePixelRatio, mobile ? 1.25 : 1.6));
      renderer.setClearColor(0x000000, 0);
      const scene = new THREE.Scene();
      setupStudio(renderer, scene);
      const camera = new THREE.PerspectiveCamera(32, 1, 0.1, 50);
      camera.position.set(0, 0, 7);

      const kinds = propsKey.split(",") as (keyof typeof PROP_KINDS)[];
      const count = mobile ? 12 : 26;
      type Item = { m: InstanceType<typeof THREE.Object3D>; vx: number; vy: number; spin: [number, number, number]; phase: number; z: number };
      const items: Item[] = [];
      let halfW = 5, halfH = 1.6;

      const resize = () => {
        const w = canvas.clientWidth, h = canvas.clientHeight;
        renderer.setSize(w, h, false);
        camera.aspect = w / h;
        camera.updateProjectionMatrix();
        halfH = Math.tan((camera.fov / 2) * Math.PI / 180) * camera.position.z;
        halfW = halfH * camera.aspect;
      };
      resize();

      for (let i = 0; i < count; i++) {
        const make = PROP_KINDS[kinds[i % kinds.length]];
        if (!make) continue;
        const m = make();
        const z = -1.5 + Math.random() * 3;
        const s = 1.1 + Math.random() * 1.1;
        m.scale.multiplyScalar(s);
        m.position.set((Math.random() * 2 - 1) * halfW * 1.1, (Math.random() * 2 - 1) * halfH * 0.9, z);
        m.rotation.set(Math.random() * 6, Math.random() * 6, Math.random() * 6);
        scene.add(m);
        items.push({ m, vx: 0.12 + Math.random() * 0.22, vy: (Math.random() - 0.5) * 0.08, spin: [(Math.random() - 0.5) * 0.9, (Math.random() - 0.5) * 0.9, (Math.random() - 0.5) * 0.5], phase: Math.random() * 6, z });
      }

      // pointer pushes props away gently (listens on window: the canvas itself ignores the mouse)
      const ptr = { x: 99, y: 99, on: false };
      const onMove = (e: PointerEvent) => {
        const r = canvas.getBoundingClientRect();
        ptr.on = e.clientY >= r.top && e.clientY <= r.bottom;
        ptr.x = ((e.clientX - r.left) / r.width * 2 - 1) * halfW;
        ptr.y = -((e.clientY - r.top) / r.height * 2 - 1) * halfH;
      };
      window.addEventListener("pointermove", onMove, { passive: true });
      window.addEventListener("resize", resize);

      let inView = true, raf = 0;
      const io = new IntersectionObserver(([e]) => { inView = e.isIntersecting; if (inView) { cancelAnimationFrame(raf); last = performance.now(); raf = requestAnimationFrame(loop); } });
      io.observe(canvas);

      let last = performance.now();
      const loop = (now: number) => {
        if (disposed || !inView || document.hidden) return;
        const dt = Math.min(0.05, (now - last) / 1000);
        last = now;
        const t = now / 1000;
        for (const it of items) {
          const p = it.m.position;
          p.x += it.vx * dt;
          p.y += (it.vy + Math.sin(t * 0.8 + it.phase) * 0.12) * dt;
          if (ptr.on) {
            const dx = p.x - ptr.x, dy = p.y - ptr.y, d2 = dx * dx + dy * dy;
            if (d2 < 1.4) { const f = (1.4 - d2) * 1.6 * dt; p.x += dx * f; p.y += dy * f; }
          }
          if (p.x > halfW * 1.25) { p.x = -halfW * 1.25; p.y = (Math.random() * 2 - 1) * halfH * 0.9; }
          if (p.y > halfH * 1.2) p.y = -halfH * 1.2;
          if (p.y < -halfH * 1.2) p.y = halfH * 1.2;
          it.m.rotation.x += it.spin[0] * dt;
          it.m.rotation.y += it.spin[1] * dt;
          it.m.rotation.z += it.spin[2] * dt;
        }
        renderer.render(scene, camera);
        raf = requestAnimationFrame(loop);
      };
      raf = requestAnimationFrame(loop);
      canvas.style.opacity = "1";

      cleanup = () => {
        cancelAnimationFrame(raf);
        io.disconnect();
        window.removeEventListener("pointermove", onMove);
        window.removeEventListener("resize", resize);
        scene.traverse((o) => {
          const mesh = o as unknown as { geometry?: { dispose(): void }; material?: { dispose(): void } | { dispose(): void }[] };
          mesh.geometry?.dispose();
          if (Array.isArray(mesh.material)) mesh.material.forEach((m) => m.dispose()); else mesh.material?.dispose();
        });
        renderer.dispose();
      };
    }

    return () => { disposed = true; START_EVENTS.forEach((e) => window.removeEventListener(e, start)); cleanup(); };
  }, [propsKey]);

  return (
    <section style={{ position: "relative", overflow: "hidden", background: theme.bg, borderRadius: "clamp(22px,3vw,36px)", margin: "0 clamp(10px,2vw,24px) 14px", minHeight: "clamp(200px,26vw,320px)", display: "flex", alignItems: "flex-end", transition: "background 0.6s ease" }}>
      <span className="bg-word" style={{ right: "-1vw", bottom: "-3vw", color: "#fff", opacity: 0.6, fontSize: "clamp(90px,16vw,260px)" }}>{theme.word}</span>
      <canvas ref={canvasRef} aria-hidden="true" style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", opacity: 0, transition: "opacity 0.8s ease", zIndex: 1 }} />
      <div style={{ position: "relative", zIndex: 2, padding: "clamp(22px,3vw,40px)", pointerEvents: "none" }}>
        <h1 className="h-mega" style={{ fontSize: "clamp(34px,5.4vw,80px)" }}>
          {title}
          <span className="h-script">{theme.script}</span>
        </h1>
        {typeof count === "number" && count > 0 && (
          <p style={{ margin: "14px 0 0", fontSize: 14, fontWeight: 600, color: "rgba(18,18,18,0.65)" }}>{count.toLocaleString("ru-RU")} товаров</p>
        )}
      </div>
    </section>
  );
}
