"use client";
import { useEffect, useRef } from "react";

/**
 * Scroll-driven signature flacon for the landing page.
 * Sections declare where the bottle should be with data-flacon='{"x":0.25,"y":0,"ry":-0.4,"s":1,"family":"floral"}'
 * (x/y are fractions of the viewport from its centre, s = scale). "hide":true flies the bottle out.
 * One fixed, pointer-events:none canvas; stages are interpolated by scroll position.
 * No WebGL / reduced motion → html.no-flacon, and sections show their static .mv-flacon-fallback image.
 */
interface Stage { x: number; y: number; ry: number; rz: number; s: number; family: string; hide?: boolean; mx?: number; my?: number }

const DEFAULTS: Stage = { x: 0.25, y: 0, ry: -0.35, rz: 0, s: 1, family: "floral" };
const smooth = (t: number) => t * t * (3 - 2 * t);

export default function FlaconStory() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvasEl = canvasRef.current;
    if (!canvasEl) return;
    const canvas: HTMLCanvasElement = canvasEl;
    const root = document.documentElement;
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const probe = document.createElement("canvas");
    if (reduced || !(probe.getContext("webgl2") || probe.getContext("webgl"))) { root.classList.add("no-flacon"); return; }

    let disposed = false;
    let cleanup = () => {};

    // three.js (~600 KB) + a render loop would block a phone's main thread right at load
    // (PageSpeed TBT 18 s). The static bottle is shown until the visitor interacts — first
    // scroll / touch / mouse move / key — then the 3D scene loads and fades in over it.
    const START_EVENTS = ["scroll", "pointerdown", "pointermove", "touchstart", "keydown", "wheel"] as const;
    let started = false;
    const start = () => {
      if (started || disposed) return;
      started = true;
      START_EVENTS.forEach((e) => window.removeEventListener(e, start));
      boot().catch(() => root.classList.add("no-flacon"));
    };
    START_EVENTS.forEach((e) => window.addEventListener(e, start, { passive: true, once: true }));
    // already scrolled (back navigation, anchor) → start right away
    if (window.scrollY > 0) start();

    // function declaration: hoisted, so start() may run before this line
    async function boot() {
      const { createFlacon, createNotes, animateNotes, setupStudio, FAMILIES, THREE } = await import("@/lib/flacon");
      if (disposed) return;
      await Promise.allSettled([document.fonts.load("800 80px Unbounded"), document.fonts.load("80px 'Marck Script'"), document.fonts.load("700 30px Onest")]);
      if (disposed) return;

      const mobile = window.matchMedia("(max-width: 767px)").matches;
      const renderer = new THREE.WebGLRenderer({ canvas, antialias: !mobile, alpha: true, powerPreference: "high-performance" });
      renderer.setPixelRatio(Math.min(window.devicePixelRatio, mobile ? 1.25 : 1.6));
      renderer.setClearColor(0x000000, 0);
      const scene = new THREE.Scene();
      setupStudio(renderer, scene);
      const camera = new THREE.PerspectiveCamera(30, 1, 0.1, 50);
      camera.position.set(0, 0.3, 8);
      camera.lookAt(0, 0.2, 0);

      const flacon = createFlacon({ family: "floral", lite: mobile, contactShadow: false });
      const rig = new THREE.Group();
      rig.add(flacon.group);
      scene.add(rig);

      const notes: Record<string, InstanceType<typeof THREE.Group>> = {};
      const PROPS: Record<string, string[]> = {
        floral: ["petals"], fresh: ["drops", "leaves"], woody: ["sticks", "leaves"], oriental: ["orbs"],
        niche: ["sparks"], citrus: ["slices", "leaves"], gift: ["sparks", "petals"],
      };
      Object.keys(FAMILIES).forEach((k, i) => {
        const g = createNotes(PROPS[k] ?? ["sparks"], mobile ? 6 : 10, i * 13 + 5);
        g.scale.setScalar(0.001);
        rig.add(g);
        notes[k] = g;
      });

      const resize = () => {
        const w = window.innerWidth, h = window.innerHeight;
        renderer.setSize(w, h, false);
        camera.aspect = w / h;
        camera.updateProjectionMatrix();
      };
      resize();
      window.addEventListener("resize", resize);

      const halfH = Math.tan((camera.fov / 2) * Math.PI / 180) * camera.position.z;
      const pointer = { x: 0, y: 0, tx: 0, ty: 0 };
      const onPointer = (e: PointerEvent) => { pointer.tx = e.clientX / window.innerWidth - 0.5; pointer.ty = e.clientY / window.innerHeight - 0.5; };
      window.addEventListener("pointermove", onPointer, { passive: true });

      const readStages = () => Array.from(document.querySelectorAll<HTMLElement>("[data-flacon]")).map((el) => {
        let cfg: Partial<Stage> = {};
        try { cfg = JSON.parse(el.dataset.flacon || "{}"); } catch { /* ignore */ }
        const st = { ...DEFAULTS, ...cfg } as Stage;
        // phones: optional mx/my override (bottle usually parks above the copy)
        if (mobile) { st.x = cfg.mx ?? 0; st.y = cfg.my ?? -0.26; st.s = Math.min(st.s, 1.05); }
        return { el, cfg: st };
      });
      let stages = readStages();
      const mo = new MutationObserver(() => { stages = readStages(); });
      mo.observe(document.body, { childList: true, subtree: true });

      const cur = { x: 0.25, y: 0, ry: -0.35, rz: 0, s: 0.001 };
      let family = "floral";
      const clock = new THREE.Clock();
      let raf = 0;
      let visible = true;
      const onVis = () => { visible = document.visibilityState === "visible"; if (visible) loop(); };
      document.addEventListener("visibilitychange", onVis);
      root.classList.add("has-flacon");

      function target(): Stage {
        const mid = window.innerHeight / 2;
        const pts = stages.map((s) => { const r = s.el.getBoundingClientRect(); return { c: r.top + r.height / 2, bottom: r.bottom, cfg: s.cfg }; });
        if (!pts.length) return { ...DEFAULTS, hide: true };
        if (mid <= pts[0].c) return pts[0].cfg;
        const last = pts[pts.length - 1];
        if (mid >= last.c) return mid > last.bottom ? { ...last.cfg, hide: true } : last.cfg;
        for (let i = 0; i < pts.length - 1; i++) {
          const a = pts[i], b = pts[i + 1];
          if (mid >= a.c && mid <= b.c) {
            const t = (mid - a.c) / Math.max(1, b.c - a.c);
            // hold each stage around its centre, travel in the middle of the gap
            return mix(a.cfg, b.cfg, smooth(Math.min(1, Math.max(0, (t - 0.25) / 0.5))));
          }
        }
        return last.cfg;
      }
      function mix(a: Stage, b: Stage, t: number): Stage {
        const ah = a.hide ? 1 : 0, bh = b.hide ? 1 : 0;
        return {
          x: a.x + (b.x - a.x) * t, y: a.y + (b.y - a.y) * t, ry: a.ry + (b.ry - a.ry) * t, rz: a.rz + (b.rz - a.rz) * t,
          s: a.s + (b.s - a.s) * t, family: t < 0.5 ? a.family : b.family, hide: ah + (bh - ah) * t > 0.5,
        };
      }

      // Phones: the bottle rides with its section's top band instead of hanging fixed,
      // so headings and copy never scroll underneath it. It pops in per section.
      let mobileIdx = -1;
      function mobileTarget(): { tg: Stage; anchorY: number; changed: boolean } {
        const H = window.innerHeight;
        let idx = 0;
        stages.forEach((s, i) => { if (s.el.getBoundingClientRect().top <= H * 0.6) idx = i; });
        const st = stages[idx];
        if (!st) return { tg: { ...DEFAULTS, hide: true }, anchorY: 0, changed: false };
        const r = st.el.getBoundingClientRect();
        const anchorPx = r.top + H * (0.5 + st.cfg.y);
        const changed = idx !== mobileIdx;
        mobileIdx = idx;
        const offscreen = anchorPx < -H * 0.25 || r.bottom < H * 0.2;
        return { tg: { ...st.cfg, hide: st.cfg.hide || offscreen }, anchorY: -(anchorPx / H - 0.5) * 2 * halfH, changed };
      }

      function loop() {
        if (disposed || !visible) return;
        const t = clock.getElapsedTime();
        const m = mobile ? mobileTarget() : null;
        const tg = m ? m.tg : target();
        const aspect = camera.aspect;
        const halfW = halfH * aspect;
        const scaleMul = mobile ? 0.56 : aspect < 1.2 ? 0.8 : 1;
        const tx = tg.x * 2 * halfW;
        const ty = m ? m.anchorY : tg.hide ? halfH * 2.2 : -tg.y * 2 * halfH;
        const ts = tg.hide ? (m ? 0.001 : 0.4) : tg.s * scaleMul;
        const k = 0.075;
        if (m?.changed) {
          cur.x = tx; cur.s = 0.001;
          const fam = FAMILIES[tg.family as keyof typeof FAMILIES];
          if (fam) { family = tg.family; flacon.setFamily(family); flacon.setLiquid(fam.liquid); }
        }
        pointer.x += (pointer.tx - pointer.x) * 0.05;
        pointer.y += (pointer.ty - pointer.y) * 0.05;
        cur.x += (tx - cur.x) * k;
        cur.y = m ? ty : cur.y + (ty - cur.y) * k; // phones: glued to the scroll, no lag
        cur.s += (ts - cur.s) * (m ? 0.12 : k);
        cur.ry += (tg.ry - cur.ry) * k;
        cur.rz += (tg.rz - cur.rz) * k;
        rig.position.set(cur.x, cur.y + Math.sin(t * 1.1) * 0.06, 0);
        rig.scale.setScalar(Math.max(0.001, cur.s));
        flacon.group.rotation.set(-pointer.y * 0.25 + 0.05, cur.ry + Math.sin(t * 0.6) * 0.12 + pointer.x * 0.5, cur.rz);

        if (tg.family !== family && FAMILIES[tg.family as keyof typeof FAMILIES]) { family = tg.family; flacon.setFamily(family); }
        flacon.lerpLiquid(FAMILIES[family as keyof typeof FAMILIES].liquid, 0.06);
        for (const [key, g] of Object.entries(notes)) {
          const want = key === family ? 1 : 0.001;
          const sc = g.scale.x + (want - g.scale.x) * 0.06;
          g.scale.setScalar(sc);
          g.visible = sc > 0.01;
          if (g.visible) { animateNotes(g, t); g.rotation.y = t * 0.08; }
        }
        renderer.render(scene, camera);
        raf = requestAnimationFrame(loop);
      }
      loop();

      cleanup = () => {
        cancelAnimationFrame(raf);
        window.removeEventListener("resize", resize);
        window.removeEventListener("pointermove", onPointer);
        document.removeEventListener("visibilitychange", onVis);
        mo.disconnect();
        flacon.dispose();
        renderer.dispose();
        root.classList.remove("has-flacon");
      };
    }

    return () => {
      disposed = true;
      START_EVENTS.forEach((e) => window.removeEventListener(e, start));
      cleanup();
    };
  }, []);

  return <canvas ref={canvasRef} className="mv-flacon-layer" aria-hidden="true" style={{ width: "100vw", height: "100vh" }} />;
}
