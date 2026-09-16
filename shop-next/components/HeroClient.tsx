"use client";
import { useEffect, useRef } from "react";
import Link from "next/link";

/* ── WebGL shader ──────────────────────────────────────────── */
const VERT = `#version 300 es
in vec2 a_pos;
void main() { gl_Position = vec4(a_pos, 0.0, 1.0); }
`;
const FRAG = `#version 300 es
precision mediump float;
uniform float u_time;
uniform vec2  u_res;
out vec4 fragColor;
float hash(vec2 p){return fract(sin(dot(p,vec2(127.1,311.7)))*43758.5453);}
float noise(vec2 p){vec2 i=floor(p);vec2 f=fract(p);f=f*f*(3.0-2.0*f);return mix(mix(hash(i),hash(i+vec2(1,0)),f.x),mix(hash(i+vec2(0,1)),hash(i+vec2(1,1)),f.x),f.y);}
float ridge(vec2 p){return 1.0-abs(noise(p)*2.0-1.0);}
float fbm(vec2 p){float v=0.0;float a=0.5;for(int i=0;i<5;i++){v+=a*noise(p);p=p*2.1+vec2(cos(u_time*0.07+float(i)),sin(u_time*0.05+float(i)*1.3))*0.6;a*=0.52;}return v;}
float rfbm(vec2 p){float v=0.0;float a=0.5;for(int i=0;i<4;i++){v+=a*ridge(p);p=p*2.3-vec2(sin(u_time*0.09+float(i)*0.71),cos(u_time*0.06+float(i)*1.13))*0.45;a*=0.46;}return v;}
void main(){
  vec2 uv=gl_FragCoord.xy/u_res;
  float t=u_time*0.14;
  vec2 p=(uv-0.5)*3.0;
  vec2 q=vec2(fbm(p),fbm(p+vec2(5.2,1.3)));
  vec2 r=vec2(fbm(p+4.0*q+vec2(1.7,9.2)+t*0.20),fbm(p+4.0*q+vec2(8.3,2.8)+t*0.16));
  float n=fbm(p+4.0*r);
  float m=rfbm(p*0.75+q*1.8+vec2(t*0.28));
  float spec=pow(m,2.8)*smoothstep(0.48,0.82,n+m*0.28);
  vec3 c0=vec3(0.04,0.028,0.007);
  vec3 c1=vec3(0.32,0.23,0.09);
  vec3 c2=vec3(0.62,0.49,0.22);
  vec3 c3=vec3(0.88,0.74,0.42);
  vec3 c4=vec3(1.00,0.96,0.80);
  vec3 col=c0;
  col=mix(col,c1,smoothstep(0.18,0.42,n));
  col=mix(col,c2,smoothstep(0.40,0.60,n));
  col=mix(col,c3,smoothstep(0.58,0.78,n)*0.88);
  col=mix(col,c4,spec*0.82);
  float edge=smoothstep(0.0,0.36,uv.x)*smoothstep(1.0,0.64,uv.x)*smoothstep(0.0,0.52,uv.y)*smoothstep(1.0,0.40,uv.y);
  float alpha=clamp((n*0.54+spec*0.22)*edge,0.0,1.0);
  fragColor=vec4(col,alpha);
}
`;

function compileShader(gl: WebGL2RenderingContext, type: number, src: string) {
  const s = gl.createShader(type);
  if (!s) return null;
  gl.shaderSource(s, src);
  gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) { gl.deleteShader(s); return null; }
  return s;
}

function HeroShader() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const gl = canvas.getContext("webgl2", { alpha: true, premultipliedAlpha: false });
    if (!gl) return;
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const vert = compileShader(gl, gl.VERTEX_SHADER, VERT);
    const frag = compileShader(gl, gl.FRAGMENT_SHADER, FRAG);
    if (!vert || !frag) return;
    const prog = gl.createProgram();
    if (!prog) return;
    gl.attachShader(prog, vert); gl.attachShader(prog, frag); gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) return;
    gl.useProgram(prog);
    const verts = new Float32Array([-1,-1, 1,-1, -1,1, -1,1, 1,-1, 1,1]);
    const buf = gl.createBuffer(); gl.bindBuffer(gl.ARRAY_BUFFER, buf); gl.bufferData(gl.ARRAY_BUFFER, verts, gl.STATIC_DRAW);
    const aPos = gl.getAttribLocation(prog, "a_pos");
    gl.enableVertexAttribArray(aPos); gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);
    const uTime = gl.getUniformLocation(prog, "u_time");
    const uRes = gl.getUniformLocation(prog, "u_res");
    gl.enable(gl.BLEND); gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA); gl.clearColor(0,0,0,0);
    const ro = new ResizeObserver(() => {
      const w = canvas.offsetWidth, h = canvas.offsetHeight;
      if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; gl.viewport(0,0,w,h); }
    });
    ro.observe(canvas);
    const start = performance.now();
    let raf = 0;
    function render() {
      const w = canvas!.offsetWidth, h = canvas!.offsetHeight;
      if (canvas!.width !== w || canvas!.height !== h) { canvas!.width = w; canvas!.height = h; gl!.viewport(0,0,w,h); }
      const t = (performance.now() - start) / 1000;
      gl!.clear(gl!.COLOR_BUFFER_BIT);
      gl!.uniform1f(uTime, t); gl!.uniform2f(uRes, canvas!.width, canvas!.height);
      gl!.drawArrays(gl!.TRIANGLES, 0, 6);
      if (!reduced) raf = requestAnimationFrame(render);
    }
    raf = requestAnimationFrame(render);
    return () => { cancelAnimationFrame(raf); ro.disconnect(); const ext = gl.getExtension("WEBGL_lose_context"); if (ext) ext.loseContext(); };
  }, []);
  return <canvas ref={canvasRef} style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", zIndex: 0 }} />;
}

export default function HeroClient() {
  const auraRef      = useRef<HTMLDivElement>(null);
  const particlesRef = useRef<HTMLDivElement>(null);
  const tiltRef      = useRef<HTMLDivElement>(null);
  const glowRef      = useRef<HTMLDivElement>(null);
  const heroBtnsRef  = useRef<HTMLDivElement>(null);
  const heroGradRef  = useRef<HTMLDivElement>(null);

  /* cursor aura */
  useEffect(() => {
    const aura = auraRef.current;
    if (!aura) return;
    let raf: number;
    let tx = -500, ty = -500, cx = -500, cy = -500;
    const onMove = (e: MouseEvent) => { tx = e.clientX; ty = e.clientY; };
    const loop = () => { cx += (tx - cx) * 0.07; cy += (ty - cy) * 0.07; aura.style.transform = `translate3d(${cx}px,${cy}px,0)`; raf = requestAnimationFrame(loop); };
    document.addEventListener("mousemove", onMove);
    raf = requestAnimationFrame(loop);
    return () => { document.removeEventListener("mousemove", onMove); cancelAnimationFrame(raf); };
  }, []);

  /* floating particles */
  useEffect(() => {
    const container = particlesRef.current;
    if (!container) return;
    const spawn = () => {
      const p = document.createElement("div");
      const dx = (Math.random() - 0.5) * 40;
      p.style.cssText = `position:absolute;left:${Math.random()*100}%;bottom:0;width:${2+Math.random()*3}px;height:${2+Math.random()*3}px;border-radius:50%;background:rgba(201,162,94,${0.3+Math.random()*0.5});--mv-dx:${dx}px;animation:mv-drift ${4+Math.random()*6}s ease-out ${Math.random()*2}s both;pointer-events:none;`;
      container.appendChild(p);
      setTimeout(() => p.remove(), 10000);
    };
    const id = setInterval(spawn, 500);
    return () => clearInterval(id);
  }, []);

  /* 3D tilt */
  useEffect(() => {
    const el = tiltRef.current;
    const glow = glowRef.current;
    if (!el) return;
    const onMove = (e: MouseEvent) => {
      const r = el.getBoundingClientRect();
      const x = (e.clientX - r.left - r.width / 2) / r.width;
      const y = (e.clientY - r.top - r.height / 2) / r.height;
      el.style.transition = "transform 0.1s ease";
      el.style.transform = `perspective(1000px) rotateX(${-y*5}deg) rotateY(${x*8}deg)`;
      if (glow) glow.style.transform = `translate(${x*16}px,${y*16}px)`;
    };
    const onLeave = () => {
      el.style.transition = "transform 0.8s cubic-bezier(0.16,1,0.3,1)";
      el.style.transform = "perspective(1000px) rotateX(0) rotateY(0)";
      if (glow) glow.style.transform = "";
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseleave", onLeave);
    return () => { document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseleave", onLeave); };
  }, []);

  /* hero gradient parallax */
  useEffect(() => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const el = heroGradRef.current;
    if (!el) return;
    let raf = 0;
    const onScroll = () => { cancelAnimationFrame(raf); raf = requestAnimationFrame(() => { el.style.transform = `translateY(${window.scrollY * 0.28}px)`; }); };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => { window.removeEventListener("scroll", onScroll); cancelAnimationFrame(raf); };
  }, []);

  /* magnetic buttons */
  useEffect(() => {
    const c = heroBtnsRef.current;
    if (!c) return;
    const btns = c.querySelectorAll<HTMLElement>("a");
    const cleanup: Array<() => void> = [];
    btns.forEach(btn => {
      const onMove = (e: MouseEvent) => {
        const r = btn.getBoundingClientRect();
        const dx = e.clientX - (r.left + r.width / 2);
        const dy = e.clientY - (r.top + r.height / 2);
        const dist = Math.hypot(dx, dy);
        if (dist < 110) { btn.style.transition = "transform 0.12s ease"; btn.style.transform = `translate(${dx*0.26}px,${dy*0.26}px)`; }
        else if (btn.style.transform) { btn.style.transition = "transform 0.65s cubic-bezier(0.16,1,0.3,1)"; btn.style.transform = ""; }
      };
      document.addEventListener("mousemove", onMove);
      cleanup.push(() => document.removeEventListener("mousemove", onMove));
    });
    return () => cleanup.forEach(f => f());
  }, []);

  return (
    <>
      {/* Cursor aura */}
      <div ref={auraRef} aria-hidden="true" className="cursor-aura" />

      {/* Hero */}
      <section style={{ position: "relative", overflow: "hidden", minHeight: "84vh", display: "flex", alignItems: "center", justifyContent: "center", padding: "9vh clamp(18px,4vw,56px)" }}>
        <HeroShader />
        <div ref={heroGradRef} style={{ position: "absolute", inset: "-10%", pointerEvents: "none", background: "radial-gradient(44% 38% at 50% 44%, rgba(201,162,94,0.16) 0%, rgba(201,162,94,0.05) 40%, rgba(11,11,11,0) 72%)", willChange: "transform" }} />
        <div ref={particlesRef} style={{ position: "absolute", inset: "-8%", pointerEvents: "none", willChange: "transform" }} />

        <div id="hero-parallax-inner" style={{ position: "relative", width: "100%", maxWidth: 1040, display: "flex", flexDirection: "column", alignItems: "center", gap: "clamp(20px,3vw,36px)", textAlign: "center", willChange: "transform" }}>
          <p className="eyebrow anim-fade-in" style={{ animationDelay: "0.1s" }}>Оригинальная парфюмерия</p>

          <div style={{ perspective: 1000, width: "100%" }}>
            <div ref={tiltRef} style={{ position: "relative", transformStyle: "preserve-3d", willChange: "transform" }}>
              <div ref={glowRef} style={{ position: "absolute", left: "50%", top: "50%", width: "76%", height: "128%", marginLeft: "-38%", marginTop: "-64%", borderRadius: "50%", pointerEvents: "none", background: "radial-gradient(50% 50% at 50% 50%, rgba(240,220,170,0.28) 0%, rgba(201,162,94,0.14) 42%, rgba(201,162,94,0) 74%)", filter: "blur(28px)", transition: "transform 0.3s ease" }} />
              <div className="serif anim-slide-up" style={{ position: "relative", fontWeight: 300, fontStyle: "italic", fontSize: "clamp(60px,13vw,176px)", lineHeight: 0.88, animationDelay: "0.15s" }}>
                <div style={{ position: "relative" }}>
                  <span style={{ display: "block", color: "#f5f4f0" }}>Magic</span>
                  <span aria-hidden="true" style={{ position: "absolute", left: 0, top: 0, width: "100%", display: "block", color: "transparent", backgroundImage: "linear-gradient(104deg,#fffdf7 0%,#f5f4f0 26%,#e9d2a0 46%,#fffdf7 60%,#d7b880 82%,#f5f4f0 100%)", backgroundSize: "260% 100%", WebkitBackgroundClip: "text", backgroundClip: "text", animation: "mv-sheen 11s linear infinite alternate" }}>Magic</span>
                </div>
                <div style={{ position: "relative", marginLeft: "clamp(18px,5vw,84px)" }}>
                  <span style={{ display: "block", color: "#f5f4f0" }}>Vibes</span>
                  <span aria-hidden="true" style={{ position: "absolute", left: 0, top: 0, width: "100%", display: "block", color: "transparent", backgroundImage: "linear-gradient(104deg,#fffdf7 0%,#f5f4f0 26%,#e9d2a0 46%,#fffdf7 60%,#d7b880 82%,#f5f4f0 100%)", backgroundSize: "260% 100%", WebkitBackgroundClip: "text", backgroundClip: "text", animation: "mv-sheen 11s linear infinite alternate reverse" }}>Vibes</span>
                </div>
              </div>
            </div>
          </div>

          <div className="anim-fade-in" style={{ width: "min(400px,60%)", height: 1, background: "linear-gradient(90deg,rgba(201,162,94,0) 0%,rgba(201,162,94,0.8) 50%,rgba(201,162,94,0) 100%)", animationDelay: "0.3s" }} />

          <p className="anim-slide-up" style={{ margin: 0, maxWidth: "30ch", fontSize: "clamp(14px,1.3vw,17px)", lineHeight: 1.65, color: "#888888", animationDelay: "0.35s" }}>
            Мировые ароматы с доставкой по России через Ozon
          </p>

          <div ref={heroBtnsRef} className="anim-slide-up" style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "center", gap: 14, animationDelay: "0.45s" }}>
            <Link href="/catalog" className="btn-primary">Каталог ароматов</Link>
            <Link href="/brands" className="btn-ghost">Все бренды →</Link>
          </div>
        </div>

        <div style={{ position: "absolute", bottom: 28, left: "50%", transform: "translateX(-50%)", display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
          <div style={{ width: 1, height: 36, background: "linear-gradient(to bottom, transparent, rgba(201,162,94,0.4))" }} />
        </div>
      </section>
    </>
  );
}
