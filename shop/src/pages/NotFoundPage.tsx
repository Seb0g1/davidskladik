import { useEffect, useRef } from "react";
import { Link } from "react-router-dom";

export default function NotFoundPage() {
  const auraRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const aura = auraRef.current;
    if (!aura) return;
    let raf: number;
    let tx = window.innerWidth / 2, ty = window.innerHeight / 2;
    let cx = tx, cy = ty;
    const onMove = (e: MouseEvent) => { tx = e.clientX; ty = e.clientY; };
    const loop = () => {
      cx += (tx - cx) * 0.06;
      cy += (ty - cy) * 0.06;
      aura.style.transform = `translate3d(${cx - 240}px,${cy - 240}px,0)`;
      raf = requestAnimationFrame(loop);
    };
    document.addEventListener("mousemove", onMove);
    raf = requestAnimationFrame(loop);
    return () => { document.removeEventListener("mousemove", onMove); cancelAnimationFrame(raf); };
  }, []);

  return (
    <div style={{
      background: "#09090b",
      minHeight: "100vh",
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      padding: "clamp(32px,6vw,80px) clamp(16px,4vw,48px)",
      textAlign: "center",
      position: "relative",
      overflow: "hidden",
    }}>
      <style>{`
        @keyframes mv-404-float {
          0%, 100% { transform: translateY(0); }
          50% { transform: translateY(-12px); }
        }
        @keyframes mv-404-shimmer {
          0% { background-position: -200% 0; }
          100% { background-position: 200% 0; }
        }
        @keyframes mv-404-fade-in {
          from { opacity: 0; transform: translateY(20px); }
          to   { opacity: 1; transform: none; }
        }
        @keyframes mv-404-particle {
          0%   { transform: translateY(0) translateX(0); opacity: 0.6; }
          100% { transform: translateY(-160px) translateX(var(--dx, 0px)); opacity: 0; }
        }
        .mv-404-particle {
          position: absolute;
          border-radius: 50%;
          background: rgba(201,162,94,0.5);
          pointer-events: none;
          animation: mv-404-particle 5s ease-out both;
        }
        .mv-404-link {
          display: inline-flex; align-items: center; gap: 8px;
          padding: 13px 28px;
          border-radius: 2px;
          font-size: 12px;
          letter-spacing: 0.14em;
          text-transform: uppercase;
          text-decoration: none;
          cursor: pointer;
          transition: background 0.3s ease, color 0.3s ease, border-color 0.3s ease;
          font-family: inherit;
        }
        .mv-404-link-primary {
          background: rgba(201,162,94,0.12);
          border: 1px solid rgba(201,162,94,0.45);
          color: #c9a25e;
        }
        .mv-404-link-primary:hover {
          background: rgba(201,162,94,0.22);
          border-color: rgba(201,162,94,0.75);
          color: #e8d5a3;
        }
        .mv-404-link-ghost {
          background: transparent;
          border: 1px solid rgba(255,255,255,0.1);
          color: rgba(245,244,240,0.5);
        }
        .mv-404-link-ghost:hover {
          border-color: rgba(255,255,255,0.22);
          color: rgba(245,244,240,0.8);
        }
      `}</style>

      {/* Cursor aura */}
      <div ref={auraRef} aria-hidden="true" style={{
        position: "fixed",
        width: 480, height: 480,
        borderRadius: "50%",
        background: "radial-gradient(circle, rgba(201,162,94,0.07) 0%, transparent 65%)",
        pointerEvents: "none",
        zIndex: 0,
        top: 0, left: 0,
        transition: "none",
      }} />

      {/* Background gradient */}
      <div aria-hidden="true" style={{
        position: "absolute",
        left: "50%", top: 0,
        width: 700, height: 500,
        transform: "translateX(-50%)",
        background: "radial-gradient(ellipse 55% 50% at 50% 0%, rgba(201,162,94,0.1) 0%, transparent 70%)",
        pointerEvents: "none",
      }} />

      {/* Particles */}
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} className="mv-404-particle" aria-hidden="true" style={{
          width: 2 + (i % 3) * 1.5,
          height: 2 + (i % 3) * 1.5,
          bottom: "20%",
          left: `${15 + i * 9}%`,
          animationDelay: `${i * 0.7}s`,
          animationDuration: `${4 + (i % 3)}s`,
          ["--dx" as string]: `${(i % 2 === 0 ? 1 : -1) * (10 + i * 5)}px`,
          animationIterationCount: "infinite",
          opacity: 0,
        }} />
      ))}

      <div style={{ position: "relative", zIndex: 1 }}>

        {/* Eyebrow */}
        <p style={{
          fontSize: 10,
          fontWeight: 600,
          letterSpacing: "0.3em",
          textTransform: "uppercase",
          color: "#c9a25e",
          margin: "0 0 clamp(24px,4vw,40px)",
          animation: "mv-404-fade-in 0.6s ease 0.1s both",
        }}>
          Magic Vibes · Страница не найдена
        </p>

        {/* 404 number */}
        <div style={{
          position: "relative",
          animation: "mv-404-float 6s ease-in-out infinite",
          marginBottom: "clamp(24px,4vw,40px)",
        }}>
          {/* Base text */}
          <span style={{
            display: "block",
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: "italic",
            fontWeight: 300,
            fontSize: "clamp(140px,28vw,260px)",
            lineHeight: 0.85,
            color: "rgba(245,244,240,0.06)",
            userSelect: "none",
          }}>404</span>
          {/* Shimmer overlay */}
          <span aria-hidden="true" style={{
            position: "absolute",
            inset: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: "italic",
            fontWeight: 300,
            fontSize: "clamp(140px,28vw,260px)",
            lineHeight: 0.85,
            backgroundImage: "linear-gradient(104deg, #fffdf7 0%, #f5f4f0 20%, #e9d2a0 40%, #fffdf7 55%, #d7b880 75%, #f5f4f0 100%)",
            backgroundSize: "250% 100%",
            WebkitBackgroundClip: "text",
            backgroundClip: "text",
            color: "transparent",
            animation: "mv-404-shimmer 8s linear infinite",
          }}>404</span>
        </div>

        {/* Divider */}
        <div style={{
          width: "min(320px,50%)",
          height: 1,
          background: "linear-gradient(90deg, transparent, rgba(201,162,94,0.55), transparent)",
          margin: "0 auto clamp(24px,4vw,40px)",
          animation: "mv-404-fade-in 0.6s ease 0.25s both",
        }} />

        {/* Message */}
        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic",
          fontWeight: 400,
          fontSize: "clamp(22px,3.5vw,38px)",
          color: "#f5f4f0",
          margin: "0 0 16px",
          lineHeight: 1.15,
          animation: "mv-404-fade-in 0.6s ease 0.3s both",
        }}>
          Этот аромат улетучился
        </h1>

        <p style={{
          fontSize: "clamp(13px,1.3vw,15px)",
          color: "#7d7a73",
          lineHeight: 1.75,
          maxWidth: "38ch",
          margin: "0 auto clamp(32px,5vw,52px)",
          animation: "mv-404-fade-in 0.6s ease 0.4s both",
        }}>
          Страница, которую вы ищете, не существует или была перемещена.
          Возможно, вы найдёте что-то ещё более чудесное в нашем каталоге.
        </p>

        {/* CTA buttons */}
        <div style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 12,
          justifyContent: "center",
          animation: "mv-404-fade-in 0.6s ease 0.5s both",
          marginBottom: "clamp(48px,6vw,72px)",
        }}>
          <Link to="/catalog" className="mv-404-link mv-404-link-primary">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M6 2L3 6v14a2 2 0 002 2h14a2 2 0 002-2V6l-3-4z"/><line x1="3" y1="6" x2="21" y2="6"/><path d="M16 10a4 4 0 01-8 0"/></svg>
            Каталог ароматов
          </Link>
          <Link to="/" className="mv-404-link mv-404-link-ghost">
            На главную
          </Link>
        </div>

        {/* Popular links */}
        <div style={{
          animation: "mv-404-fade-in 0.6s ease 0.6s both",
        }}>
          <p style={{
            fontSize: 10,
            letterSpacing: "0.22em",
            textTransform: "uppercase",
            color: "#4a473f",
            marginBottom: 16,
          }}>Популярные разделы</p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center" }}>
            {[
              { label: "Новинки",     to: "/new" },
              { label: "Бренды",      to: "/brands" },
              { label: "Подарки",     to: "/gift" },
              { label: "Доставка",    to: "/delivery" },
              { label: "FAQ",         to: "/faq" },
            ].map(({ label, to }) => (
              <Link
                key={to}
                to={to}
                style={{
                  padding: "7px 14px",
                  background: "rgba(255,255,255,0.03)",
                  border: "1px solid rgba(255,255,255,0.07)",
                  borderRadius: 2,
                  fontSize: 11.5,
                  color: "#6f6c66",
                  textDecoration: "none",
                  letterSpacing: "0.06em",
                  transition: "border-color 0.25s ease, color 0.25s ease",
                }}
                onMouseEnter={e => {
                  e.currentTarget.style.borderColor = "rgba(201,162,94,0.3)";
                  e.currentTarget.style.color = "#c9a25e";
                }}
                onMouseLeave={e => {
                  e.currentTarget.style.borderColor = "rgba(255,255,255,0.07)";
                  e.currentTarget.style.color = "#6f6c66";
                }}
              >
                {label}
              </Link>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
