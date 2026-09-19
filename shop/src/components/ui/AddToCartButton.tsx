import { useState, useCallback } from "react";
import { motion, animate, useMotionValue } from "motion/react";
import { ShoppingBag, Check } from "lucide-react";
import type { ShopProduct } from "../../types";
import { useCart } from "../../CartContext";

type Phase = "idle" | "launching" | "added";

interface Particle {
  id: number;
  angle: number;
  radius: number;
  size: number;
}

function quadratic(t: number, p0: number, p1: number, p2: number) {
  const r = 1 - t;
  return r * r * p0 + 2 * r * t * p1 + t * t * p2;
}

function SprayIcon() {
  return (
    <svg
      width="13" height="13" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round"
      aria-hidden
    >
      <path d="M5 22V12a7 7 0 0 1 14 0v10" />
      <path d="M5 12a7 7 0 0 1 7-7" />
      <path d="M12 5V2" />
      <path d="M17 3l-5 2" />
      <path d="M17 7c1.5.5 2 1.5 2 3" />
    </svg>
  );
}

const VARIANTS = {
  idle:      { backgroundColor: "transparent",           borderColor: "rgba(201,162,94,0.4)",  color: "#d9c79c" },
  hover:     { backgroundColor: "#c9a25e",               borderColor: "#c9a25e",               color: "#14120f" },
  launching: { backgroundColor: "rgba(201,162,94,0.15)", borderColor: "rgba(201,162,94,0.8)",  color: "#e9d2a0" },
  added:     { backgroundColor: "rgba(90,185,110,0.09)", borderColor: "rgba(90,185,110,0.35)", color: "#7ecb8a" },
};

interface Props {
  product: ShopProduct;
  size?: "sm" | "md";
}

export default function AddToCartButton({ product, size = "sm" }: Props) {
  const { add } = useCart();
  const [phase, setPhase]         = useState<Phase>("idle");
  const [hovered, setHovered]     = useState(false);
  const [particles, setParticles] = useState<Particle[]>([]);

  const flyX      = useMotionValue(0);
  const flyY      = useMotionValue(0);
  const flyScale  = useMotionValue(0);
  const flyOp     = useMotionValue(0);
  const flyRotate = useMotionValue(0);

  const effectiveVariant =
    phase === "idle" ? (hovered ? "hover" : "idle") : phase;

  const handleAdd = useCallback(async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (phase !== "idle") return;
    add(product);
    setHovered(false);
    setPhase("launching");

    setParticles(
      Array.from({ length: 12 }, (_, i) => ({
        id: Date.now() + i,
        angle: (360 / 12) * i + (Math.random() - 0.5) * 14,
        radius: 20 + Math.random() * 22,
        size: 2.2 + Math.random() * 2.4,
      }))
    );

    flyX.set(0); flyY.set(0); flyScale.set(1); flyOp.set(1); flyRotate.set(0);

    await animate(0, 1, {
      duration: 0.6,
      ease: [0.34, 1.4, 0.64, 1],
      onUpdate: (t) => {
        flyX.set(quadratic(t, 0, 22, -6));
        flyY.set(quadratic(t, 0, -60, -108));
        flyScale.set(quadratic(t, 1, 0.85, 0.25));
        flyOp.set(t < 0.7 ? 1 : 1 - (t - 0.7) / 0.3);
        flyRotate.set(-38 * t);
      },
    });

    flyOp.set(0);
    setParticles([]);
    setPhase("added");
    setTimeout(() => setPhase("idle"), 2100);
  }, [phase, product, add, flyX, flyY, flyScale, flyOp, flyRotate]);

  const h  = size === "md" ? 40 : 34;
  const px = size === "md" ? 18 : 13;
  const fs = size === "md" ? 12 : 11;

  return (
    <div style={{ position: "relative", display: "inline-flex", flexShrink: 0 }}>

      {/* Flying spray icon */}
      <motion.div
        aria-hidden
        style={{
          position: "absolute",
          bottom: "50%", left: "50%",
          marginLeft: -6, marginBottom: -6,
          x: flyX, y: flyY, scale: flyScale, opacity: flyOp, rotate: flyRotate,
          pointerEvents: "none", zIndex: 30,
          color: "#e9d2a0",
          filter: "drop-shadow(0 0 4px rgba(201,162,94,0.6))",
        }}
      >
        <SprayIcon />
      </motion.div>

      {/* Gold particle burst */}
      {particles.map((p) => (
        <motion.span
          key={p.id}
          aria-hidden
          initial={{ x: 0, y: 0, opacity: 1, scale: 1 }}
          animate={{
            x: Math.cos((p.angle * Math.PI) / 180) * p.radius,
            y: Math.sin((p.angle * Math.PI) / 180) * p.radius - 6,
            opacity: 0, scale: 0.15,
          }}
          transition={{ duration: 0.52, ease: "easeOut" }}
          style={{
            position: "absolute",
            bottom: "50%", left: "50%",
            width: p.size, height: p.size,
            borderRadius: "50%",
            background: "radial-gradient(circle, #f0dcae 0%, #c9a25e 100%)",
            boxShadow: "0 0 4px rgba(201,162,94,0.6)",
            pointerEvents: "none", zIndex: 20,
            marginLeft: -(p.size / 2), marginBottom: -(p.size / 2),
          }}
        />
      ))}

      {/* Button */}
      <motion.button
        type="button"
        onClick={handleAdd}
        disabled={phase !== "idle"}
        variants={VARIANTS}
        animate={effectiveVariant}
        whileTap={phase === "idle" ? { scale: 0.96 } : undefined}
        transition={{ duration: 0.22 }}
        onHoverStart={() => phase === "idle" && setHovered(true)}
        onHoverEnd={() => setHovered(false)}
        aria-label="В корзину"
        style={{
          height: h, padding: `0 ${px}px`,
          borderRadius: 2,
          border: "1px solid rgba(201,162,94,0.4)",
          fontSize: fs, letterSpacing: "0.1em", textTransform: "uppercase",
          fontFamily: "inherit",
          cursor: phase === "idle" ? "pointer" : "default",
          whiteSpace: "nowrap", flexShrink: 0,
          display: "flex", alignItems: "center", justifyContent: "center",
          gap: 6, overflow: "hidden", minWidth: 114, position: "relative",
        }}
      >
        {/* Shimmer on launch */}
        {phase === "launching" && (
          <motion.span
            aria-hidden
            initial={{ x: "-130%" }}
            animate={{ x: "270%" }}
            transition={{ duration: 0.36, ease: "easeOut" }}
            style={{
              position: "absolute", inset: 0, width: "52%",
              background: "linear-gradient(90deg, transparent, rgba(240,220,174,0.45), transparent)",
              transform: "skewX(-18deg)", pointerEvents: "none", zIndex: 1,
            }}
          />
        )}

        {/* "В корзину" */}
        <motion.span
          animate={{ opacity: phase === "idle" ? 1 : 0, y: phase === "idle" ? 0 : -10 }}
          transition={{ duration: 0.17 }}
          style={{ display: "flex", alignItems: "center", gap: 6, position: "absolute", pointerEvents: "none", zIndex: 2 }}
        >
          <ShoppingBag size={11} strokeWidth={1.8} />
          В корзину
        </motion.span>

        {/* "Добавлено ✓" */}
        <motion.span
          animate={{ opacity: phase === "added" ? 1 : 0, y: phase === "added" ? 0 : 10 }}
          transition={{ duration: 0.22 }}
          style={{ display: "flex", alignItems: "center", gap: 6, position: "absolute", pointerEvents: "none", zIndex: 2 }}
        >
          <Check size={11} strokeWidth={2.5} />
          Добавлено
        </motion.span>

        {/* Sizing ghost */}
        <span aria-hidden style={{ opacity: 0, display: "flex", alignItems: "center", gap: 6, pointerEvents: "none" }}>
          <ShoppingBag size={11} />В корзину
        </span>
      </motion.button>
    </div>
  );
}
