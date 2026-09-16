"use client";
import { useEffect } from "react";
import { usePathname } from "next/navigation";

export default function PageEffects() {
  const pathname = usePathname();

  useEffect(() => {
    /* Scroll reveal */
    const els = document.querySelectorAll(".reveal-section");
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) { e.target.classList.add("is-visible"); io.unobserve(e.target); }
      });
    }, { threshold: 0.06 });
    els.forEach((el) => io.observe(el));

    /* Card reveal */
    const cards = document.querySelectorAll("[data-mv-card]");
    const io2 = new IntersectionObserver((entries) => {
      entries.forEach((e, i) => {
        if (e.isIntersecting) {
          setTimeout(() => e.target.classList.add("mv-in"), i * 60);
          io2.unobserve(e.target);
        }
      });
    }, { threshold: 0.1 });
    cards.forEach((el) => io2.observe(el));

    /* Hero parallax */
    const hero = document.getElementById("hero-parallax-inner");
    let raf = 0;
    const onScroll = () => {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        if (hero) hero.style.transform = `translateY(${window.scrollY * 0.22}px)`;
      });
    };
    window.addEventListener("scroll", onScroll, { passive: true });

    return () => {
      io.disconnect();
      io2.disconnect();
      window.removeEventListener("scroll", onScroll);
      cancelAnimationFrame(raf);
    };
  }, [pathname]);

  return null;
}
