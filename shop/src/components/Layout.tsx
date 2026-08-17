import { Outlet, useLocation } from "react-router-dom";
import { useEffect } from "react";
import { MessageCircle } from "lucide-react";
import Header from "./Header";
import Footer from "./Footer";

function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => { window.scrollTo(0, 0); }, [pathname]);
  return null;
}

function RevealObserver() {
  const { pathname } = useLocation();
  useEffect(() => {
    const obs = new IntersectionObserver(
      (entries) => entries.forEach((e) => {
        if (e.isIntersecting) { e.target.classList.add("is-visible"); obs.unobserve(e.target); }
      }),
      { threshold: 0.06, rootMargin: "0px 0px -32px 0px" }
    );
    document.querySelectorAll(".reveal-section").forEach((el) => obs.observe(el));
    return () => obs.disconnect();
  }, [pathname]);
  return null;
}

export default function Layout() {
  return (
    /* pb-14 on mobile reserves space for the fixed bottom nav */
    <div className="min-h-screen flex flex-col pb-14 md:pb-0">
      <ScrollToTop />
      <RevealObserver />
      <Header />
      <main className="flex-1">
        <Outlet />
      </main>
      <Footer />
      {/* Floating support button */}
      <a
        href="https://davidsklad.ru"
        target="_blank"
        rel="noopener noreferrer"
        className="support-fab"
        title="Техподдержка"
      >
        <MessageCircle size={20} />
      </a>
    </div>
  );
}
