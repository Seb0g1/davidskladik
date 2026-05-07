/**
 * Stepper + scroll spy for product builder pages (Ozon / Yandex).
 * Expects: main[data-product-builder], .builder-stepper .builder-step[data-scroll-target],
 * sections with matching id and .builder-track-section.
 */
(function initProductBuilderUi() {
  function setActiveStep(stepper, activeId) {
    stepper.querySelectorAll(".builder-step[data-scroll-target]").forEach((btn) => {
      const on = btn.dataset.scrollTarget === activeId;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-current", on ? "step" : "false");
    });
  }

  function run() {
    const root = document.querySelector("[data-product-builder]");
    if (!root) return;

    const stepper = root.querySelector(".builder-stepper");
    if (!stepper) return;

    const steps = [...stepper.querySelectorAll(".builder-step[data-scroll-target]")];
    const sectionIds = steps.map((s) => s.dataset.scrollTarget).filter(Boolean);
    const sections = sectionIds.map((id) => document.getElementById(id)).filter(Boolean);

    steps.forEach((btn) => {
      btn.addEventListener("click", () => {
        const id = btn.dataset.scrollTarget;
        const el = id ? document.getElementById(id) : null;
        if (el) {
          el.scrollIntoView({ behavior: "smooth", block: "start" });
          setActiveStep(stepper, id);
        }
      });
    });

    if (!sections.length || typeof IntersectionObserver === "undefined") return;

    let ticking = false;
    const ratios = new Map();

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.target.id) ratios.set(entry.target.id, entry.isIntersecting ? entry.intersectionRatio : 0);
        }
        if (ticking) return;
        ticking = true;
        requestAnimationFrame(() => {
          ticking = false;
          let bestId = sectionIds[0];
          let best = -1;
          for (const id of sectionIds) {
            const r = ratios.get(id) ?? 0;
            if (r > best) {
              best = r;
              bestId = id;
            }
          }
          if (best <= 0 && sections[0]) bestId = sections[0].id;
          setActiveStep(stepper, bestId);
        });
      },
      {
        root: null,
        rootMargin: "-12% 0px -50% 0px",
        threshold: [0, 0.08, 0.15, 0.3, 0.5, 0.75, 1],
      },
    );

    sections.forEach((sec) => observer.observe(sec));
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run);
  else run();
})();
