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

(function initProductActionConfirm() {
  function ensureModal() {
    let backdrop = document.getElementById("productActionConfirmModal");
    if (backdrop) return backdrop;

    backdrop = document.createElement("div");
    backdrop.id = "productActionConfirmModal";
    backdrop.className = "cleanup-modal-backdrop";
    backdrop.hidden = true;
    backdrop.innerHTML = `
      <section class="cleanup-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="productActionConfirmTitle">
        <div class="cleanup-confirm-head">
          <div>
            <p id="productActionConfirmEyebrow" class="eyebrow">Подтверждение</p>
            <h2 id="productActionConfirmTitle">Подтвердите действие</h2>
          </div>
          <button id="productActionConfirmClose" class="modal-icon-button" type="button" aria-label="Закрыть">×</button>
        </div>
        <div id="productActionConfirmBody" class="action-confirm-body"></div>
        <div class="cleanup-confirm-actions">
          <button id="productActionConfirmCancel" class="secondary-button" type="button">Отмена</button>
          <button id="productActionConfirmSubmit" class="primary-button" type="button">Продолжить</button>
        </div>
      </section>
    `;
    document.body.appendChild(backdrop);
    return backdrop;
  }

  window.openProductActionConfirm = function openProductActionConfirm({
    eyebrow = "Подтверждение",
    title = "Подтвердите действие",
    lines = [],
    confirmLabel = "Продолжить",
  } = {}) {
    const backdrop = ensureModal();
    const titleNode = backdrop.querySelector("#productActionConfirmTitle");
    const eyebrowNode = backdrop.querySelector("#productActionConfirmEyebrow");
    const bodyNode = backdrop.querySelector("#productActionConfirmBody");
    const closeButton = backdrop.querySelector("#productActionConfirmClose");
    const cancelButton = backdrop.querySelector("#productActionConfirmCancel");
    const submitButton = backdrop.querySelector("#productActionConfirmSubmit");

    eyebrowNode.textContent = eyebrow;
    titleNode.textContent = title;
    submitButton.textContent = confirmLabel;
    bodyNode.innerHTML = (Array.isArray(lines) ? lines : [lines])
      .filter(Boolean)
      .map((line) => `<p>${String(line).replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;",
      }[char]))}</p>`)
      .join("");

    backdrop.hidden = false;
    submitButton.focus();

    return new Promise((resolve) => {
      let settled = false;
      const cleanup = (result) => {
        if (settled) return;
        settled = true;
        backdrop.hidden = true;
        document.removeEventListener("keydown", onKeydown);
        closeButton.removeEventListener("click", onCancel);
        cancelButton.removeEventListener("click", onCancel);
        submitButton.removeEventListener("click", onSubmit);
        backdrop.removeEventListener("click", onBackdropClick);
        resolve(result);
      };
      const onCancel = () => cleanup(false);
      const onSubmit = () => cleanup(true);
      const onBackdropClick = (event) => {
        if (event.target === backdrop) cleanup(false);
      };
      const onKeydown = (event) => {
        if (event.key === "Escape") cleanup(false);
      };

      closeButton.addEventListener("click", onCancel);
      cancelButton.addEventListener("click", onCancel);
      submitButton.addEventListener("click", onSubmit);
      backdrop.addEventListener("click", onBackdropClick);
      document.addEventListener("keydown", onKeydown);
    });
  };
})();
