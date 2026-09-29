// Seller / personal-data operator details and consent versions — one place for all legal pages and forms.
export const SELLER = {
  name: "Индивидуальный предприниматель Шальнев Давид Алиевич",
  short: "ИП Шальнев Д. А.",
  ogrnip: "323861700065205",
  inn: "860203590860",
  site: "magicvibes.ru",
};

// bump when the text of /privacy, /consent or /consent-ads changes; stored with every order
export const CONSENT_VERSION = "2026-09-29";
export const LEGAL_REVISION = "29 сентября 2026 г.";

export const COOKIE_CONSENT_KEY = "mv_cookie_consent";
export type CookieConsent = { v: 1; all: boolean; at: string };

export function readCookieConsent(): CookieConsent | null {
  try {
    const raw = localStorage.getItem(COOKIE_CONSENT_KEY);
    const c = raw ? JSON.parse(raw) : null;
    // ask again after 12 months (as stated on /cookies)
    return c && c.v === 1 && Date.now() - Date.parse(c.at) < 365 * 864e5 ? c : null;
  } catch { return null; }
}

export function saveCookieConsent(all: boolean) {
  const c: CookieConsent = { v: 1, all, at: new Date().toISOString() };
  try { localStorage.setItem(COOKIE_CONSENT_KEY, JSON.stringify(c)); } catch { /* private mode */ }
  window.dispatchEvent(new CustomEvent("mv-cookie-consent", { detail: c }));
}
