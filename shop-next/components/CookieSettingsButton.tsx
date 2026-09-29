"use client";

export default function CookieSettingsButton() {
  return (
    <button type="button" className="mv-co-link" style={{ fontSize: 15 }}
      onClick={() => window.dispatchEvent(new Event("mv-cookie-settings"))}>
      Изменить настройки cookie →
    </button>
  );
}
