"use client";
import Link from "next/link";

// Unticked-by-default consent checkbox (152-ФЗ: consent is separate and active; 38-ФЗ: ads opt-in)
// kind="ads" with subscribe → the ads consent is the point of the form, so it's required
export default function ConsentCheck({ kind = "pd", checked, onChange, tone = "light", compact, subscribe }: {
  kind?: "pd" | "ads"; checked: boolean; onChange: (v: boolean) => void; tone?: "light" | "dark"; compact?: boolean; subscribe?: boolean;
}) {
  return (
    <label className={`mv-consent${tone === "dark" ? " dark" : ""}${compact ? " compact" : ""}`}>
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} required={kind === "pd" || subscribe} />
      <span>
        {kind === "pd"
          ? <>Я даю <Link prefetch={false} href="/consent" target="_blank">согласие на обработку персональных данных</Link> и ознакомлен(а) с <Link prefetch={false} href="/privacy" target="_blank">политикой</Link></>
          : subscribe
            ? <>Согласен(на) получать рекламные письма — <Link prefetch={false} href="/consent-ads" target="_blank">согласие на рассылку</Link></>
            : <>Хочу получать скидки и подборки на email — <Link prefetch={false} href="/consent-ads" target="_blank">согласие на рассылку</Link> (необязательно)</>}
      </span>
    </label>
  );
}
