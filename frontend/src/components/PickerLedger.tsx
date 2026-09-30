import { useMutation, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Check, CheckCircle2, Download, Loader2, Pencil, Trash2, X } from "lucide-react";
import { useMemo, useState } from "react";
import { fetchJson, patchBody } from "../api";
import { toast, undoable } from "../lib/toast";
import { PickerBalanceSchema } from "../types";

// Журнал сборщика (Сборка → Балансы): выдачи, оплаты поставщикам и возвраты наличных
// по дням с ежедневной сверкой, фильтр периода с итогами и выгрузка в Excel.
// Суммы хранятся в рублях; на экране — в долларах по текущему курсу, как и раньше.

type Credit = { id: string; amount: number; note?: string | null; createdAt?: string | null; originalUsd?: unknown };
type Spend = { id?: string; supplierName?: string | null; amount: number; currency?: string | null; note?: string | null; occurredAt?: string | null };
type Kind = "issue" | "spend" | "return";
type Period = "today" | "yesterday" | "7d" | "30d" | "month" | "all" | "custom";

const PERIODS: Array<{ id: Period; label: string }> = [
  { id: "today", label: "Сегодня" },
  { id: "yesterday", label: "Вчера" },
  { id: "7d", label: "7 дней" },
  { id: "30d", label: "30 дней" },
  { id: "month", label: "Этот месяц" },
  { id: "all", label: "Всё" },
];

function kindOf(credit: Credit): Kind {
  if (Number(credit.amount) > 0) return "issue";
  // Оплаты поставщикам записываются с id «payment:<id записи в долгах поставщика>».
  if (String(credit.id || "").startsWith("payment:")) return "spend";
  return "return";
}
const KIND_LABEL: Record<Kind, string> = { issue: "Выдано", spend: "Оплата поставщику", return: "Возврат наличных" };

function localKey(date: Date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}
function keyOf(value?: string | null) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : localKey(date);
}
function shiftDays(days: number) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return localKey(date);
}
function periodRange(period: Period, from: string, to: string): [string, string] {
  const today = localKey(new Date());
  switch (period) {
    case "today": return [today, today];
    case "yesterday": return [shiftDays(-1), shiftDays(-1)];
    case "7d": return [shiftDays(-6), today];
    case "30d": return [shiftDays(-29), today];
    case "month": return [`${today.slice(0, 8)}01`, today];
    case "custom": return [from || "0000-00-00", to || "9999-99-99"];
    default: return ["0000-00-00", "9999-99-99"];
  }
}
function dayLabel(key: string) {
  if (!key) return "Без даты";
  if (key === localKey(new Date())) return "Сегодня";
  if (key === shiftDays(-1)) return "Вчера";
  const [y, m, d] = key.split("-").map(Number);
  const label = new Date(y, m - 1, d).toLocaleDateString("ru-RU", { weekday: "short", day: "numeric", month: "long", ...(y === new Date().getFullYear() ? {} : { year: "numeric" }) });
  return label.charAt(0).toUpperCase() + label.slice(1);
}
function timeOf(value?: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" });
}
function ruDate(key: string) {
  if (!key) return "";
  const [y, m, d] = key.split("-");
  return `${d}.${m}.${y}`;
}

type Day = {
  key: string;
  label: string;
  items: Credit[];
  opening: number;
  issued: number;
  spent: number;
  returned: number;
  closing: number;
  status: "ok" | "short" | "over" | "carry" | "zero";
};

export function PickerLedger({
  username,
  credits,
  usdRate,
  spending,
  spendingLoading,
}: {
  username: string;
  credits: Credit[];
  usdRate: number;
  spending: Spend[];
  spendingLoading: boolean;
}) {
  const queryClient = useQueryClient();
  const [period, setPeriod] = useState<Period>("7d");
  const [from, setFrom] = useState(shiftDays(-6));
  const [to, setTo] = useState(localKey(new Date()));
  const [onlyMismatch, setOnlyMismatch] = useState(false);
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [editing, setEditing] = useState<{ id: string; usd: string; note: string; positive: boolean } | null>(null);
  const [exporting, setExporting] = useState(false);

  const rate = usdRate > 0 ? usdRate : 95;
  const usd = (rub: number) => rub / rate;
  const money = (rub: number, sign = false) => `${sign && rub > 0 ? "+" : ""}${rub < 0 ? "−" : ""}${Math.abs(usd(rub)).toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} $`;
  // Меньше полудоллара — это округление курса, а не потерянный чек.
  const tolerance = rate * 0.5;

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ["picker-balances"] });
    void queryClient.invalidateQueries({ queryKey: ["picker-balance"] });
    void queryClient.invalidateQueries({ queryKey: ["picker-spending"] });
  };
  const deleteMutation = useMutation({
    mutationFn: (id: string) => fetchJson(`/api/picker-cash/balance/${encodeURIComponent(username)}/${encodeURIComponent(id)}`, PickerBalanceSchema, { method: "DELETE" }),
    onSuccess: invalidate,
    onError: (_error, id) => setHidden((prev) => { const next = new Set(prev); next.delete(id); return next; }),
  });
  const editMutation = useMutation({
    mutationFn: (input: { id: string; amount?: number; originalUsd?: number; note: string }) =>
      fetchJson(`/api/picker-cash/balance/${encodeURIComponent(username)}/${encodeURIComponent(input.id)}`, PickerBalanceSchema, patchBody({ amount: input.amount, originalUsd: input.originalUsd, note: input.note })),
    onSuccess: () => { setEditing(null); invalidate(); toast.success("Запись изменена"); },
  });

  const visibleCredits = useMemo(() => credits.filter((credit) => !hidden.has(credit.id)), [credits, hidden]);

  // Сверка идёт по всей истории (иначе остаток на начало периода был бы неверным),
  // а на экран попадают только дни выбранного периода.
  const allDays = useMemo<Day[]>(() => {
    const sorted = visibleCredits.slice().sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
    const map = new Map<string, Credit[]>();
    for (const credit of sorted) {
      const key = keyOf(credit.createdAt);
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(credit);
    }
    let running = 0;
    const days: Day[] = [];
    for (const [key, items] of Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]))) {
      const opening = running;
      let issued = 0;
      let spent = 0;
      let returned = 0;
      for (const credit of items) {
        const amount = Number(credit.amount) || 0;
        const kind = kindOf(credit);
        if (kind === "issue") issued += amount;
        else if (kind === "spend") spent += -amount;
        else returned += -amount;
      }
      const closing = opening + issued - spent - returned;
      running = closing;
      let status: Day["status"];
      if (returned > 0) status = Math.abs(closing) <= tolerance ? "ok" : closing > 0 ? "short" : "over";
      else status = Math.abs(closing) <= tolerance ? "zero" : "carry";
      days.push({ key, label: dayLabel(key), items: items.slice().reverse(), opening, issued, spent, returned, closing, status });
    }
    return days.reverse();
  }, [visibleCredits, tolerance]);

  const [rangeFrom, rangeTo] = periodRange(period, from, to);
  const periodDays = allDays.filter((day) => day.key >= rangeFrom && day.key <= rangeTo);
  const shownDays = onlyMismatch ? periodDays.filter((day) => day.status === "short" || day.status === "over") : periodDays;
  const mismatchCount = periodDays.filter((day) => day.status === "short" || day.status === "over").length;
  const totals = periodDays.reduce((acc, day) => ({ issued: acc.issued + day.issued, spent: acc.spent + day.spent, returned: acc.returned + day.returned }), { issued: 0, spent: 0, returned: 0 });
  const periodOpening = periodDays.length ? periodDays[periodDays.length - 1].opening : 0;
  const periodClosing = periodDays.length ? periodDays[0].closing : (allDays.find((day) => day.key < rangeFrom)?.closing ?? 0);

  const periodSpending = spending.filter((entry) => {
    const key = keyOf(entry.occurredAt);
    return key >= rangeFrom && key <= rangeTo;
  });
  const spendUsd = periodSpending.filter((entry) => String(entry.currency || "").toUpperCase() === "USD").reduce((sum, entry) => sum + Math.abs(entry.amount), 0);
  const spendRub = periodSpending.filter((entry) => String(entry.currency || "").toUpperCase() !== "USD").reduce((sum, entry) => sum + Math.abs(entry.amount), 0);

  const requestDelete = (credit: Credit) => {
    setHidden((prev) => new Set(prev).add(credit.id));
    const extra = kindOf(credit) === "spend" ? " Оплата в долгах поставщика тоже отменится." : "";
    undoable(
      <span>Запись {money(Number(credit.amount), true)} удалена.{extra}</span>,
      () => deleteMutation.mutate(credit.id),
      { onUndo: () => setHidden((prev) => { const next = new Set(prev); next.delete(credit.id); return next; }) },
    );
  };

  const exportExcel = async () => {
    setExporting(true);
    try {
      const XLSX = await import("xlsx");
      const workbook = XLSX.utils.book_new();
      const operations: Array<Record<string, unknown>> = [];
      for (const day of periodDays.slice().reverse()) {
        let running = day.opening;
        for (const credit of day.items.slice().reverse()) {
          running += Number(credit.amount) || 0;
          operations.push({
            "Дата": ruDate(day.key),
            "Время": timeOf(credit.createdAt),
            "Тип": KIND_LABEL[kindOf(credit)],
            "Комментарий": credit.note || "",
            "Сумма, $": Math.round(usd(Number(credit.amount)) * 100) / 100,
            "Сумма, ₽": Math.round(Number(credit.amount)),
            "На руках после, $": Math.round(usd(running) * 100) / 100,
          });
        }
      }
      const daily = periodDays.slice().reverse().map((day) => ({
        "Дата": ruDate(day.key),
        "На начало, $": Math.round(usd(day.opening) * 100) / 100,
        "Выдано, $": Math.round(usd(day.issued) * 100) / 100,
        "Оплачено поставщикам, $": Math.round(usd(day.spent) * 100) / 100,
        "Возвращено, $": Math.round(usd(day.returned) * 100) / 100,
        "На руках в конце дня, $": Math.round(usd(day.closing) * 100) / 100,
        "Сверка": statusText(day),
      }));
      daily.push({
        "Дата": "Итого",
        "На начало, $": Math.round(usd(periodOpening) * 100) / 100,
        "Выдано, $": Math.round(usd(totals.issued) * 100) / 100,
        "Оплачено поставщикам, $": Math.round(usd(totals.spent) * 100) / 100,
        "Возвращено, $": Math.round(usd(totals.returned) * 100) / 100,
        "На руках в конце дня, $": Math.round(usd(periodClosing) * 100) / 100,
        "Сверка": mismatchCount ? `Расхождений: ${mismatchCount}` : "Всё сходится",
      });
      const suppliers = periodSpending.slice().reverse().map((entry) => ({
        "Дата": ruDate(keyOf(entry.occurredAt)),
        "Время": timeOf(entry.occurredAt),
        "Поставщик": entry.supplierName || "",
        "Сумма": Math.abs(entry.amount),
        "Валюта": String(entry.currency || "RUB").toUpperCase(),
        "Комментарий": entry.note || "",
      }));
      const addSheet = (rows: Array<Record<string, unknown>>, name: string, widths: number[]) => {
        const sheet = XLSX.utils.json_to_sheet(rows.length ? rows : [{ "": "Нет данных за период" }]);
        sheet["!cols"] = widths.map((wch) => ({ wch }));
        XLSX.utils.book_append_sheet(workbook, sheet, name);
      };
      addSheet(daily, "Сверка по дням", [12, 14, 12, 22, 14, 22, 36]);
      addSheet(operations, "Операции", [12, 8, 20, 36, 12, 12, 18]);
      addSheet(suppliers, "Поставщики", [12, 8, 24, 12, 8, 30]);
      const suffix = period === "all" ? "всё" : `${rangeFrom === "0000-00-00" ? "" : rangeFrom}_${rangeTo === "9999-99-99" ? "" : rangeTo}`;
      XLSX.writeFile(workbook, `Сборщик-${username}-${suffix}.xlsx`);
    } catch (error) {
      toast.error(`Не удалось выгрузить: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setExporting(false);
    }
  };

  function statusText(day: Day) {
    if (day.status === "ok") return "Сошлось";
    if (day.status === "short") return `Не хватает ${money(day.closing)} — вернул меньше, чем осталось`;
    if (day.status === "over") return `Вернул больше на ${money(-day.closing)}`;
    if (day.status === "carry") return `На руках ${money(day.closing)} (возврата не было)`;
    return "—";
  }

  return (
    <div className="picker-ledger">
      <div className="picker-ledger-bar">
        <div className="settings-tabs picker-ledger-periods">
          {PERIODS.map((item) => (
            <button key={item.id} type="button" className={period === item.id ? "is-active" : ""} onClick={() => setPeriod(item.id)}>{item.label}</button>
          ))}
          <button type="button" className={period === "custom" ? "is-active" : ""} onClick={() => setPeriod("custom")}>Период…</button>
        </div>
        {period === "custom" ? (
          <div className="picker-ledger-range">
            <input type="date" value={from} max={to || undefined} onChange={(event) => setFrom(event.target.value)} aria-label="С даты" />
            <span>—</span>
            <input type="date" value={to} min={from || undefined} onChange={(event) => setTo(event.target.value)} aria-label="По дату" />
          </div>
        ) : null}
        <button type="button" className="secondary-action picker-ledger-export" onClick={() => void exportExcel()} disabled={exporting}>
          {exporting ? <Loader2 className="spin" size={15} /> : <Download size={15} />} Excel
        </button>
      </div>

      <div className="picker-ledger-totals">
        <div><span>На начало</span><strong>{money(periodOpening)}</strong></div>
        <div><span>Выдано</span><strong className="tone-success">{money(totals.issued)}</strong></div>
        <div><span>Оплачено поставщикам</span><strong className="tone-warn">{money(totals.spent)}</strong></div>
        <div><span>Возвращено</span><strong>{money(totals.returned)}</strong></div>
        <div><span>На руках сейчас</span><strong className={Math.abs(periodClosing) > tolerance ? "tone-danger" : ""}>{money(periodClosing)}</strong></div>
        <button
          type="button"
          className={`picker-ledger-check${mismatchCount ? " is-bad" : " is-good"}${onlyMismatch ? " is-on" : ""}`}
          onClick={() => mismatchCount && setOnlyMismatch((value) => !value)}
          title={mismatchCount ? "Показать только дни с расхождением" : "Все возвраты сходятся с остатком"}
        >
          {mismatchCount ? <AlertTriangle size={15} /> : <CheckCircle2 size={15} />}
          <span>{mismatchCount ? `Расхождений: ${mismatchCount}` : "Всё сходится"}</span>
        </button>
      </div>

      <div className="picker-ledger-columns">
        <section className="picker-ledger-panel">
          <div className="picker-ledger-panel-title">История — {username} <span>{periodDays.reduce((sum, day) => sum + day.items.length, 0)} опер.</span></div>
          {!shownDays.length ? <p className="picker-balance-empty-hint">{onlyMismatch ? "Расхождений нет." : "За период операций нет."}</p> : null}
          {shownDays.map((day) => (
            <div className={`picker-day-group status-${day.status}`} key={day.key || "none"}>
              <div className="picker-day-head">
                <span className="picker-day-head-label">{day.label}</span>
                <span className="picker-day-head-count">{day.items.length} опер.</span>
                {day.issued ? <span className="tone-success">+{money(day.issued)}</span> : null}
                {day.spent ? <span className="tone-warn">−{money(day.spent)}</span> : null}
                {day.returned ? <span>↩ {money(day.returned)}</span> : null}
              </div>
              <div className={`picker-day-check status-${day.status}`}>
                {day.status === "short" || day.status === "over" ? <AlertTriangle size={13} /> : day.status === "ok" ? <CheckCircle2 size={13} /> : null}
                <span>
                  {money(day.opening)} на начало {day.issued ? `+ ${money(day.issued)} выдано ` : ""}{day.spent ? `− ${money(day.spent)} оплат ` : ""}= {money(day.opening + day.issued - day.spent)} должен вернуть
                  {day.returned ? `, вернул ${money(day.returned)}` : ""}
                </span>
                <strong>{statusText(day)}</strong>
              </div>
              {day.items.map((credit) => {
                const kind = kindOf(credit);
                const isEditing = editing?.id === credit.id;
                if (isEditing && editing) {
                  return (
                    <div className="picker-credit-row picker-credit-row--edit" key={credit.id}>
                      {editing.positive ? (
                        <input type="number" min="0.01" step="0.01" className="picker-credit-edit-amount" value={editing.usd} autoFocus onChange={(event) => setEditing({ ...editing, usd: event.target.value })} aria-label="Сумма, $" />
                      ) : null}
                      <input
                        className="picker-credit-edit-note"
                        placeholder="Комментарий"
                        value={editing.note}
                        autoFocus={!editing.positive}
                        onChange={(event) => setEditing({ ...editing, note: event.target.value })}
                        onKeyDown={(event) => { if (event.key === "Escape") setEditing(null); }}
                      />
                      <button
                        className="icon-action success-action"
                        type="button"
                        title="Сохранить"
                        disabled={editMutation.isPending || (editing.positive && !(Number(editing.usd) > 0))}
                        onClick={() => editMutation.mutate(editing.positive
                          ? { id: credit.id, amount: Math.round(Number(editing.usd) * rate), originalUsd: Number(editing.usd), note: editing.note }
                          : { id: credit.id, note: editing.note })}
                      >
                        {editMutation.isPending ? <Loader2 className="spin" size={12} /> : <Check size={12} />}
                      </button>
                      <button className="icon-action" type="button" title="Отмена" onClick={() => setEditing(null)}><X size={12} /></button>
                    </div>
                  );
                }
                return (
                  <div className={`picker-credit-row kind-${kind}`} key={credit.id}>
                    <span className={`picker-credit-amount${Number(credit.amount) >= 0 ? " tone-success" : kind === "spend" ? " tone-warn" : " tone-danger"}`}>{money(Number(credit.amount), true)}</span>
                    <span className="muted-note picker-credit-note">{kind === "issue" ? `Выдано${credit.note ? ` · ${credit.note}` : ""}` : (credit.note || KIND_LABEL[kind])}</span>
                    <span className="muted-note picker-credit-date" title={credit.createdAt || ""}>{timeOf(credit.createdAt)}</span>
                    <button
                      className="icon-action"
                      type="button"
                      title={kind === "issue" ? "Изменить сумму или комментарий" : "Изменить комментарий"}
                      onClick={() => setEditing({ id: credit.id, positive: kind === "issue", usd: String(Math.round(usd(Number(credit.amount)) * 100) / 100), note: credit.note || "" })}
                    >
                      <Pencil size={11} />
                    </button>
                    <button className="icon-action danger-action" type="button" title="Удалить (можно отменить)" onClick={() => requestDelete(credit)}>
                      <Trash2 size={11} />
                    </button>
                  </div>
                );
              })}
            </div>
          ))}
        </section>

        <section className="picker-ledger-panel">
          <div className="picker-ledger-panel-title">
            Выплачено поставщикам
            <span>{[spendUsd ? `${spendUsd.toLocaleString("ru-RU", { maximumFractionDigits: 2 })} $` : "", spendRub ? `${Math.round(spendRub).toLocaleString("ru-RU")} ₽` : ""].filter(Boolean).join(" + ") || "—"}</span>
          </div>
          {spendingLoading ? <p className="picker-balance-empty-hint">Загрузка…</p> : null}
          {!spendingLoading && !periodSpending.length ? <p className="picker-balance-empty-hint">За период выплат нет.</p> : null}
          {Object.entries(periodSpending.reduce<Record<string, Spend[]>>((acc, entry) => {
            const key = keyOf(entry.occurredAt);
            (acc[key] ||= []).push(entry);
            return acc;
          }, {})).sort((a, b) => b[0].localeCompare(a[0])).map(([key, entries]) => (
            <div className="picker-day-group" key={key || "none"}>
              <div className="picker-day-head">
                <span className="picker-day-head-label">{dayLabel(key)}</span>
                <span className="picker-day-head-count">{entries.length} опл.</span>
              </div>
              {entries.map((entry, index) => (
                <div className="picker-credit-row" key={entry.id || `${key}-${index}`}>
                  <span className="picker-credit-amount tone-warn">−{Math.abs(entry.amount).toLocaleString("ru-RU", { maximumFractionDigits: 2 })} {String(entry.currency || "RUB").toUpperCase() === "USD" ? "$" : "₽"}</span>
                  <span className="muted-note picker-credit-note">{entry.supplierName || "—"}{entry.note ? ` · ${entry.note}` : ""}</span>
                  <span className="muted-note picker-credit-date">{timeOf(entry.occurredAt)}</span>
                </div>
              ))}
            </div>
          ))}
          {spending.length >= 50 ? <p className="picker-balance-empty-hint">Сервер отдаёт последние 50 оплат — более старые смотрите в «Поставщиках».</p> : null}
        </section>
      </div>
    </div>
  );
}
