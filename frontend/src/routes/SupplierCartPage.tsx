import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Clock3, Loader2, RefreshCw } from "lucide-react";
import { z } from "zod";
import { fetchJson, patchBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SupplierCartPanel } from "./OperationsPage";

const SupplierCartScheduleSchema = z.object({
  ok: z.boolean().optional(),
  settings: z.record(z.string(), z.unknown()).optional().default({}),
  autoRunning: z.boolean().optional().default(false),
  lastAutoRunAt: z.coerce.string().optional().nullable(),
  nextAutoRunAt: z.coerce.string().optional().nullable(),
  lastAutoResult: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

const text = (value: unknown) => String(value ?? "").trim();
const formatDate = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};

export function SupplierCartPage() {
  const queryClient = useQueryClient();
  const schedule = useQuery({
    queryKey: ["supplier-cart", "schedule"],
    queryFn: () => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema),
    refetchInterval: 30_000,
  });
  const toggle = useMutation({
    mutationFn: (autoEnabled: boolean) => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema, patchBody({ autoEnabled })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart", "schedule"] });
    },
  });
  const settings = schedule.data?.settings || {};
  const times = Array.isArray(settings.scheduleTimes) ? settings.scheduleTimes.map(text).filter(Boolean) : ["09:30", "12:00", "15:00"];
  const last = schedule.data?.lastAutoResult;
  return (
    <>
      <PageHeader
        title="Автокорзина"
        subtitle="Заказы Ozon/Yandex автоматически собираются и отправляются в корзину PriceMaster по расписанию."
        action={<a className="secondary-action" href="/app/operations"><RefreshCw size={16} /> Операции</a>}
      />
      <section className="table-panel supplier-cart-schedule">
        <div className="section-title">
          <div>
            <span>Расписание</span>
            <h3>Автогенерация корзины</h3>
          </div>
          <button className="secondary-action" type="button" disabled={toggle.isPending} onClick={() => toggle.mutate(!(settings.autoEnabled !== false))}>
            {toggle.isPending ? <Loader2 className="spin" size={16} /> : <Clock3 size={16} />} {settings.autoEnabled !== false ? "Выключить авто" : "Включить авто"}
          </button>
        </div>
        <div className="summary-grid">
          <div><span>Статус</span><strong>{settings.autoEnabled !== false ? "включено" : "выключено"}</strong></div>
          <div><span>Время</span><strong>{times.join(" · ")}</strong></div>
          <div><span>Следующий запуск</span><strong>{formatDate(schedule.data?.nextAutoRunAt)}</strong></div>
          <div><span>Последний запуск</span><strong>{formatDate(schedule.data?.lastAutoRunAt)}</strong></div>
        </div>
        {last ? (
          <div className="success-strip">
            Последний запуск: всего {Number(last.total || 0)}, готово {Number(last.ready || 0)}, добавлено в PriceMaster {Number(last.inserted || 0)}, создано строк сборки {Number(last.pickingCreated || 0)}, пропущено {Number(last.skipped || 0)}.
          </div>
        ) : null}
      </section>
      <SupplierCartPanel />
    </>
  );
}
