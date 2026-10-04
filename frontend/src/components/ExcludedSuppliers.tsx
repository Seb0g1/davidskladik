import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Ban, Plus, X } from "lucide-react";
import { useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson } from "../api";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import "./excluded-suppliers.css";

// Поставщики, которых не предлагаем: «Подбор поставщиков» и подсказки привязок на «Фрагрантике» (общий список).

type Supplier = { partnerId: string; name: string };
type Data = { excluded: Supplier[]; suppliers: Supplier[] };
const anyJson = <T,>(url: string, init?: RequestInit) => fetchJson<T>(url, z.custom<T>(() => true), init);
const key = (s: Supplier) => s.partnerId || s.name.toLowerCase();

export function ExcludedSuppliers({ onChange }: { onChange?: () => void }) {
  const queryClient = useQueryClient();
  const [adding, setAdding] = useState(false);
  const [search, setSearch] = useState("");
  const query = useQuery({ queryKey: ["supplier-match-excluded"], queryFn: () => anyJson<Data>("/api/supplier-match/excluded") });
  const excluded = query.data?.excluded || [];
  const save = useMutation({
    mutationFn: (suppliers: Supplier[]) => anyJson<Data>("/api/supplier-match/excluded", { method: "PUT", body: JSON.stringify({ suppliers }) }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-match-excluded"] });
      onChange?.();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const options = useMemo(() => {
    const taken = new Set(excluded.map(key));
    const q = search.trim().toLowerCase();
    return (query.data?.suppliers || []).filter((s) => !taken.has(key(s)) && (!q || s.name.toLowerCase().includes(q))).slice(0, 30);
  }, [query.data, excluded, search]);

  return (
    <div className="xs-box">
      <span className="xs-label"><Ban size={14} /> Не предлагать:</span>
      {excluded.map((s) => (
        <span key={key(s)} className="xs-chip">
          {s.name}
          <button type="button" aria-label={`Вернуть ${s.name}`} disabled={save.isPending} onClick={() => save.mutate(excluded.filter((x) => key(x) !== key(s)))}><X size={12} /></button>
        </span>
      ))}
      {!excluded.length && !query.isLoading ? <span className="xs-empty">все поставщики</span> : null}
      <span className="xs-add">
        <button type="button" className="secondary-action compact" onClick={() => setAdding((v) => !v)}><Plus size={13} /> Поставщик</button>
        {adding ? (
          <span className="xs-pop" onMouseLeave={() => setAdding(false)}>
            <input autoFocus value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Найти поставщика" />
            {options.map((s) => (
              <button key={key(s)} type="button" onClick={() => { save.mutate([...excluded, s]); setAdding(false); setSearch(""); toast.success(`${s.name} больше не предлагается`); }}>{s.name}</button>
            ))}
            {!options.length ? <span className="xs-empty">Никого не нашли</span> : null}
          </span>
        ) : null}
      </span>
    </div>
  );
}
