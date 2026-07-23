import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2, Edit2, Save, X, Loader2, ExternalLink, Image as ImageIcon } from "lucide-react";

interface ShopBanner {
  id: string;
  imageUrl: string;
  title?: string;
  subtitle?: string;
  linkUrl?: string;
  linkText?: string;
  active: boolean;
  order: number;
}

interface ShopCategory {
  id: string;
  name: string;
  slug: string;
  imageUrl?: string;
  order: number;
  filterTag?: string;
}

interface ShopSettings {
  markup: number;
  shopName: string;
  shopDescription: string;
  contactEmail?: string;
  contactPhone?: string;
  deliveryDays?: number;
  freeDeliveryFrom?: number;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...init });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json() as Promise<T>;
}

const SHOP_URL = import.meta.env.VITE_SHOP_URL || "https://magicvibes.ru";

// ── Banners ──────────────────────────────────────────────────────────────────

function BannerRow({ banner, onEdit, onDelete }: { banner: ShopBanner; onEdit: () => void; onDelete: () => void }) {
  return (
    <div className="flex items-center gap-3 p-3 bg-white border border-gray-100 rounded-xl">
      <div className="w-16 h-10 rounded-lg bg-gray-100 flex-shrink-0 overflow-hidden">
        {banner.imageUrl
          ? <img src={banner.imageUrl} alt="" className="w-full h-full object-cover" />
          : <div className="w-full h-full flex items-center justify-center text-gray-300"><ImageIcon size={16} /></div>
        }
      </div>
      <div className="flex-1 min-w-0">
        <div className="font-medium text-sm text-gray-800 truncate">{banner.title || "(без названия)"}</div>
        <div className="text-xs text-gray-400 truncate">{banner.subtitle || banner.linkUrl || ""}</div>
      </div>
      <div className={`text-xs px-2 py-0.5 rounded-full ${banner.active ? "bg-green-100 text-green-700" : "bg-gray-100 text-gray-500"}`}>
        {banner.active ? "Активен" : "Скрыт"}
      </div>
      <button onClick={onEdit} className="p-1.5 rounded-lg hover:bg-gray-100 text-gray-500 hover:text-violet-600 transition-colors"><Edit2 size={15} /></button>
      <button onClick={onDelete} className="p-1.5 rounded-lg hover:bg-red-50 text-gray-400 hover:text-red-500 transition-colors"><Trash2 size={15} /></button>
    </div>
  );
}

function BannerForm({ banner, onSave, onCancel }: {
  banner?: Partial<ShopBanner>;
  onSave: (data: Partial<ShopBanner>) => void;
  onCancel: () => void;
}) {
  const [form, setForm] = useState<Partial<ShopBanner>>({
    imageUrl: "", title: "", subtitle: "", linkUrl: "", linkText: "", active: true,
    ...banner,
  });
  const set = (k: keyof ShopBanner) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm((f) => ({ ...f, [k]: e.target.type === "checkbox" ? e.target.checked : e.target.value }));

  return (
    <div className="bg-violet-50 border border-violet-100 rounded-xl p-4 space-y-3">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">URL изображения</label>
          <input value={form.imageUrl || ""} onChange={set("imageUrl")} placeholder="https://..." className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Заголовок</label>
          <input value={form.title || ""} onChange={set("title")} placeholder="Летняя коллекция" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Подзаголовок</label>
          <input value={form.subtitle || ""} onChange={set("subtitle")} placeholder="Скидки до 50%" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Ссылка</label>
          <input value={form.linkUrl || ""} onChange={set("linkUrl")} placeholder="/catalog/sale" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Текст кнопки</label>
          <input value={form.linkText || ""} onChange={set("linkText")} placeholder="Смотреть акции" className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400" />
        </div>
        <div className="flex items-center gap-2 pt-5">
          <input type="checkbox" id="banner-active" checked={form.active !== false} onChange={set("active")} className="accent-violet-600" />
          <label htmlFor="banner-active" className="text-sm text-gray-700">Активен (показывать на сайте)</label>
        </div>
      </div>
      <div className="flex gap-2 pt-1">
        <button onClick={() => onSave(form)} className="flex items-center gap-1.5 bg-violet-600 text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-violet-700 transition-colors"><Save size={14} /> Сохранить</button>
        <button onClick={onCancel} className="flex items-center gap-1.5 border border-gray-200 px-4 py-2 rounded-lg text-sm text-gray-600 hover:bg-gray-50 transition-colors"><X size={14} /> Отмена</button>
      </div>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

export default function ShopAdminPage() {
  const qc = useQueryClient();
  const [bannerEdit, setBannerEdit] = useState<string | "new" | null>(null);
  const [activeTab, setActiveTab] = useState<"banners" | "settings">("banners");

  const { data: banners = [], isLoading: bannersLoading } = useQuery({
    queryKey: ["shop-admin-banners"],
    queryFn: () => apiFetch<ShopBanner[]>("/api/shop/admin/banners"),
  });

  const { data: settings, isLoading: settingsLoading } = useQuery({
    queryKey: ["shop-admin-settings"],
    queryFn: () => apiFetch<ShopSettings>("/api/shop/admin/settings"),
  });

  const [settingsForm, setSettingsForm] = useState<Partial<ShopSettings>>({});

  const saveBannerMut = useMutation({
    mutationFn: (data: Partial<ShopBanner> & { id?: string }) =>
      apiFetch<{ ok: boolean }>(`/api/shop/admin/banners${data.id ? `/${data.id}` : ""}`, {
        method: data.id ? "PUT" : "POST",
        body: JSON.stringify(data),
      }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["shop-admin-banners"] }); setBannerEdit(null); },
  });

  const deleteBannerMut = useMutation({
    mutationFn: (id: string) => apiFetch<{ ok: boolean }>(`/api/shop/admin/banners/${id}`, { method: "DELETE" }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-banners"] }),
  });

  const saveSettingsMut = useMutation({
    mutationFn: (data: Partial<ShopSettings>) => apiFetch<{ ok: boolean; settings: ShopSettings }>("/api/shop/admin/settings", {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["shop-admin-settings"] }),
  });

  const effectiveSettings = { ...settings, ...settingsForm };

  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Magic Vibes — Управление магазином</h1>
          <p className="text-sm text-gray-500 mt-0.5">Баннеры, настройки и коэффициент наценки</p>
        </div>
        <a
          href={SHOP_URL}
          target="_blank"
          rel="noreferrer"
          className="flex items-center gap-1.5 text-sm text-violet-600 hover:text-violet-800 transition-colors"
        >
          <ExternalLink size={15} /> Открыть магазин
        </a>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 bg-gray-100 p-1 rounded-xl w-fit">
        {(["banners", "settings"] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${activeTab === tab ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"}`}
          >
            {tab === "banners" ? "Баннеры" : "Настройки"}
          </button>
        ))}
      </div>

      {/* Banners tab */}
      {activeTab === "banners" && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-semibold text-gray-800">Баннеры главной страницы</h2>
            <button
              onClick={() => setBannerEdit("new")}
              className="flex items-center gap-1.5 bg-violet-600 text-white px-4 py-2 rounded-xl text-sm font-medium hover:bg-violet-700 transition-colors"
            >
              <Plus size={16} /> Добавить баннер
            </button>
          </div>

          {bannerEdit === "new" && (
            <BannerForm onSave={(data) => saveBannerMut.mutate(data)} onCancel={() => setBannerEdit(null)} />
          )}

          {bannersLoading ? (
            <div className="flex justify-center py-8"><Loader2 className="animate-spin text-violet-600" size={24} /></div>
          ) : banners.length === 0 ? (
            <div className="text-center py-10 text-gray-400 border-2 border-dashed border-gray-200 rounded-xl">
              <ImageIcon size={32} className="mx-auto mb-2 text-gray-300" />
              <p className="text-sm">Баннеры не добавлены. Без баннеров на главной показывается градиентный фон.</p>
            </div>
          ) : (
            <div className="space-y-2">
              {banners.map((b) => (
                <div key={b.id}>
                  <BannerRow
                    banner={b}
                    onEdit={() => setBannerEdit(bannerEdit === b.id ? null : b.id)}
                    onDelete={() => { if (confirm("Удалить баннер?")) deleteBannerMut.mutate(b.id); }}
                  />
                  {bannerEdit === b.id && (
                    <div className="mt-2">
                      <BannerForm
                        banner={b}
                        onSave={(data) => saveBannerMut.mutate({ ...data, id: b.id })}
                        onCancel={() => setBannerEdit(null)}
                      />
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Settings tab */}
      {activeTab === "settings" && (
        <div className="space-y-4">
          <h2 className="font-semibold text-gray-800">Настройки магазина</h2>
          {settingsLoading ? (
            <div className="flex justify-center py-8"><Loader2 className="animate-spin text-violet-600" size={24} /></div>
          ) : (
            <div className="bg-white border border-gray-100 rounded-xl p-5 space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                {([
                  ["shopName", "Название магазина", "Magic Vibes"],
                  ["shopDescription", "Описание магазина", "Оригинальная парфюмерия..."],
                  ["contactEmail", "Email для связи", "info@magicvibes.ru"],
                  ["contactPhone", "Телефон", "+7 800 123-45-67"],
                ] as const).map(([key, label, placeholder]) => (
                  <div key={key}>
                    <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
                    <input
                      value={(effectiveSettings as Record<string, unknown>)[key] as string ?? ""}
                      onChange={(e) => setSettingsForm((f) => ({ ...f, [key]: e.target.value }))}
                      placeholder={placeholder}
                      className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400"
                    />
                  </div>
                ))}
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">
                    Коэффициент наценки
                  </label>
                  <div className="flex items-center gap-2">
                    <input
                      type="number"
                      step="0.1"
                      min="0.5"
                      max="20"
                      value={effectiveSettings.markup ?? 2.2}
                      onChange={(e) => setSettingsForm((f) => ({ ...f, markup: Number(e.target.value) }))}
                      className="w-32 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400"
                    />
                    <span className="text-xs text-gray-500">
                      USD × курс × <strong>{effectiveSettings.markup ?? 2.2}</strong>
                    </span>
                  </div>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Бесплатная доставка от (₽)</label>
                  <input
                    type="number"
                    value={effectiveSettings.freeDeliveryFrom ?? 3000}
                    onChange={(e) => setSettingsForm((f) => ({ ...f, freeDeliveryFrom: Number(e.target.value) }))}
                    className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:border-violet-400"
                  />
                </div>
              </div>

              {saveSettingsMut.isSuccess && (
                <div className="text-sm text-green-600 font-medium">Настройки сохранены!</div>
              )}

              <button
                onClick={() => saveSettingsMut.mutate(settingsForm)}
                disabled={saveSettingsMut.isPending || Object.keys(settingsForm).length === 0}
                className="flex items-center gap-2 bg-violet-600 text-white px-5 py-2.5 rounded-xl text-sm font-medium hover:bg-violet-700 disabled:opacity-50 transition-colors"
              >
                {saveSettingsMut.isPending ? <Loader2 size={16} className="animate-spin" /> : <Save size={16} />}
                Сохранить настройки
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
