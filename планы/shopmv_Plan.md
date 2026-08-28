# ShopAdminPage — Plan

## Current State

Страница управления магазином magicvibes.ru с табами: Обзор, Заказы, Покупатели, Баннеры, Категории, Новости, Отзывы, Настройки. Отдельный `apiFetch` вместо общего `fetchJson` из `api.ts`. Нет Zod-валидации ответов — все типы кастовые интерфейсы.

---

## Bugs / Issues Found

**1. `apiFetch` не проверяет Content-Type ответа (строка 45–49)**

```ts
async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...init });
  if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
  return res.json() as Promise<T>;
}
```
При ошибке сервер может вернуть HTML (nginx 502, например). `res.json()` выбросит SyntaxError, который `catch(() => ({}))` поглотит и вернёт пустой объект, а `{}.error = undefined` даст `res.statusText` — что правильно. Но при `res.ok = true` с HTML-ответом (CDN cache) `res.json()` выбросит необработанный `SyntaxError`, который улетит в UI как пустое состояние. Нет защитного `catch`.

**2. `SettingsTab` — `sortedRules` используется для рендеринга, но `updateRule`/`removeRule` работают с оригинальными индексами `rules` (строки 582, 651–672)**

```ts
const sortedRules = [...rules].sort((a, b) => a.minUsd - b.minUsd);
...
const origIdx = rules.indexOf(rule);
```
`rules.indexOf(rule)` ищет по ссылке объекта. После `sort` элементы `sortedRules` — это те же объекты (`sort` не клонирует), поэтому `indexOf` найдёт правильный индекс. Но если два правила имеют одинаковый `minUsd` и `coefficient`, `indexOf` вернёт первый совпавший индекс — не тот элемент, который редактируют. Баг воспроизводится при дублирующихся правилах.

**3. `BannerForm` — нет валидации `imageUrl` перед сохранением**

Поле `imageUrl` — просто текстовый input. Пользователь может ввести пустой URL или невалидную строку. Бэкенд (`/api/shop/admin/banners POST`) сохраняет без проверки (строка 1180–1188 в `02d-shop-api-routes.js`): `imageUrl: cleanText(request.body.imageUrl || "")`. Баннер с пустым imageUrl появится в публичном API и не отобразится.

**4. `OrdersTab` — при смене statusFilter страница не сбрасывается в 1 (строка 153–160)**

```tsx
const [page, setPage] = useState(1);
const [statusFilter, setStatusFilter] = useState("");
```
Смена `statusFilter` (строка 180): `setStatusFilter(s); setPage(1)` — это правильно. Однако при изменении только `statusFilter` без явного `setPage(1)` (например, если фильтр меняется программно) пагинация не сбросится. В текущем коде фильтр меняется только по клику с явным `setPage(1)` — пока ок, но хрупкий паттерн.

**5. `NewsTab` — `importMut.onSuccess` использует `setTimeout` с `void refetch()` (строка 714)**

```tsx
onSuccess: () => setTimeout(() => void refetch(), 2000),
```
`setTimeout` вызывается в `onSuccess` без очистки. Если компонент `NewsTab` размонтируется до истечения 2 секунд (пользователь переключил таб), `refetch()` выполнится на уже несмонтированном компоненте. TanStack Query обработает это корректно (нет обновления state), но это плохой паттерн — лучше использовать `queryClient.invalidateQueries`.

**6. `ReviewsTab` — нет пагинации (строки 816–856 в `02f-shop-reviews.js`)**

Бэкенд `GET /api/shop/admin/reviews` возвращает `take: 200` без пагинации. UI показывает все отзывы в одном списке. При росте числа отзывов страница станет тяжёлой.

**7. Бэкенд: `GET /api/shop/admin/reviews` использует `requireStaff`, а `PATCH` использует `requireAdmin` (строки 79 и 92 в `02f-shop-reviews.js`)**

Менеджер (`requireStaff`) может просматривать все отзывы (включая неодобренные с email покупателя), но не может их одобрять или удалять. Это может быть намеренным, но выглядит непоследовательно: менеджер видит скрытые отзывы, но не может ничего с ними сделать.

**8. Бэкенд: `writeShopData` при каждом сохранении читает и перезаписывает весь `appSettings` (строки 148–151 в `02d-shop-api-routes.js`)**

```js
async function writeShopData(key, value) {
  const appSettings = await readAppSettings();
  await writeAppSettings({ ...appSettings, [key]: value });
}
```
Баннеры, категории и настройки магазина хранятся внутри общего `AppSetting` в PostgreSQL. Каждое изменение баннера делает полный read-write всей структуры настроек. При параллельных запросах (маловероятно, но возможно) — race condition: второй запрос перезапишет изменения первого.

**9. `DashboardTab` — queryKey заказов пересекается с `OrdersTab` (строки 99–101)**

```tsx
queryKey: ["shop-admin-orders", { page: 1, statusFilter: "" }],
queryFn: () => apiFetch<{ orders: ShopOrder[] }>("/api/shop/admin/orders?pageSize=5"),
```
DashboardTab использует `pageSize=5`, но ключ кэша `["shop-admin-orders", { page: 1, statusFilter: "" }]` совпадает с тем, что OrdersTab использует для `page=1, statusFilter=""` с `pageSize=20`. Если TanStack Query возвращает данные из кэша, Dashboard может получить 20 заказов вместо 5, или OrdersTab получит 5 строк вместо 20.

**10. Бэкенд `signShopToken` / `verifyShopToken`: отсутствует проверка срока действия токена (строки 29–47 в `02d-shop-api-routes.js`)**

```js
const b = Buffer.from(JSON.stringify({ ...payload, iat: Date.now() })).toString("base64url");
```
Поле `iat` записывается, но в `verifyShopToken` проверяется только подпись, а не `iat`. Токен покупателя действует вечно — до тех пор, пока не изменится `APP_SESSION_SECRET`. Это соответствует магазинной практике «запомнить меня», но является security issue при компрометации токена.

---

## Improvement Ideas

- **[HIGH]** Добавить обёртку-try/catch вокруг `res.json()` в `apiFetch` при `res.ok = true`.
- **[HIGH]** Исправить дублирующийся queryKey в DashboardTab — добавить `pageSize: 5` в ключ.
- **[HIGH]** Добавить срок действия токена покупателя (`exp: Date.now() + 30 * 24 * 60 * 60 * 1000`) и проверку в `verifyShopToken`.
- **[MEDIUM]** Заменить `rules.indexOf(rule)` на поиск по уникальному полю (добавить `id` к каждому правилу при создании) в `SettingsTab`.
- **[MEDIUM]** Добавить пагинацию в `ReviewsTab` и на бэкенде.
- **[MEDIUM]** Перевести `writeShopData` на атомарный upsert только конкретного ключа в PostgreSQL, избегая полного read-write `appSettings`.
- **[MEDIUM]** Добавить валидацию `imageUrl` в `BannerForm` (проверка на непустую строку и начало с `http`).
- **[LOW]** Заменить `setTimeout(() => void refetch(), 2000)` в `NewsTab` на `queryClient.invalidateQueries` с задержкой через TanStack Query `refetchInterval` или дополнительный `useEffect` с cleanup.
- **[LOW]** Рассмотреть повышение `requireStaff` до `requireAdmin` для `GET /api/shop/admin/reviews`, чтобы скрыть email покупателей от менеджеров.

---

## Code Notes

- Frontend: `frontend/src/routes/ShopAdminPage.tsx`
- Backend: `server/parts/02d-shop-api-routes.js`, `server/parts/02f-shop-reviews.js`
