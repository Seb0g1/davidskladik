# Агент: Заглушки в конце фото + Брендинг магазинов

## Задача
Добавить поддержку фото-заглушек в КОНЦЕ галереи товара для 3 магазинов: Magic Stick (Ozon), parfumerius (Yandex Market), AURA (Ozon). Каждый магазин имеет 2-5 своих заглушек.

## КРИТИЧЕСКИ ВАЖНО
**Заглушки добавляются ТОЛЬКО В КОНЕЦ галереи** — после всех основных фото товара. Никогда не заменять и не вставлять перед основными фото.

## Исследование перед разработкой (ОБЯЗАТЕЛЬНО)

### 1. Проверить Ozon API для фото:
Изучи `server/parts/02d-*.js` файлы где работа с Ozon продуктами. Найди как сейчас загружаются/обновляются фото на Ozon.
Ozon API для фото: `POST /v1/product/pictures` — загрузка, `POST /v1/product/pictures/import` — установка.
Важно: Ozon принимает массив `images` и `images360` в product update. **Первое фото = главное**.
Убедись что заглушки добавляются ПОСЛЕ существующих images, а не вместо них.

### 2. Проверить Yandex Market API для фото:
Найди в коде как обновляются товары ЯМ. Yandex Market API: `PUT /campaigns/{campaignId}/offer-mapping-entries` или `PUT /businesses/{businessId}/offer-mappings`.
Поле `pictures` — массив URL. Порядок: первое = главное. Заглушки добавлять последними.

### 3. Прочитать существующий код premium-фото:
Файл `server/parts/02d-warehouse-ai-image-generate-routes.js` — как сейчас реализована отправка фото.
Файл `server/parts/02d-warehouse-ai-image-runners.js` — логика генерации.
Файл `server/parts/02a-ai-image-presets.js` — пресеты.

## Что нужно сделать

### 1. Настройки заглушек (backend)

В `app_settings` (PostgreSQL, ключ "app") добавить поле:
```json
{
  "shopStubs": {
    "ozon": {
      "enabled": true,
      "stubUrls": ["url1", "url2"],
      "position": "end"
    },
    "yandex": {
      "enabled": true,
      "stubUrls": ["url1", "url2"],
      "position": "end"
    },
    "ozon-aura": {
      "enabled": true,
      "stubUrls": ["url1", "url2"],
      "position": "end"
    }
  }
}
```

Роут `GET /api/settings/shop-stubs` — получить текущие заглушки.
Роут `PUT /api/settings/shop-stubs` — обновить заглушки (принимает URLs картинок).

### 2. Функция добавления заглушек

В новом файле `server/parts/02a-shop-stubs.js`:
```js
function appendShopStubsToImages(existingImages, marketplace, accountId, appSettings) {
  // existingImages — массив URL основных фото
  // marketplace — 'ozon' | 'yandex'
  // accountId — 'ozon' | 'ozon-3d10ec43' (AURA)
  // Возвращает новый массив: [...existingImages, ...stubUrls]
  // Если заглушки не настроены или disabled — возвращает existingImages без изменений
  // Никогда не ставить заглушки первыми!
}
```

### 3. Интеграция в процесс синхронизации фото

Найди в коде где происходит отправка/обновление фото на маркетплейсы. Вызвать `appendShopStubsToImages` перед отправкой.

Для Ozon: ищи место где формируется `images: [...]` в запросах к API.
Для Yandex: ищи место где формируется `pictures: [...]`.

### 4. Frontend настройки

В `frontend/src/routes/SettingsPage.tsx` (или BrandingSettings если есть) добавить секцию:

```
📸 Заглушки в конце фотогалереи
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Magic Stick (Ozon основной)     [вкл/выкл]
  [Фото 1] [Фото 2] [+ Добавить] (макс 5)

parfumerius (Yandex Market)     [вкл/выкл]
  [Фото 1] [Фото 2] [+ Добавить] (макс 5)

AURA (Ozon второй аккаунт)      [вкл/выкл]
  [Фото 1] [Фото 2] [+ Добавить] (макс 5)
```

Загрузка фото: через существующий upload endpoint (`/api/upload` или похожий).
Максимум 5 заглушек на магазин (не 2!).

### 5. Безопасность
- При обновлении фото на маркетплейсе: убедиться что заглушки не дублируются (проверять есть ли URL уже в списке)
- Если заглушка недоступна (404) — пропустить её, не добавлять битую ссылку
- Логировать: "appended N stubs to product X for marketplace Y"

## Файлы для изменения
- `server/parts/02a-app-settings.js` — добавить `shopStubs` в `defaultAppSettings()` и `normalizeAppSettings()`
- `server/parts/02a-shop-stubs.js` — НОВЫЙ файл с функцией `appendShopStubsToImages`
- `server/source.js` — зарегистрировать новый файл
- `server/parts/02d-*.js` — добавить роуты для shop-stubs settings
- `frontend/src/routes/SettingsPage.tsx` или BrandingPage — UI секция заглушек
- `frontend/src/api.ts` — API функции для shop-stubs

## Проверка что ты делаешь правильно
Перед отправкой изменений убедись:
1. `console.log` или тест: images[0] — это всегда оригинальное фото товара (НЕ заглушка)
2. images[last] — это заглушка (если включено)
3. Количество images = original_count + stubs_count
4. При отключении заглушек — images = только оригинальные
