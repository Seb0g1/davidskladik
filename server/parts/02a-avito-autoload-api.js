// Полный набор методов Avito Автозагрузки (https://api.avito.ru/autoload/*).
// Актуальные методы: профиль v2, запуск загрузки, дерево категорий, ID-маппинги,
// загрузки v4. Отчёты v2/v3 помечены deprecated (Avito удалит их 08.03.2027),
// но оставлены для совместимости до перехода на v4.

function joinAvitoIdsQuery(ids) {
  const list = Array.isArray(ids) ? ids : splitList(ids);
  return list.map((value) => cleanText(value)).filter(Boolean).join("|");
}

// --- Профиль автозагрузки ---

async function getAvitoAutoloadProfile(account) {
  return avitoRequest("/autoload/v2/profile", { account });
}

// payload: { agreement?, autoload_enabled, feeds_data: [{feed_name, feed_url}], report_email, schedule: [{rate, time_slots, weekdays}] }
async function saveAvitoAutoloadProfile(account, payload) {
  return avitoRequest("/autoload/v2/profile", { method: "POST", body: payload || {}, account });
}

/** @deprecated Используйте getAvitoAutoloadProfile (v2). */
async function getAvitoAutoloadProfileV1(account) {
  return avitoRequest("/autoload/v1/profile", { account });
}

/** @deprecated Используйте saveAvitoAutoloadProfile (v2). */
async function saveAvitoAutoloadProfileV1(account, payload) {
  return avitoRequest("/autoload/v1/profile", { method: "POST", body: payload || {}, account });
}

// --- Запуск загрузки ---

// Запускает выгрузку из файла по ссылке из настроек профиля. Не чаще раза в час.
// Лимиты публикаций из настроек автозагрузки на такие загрузки не действуют.
async function triggerAvitoAutoloadUpload(account) {
  return avitoRequest("/autoload/v1/upload", { method: "POST", account });
}

// --- Справочники категорий ---

async function getAvitoCategoriesTree(account, { ifModifiedSince = "" } = {}) {
  return avitoRequest("/autoload/v1/user-docs/tree", {
    account,
    headers: ifModifiedSince ? { "If-Modified-Since": ifModifiedSince } : {},
  });
}

async function getAvitoCategoryFields(account, nodeSlug, { ifModifiedSince = "" } = {}) {
  const slug = cleanText(nodeSlug);
  if (!slug) {
    const error = new Error("node_slug категории Avito обязателен.");
    error.statusCode = 400;
    throw error;
  }
  return avitoRequest(`/autoload/v1/user-docs/node/${encodeURIComponent(slug)}/fields`, {
    account,
    headers: ifModifiedSince ? { "If-Modified-Since": ifModifiedSince } : {},
  });
}

async function getAvitoCategoryFieldValues(account, valuesLinkJson) {
  const url = cleanText(valuesLinkJson);
  if (!url || !url.startsWith("https://api.avito.ru/")) {
    const error = new Error("Неверный URL значений поля Avito.");
    error.statusCode = 400;
    throw error;
  }
  const pathname = url.slice("https://api.avito.ru".length);
  return avitoRequest(pathname, { account });
}

// --- Соответствие ID объявлений ---

// По номерам объявлений на Авито возвращает Id объявлений из файла автозагрузки.
async function getAvitoAdIdsByAvitoIds(account, avitoIds) {
  return avitoRequest("/autoload/v2/items/ad_ids", { account, query: { query: joinAvitoIdsQuery(avitoIds) } });
}

// По Id объявлений из файла автозагрузки возвращает номера объявлений на Авито.
async function getAvitoIdsByAdIds(account, adIds) {
  return avitoRequest("/autoload/v2/items/avito_ids", { account, query: { query: joinAvitoIdsQuery(adIds) } });
}

// --- Загрузки v4 (актуальные методы) ---

async function getAvitoUploads(account, { perPage = 10, page = 1, dateFrom = "", dateTo = "" } = {}) {
  return avitoRequest("/autoload/v4/uploads", {
    account,
    query: { perPage, page, dateFrom, dateTo },
  });
}

async function getAvitoCurrentUpload(account) {
  return avitoRequest("/autoload/v4/uploads/current", { account });
}

async function getAvitoLastSuccessfulUpload(account) {
  return avitoRequest("/autoload/v4/uploads/last_successful", { account });
}

async function getAvitoCurrentUploadItems(account, { query = "", sections = "", perPage = 20, page = 1 } = {}) {
  return avitoRequest("/autoload/v4/uploads/current/items", {
    account,
    query: { query: joinAvitoIdsQuery(query), sections: cleanText(sections), perPage, page },
  });
}

async function getAvitoLastSuccessfulUploadItems(account, { query = "", sections = "", perPage = 20, page = 1 } = {}) {
  return avitoRequest("/autoload/v4/uploads/last_successful/items", {
    account,
    query: { query: joinAvitoIdsQuery(query), sections: cleanText(sections), perPage, page },
  });
}

// --- Отчёты v2/v3 (deprecated, удаляются Avito 08.03.2027) ---

/** @deprecated Используйте getAvitoUploads (v4). */
async function getAvitoReportsV2(account, { perPage = 50, page = 0, dateFrom = "", dateTo = "" } = {}) {
  return avitoRequest("/autoload/v2/reports", {
    account,
    query: { per_page: perPage, page, date_from: dateFrom, date_to: dateTo },
  });
}

/** @deprecated Используйте getAvitoUploads (v4). */
async function getAvitoReportByIdV3(account, reportId) {
  return avitoRequest(`/autoload/v3/reports/${encodeURIComponent(cleanText(reportId))}`, { account });
}

/** @deprecated Используйте getAvitoLastSuccessfulUpload (v4). */
async function getAvitoLastCompletedReportV3(account) {
  return avitoRequest("/autoload/v3/reports/last_completed_report", { account });
}

/** @deprecated Используйте getAvitoLastSuccessfulUploadItems (v4). */
async function getAvitoAutoloadItemsInfoV2(account, adIds) {
  return avitoRequest("/autoload/v2/reports/items", { account, query: { query: joinAvitoIdsQuery(adIds) } });
}

/** @deprecated Используйте getAvitoLastSuccessfulUploadItems (v4). */
async function getAvitoReportItemsById(account, reportId, { query = "", sections = "", perPage = 50, page = 0 } = {}) {
  return avitoRequest(`/autoload/v2/reports/${encodeURIComponent(cleanText(reportId))}/items`, {
    account,
    query: { query: joinAvitoIdsQuery(query), sections: cleanText(sections), per_page: perPage, page },
  });
}

/** @deprecated Без замены: история операций доступна через кошелёк пользователя. */
async function getAvitoReportItemsFeesById(account, reportId) {
  return avitoRequest(`/autoload/v2/reports/${encodeURIComponent(cleanText(reportId))}/items/fees`, { account });
}
