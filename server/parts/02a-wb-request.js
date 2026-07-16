// Клиент API Wildberries. Один токен продавца (Authorization) работает на всех
// хостах; хост выбирается по разделу API. Ретраи на 429/5xx как у Яндекса.
const WB_API_HOSTS = {
  common: "https://common-api.wildberries.ru",
  content: "https://content-api.wildberries.ru",
  prices: "https://discounts-prices-api.wildberries.ru",
  marketplace: "https://marketplace-api.wildberries.ru",
  statistics: "https://statistics-api.wildberries.ru",
};

async function wbRequest(account, host, method, pathname, body) {
  if (!account?.apiKey) {
    const error = new Error("Не задан API-токен Wildberries: добавьте кабинет WB в настройках маркетплейсов.");
    error.statusCode = 400;
    throw error;
  }
  const baseUrl = WB_API_HOSTS[host];
  if (!baseUrl) throw new Error(`Неизвестный хост WB API: ${host}`);

  const attempts = Math.max(1, Number(process.env.WB_REQUEST_MAX_ATTEMPTS || 3) || 3);
  const timeoutMs = Math.max(1000, Number(process.env.WB_REQUEST_TIMEOUT_MS || 30000) || 30000);
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(`${baseUrl}${pathname}`, {
        method,
        headers: {
          Authorization: account.apiKey,
          "Content-Type": "application/json",
        },
        body: body !== undefined && body !== null ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      clearTimeout(timer);

      const text = await response.text();
      const data = text ? parseApiResponse(text) : {};

      if (!response.ok) {
        const error = new Error(summarizeApiErrorPayload(data, `Wildberries API error ${response.status}`));
        error.statusCode = response.status;
        error.wb = data;
        // Token Bucket WB: X-Ratelimit-Retry — сколько секунд ждать до
        // следующей попытки. Игнорировать его = кормить штраф лимитера.
        error.retryAfterSec = Number(response.headers.get("x-ratelimit-retry") || response.headers.get("retry-after") || 0) || 0;
        // 429 у WB — жёсткие поминутные лимиты, ждём дольше обычного.
        if (![429, 500, 502, 503, 504].includes(response.status) || attempt >= attempts) throw error;
        lastError = error;
      } else {
        return data;
      }
    } catch (error) {
      clearTimeout(timer);
      lastError = error?.name === "AbortError" ? new Error(`Wildberries API timeout ${Math.round(timeoutMs / 1000)}s`) : error;
      if (attempt >= attempts) throw lastError;
    }

    const retryAfterMs = Number(lastError?.retryAfterSec) > 0 ? Math.min(180000, lastError.retryAfterSec * 1000 + 500) : 0;
    const delayMs = lastError?.statusCode === 429
      ? Math.max(retryAfterMs, Math.min(30000, 5000 * attempt))
      : Math.min(5000, 500 * attempt * attempt);
    await sleep(delayMs);
  }

  throw lastError || new Error("Wildberries API request failed");
}
