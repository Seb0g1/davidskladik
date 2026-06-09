async function yandexRequest(shop, method, pathname, body) {
  if (!shop?.apiKey || !shop?.businessId) {
    const error = new Error("Yandex shop apiKey and businessId must be set in .env");
    error.statusCode = 400;
    throw error;
  }

  const attempts = Math.max(1, Number(process.env.YANDEX_REQUEST_MAX_ATTEMPTS || 3) || 3);
  const timeoutMs = Math.max(1000, Number(process.env.YANDEX_REQUEST_TIMEOUT_MS || 20000) || 20000);
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(`${yandexBaseUrl}${pathname}`, {
        method,
        headers: {
          "Api-Key": shop.apiKey,
          "Content-Type": "application/json",
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      clearTimeout(timer);

      const text = await response.text();
      const data = parseApiResponse(text);

      if (!response.ok) {
        const error = new Error(summarizeApiErrorPayload(data, `Yandex Market API error ${response.status}`));
        error.statusCode = response.status;
        error.yandex = data;
        if (![420, 423, 429, 500, 502, 503, 504].includes(response.status) || attempt >= attempts) throw error;
        lastError = error;
      } else {
        return data;
      }
    } catch (error) {
      clearTimeout(timer);
      lastError = error?.name === "AbortError" ? new Error(`Yandex Market API timeout ${Math.round(timeoutMs / 1000)}s`) : error;
      if (attempt >= attempts) throw lastError;
    }

    const delayMs = Math.min(5000, 500 * attempt * attempt);
    await sleep(delayMs);
  }

  throw lastError || new Error("Yandex Market API request failed");
}

