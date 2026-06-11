async function ozonRequest(pathname, body, account = null) {
  const selectedAccount = account || getOzonAccountByTarget("ozon");
  const clientId = selectedAccount?.clientId;
  const apiKey = selectedAccount?.apiKey;

  if (!clientId || !apiKey) {
    const error = new Error("Добавьте Client-Id и Api-Key Ozon в настройках кабинетов или в .env.");
    error.statusCode = 400;
    throw error;
  }

  return enqueueOzonRequest(async () => {
    const maxAttempts = Math.max(1, Number(process.env.OZON_REQUEST_MAX_ATTEMPTS || 4) || 4);
    let lastError = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const response = await fetch(`${ozonBaseUrl}${pathname}`, {
          method: "POST",
          headers: {
            "Client-Id": clientId,
            "Api-Key": apiKey,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(body || {}),
          // Without a timeout one hung request blocks the global serial Ozon queue forever.
          signal: AbortSignal.timeout(Math.max(5000, Number(process.env.OZON_REQUEST_TIMEOUT_MS || 60000) || 60000)),
        });

        const text = await response.text();
        const data = parseApiResponse(text);

        if (!response.ok) {
          const message = data.message || data.error || `Ozon API error ${response.status}`;
          const error = new Error(message);
          error.statusCode = response.status;
          error.ozon = data;
          if (!isOzonRateLimitError(error) || attempt >= maxAttempts) throw error;
          lastError = error;
          await sleep(ozonRetryDelayMs(attempt, response));
          continue;
        }

        return data;
      } catch (error) {
        if (!isOzonRateLimitError(error) || attempt >= maxAttempts) throw error;
        lastError = error;
        await sleep(ozonRetryDelayMs(attempt));
      }
    }
    throw lastError || new Error("Ozon API request failed");
  });
}

async function sendOzonPriceBatch(account, prices) {
  const maxAttempts = getOzonPriceBatchMaxAttempts();
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await ozonRequest("/v1/product/import/prices", { prices }, account);
      const delayMs = getOzonPriceBatchDelayMs();
      if (delayMs > 0) await sleep(delayMs);
      return result;
    } catch (error) {
      lastError = error;
      if (!isOzonRateLimitError(error) && !isOzonResourceExhaustedError(error)) throw error;
      if (isOzonPerItemPriceLimitError(error)) throw error;
      if (attempt >= maxAttempts) throw error;
      const delayMs = Math.min(60_000, getOzonPriceBatchBackoffMs() * attempt * attempt);
      logger.warn("ozon price batch rate limited, retrying", {
        account: account?.id || account?.name || "ozon",
        items: prices.length,
        attempt,
        maxAttempts,
        delayMs,
        detail: error?.message || String(error),
      });
      await sleep(delayMs);
    }
  }
  throw lastError || new Error("Ozon price batch failed");
}

