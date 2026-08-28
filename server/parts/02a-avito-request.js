// Avito API: OAuth2 client_credentials token + generic request wrapper.
// Токен живёт ~24 часа, кэшируется по clientId и обновляется заранее.
const avitoTokenCache = new Map();

async function getAvitoAccessToken(account, { forceRefresh = false } = {}) {
  const clientId = cleanText(account?.clientId);
  const clientSecret = cleanText(account?.apiKey);
  if (!clientId || !clientSecret) {
    const error = new Error("Добавьте Client ID и Client Secret Avito в настройках кабинетов или в .env.");
    error.statusCode = 400;
    throw error;
  }

  const cached = avitoTokenCache.get(clientId);
  if (!forceRefresh && cached && cached.expiresAt > Date.now() + 60_000) return cached.token;

  const response = await fetch(`${avitoBaseUrl}/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
    }).toString(),
    signal: AbortSignal.timeout(Math.max(5000, Number(process.env.AVITO_TOKEN_TIMEOUT_MS || 20000) || 20000)),
  });

  const text = await response.text();
  const data = parseApiResponse(text);
  if (!response.ok || !data.access_token) {
    const message = data.error_description || data.error || data.message || `Avito token error ${response.status}`;
    const error = new Error(message);
    error.statusCode = response.status === 200 ? 502 : response.status;
    error.avito = data;
    avitoTokenCache.delete(clientId);
    throw error;
  }

  const expiresInMs = Math.max(60, Number(data.expires_in || 86400) || 86400) * 1000;
  avitoTokenCache.set(clientId, { token: data.access_token, expiresAt: Date.now() + expiresInMs });
  return data.access_token;
}

function buildAvitoQueryString(query) {
  if (!query || typeof query !== "object") return "";
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

function isAvitoRetryableStatus(status) {
  return [429, 500, 502, 503, 504].includes(Number(status));
}

async function avitoRequest(pathname, { method = "GET", query = null, body = undefined, headers = {}, account = null } = {}) {
  const selectedAccount = account || getAvitoAccountByTarget("avito");
  if (!selectedAccount) {
    const error = new Error("Кабинет Avito не настроен. Добавьте Client ID и Client Secret.");
    error.statusCode = 400;
    throw error;
  }

  const maxAttempts = Math.max(1, Number(process.env.AVITO_REQUEST_MAX_ATTEMPTS || 3) || 3);
  const timeoutMs = Math.max(5000, Number(process.env.AVITO_REQUEST_TIMEOUT_MS || 30000) || 30000);
  const url = `${avitoBaseUrl}${pathname}${buildAvitoQueryString(query)}`;
  let tokenRefreshed = false;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const token = await getAvitoAccessToken(selectedAccount, { forceRefresh: tokenRefreshed });
      const response = await fetch(url, {
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
          ...headers,
        },
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(timeoutMs),
      });

      const text = await response.text();
      const data = parseApiResponse(text);

      if (response.status === 401 && !tokenRefreshed) {
        // Токен мог быть отозван раньше expires_in — обновляем один раз и повторяем.
        tokenRefreshed = true;
        continue;
      }

      if (!response.ok) {
        const message = data.error?.message || data.message || data.error_description
          || (typeof data.error === "string" ? data.error : "") || `Avito API error ${response.status}`;
        const error = new Error(message);
        error.statusCode = response.status;
        error.avito = data;
        if (!isAvitoRetryableStatus(response.status) || attempt >= maxAttempts) throw error;
        lastError = error;
      } else {
        return data;
      }
    } catch (error) {
      if (error?.statusCode && !isAvitoRetryableStatus(error.statusCode)) throw error;
      lastError = error?.name === "TimeoutError" || error?.name === "AbortError"
        ? new Error(`Avito API timeout ${Math.round(timeoutMs / 1000)}s (${pathname})`)
        : error;
      if (attempt >= maxAttempts) throw lastError;
    }

    const delayMs = Math.min(10_000, 700 * attempt * attempt);
    await sleep(delayMs);
  }

  throw lastError || new Error("Avito API request failed");
}
