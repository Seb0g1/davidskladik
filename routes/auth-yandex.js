"use strict";

const crypto = require("crypto");

const OAUTH_STATE_TTL_MS = 10 * 60 * 1000;
const pendingOAuthStates = new Map();

function cleanExpiredStates() {
  const now = Date.now();
  for (const [key, createdAt] of pendingOAuthStates.entries()) {
    if (now - createdAt > OAUTH_STATE_TTL_MS) pendingOAuthStates.delete(key);
  }
}

async function yandexTokenExchange(code, clientId, clientSecret, redirectUri) {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    client_id: clientId,
    client_secret: clientSecret,
    redirect_uri: redirectUri,
  }).toString();

  const response = await fetch("https://oauth.yandex.ru/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const data = await response.json();
  if (!response.ok || !data.access_token) {
    throw new Error(data.error_description || data.error || "Yandex token exchange failed");
  }
  return data.access_token;
}

async function fetchYandexProfile(accessToken) {
  const response = await fetch("https://login.yandex.ru/info?format=json", {
    headers: { "Authorization": `OAuth ${accessToken}` },
  });
  const data = await response.json();
  if (!response.ok || !data.id) {
    throw new Error("Failed to fetch Yandex profile");
  }
  return data;
}

function buildAvatarUrl(profile) {
  if (!profile.default_avatar_id || profile.is_avatar_empty) return null;
  return `https://avatars.yandex.net/get-yapic/${profile.default_avatar_id}/islands-200`;
}

function registerYandexAuthRoutes(app, deps) {
  const {
    loginLimiter,
    configuredUsersAsync,
    createSessionToken,
    sessionCookieName,
    sessionTtlMs,
    isSecureSessionCookie,
    getOrCreateYandexUser,
    log,
  } = deps;

  const clientId = process.env.YANDEX_OAUTH_CLIENT_ID;
  const clientSecret = process.env.YANDEX_OAUTH_CLIENT_SECRET;
  const redirectUri = process.env.YANDEX_OAUTH_REDIRECT_URI || "https://magicvibes.ru/";

  if (!clientId || !clientSecret) {
    if (log) log.warn("Yandex OAuth disabled: set YANDEX_OAUTH_CLIENT_ID and YANDEX_OAUTH_CLIENT_SECRET");
    return;
  }

  // Returns the Yandex authorization URL — frontend redirects user there
  app.get("/api/auth/yandex/start", (_request, response) => {
    cleanExpiredStates();
    const state = crypto.randomBytes(20).toString("hex");
    pendingOAuthStates.set(state, Date.now());

    const url = new URL("https://oauth.yandex.ru/authorize");
    url.searchParams.set("response_type", "code");
    url.searchParams.set("client_id", clientId);
    url.searchParams.set("redirect_uri", redirectUri);
    url.searchParams.set("state", state);
    url.searchParams.set("force_confirm", "no");

    response.json({ url: url.toString() });
  });

  // Exchanges authorization code for a session cookie
  app.post("/api/auth/yandex/callback", loginLimiter, async (request, response, next) => {
    const { code, state } = request.body || {};

    if (!code || !state) {
      return response.status(400).json({ error: "Отсутствует code или state" });
    }

    cleanExpiredStates();
    if (!pendingOAuthStates.has(state)) {
      return response.status(400).json({ error: "Недействительный state. Попробуйте войти заново." });
    }
    pendingOAuthStates.delete(state);

    try {
      const accessToken = await yandexTokenExchange(code, clientId, clientSecret, redirectUri);
      const profile = await fetchYandexProfile(accessToken);

      const yandexId = String(profile.id);
      const username = `yandex:${yandexId}`;
      const displayName = profile.real_name || profile.display_name || profile.login || username;
      const avatarUrl = buildAvatarUrl(profile);

      const user = await getOrCreateYandexUser(username, { configuredUsersAsync });
      if (!user) {
        return response.status(403).json({ error: "Доступ запрещён." });
      }
      if (user.disabled) {
        return response.status(403).json({ error: "Аккаунт отключён. Обратитесь к администратору." });
      }

      const token = createSessionToken({ username, role: user.role || "manager", displayName, avatarUrl });
      const secure = isSecureSessionCookie();

      response.cookie(sessionCookieName, token, {
        httpOnly: true,
        sameSite: "lax",
        secure,
        maxAge: sessionTtlMs,
        path: "/",
      });

      return response.json({ ok: true, username, role: user.role || "manager", displayName, avatarUrl });
    } catch (error) {
      if (log) log.warn("Yandex OAuth callback error", { detail: error?.message || String(error) });
      return next(error);
    }
  });
}

module.exports = { registerYandexAuthRoutes };
