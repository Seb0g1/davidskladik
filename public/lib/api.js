/**
 * Общий клиент API для фронта.
 *
 *   - всегда credentials: same-origin;
 *   - 401 → редирект на /login.html;
 *   - JSON-парсинг и поднятие осмысленной ошибки;
 *   - удобные шорткаты get/post/put/del.
 *
 * Глобально экспортируется как window.api (без модулей, чтобы работать с текущим
 * подходом «тег <script> на странице»).
 */
(function initApiClient(global) {
  function redirectToLogin() {
    if (typeof window !== "undefined" && window.location) {
      window.location.href = "/login.html";
    }
  }

  async function api(path, options = {}) {
    const init = {
      credentials: "same-origin",
      ...options,
    };
    const response = await fetch(path, init);
    if (response.status === 401) {
      redirectToLogin();
      throw new Error("Требуется вход");
    }
    const text = await response.text();
    let payload = {};
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (_e) {
        payload = { raw: text };
      }
    }
    if (!response.ok) {
      const message = payload.error || payload.detail || `Ошибка запроса (${response.status})`;
      const error = new Error(message);
      error.status = response.status;
      error.payload = payload;
      throw error;
    }
    return payload;
  }

  function withJson(method) {
    return (path, body, options = {}) =>
      api(path, {
        method,
        headers: { "Content-Type": "application/json", ...(options.headers || {}) },
        body: body == null ? undefined : JSON.stringify(body),
        ...options,
      });
  }

  global.api = api;
  global.api.get = (path, options = {}) => api(path, { method: "GET", ...options });
  global.api.post = withJson("POST");
  global.api.put = withJson("PUT");
  global.api.patch = withJson("PATCH");
  global.api.del = (path, options = {}) => api(path, { method: "DELETE", ...options });
})(typeof window !== "undefined" ? window : globalThis);
