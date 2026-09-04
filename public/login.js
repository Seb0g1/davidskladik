const form = document.querySelector("#loginForm");
const errorBox = document.querySelector("#loginError");
const yandexBtn = document.querySelector("#yandexLoginBtn");

// Handle Yandex OAuth callback: redirect URI sends code+state to root, which
// gets relayed to /login.html?code=...&state=... by the server
const urlParams = new URLSearchParams(window.location.search);
const oauthCode = urlParams.get("code");
const oauthState = urlParams.get("state");

if (oauthCode && oauthState) {
  window.history.replaceState({}, "", "/login.html");
  handleYandexCallback(oauthCode, oauthState);
} else {
  fetch("/api/session")
    .then((response) => response.json())
    .then((session) => {
      if (session.authenticated) window.location.href = "/app/dashboard";
    })
    .catch(() => {});
}

async function handleYandexCallback(code, state) {
  if (yandexBtn) {
    yandexBtn.disabled = true;
    yandexBtn.textContent = "Входим...";
  }
  errorBox.textContent = "";
  try {
    const response = await fetch("/api/auth/yandex/callback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code, state }),
    });
    if (response.ok) {
      window.location.href = "/app/dashboard";
      return;
    }
    const data = await response.json().catch(() => ({}));
    errorBox.textContent = data.error || "Не удалось войти через Яндекс";
  } catch (_error) {
    errorBox.textContent = "Не удалось подключиться к серверу";
  } finally {
    if (yandexBtn) {
      yandexBtn.disabled = false;
      yandexBtn.textContent = "Войти с Яндекс ID";
    }
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  errorBox.textContent = "";
  const button = form.querySelector("button[type='submit']");
  const previousText = button?.textContent || "Войти";
  if (button) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.textContent = "Входим...";
  }
  const formData = new FormData(form);

  try {
    const response = await fetch("/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: formData.get("username"),
        password: formData.get("password"),
      }),
    });

    if (response.ok) {
      window.location.href = "/app/dashboard";
      return;
    }

    const data = await response.json().catch(() => ({}));
    errorBox.textContent = data.error || "Не удалось войти";
  } catch (_error) {
    errorBox.textContent = "Не удалось подключиться к серверу";
  } finally {
    if (button) {
      button.disabled = false;
      button.classList.remove("is-loading");
      button.textContent = previousText;
    }
  }
});

if (yandexBtn) {
  yandexBtn.addEventListener("click", async () => {
    yandexBtn.disabled = true;
    yandexBtn.textContent = "Переходим к Яндексу...";
    errorBox.textContent = "";
    try {
      const response = await fetch("/api/auth/yandex/start");
      if (!response.ok) throw new Error("server error");
      const { url } = await response.json();
      if (!url) throw new Error("no url");
      window.location.href = url;
    } catch (_error) {
      errorBox.textContent = "Не удалось запустить авторизацию Яндекса";
      yandexBtn.disabled = false;
      yandexBtn.textContent = "Войти с Яндекс ID";
    }
  });
}
