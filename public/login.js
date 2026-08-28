const form = document.querySelector("#loginForm");
const errorBox = document.querySelector("#loginError");

fetch("/api/session")
  .then((response) => response.json())
  .then((session) => {
    if (session.authenticated) window.location.href = "/app/dashboard";
  })
  .catch(() => {});

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
