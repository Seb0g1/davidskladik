const form = document.querySelector("#loginForm");
const errorBox = document.querySelector("#loginError");

fetch("/api/session")
  .then((response) => response.json())
  .then((session) => {
    if (session.authenticated) window.location.href = "/";
  })
  .catch(() => {});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  errorBox.textContent = "";
  const formData = new FormData(form);

  const response = await fetch("/api/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: formData.get("username"),
      password: formData.get("password"),
    }),
  });

  if (response.ok) {
    window.location.href = "/";
    return;
  }

  const data = await response.json().catch(() => ({}));
  errorBox.textContent = data.error || "Не удалось войти";
});
