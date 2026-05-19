function registerMarketplaceRoutes(app, deps) {
  const {
    defaultUsdRate,
    defaultOzonMarkup,
    defaultYandexMarkup,
    readAppSettings,
    defaultAppSettings,
    marketplaceTargets,
    getMarketplaceAccounts,
    sanitizeMarketplaceAccount,
    getHiddenMarketplaceAccounts,
    findMarketplaceAccount,
    testMarketplaceAccountConnection,
    logger,
    readMarketplaceAccounts,
    normalizeMarketplaceAccount,
    writeMarketplaceAccounts,
    appendAudit,
    getEnvOzonAccounts,
    getEnvYandexShops,
    accountPayloadWithSecretFallback,
  } = deps;

app.get("/api/marketplaces", (_request, response) => {
  readAppSettings()
    .then((settings) => {
      response.json({
        defaults: {
          usdRate: Number(settings.fixedUsdRate || defaultUsdRate || 95),
          ozonMarkup: Number(settings.defaultMarkups?.ozon || defaultOzonMarkup || 1.7),
          yandexMarkup: Number(settings.defaultMarkups?.yandex || defaultYandexMarkup || 1.6),
        },
        settings,
        targets: marketplaceTargets(),
        accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
        hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      });
    })
    .catch(() => {
      response.json({
        defaults: {
          usdRate: Number(defaultUsdRate || 95),
          ozonMarkup: Number(defaultOzonMarkup || 1.7),
          yandexMarkup: Number(defaultYandexMarkup || 1.6),
        },
        settings: defaultAppSettings(),
        targets: marketplaceTargets(),
        accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
        hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      });
    });
});

app.get("/api/marketplace-accounts", (_request, response) => {
  response.json({
    accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
    hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
    targets: marketplaceTargets(),
  });
});

app.post("/api/marketplace-accounts/:id/test", async (request, response) => {
  const account = findMarketplaceAccount(request.params.id);
  if (!account) return response.status(404).json({ ok: false, error: "Кабинет не найден." });
  try {
    const result = await testMarketplaceAccountConnection(account);
    return response.json({
      ...result,
      id: account.id,
      name: account.name,
    });
  } catch (error) {
    logger.warn("marketplace account test failed", {
      id: account.id,
      marketplace: account.marketplace,
      detail: error?.message || String(error),
    });
    return response.status(error.statusCode || 400).json({
      ok: false,
      id: account.id,
      name: account.name,
      marketplace: account.marketplace,
      error: error?.message || "Не удалось проверить подключение.",
      checkedAt: new Date().toISOString(),
    });
  }
});

app.post("/api/marketplace-accounts", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const input = normalizeMarketplaceAccount(request.body);
    if (!input.name) return response.status(400).json({ error: "Укажите название кабинета." });
    if (input.marketplace === "ozon" && (!input.clientId || !input.apiKey)) {
      return response.status(400).json({ error: "Для Ozon нужны Client-Id и Api-Key." });
    }
    if (input.marketplace === "yandex" && (!input.businessId || !input.apiKey)) {
      return response.status(400).json({ error: "Для Yandex нужны Business ID и Api-Key." });
    }

    const index = localAccounts.findIndex((account) => account.id === input.id);
    if (index >= 0) localAccounts[index] = normalizeMarketplaceAccount(input, localAccounts[index]);
    else localAccounts.push(input);

    await writeMarketplaceAccounts(localAccounts);
    await appendAudit(request, "marketplace_account.save", { id: input.id, marketplace: input.marketplace, name: input.name });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/marketplace-accounts/:id", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const index = localAccounts.findIndex((account) => account.id === request.params.id);
    const envAccount = [...getEnvOzonAccounts(), ...getEnvYandexShops()].find((account) => account.id === request.params.id);
    if (index < 0 && !envAccount) return response.status(404).json({ error: "Кабинет не найден." });

    if (index >= 0) {
      localAccounts[index] = normalizeMarketplaceAccount(
        accountPayloadWithSecretFallback({ ...localAccounts[index], ...request.body, id: request.params.id, hidden: false }, localAccounts[index]),
        localAccounts[index],
      );
    } else {
      localAccounts.push(
        normalizeMarketplaceAccount(
          accountPayloadWithSecretFallback({ ...envAccount, ...request.body, id: request.params.id, hidden: false }, envAccount),
          envAccount,
        ),
      );
    }

    await writeMarketplaceAccounts(localAccounts);
    await appendAudit(request, "marketplace_account.update", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/marketplace-accounts/:id", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const envAccount = [...getEnvOzonAccounts(), ...getEnvYandexShops()].find((account) => account.id === request.params.id);
    const index = localAccounts.findIndex((account) => account.id === request.params.id);
    let nextAccounts = localAccounts.filter((account) => account.id !== request.params.id);
    if (envAccount) {
      nextAccounts.push(normalizeMarketplaceAccount({ ...envAccount, hidden: true }, envAccount));
    } else if (index < 0) {
      return response.status(404).json({ error: "Кабинет не найден." });
    }
    await writeMarketplaceAccounts(nextAccounts);
    await appendAudit(request, envAccount ? "marketplace_account.hide" : "marketplace_account.delete", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplace-accounts/:id/restore", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const nextAccounts = localAccounts.filter((account) => !(account.id === request.params.id && account.hidden));
    if (nextAccounts.length === localAccounts.length) return response.status(404).json({ error: "Скрытый кабинет не найден." });
    await writeMarketplaceAccounts(nextAccounts);
    await appendAudit(request, "marketplace_account.restore", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});
}

module.exports = {
  registerMarketplaceRoutes,
};
