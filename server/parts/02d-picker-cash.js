function pickerCashDateKey(value) {
  if (!value) return new Date().toISOString().slice(0, 10);
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  return new Date().toISOString().slice(0, 10);
}

async function loadPickerCash(dateStr) {
  try {
    const setting = await getPrisma().appSetting.findUnique({ where: { key: `picker_cash:${dateStr}` } });
    const raw = setting?.value;
    return { advances: Array.isArray(raw?.advances) ? raw.advances : [] };
  } catch {
    return { advances: [] };
  }
}

async function savePickerCash(dateStr, data) {
  const key = `picker_cash:${dateStr}`;
  return getPrisma().appSetting.upsert({
    where: { key },
    create: { key, value: data },
    update: { value: data },
  });
}

async function pickerSpentOnDate(dateStr) {
  if (!shouldUsePostgresStorage()) return { spent: 0, count: 0 };
  const start = new Date(`${dateStr}T00:00:00.000Z`);
  const end = new Date(`${dateStr}T23:59:59.999Z`);
  try {
    const entries = await getPrisma().supplierLedgerEntry.findMany({
      where: { entryType: "purchase_debt", status: "active", occurredAt: { gte: start, lte: end } },
      select: { amount: true },
    });
    const spent = entries.reduce((sum, e) => sum + Math.abs(normalizeFinanceMoney(e.amount, 0)), 0);
    return { spent: Math.round(spent), count: entries.length };
  } catch (error) {
    logger.warn("picker cash spent query failed", { detail: error?.message || String(error) });
    return { spent: 0, count: 0 };
  }
}

function pickerCashResponseBody(dateStr, cash, spent) {
  const totalIssued = cash.advances.reduce((sum, a) => sum + Number(a.amount || 0), 0);
  return {
    ok: true,
    date: dateStr,
    advances: cash.advances,
    totalIssued: Math.round(totalIssued),
    spent: spent.spent,
    spentCount: spent.count,
    remaining: Math.round(totalIssued - spent.spent),
  };
}

app.get("/api/picker-cash", requireStaff, async (request, response, next) => {
  try {
    const dateStr = pickerCashDateKey(cleanText(request.query.date || ""));
    const [cash, spent] = await Promise.all([loadPickerCash(dateStr), pickerSpentOnDate(dateStr)]);
    response.json(pickerCashResponseBody(dateStr, cash, spent));
  } catch (error) {
    next(error);
  }
});

app.post("/api/picker-cash", requireAdmin, async (request, response, next) => {
  try {
    const dateStr = pickerCashDateKey(cleanText(request.body?.date || ""));
    const amount = normalizeFinanceMoney(request.body?.amount, 0);
    if (!(amount > 0)) return response.status(400).json({ error: "Укажите сумму выдачи (больше нуля)." });
    const cash = await loadPickerCash(dateStr);
    cash.advances.push({
      id: crypto.randomUUID(),
      amount,
      note: cleanText(request.body?.note || ""),
      createdAt: new Date().toISOString(),
      createdBy: requestUsername(request),
    });
    await savePickerCash(dateStr, cash);
    const spent = await pickerSpentOnDate(dateStr);
    response.json(pickerCashResponseBody(dateStr, cash, spent));
  } catch (error) {
    next(error);
  }
});

app.delete("/api/picker-cash/:id", requireAdmin, async (request, response, next) => {
  try {
    const dateStr = pickerCashDateKey(cleanText(request.query.date || ""));
    const cash = await loadPickerCash(dateStr);
    cash.advances = cash.advances.filter((a) => String(a.id) !== request.params.id);
    await savePickerCash(dateStr, cash);
    const spent = await pickerSpentOnDate(dateStr);
    response.json(pickerCashResponseBody(dateStr, cash, spent));
  } catch (error) {
    next(error);
  }
});

// ─── Per-picker persistent balances ─────────────────────────────────────────

async function loadPickerBalance(username) {
  try {
    const key = `picker_balance:${cleanText(username)}`;
    const setting = await getPrisma().appSetting.findUnique({ where: { key } });
    const raw = setting?.value;
    return { credits: Array.isArray(raw?.credits) ? raw.credits : [] };
  } catch {
    return { credits: [] };
  }
}

async function savePickerBalance(username, data) {
  const key = `picker_balance:${cleanText(username)}`;
  return getPrisma().appSetting.upsert({
    where: { key },
    create: { key, value: data },
    update: { value: data },
  });
}

function pickerBalanceBody(username, balance) {
  const total = balance.credits.reduce((sum, c) => sum + Number(c.amount || 0), 0);
  return { ok: true, username: cleanText(username), total: Math.round(total), credits: balance.credits };
}

app.get("/api/picker-cash/balance", requireStaff, async (request, response, next) => {
  try {
    const username = requestUsername(request);
    if (!username) return response.status(400).json({ error: "Нет имени пользователя." });
    const balance = await loadPickerBalance(username);
    response.json(pickerBalanceBody(username, balance));
  } catch (error) {
    next(error);
  }
});

app.get("/api/picker-cash/balances", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, balances: [] });
    const settings = await prisma.appSetting.findMany({
      where: { key: { startsWith: "picker_balance:" } },
    });
    const balances = settings.map((s) => {
      const username = String(s.key).replace("picker_balance:", "");
      const raw = s.value;
      const credits = Array.isArray(raw?.credits) ? raw.credits : [];
      const total = credits.reduce((sum, c) => sum + Number(c.amount || 0), 0);
      return { username, total: Math.round(total), credits };
    });
    response.json({ ok: true, balances });
  } catch (error) {
    next(error);
  }
});

app.post("/api/picker-cash/balance", requireAdmin, async (request, response, next) => {
  try {
    const pickerUsername = cleanText(request.body?.pickerUsername || "");
    if (!pickerUsername) return response.status(400).json({ error: "Укажите имя сборщика." });
    const amount = normalizeFinanceMoney(request.body?.amount, 0);
    if (!(amount > 0)) return response.status(400).json({ error: "Укажите сумму больше нуля." });
    const balance = await loadPickerBalance(pickerUsername);
    balance.credits.push({
      id: crypto.randomUUID(),
      amount,
      note: cleanText(request.body?.note || ""),
      createdAt: new Date().toISOString(),
      createdBy: requestUsername(request),
    });
    await savePickerBalance(pickerUsername, balance);
    response.json(pickerBalanceBody(pickerUsername, balance));
  } catch (error) {
    next(error);
  }
});

app.patch("/api/picker-cash/balance/:username/:id", requireAdmin, async (request, response, next) => {
  try {
    const pickerUsername = cleanText(request.params.username);
    if (!pickerUsername) return response.status(400).json({ error: "Укажите имя сборщика." });
    const balance = await loadPickerBalance(pickerUsername);
    const credit = balance.credits.find((c) => String(c.id) === request.params.id);
    if (!credit) return response.status(404).json({ error: "Запись не найдена." });
    if (request.body?.amount !== undefined) {
      const amount = normalizeFinanceMoney(request.body.amount, 0);
      if (!(amount > 0)) return response.status(400).json({ error: "Сумма должна быть больше нуля." });
      credit.amount = amount;
    }
    if (request.body?.note !== undefined) credit.note = cleanText(request.body.note || "");
    credit.updatedAt = new Date().toISOString();
    credit.updatedBy = requestUsername(request);
    await savePickerBalance(pickerUsername, balance);
    response.json(pickerBalanceBody(pickerUsername, balance));
  } catch (error) {
    next(error);
  }
});

app.delete("/api/picker-cash/balance/:username/:id", requireAdmin, async (request, response, next) => {
  try {
    const pickerUsername = cleanText(request.params.username);
    if (!pickerUsername) return response.status(400).json({ error: "Укажите имя сборщика." });
    const balance = await loadPickerBalance(pickerUsername);
    balance.credits = balance.credits.filter((c) => String(c.id) !== request.params.id);
    await savePickerBalance(pickerUsername, balance);
    response.json(pickerBalanceBody(pickerUsername, balance));
  } catch (error) {
    next(error);
  }
});
