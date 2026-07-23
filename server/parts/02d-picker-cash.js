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
