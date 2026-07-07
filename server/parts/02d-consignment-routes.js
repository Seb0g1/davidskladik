const CONSIGNMENT_PAYOUT_KINDS = ["sponsor_payout", "sponsor_profit_payout", "my_profit_payout"];

function consignmentStorageUnavailable(response) {
  if (shouldUsePostgresStorage()) return false;
  response.status(503).json({ error: "Consignment module requires PostgreSQL storage.", code: "consignment_postgres_required" });
  return true;
}

function consignmentItemFromPostgres(row = {}) {
  return {
    id: row.id,
    name: cleanText(row.name),
    article: cleanText(row.article),
    supplierName: cleanText(row.supplierName),
    partnerId: cleanText(row.partnerId),
    purchasePrice: normalizeFinanceMoney(row.purchasePrice, 0),
    salePrice: normalizeFinanceMoney(row.salePrice, 0),
    quantity: Math.max(0, Math.round(Number(row.quantity || 0) || 0)),
    note: cleanText(row.note),
    archived: Boolean(row.archived),
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  };
}

function consignmentOperationFromPostgres(row = {}) {
  return {
    id: row.id,
    sourceKey: row.sourceKey || null,
    itemId: row.itemId || null,
    itemName: cleanText(row.itemName),
    type: cleanText(row.type),
    quantity: Math.max(0, Math.round(Number(row.quantity || 0) || 0)),
    unitPurchase: normalizeFinanceMoney(row.unitPurchase, 0),
    unitSale: normalizeFinanceMoney(row.unitSale, 0),
    balanceDelta: normalizeFinanceMoney(row.balanceDelta, 0),
    sponsorDelta: normalizeFinanceMoney(row.sponsorDelta, 0),
    myDelta: normalizeFinanceMoney(row.myDelta, 0),
    note: cleanText(row.note),
    status: cleanText(row.status || "active") || "active",
    relatedOperationId: row.relatedOperationId || null,
    createdBy: cleanText(row.createdBy),
    createdAt: row.createdAt?.toISOString?.() || null,
  };
}

function consignmentQuantityInput(value) {
  const quantity = Math.round(Number(value || 0) || 0);
  return quantity > 0 ? quantity : 0;
}

function consignmentSummaryFromRows(items = [], operations = []) {
  const activeItems = items.filter((item) => !item.archived);
  const stock = activeItems.reduce((acc, item) => {
    acc.quantity += item.quantity;
    acc.purchaseValue += item.purchasePrice * item.quantity;
    acc.saleValue += item.salePrice * item.quantity;
    return acc;
  }, { quantity: 0, purchaseValue: 0, saleValue: 0 });
  const totals = operations.reduce((acc, op) => {
    acc.balance += op.balanceDelta;
    acc.sponsorProfit += op.sponsorDelta;
    acc.myProfit += op.myDelta;
    if (op.type === "sale") {
      acc.soldQuantity += op.quantity;
      acc.salesRevenue += op.unitSale * op.quantity;
      acc.profitTotal += op.sponsorDelta + op.myDelta;
    }
    if (op.type === "return") {
      acc.returnedQuantity += op.quantity;
      acc.salesRevenue -= op.unitSale * op.quantity;
      acc.profitTotal += op.sponsorDelta + op.myDelta;
    }
    if (op.type === "writeoff") acc.writeoffQuantity += op.quantity;
    if (op.type === "purchase") acc.purchasesFromBalance += Math.abs(op.balanceDelta);
    if (op.type === "sponsor_payout") acc.sponsorPayouts += Math.abs(op.balanceDelta);
    if (op.type === "sponsor_profit_payout") acc.sponsorProfitPaidOut += Math.abs(op.sponsorDelta);
    if (op.type === "my_profit_payout") acc.myProfitPaidOut += Math.abs(op.myDelta);
    return acc;
  }, {
    balance: 0,
    sponsorProfit: 0,
    myProfit: 0,
    soldQuantity: 0,
    returnedQuantity: 0,
    writeoffQuantity: 0,
    salesRevenue: 0,
    profitTotal: 0,
    purchasesFromBalance: 0,
    sponsorPayouts: 0,
    sponsorProfitPaidOut: 0,
    myProfitPaidOut: 0,
  });
  return {
    items: activeItems.length,
    stockQuantity: stock.quantity,
    capitalization: normalizeFinanceMoney(stock.purchaseValue, 0),
    stockSaleValue: normalizeFinanceMoney(stock.saleValue, 0),
    balance: normalizeFinanceMoney(totals.balance, 0),
    sponsorProfit: normalizeFinanceMoney(totals.sponsorProfit, 0),
    myProfit: normalizeFinanceMoney(totals.myProfit, 0),
    soldQuantity: totals.soldQuantity,
    returnedQuantity: totals.returnedQuantity,
    writeoffQuantity: totals.writeoffQuantity,
    salesRevenue: normalizeFinanceMoney(totals.salesRevenue, 0),
    profitTotal: normalizeFinanceMoney(totals.profitTotal, 0),
    purchasesFromBalance: normalizeFinanceMoney(totals.purchasesFromBalance, 0),
    sponsorPayouts: normalizeFinanceMoney(totals.sponsorPayouts, 0),
    sponsorProfitPaidOut: normalizeFinanceMoney(totals.sponsorProfitPaidOut, 0),
    myProfitPaidOut: normalizeFinanceMoney(totals.myProfitPaidOut, 0),
  };
}

async function readConsignmentSummary() {
  const [itemRows, operationRows] = await Promise.all([
    getPrisma().consignmentItem.findMany({ take: 20000 }),
    getPrisma().consignmentOperation.findMany({ take: 50000 }),
  ]);
  return consignmentSummaryFromRows(
    itemRows.map(consignmentItemFromPostgres),
    operationRows.map(consignmentOperationFromPostgres),
  );
}

app.get("/api/consignment/summary", requireAdmin, async (_request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    response.json({ ok: true, summary: await readConsignmentSummary(), updatedAt: new Date().toISOString() });
  } catch (error) {
    next(error);
  }
});

app.get("/api/consignment/items", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const q = cleanText(request.query.q || "");
    const includeArchived = String(request.query.includeArchived || "") === "true";
    const filters = [];
    if (!includeArchived) filters.push({ archived: false });
    if (q) {
      filters.push({
        OR: [
          { name: { contains: q, mode: "insensitive" } },
          { article: { contains: q, mode: "insensitive" } },
          { supplierName: { contains: q, mode: "insensitive" } },
        ],
      });
    }
    const rows = await getPrisma().consignmentItem.findMany({
      where: filters.length ? { AND: filters } : {},
      orderBy: { createdAt: "desc" },
      take: cleanLimit(request.query.limit, 500, 2000),
    });
    response.json({ ok: true, items: rows.map(consignmentItemFromPostgres) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/consignment/items", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const body = request.body || {};
    const name = cleanText(body.name);
    if (!name) return response.status(400).json({ error: "Укажите наименование товара.", code: "consignment_name_required" });
    const purchasePrice = normalizeFinanceMoney(body.purchasePrice, 0);
    const salePrice = normalizeFinanceMoney(body.salePrice, 0);
    if (!(purchasePrice >= 0) || !(salePrice >= 0)) return response.status(400).json({ error: "Цены не могут быть отрицательными.", code: "consignment_price_invalid" });
    const quantity = consignmentQuantityInput(body.quantity);
    const fromBalance = body.fromBalance === true;
    const username = requestUsername(request);
    const result = await getPrisma().$transaction(async (tx) => {
      const item = await tx.consignmentItem.create({
        data: {
          name,
          article: cleanText(body.article) || null,
          supplierName: cleanText(body.supplierName) || null,
          partnerId: cleanText(body.partnerId) || null,
          purchasePrice,
          salePrice,
          quantity,
          note: cleanText(body.note) || null,
        },
      });
      let operation = null;
      if (quantity > 0) {
        operation = await tx.consignmentOperation.create({
          data: {
            itemId: item.id,
            itemName: item.name,
            type: fromBalance ? "purchase" : "receive",
            quantity,
            unitPurchase: purchasePrice,
            unitSale: salePrice,
            balanceDelta: fromBalance ? -normalizeFinanceMoney(purchasePrice * quantity, 0) : 0,
            note: cleanText(body.note) || null,
            createdBy: username || null,
          },
        });
      }
      return { item, operation };
    });
    await appendAudit(request, "consignment.item.create", {
      entityType: "consignment_item",
      entityId: result.item.id,
      newValue: consignmentItemFromPostgres(result.item),
    });
    response.status(201).json({
      ok: true,
      item: consignmentItemFromPostgres(result.item),
      operation: result.operation ? consignmentOperationFromPostgres(result.operation) : null,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/consignment/items/bulk", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const rows = Array.isArray(request.body?.items) ? request.body.items : [];
    if (!rows.length) return response.status(400).json({ error: "Список товаров пуст.", code: "consignment_bulk_empty" });
    if (rows.length > 200) return response.status(400).json({ error: "За один раз можно загрузить не более 200 товаров.", code: "consignment_bulk_too_many" });
    const prepared = [];
    for (let index = 0; index < rows.length; index += 1) {
      const body = rows[index] || {};
      const name = cleanText(body.name);
      if (!name) return response.status(400).json({ error: `Строка ${index + 1}: укажите наименование товара.`, code: "consignment_name_required" });
      const purchasePrice = normalizeFinanceMoney(body.purchasePrice, 0);
      const salePrice = normalizeFinanceMoney(body.salePrice, 0);
      if (!(purchasePrice >= 0) || !(salePrice >= 0)) return response.status(400).json({ error: `Строка ${index + 1}: цены не могут быть отрицательными.`, code: "consignment_price_invalid" });
      prepared.push({
        name,
        article: cleanText(body.article) || null,
        supplierName: cleanText(body.supplierName) || null,
        partnerId: cleanText(body.partnerId) || null,
        purchasePrice,
        salePrice,
        quantity: consignmentQuantityInput(body.quantity),
        note: cleanText(body.note) || null,
        fromBalance: body.fromBalance === true,
      });
    }
    const username = requestUsername(request);
    const created = await getPrisma().$transaction(async (tx) => {
      const results = [];
      for (const row of prepared) {
        const { fromBalance, ...data } = row;
        const item = await tx.consignmentItem.create({ data });
        let operation = null;
        if (row.quantity > 0) {
          operation = await tx.consignmentOperation.create({
            data: {
              itemId: item.id,
              itemName: item.name,
              type: fromBalance ? "purchase" : "receive",
              quantity: row.quantity,
              unitPurchase: row.purchasePrice,
              unitSale: row.salePrice,
              balanceDelta: fromBalance ? -normalizeFinanceMoney(row.purchasePrice * row.quantity, 0) : 0,
              note: row.note,
              createdBy: username || null,
            },
          });
        }
        results.push({ item, operation });
      }
      return results;
    });
    await appendAudit(request, "consignment.item.bulk_create", {
      entityType: "consignment_item",
      entityId: created.map((row) => row.item.id).join(","),
      newValue: { count: created.length },
    });
    response.status(201).json({
      ok: true,
      created: created.length,
      items: created.map((row) => consignmentItemFromPostgres(row.item)),
      operations: created.filter((row) => row.operation).length,
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/consignment/items/:id", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const id = cleanText(request.params.id);
    const body = request.body || {};
    const data = {};
    if (body.name !== undefined) {
      const name = cleanText(body.name);
      if (!name) return response.status(400).json({ error: "Наименование не может быть пустым.", code: "consignment_name_required" });
      data.name = name;
    }
    if (body.article !== undefined) data.article = cleanText(body.article) || null;
    if (body.supplierName !== undefined) data.supplierName = cleanText(body.supplierName) || null;
    if (body.partnerId !== undefined) data.partnerId = cleanText(body.partnerId) || null;
    if (body.note !== undefined) data.note = cleanText(body.note) || null;
    if (body.archived !== undefined) data.archived = body.archived === true;
    if (body.purchasePrice !== undefined) {
      const purchasePrice = normalizeFinanceMoney(body.purchasePrice, -1);
      if (!(purchasePrice >= 0)) return response.status(400).json({ error: "Некорректная закупочная цена.", code: "consignment_price_invalid" });
      data.purchasePrice = purchasePrice;
    }
    if (body.salePrice !== undefined) {
      const salePrice = normalizeFinanceMoney(body.salePrice, -1);
      if (!(salePrice >= 0)) return response.status(400).json({ error: "Некорректная цена продажи.", code: "consignment_price_invalid" });
      data.salePrice = salePrice;
    }
    if (!Object.keys(data).length) return response.status(400).json({ error: "Нет полей для обновления.", code: "consignment_patch_empty" });
    const row = await getPrisma().consignmentItem.update({ where: { id }, data });
    await appendAudit(request, "consignment.item.update", {
      entityType: "consignment_item",
      entityId: id,
      newValue: data,
    });
    response.json({ ok: true, item: consignmentItemFromPostgres(row) });
  } catch (error) {
    if (error?.code === "P2025") return response.status(404).json({ error: "Товар не найден.", code: "consignment_item_not_found" });
    next(error);
  }
});

async function runConsignmentStockOperation(request, response, next, buildOperation) {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const id = cleanText(request.params.id);
    const body = request.body || {};
    const quantity = consignmentQuantityInput(body.quantity);
    if (!quantity) return response.status(400).json({ error: "Количество должно быть больше нуля.", code: "consignment_quantity_required" });
    const username = requestUsername(request);
    let failure = null;
    const result = await getPrisma().$transaction(async (tx) => {
      const item = await tx.consignmentItem.findUnique({ where: { id } });
      if (!item) {
        failure = { status: 404, error: "Товар не найден.", code: "consignment_item_not_found" };
        return null;
      }
      const plan = buildOperation(consignmentItemFromPostgres(item), quantity, body);
      if (plan.error) {
        failure = { status: 400, ...plan };
        return null;
      }
      const updatedItem = await tx.consignmentItem.update({
        where: { id },
        data: { quantity: plan.nextQuantity, ...(plan.itemPatch || {}) },
      });
      const operation = await tx.consignmentOperation.create({
        data: { ...plan.operation, itemId: item.id, itemName: item.name, createdBy: username || null },
      });
      return { item: updatedItem, operation };
    });
    if (failure) return response.status(failure.status).json({ error: failure.error, code: failure.code });
    await appendAudit(request, `consignment.${result.operation.type}`, {
      entityType: "consignment_operation",
      entityId: result.operation.id,
      newValue: consignmentOperationFromPostgres(result.operation),
    });
    response.status(201).json({
      ok: true,
      item: consignmentItemFromPostgres(result.item),
      operation: consignmentOperationFromPostgres(result.operation),
    });
  } catch (error) {
    next(error);
  }
}

app.post("/api/consignment/items/:id/sale", requireAdmin, (request, response, next) => {
  void runConsignmentStockOperation(request, response, next, (item, quantity, body) => {
    if (item.quantity < quantity) return { error: `Недостаточно остатка: на складе ${item.quantity} шт.`, code: "consignment_stock_not_enough" };
    const unitSale = body.salePrice === undefined || body.salePrice === null || body.salePrice === ""
      ? item.salePrice
      : normalizeFinanceMoney(body.salePrice, -1);
    if (!(unitSale >= 0)) return { error: "Некорректная цена продажи.", code: "consignment_price_invalid" };
    const profit = normalizeFinanceMoney((unitSale - item.purchasePrice) * quantity, 0);
    const sponsorHalf = normalizeFinanceMoney(profit / 2, 0);
    const myHalf = normalizeFinanceMoney(profit - sponsorHalf, 0);
    return {
      nextQuantity: item.quantity - quantity,
      operation: {
        type: "sale",
        quantity,
        unitPurchase: item.purchasePrice,
        unitSale,
        balanceDelta: normalizeFinanceMoney(item.purchasePrice * quantity, 0),
        sponsorDelta: sponsorHalf,
        myDelta: myHalf,
        note: cleanText(body.note) || null,
      },
    };
  });
});

app.post("/api/consignment/items/:id/writeoff", requireAdmin, (request, response, next) => {
  void runConsignmentStockOperation(request, response, next, (item, quantity, body) => {
    if (item.quantity < quantity) return { error: `Недостаточно остатка: на складе ${item.quantity} шт.`, code: "consignment_stock_not_enough" };
    return {
      nextQuantity: item.quantity - quantity,
      operation: {
        type: "writeoff",
        quantity,
        unitPurchase: item.purchasePrice,
        unitSale: item.salePrice,
        note: cleanText(body.note) || null,
      },
    };
  });
});

app.post("/api/consignment/items/:id/receive", requireAdmin, (request, response, next) => {
  void runConsignmentStockOperation(request, response, next, (item, quantity, body) => {
    const unitPurchase = body.purchasePrice === undefined || body.purchasePrice === null || body.purchasePrice === ""
      ? item.purchasePrice
      : normalizeFinanceMoney(body.purchasePrice, -1);
    if (!(unitPurchase >= 0)) return { error: "Некорректная закупочная цена.", code: "consignment_price_invalid" };
    const fromBalance = body.fromBalance === true;
    return {
      nextQuantity: item.quantity + quantity,
      itemPatch: unitPurchase !== item.purchasePrice ? { purchasePrice: unitPurchase } : {},
      operation: {
        type: fromBalance ? "purchase" : "receive",
        quantity,
        unitPurchase,
        unitSale: item.salePrice,
        balanceDelta: fromBalance ? -normalizeFinanceMoney(unitPurchase * quantity, 0) : 0,
        note: cleanText(body.note) || null,
      },
    };
  });
});

app.post("/api/consignment/operations/:id/return", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const id = cleanText(request.params.id);
    const note = cleanText(request.body?.note);
    const username = requestUsername(request);
    let failure = null;
    const result = await getPrisma().$transaction(async (tx) => {
      const sale = await tx.consignmentOperation.findUnique({ where: { id } });
      if (!sale || sale.type !== "sale") {
        failure = { status: 404, error: "Продажа не найдена.", code: "consignment_sale_not_found" };
        return null;
      }
      if (sale.status !== "active") {
        failure = { status: 400, error: "По этой продаже возврат уже оформлен.", code: "consignment_already_returned" };
        return null;
      }
      if (!sale.itemId) {
        failure = { status: 400, error: "Товар этой продажи удалён — возврат невозможен.", code: "consignment_item_missing" };
        return null;
      }
      const item = await tx.consignmentItem.update({
        where: { id: sale.itemId },
        data: { quantity: { increment: sale.quantity } },
      });
      const returnOperation = await tx.consignmentOperation.create({
        data: {
          itemId: sale.itemId,
          itemName: sale.itemName,
          type: "return",
          quantity: sale.quantity,
          unitPurchase: sale.unitPurchase,
          unitSale: sale.unitSale,
          balanceDelta: normalizeFinanceMoney(-Number(sale.balanceDelta || 0), 0),
          sponsorDelta: normalizeFinanceMoney(-Number(sale.sponsorDelta || 0), 0),
          myDelta: normalizeFinanceMoney(-Number(sale.myDelta || 0), 0),
          note: note || null,
          relatedOperationId: sale.id,
          createdBy: username || null,
        },
      });
      await tx.consignmentOperation.update({ where: { id: sale.id }, data: { status: "returned" } });
      return { item, returnOperation };
    });
    if (failure) return response.status(failure.status).json({ error: failure.error, code: failure.code });
    await appendAudit(request, "consignment.return", {
      entityType: "consignment_operation",
      entityId: result.returnOperation.id,
      newValue: consignmentOperationFromPostgres(result.returnOperation),
    });
    response.status(201).json({
      ok: true,
      item: consignmentItemFromPostgres(result.item),
      operation: consignmentOperationFromPostgres(result.returnOperation),
    });
  } catch (error) {
    next(error);
  }
});

// Удаление операции реализации. Деньги (баланс/прибыли) считаются суммой
// операций, поэтому удаление строки откатывает их автоматически; остаток
// товара возвращаем явно. Продажу с оформленным возвратом удалить нельзя —
// сначала удаляется возврат (он вернёт продаже статус active).
app.delete("/api/consignment/operations/:id", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const id = cleanText(request.params.id);
    let failure = null;
    const result = await getPrisma().$transaction(async (tx) => {
      const operation = await tx.consignmentOperation.findUnique({ where: { id } });
      if (!operation) {
        failure = { status: 404, error: "Операция не найдена.", code: "consignment_operation_not_found" };
        return null;
      }
      if (operation.type === "sale" && operation.status === "returned") {
        failure = { status: 400, error: "По этой продаже оформлен возврат — сначала удалите операцию возврата.", code: "consignment_sale_has_return" };
        return null;
      }
      const quantityDelta = ["sale", "writeoff"].includes(operation.type)
        ? operation.quantity
        : ["receive", "purchase", "return"].includes(operation.type) ? -operation.quantity : 0;
      let item = null;
      if (operation.itemId && quantityDelta !== 0) {
        const current = await tx.consignmentItem.findUnique({ where: { id: operation.itemId } });
        if (current) {
          item = await tx.consignmentItem.update({
            where: { id: operation.itemId },
            data: { quantity: Math.max(0, current.quantity + quantityDelta) },
          });
        }
      }
      if (operation.type === "return" && operation.relatedOperationId) {
        await tx.consignmentOperation.updateMany({
          where: { id: operation.relatedOperationId },
          data: { status: "active" },
        });
      }
      await tx.consignmentOperation.delete({ where: { id } });
      return { operation, item };
    });
    if (failure) return response.status(failure.status).json({ error: failure.error, code: failure.code });
    await appendAudit(request, "consignment.operation.delete", {
      entityType: "consignment_operation",
      entityId: id,
      oldValue: consignmentOperationFromPostgres(result.operation),
    });
    response.json({
      ok: true,
      deleted: consignmentOperationFromPostgres(result.operation),
      item: result.item ? consignmentItemFromPostgres(result.item) : null,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/consignment/operations", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const filters = [];
    const itemId = cleanText(request.query.itemId || "");
    const type = cleanText(request.query.type || "").toLowerCase();
    if (itemId) filters.push({ itemId });
    if (type && type !== "all") filters.push({ type });
    const rows = await getPrisma().consignmentOperation.findMany({
      where: filters.length ? { AND: filters } : {},
      orderBy: { createdAt: "desc" },
      take: cleanLimit(request.query.limit, 300, 2000),
    });
    response.json({ ok: true, operations: rows.map(consignmentOperationFromPostgres) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/consignment/payouts", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const body = request.body || {};
    const kind = cleanText(body.kind).toLowerCase();
    if (!CONSIGNMENT_PAYOUT_KINDS.includes(kind)) {
      return response.status(400).json({ error: "Неизвестный тип выплаты.", code: "consignment_payout_kind_invalid" });
    }
    const amount = normalizeFinanceMoney(body.amount, 0);
    if (!(amount > 0)) return response.status(400).json({ error: "Сумма выплаты должна быть больше нуля.", code: "consignment_amount_required" });
    const summary = await readConsignmentSummary();
    const available = kind === "sponsor_payout" ? summary.balance : (kind === "sponsor_profit_payout" ? summary.sponsorProfit : summary.myProfit);
    if (amount > available) {
      return response.status(400).json({ error: `Недостаточно средств: доступно ${available} ₽.`, code: "consignment_payout_insufficient" });
    }
    const operation = await getPrisma().consignmentOperation.create({
      data: {
        type: kind,
        quantity: 0,
        balanceDelta: kind === "sponsor_payout" ? -amount : 0,
        sponsorDelta: kind === "sponsor_profit_payout" ? -amount : 0,
        myDelta: kind === "my_profit_payout" ? -amount : 0,
        note: cleanText(body.note) || null,
        createdBy: requestUsername(request) || null,
      },
    });
    await appendAudit(request, `consignment.${kind}`, {
      entityType: "consignment_operation",
      entityId: operation.id,
      newValue: consignmentOperationFromPostgres(operation),
    });
    response.status(201).json({ ok: true, operation: consignmentOperationFromPostgres(operation) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/consignment/suppliers", requireAdmin, async (_request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const [itemSuppliers, managed] = await Promise.all([
      getPrisma().consignmentItem.findMany({
        where: { supplierName: { not: null } },
        select: { supplierName: true },
        distinct: ["supplierName"],
        take: 1000,
      }),
      getPrisma().managedSupplier.findMany({ where: { active: true }, select: { name: true }, take: 1000 }),
    ]);
    const suppliers = [...new Set(
      [...itemSuppliers.map((row) => cleanText(row.supplierName)), ...managed.map((row) => cleanText(row.name))].filter(Boolean),
    )].sort((left, right) => left.localeCompare(right, "ru"));
    response.json({ ok: true, suppliers });
  } catch (error) {
    next(error);
  }
});

app.get("/api/consignment/pm-search", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const q = cleanText(request.query.q || "");
    if (q.length < 2) return response.json({ ok: true, items: [] });
    const rows = await getPrisma().priceMasterSnapshotItem.findMany({
      where: {
        active: true,
        OR: [
          { article: { contains: q, mode: "insensitive" } },
          { nativeName: { contains: q, mode: "insensitive" } },
        ],
      },
      orderBy: { updatedAt: "desc" },
      take: 20,
    });
    const ratePayload = await getUsdRate().catch(() => null);
    const usdRate = Number(ratePayload?.rate || ratePayload || process.env.DEFAULT_USD_RATE || 95) || 95;
    const items = rows.map((row) => {
      const price = row.price === null || row.price === undefined ? null : normalizeFinanceMoney(row.price, 0);
      const currency = cleanText(row.currency || "USD").toUpperCase();
      return {
        article: cleanText(row.article),
        name: cleanText(row.nativeName),
        supplierName: cleanText(row.partnerName),
        partnerId: cleanText(row.partnerId),
        price,
        currency,
        priceRub: price === null ? null : normalizeFinanceMoney(currency === "RUB" || currency === "RUR" ? price : price * usdRate, 0),
      };
    });
    response.json({ ok: true, items });
  } catch (error) {
    next(error);
  }
});
