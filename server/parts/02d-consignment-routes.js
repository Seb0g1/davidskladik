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
    if (op.type === "sponsor_topup") acc.sponsorTopUps += Math.abs(op.balanceDelta);
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
    sponsorTopUps: 0,
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
    sponsorTopUps: normalizeFinanceMoney(totals.sponsorTopUps, 0),
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

// Групповые операции по товару с несколькими партиями (один артикул, разные
// лоты): продажа/списание распределяются по партиям FIFO (старые сначала),
// приход-докупка попадает в самую свежую партию. На каждую затронутую партию
// создаётся своя операция — удаление любой строки откатывает её часть.
async function runConsignmentGroupOperation(request, response, next, mode) {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const body = request.body || {};
    const itemIds = [...new Set((Array.isArray(body.itemIds) ? body.itemIds : []).map((value) => cleanText(value)).filter(Boolean))].slice(0, 200);
    if (!itemIds.length) return response.status(400).json({ error: "Передайте партии группы (itemIds).", code: "consignment_group_items_required" });
    const quantity = consignmentQuantityInput(body.quantity);
    if (!quantity) return response.status(400).json({ error: "Количество должно быть больше нуля.", code: "consignment_quantity_required" });
    const username = requestUsername(request);
    let failure = null;
    const result = await getPrisma().$transaction(async (tx) => {
      const rows = await tx.consignmentItem.findMany({ where: { id: { in: itemIds } }, orderBy: { createdAt: "asc" } });
      const lots = rows.map(consignmentItemFromPostgres);
      if (!lots.length) {
        failure = { status: 404, error: "Партии не найдены.", code: "consignment_item_not_found" };
        return null;
      }
      const items = [];
      const operations = [];
      if (mode === "receive") {
        const target = lots[lots.length - 1];
        const unitPurchase = body.purchasePrice === undefined || body.purchasePrice === null || body.purchasePrice === ""
          ? target.purchasePrice
          : normalizeFinanceMoney(body.purchasePrice, -1);
        if (!(unitPurchase >= 0)) {
          failure = { status: 400, error: "Некорректная закупочная цена.", code: "consignment_price_invalid" };
          return null;
        }
        const fromBalance = body.fromBalance === true;
        const updated = await tx.consignmentItem.update({
          where: { id: target.id },
          data: { quantity: target.quantity + quantity, ...(unitPurchase !== target.purchasePrice ? { purchasePrice: unitPurchase } : {}) },
        });
        const operation = await tx.consignmentOperation.create({
          data: {
            itemId: target.id,
            itemName: target.name,
            type: fromBalance ? "purchase" : "receive",
            quantity,
            unitPurchase,
            unitSale: target.salePrice,
            balanceDelta: fromBalance ? -normalizeFinanceMoney(unitPurchase * quantity, 0) : 0,
            note: cleanText(body.note) || null,
            createdBy: username || null,
          },
        });
        items.push(updated);
        operations.push(operation);
        return { items, operations };
      }
      const available = lots.reduce((sum, lot) => sum + lot.quantity, 0);
      if (available < quantity) {
        failure = { status: 400, error: `Недостаточно остатка: по партиям доступно ${available} шт.`, code: "consignment_stock_not_enough" };
        return null;
      }
      const overrideSale = body.salePrice === undefined || body.salePrice === null || body.salePrice === ""
        ? null
        : normalizeFinanceMoney(body.salePrice, -1);
      if (mode === "sale" && overrideSale !== null && !(overrideSale >= 0)) {
        failure = { status: 400, error: "Некорректная цена продажи.", code: "consignment_price_invalid" };
        return null;
      }
      let remaining = quantity;
      for (const lot of lots) {
        if (!remaining) break;
        const take = Math.min(lot.quantity, remaining);
        if (!take) continue;
        remaining -= take;
        const updated = await tx.consignmentItem.update({ where: { id: lot.id }, data: { quantity: lot.quantity - take } });
        let data;
        if (mode === "sale") {
          const unitSale = overrideSale !== null ? overrideSale : lot.salePrice;
          const profit = normalizeFinanceMoney((unitSale - lot.purchasePrice) * take, 0);
          const sponsorHalf = normalizeFinanceMoney(profit / 2, 0);
          data = {
            type: "sale",
            quantity: take,
            unitPurchase: lot.purchasePrice,
            unitSale,
            balanceDelta: normalizeFinanceMoney(lot.purchasePrice * take, 0),
            sponsorDelta: sponsorHalf,
            myDelta: normalizeFinanceMoney(profit - sponsorHalf, 0),
          };
        } else {
          data = { type: "writeoff", quantity: take, unitPurchase: lot.purchasePrice, unitSale: lot.salePrice };
        }
        const operation = await tx.consignmentOperation.create({
          data: { ...data, itemId: lot.id, itemName: lot.name, note: cleanText(body.note) || null, createdBy: username || null },
        });
        items.push(updated);
        operations.push(operation);
      }
      return { items, operations };
    });
    if (failure) return response.status(failure.status).json({ error: failure.error, code: failure.code });
    await appendAudit(request, `consignment.group.${mode}`, {
      entityType: "consignment_operation",
      entityId: result.operations.map((op) => op.id).join(","),
      newValue: { mode, quantity, operations: result.operations.map(consignmentOperationFromPostgres) },
    });
    response.status(201).json({
      ok: true,
      items: result.items.map(consignmentItemFromPostgres),
      operations: result.operations.map(consignmentOperationFromPostgres),
    });
  } catch (error) {
    next(error);
  }
}

app.post("/api/consignment/groups/sale", requireAdmin, (request, response, next) => {
  void runConsignmentGroupOperation(request, response, next, "sale");
});

app.post("/api/consignment/groups/writeoff", requireAdmin, (request, response, next) => {
  void runConsignmentGroupOperation(request, response, next, "writeoff");
});

app.post("/api/consignment/groups/receive", requireAdmin, (request, response, next) => {
  void runConsignmentGroupOperation(request, response, next, "receive");
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

// Пополнение общего баланса спонсора: довнесение денег извне (например,
// компенсация за испорченный товар в поставке). Зеркальная операция к
// sponsor_payout — плюс к балансу, удаление строки откатывает деньги.
app.post("/api/consignment/topups", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const body = request.body || {};
    const amount = normalizeFinanceMoney(body.amount, 0);
    if (!(amount > 0)) return response.status(400).json({ error: "Сумма пополнения должна быть больше нуля.", code: "consignment_amount_required" });
    const operation = await getPrisma().consignmentOperation.create({
      data: {
        type: "sponsor_topup",
        quantity: 0,
        balanceDelta: amount,
        note: cleanText(body.note) || null,
        createdBy: requestUsername(request) || null,
      },
    });
    await appendAudit(request, "consignment.sponsor_topup", {
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
    const tokenGroups = pmQueryToTokenGroups(q);
    // OR pre-filter casts a wide net so n-1 tolerance in pmPassesSearchFilter can work.
    const and = [{ active: true }];
    if (tokenGroups.length) {
      const orTerms = tokenGroups.flatMap((group) =>
        group.flatMap((synonym) => [
          { article: { contains: synonym, mode: "insensitive" } },
          { nativeName: { contains: synonym, mode: "insensitive" } },
        ]),
      );
      and.push({ OR: orTerms });
    } else {
      and.push({ OR: [{ article: { contains: q, mode: "insensitive" } }, { nativeName: { contains: q, mode: "insensitive" } }] });
    }
    let rows = await getPrisma().priceMasterSnapshotItem.findMany({
      where: { AND: and },
      orderBy: { updatedAt: "desc" },
      take: 500,
    });
    if (tokenGroups.length) {
      rows = rows.filter((row) => {
        const hay = [cleanText(row.nativeName || ""), cleanText(row.article || "")].join(" ");
        return pmPassesSearchFilter(hay, tokenGroups);
      }).slice(0, 20);
    } else {
      rows = rows.slice(0, 20);
    }
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

// Синхронизирует продажи из PriceMaster (SaleRows+SaleDocs+Products) → ConsignmentOperation.
// Инкрементно: при каждом запуске берёт только строки PM с RowID > уже обработанных.
// Матчит строго по article = "pm:{productId}" — нечёткий матч по имени убран (давал ложные совпадения).
// Продажи ДО item.createdAt игнорируются. Каждая продажа:
//   - декрементирует item.quantity (clamp to 0)
//   - balanceDelta = purchasePrice * quantity (возврат себестоимости на баланс)
//   - sponsorDelta / myDelta = прибыль пополам
async function runConsignmentPmSync() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) throw new Error("Postgres недоступен");

  // Инкрементный watermark: max RowID уже обработанных строк
  const lastIdResult = await prisma.$queryRaw`
    SELECT MAX(CAST(SUBSTRING(source_key, 9) AS INTEGER)) AS max_id
    FROM consignment_operations
    WHERE source_key LIKE 'pm_sale_%'
      AND source_key ~ '^pm_sale_[0-9]+$'
  `;
  const lastRowId = Number(lastIdResult[0]?.max_id || 0);

  // При lastRowId=0 (первый запуск / после reset) начинаем с даты самого раннего
  // товара реализации — иначе запрос уходит в далёкую историю PM и все строки
  // оказываются старше item.createdAt → skippedBefore=все, created=0.
  let saleRows;
  if (lastRowId === 0) {
    const earliest = await prisma.consignmentItem.findFirst({
      where: { article: { startsWith: "pm:" } },
      orderBy: { createdAt: "asc" },
      select: { createdAt: true },
    });
    const cutoff = earliest
      ? new Date(earliest.createdAt.getTime() - 24 * 60 * 60 * 1000) // за день до первого товара
      : new Date(Date.now() - 90 * 24 * 60 * 60 * 1000); // fallback: 90 дней
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    [saleRows] = await pool.query(`
      SELECT
        sr.RowID        AS rowId,
        sr.ProductID    AS productId,
        sr.Quantity     AS quantity,
        sr.Price        AS price,
        sd.DocDate      AS docDate,
        sd.Comment      AS docComment,
        p.ProductName   AS productName,
        p.SalePrice     AS purchasePrice
      FROM SaleRows sr
      JOIN SaleDocs  sd ON sr.DocID    = sd.DocID
      JOIN Products  p  ON sr.ProductID = p.ProductID
      WHERE sr.ProductID > 0
        AND sd.DocDate >= ?
      ORDER BY sr.RowID ASC
      LIMIT 2000
    `, [cutoffStr]);
  } else {
    [saleRows] = await pool.query(`
      SELECT
        sr.RowID        AS rowId,
        sr.ProductID    AS productId,
        sr.Quantity     AS quantity,
        sr.Price        AS price,
        sd.DocDate      AS docDate,
        sd.Comment      AS docComment,
        p.ProductName   AS productName,
        p.SalePrice     AS purchasePrice
      FROM SaleRows sr
      JOIN SaleDocs  sd ON sr.DocID    = sd.DocID
      JOIN Products  p  ON sr.ProductID = p.ProductID
      WHERE sr.ProductID > 0
        AND sr.RowID > ?
      ORDER BY sr.RowID ASC
      LIMIT 2000
    `, [lastRowId]);
  }

  if (!saleRows.length) {
    return { ok: true, created: 0, skipped: 0, skippedBefore: 0, itemsMatched: 0, total: 0 };
  }

  // Матч только по article = "pm:{productId}" — нет fuzzy-матча по имени
  const productIds = [...new Set(saleRows.map((r) => r.productId))];
  const itemByProductId = new Map();
  for (const productId of productIds) {
    const article = `pm:${productId}`;
    const existing = await prisma.consignmentItem.findFirst({
      where: { article },
      select: { id: true, createdAt: true, purchasePrice: true },
    });
    if (existing) itemByProductId.set(productId, existing);
  }
  const itemsMatched = itemByProductId.size;

  let created = 0;
  let skipped = 0;
  let skippedBefore = 0;

  for (const row of saleRows) {
    const item = itemByProductId.get(row.productId);
    if (!item) { skipped++; continue; }
    const docDate = row.docDate ? new Date(row.docDate) : new Date();
    if (docDate < item.createdAt) { skippedBefore++; continue; }
    const sourceKey = `pm_sale_${row.rowId}`;
    try {
      const quantity = Math.max(1, Math.round(Number(row.quantity || 1)));
      const unitPurchase = normalizeFinanceMoney(item.purchasePrice, 0);
      const unitSale = normalizeFinanceMoney(row.price, 0);
      const profit = normalizeFinanceMoney((unitSale - unitPurchase) * quantity, 0);
      const sponsorHalf = normalizeFinanceMoney(profit / 2, 0);
      const myHalf = normalizeFinanceMoney(profit - sponsorHalf, 0);

      await prisma.$transaction(async (tx) => {
        const current = await tx.consignmentItem.findUnique({ where: { id: item.id }, select: { quantity: true } });
        await tx.consignmentItem.update({
          where: { id: item.id },
          data: { quantity: Math.max(0, (current?.quantity || 0) - quantity) },
        });
        await tx.consignmentOperation.create({
          data: {
            sourceKey,
            itemId: item.id,
            itemName: cleanText(row.productName || ""),
            type: "sale",
            quantity,
            unitPurchase,
            unitSale,
            balanceDelta: normalizeFinanceMoney(unitPurchase * quantity, 0),
            sponsorDelta: sponsorHalf,
            myDelta: myHalf,
            note: cleanText(row.docComment || "") || null,
            createdBy: "pm-sync",
            createdAt: docDate,
          },
        });
      });
      created++;
    } catch (error) {
      if (error?.code === "P2002") { skipped++; continue; }
      throw error;
    }
  }

  return { ok: true, created, skipped, skippedBefore, itemsMatched, total: saleRows.length };
}

// Автопланировщик: импорт продаж PM в реализацию каждые N часов.
const consignmentPmSyncIntervalMs = Math.max(
  30 * 60_000,
  Number(process.env.CONSIGNMENT_PM_SYNC_HOURS || 2) * 60 * 60_000 || 2 * 60 * 60_000,
);
let consignmentPmSyncTimer = null;
let consignmentPmSyncRunning = false;
let consignmentPmSyncNextRunAt = null;
let consignmentPmSyncLastResult = null;

function scheduleConsignmentPmSync(delayMs = consignmentPmSyncIntervalMs) {
  if (consignmentPmSyncTimer) clearTimeout(consignmentPmSyncTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs) || consignmentPmSyncIntervalMs);
  consignmentPmSyncNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  consignmentPmSyncTimer = setTimeout(async () => {
    if (!consignmentPmSyncRunning) {
      consignmentPmSyncRunning = true;
      try {
        const result = await runConsignmentPmSync();
        consignmentPmSyncLastResult = { ...result, at: new Date().toISOString() };
        if (result.created > 0) {
          logger.info("consignment pm sync complete", consignmentPmSyncLastResult);
        }
      } catch (error) {
        consignmentPmSyncLastResult = { ok: false, error: error?.message || String(error), at: new Date().toISOString() };
        logger.warn("consignment pm sync failed", { detail: consignmentPmSyncLastResult.error });
      } finally {
        consignmentPmSyncRunning = false;
      }
    }
    scheduleConsignmentPmSync(consignmentPmSyncIntervalMs);
  }, normalizedDelay);
  consignmentPmSyncTimer.unref?.();
}

app.post("/api/consignment/pm-sync", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const result = await runConsignmentPmSync();
    response.json(result);
  } catch (error) {
    next(error);
  }
});

// Удаляет позиции реализации автоматически созданные синком PM (те, что помечены
// "Создано автосинком из PriceMaster"). Операции этих позиций тоже удаляются.
app.post("/api/consignment/pm-sync/cleanup", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const prisma = getPrisma();
    const autoItems = await prisma.consignmentItem.findMany({
      where: { note: "Создано автосинком из PriceMaster" },
      select: { id: true },
    });
    if (!autoItems.length) return response.json({ ok: true, removedItems: 0, removedOperations: 0 });
    const ids = autoItems.map((i) => i.id);
    const opsResult = await prisma.consignmentOperation.deleteMany({ where: { itemId: { in: ids } } });
    const itemsResult = await prisma.consignmentItem.deleteMany({ where: { id: { in: ids } } });
    response.json({ ok: true, removedItems: itemsResult.count, removedOperations: opsResult.count });
  } catch (error) {
    next(error);
  }
});

// Полный сброс PM-продаж: удаляет все операции с sourceKey = "pm_sale_*" и
// восстанавливает остатки товаров (т.к. они были декрементированы этими продажами).
// После вызова — запускай /api/consignment/pm-sync чтобы заново выгрузить из PM.
app.post("/api/consignment/pm-sync/reset", requireAdmin, async (request, response, next) => {
  try {
    if (consignmentStorageUnavailable(response)) return;
    const prisma = getPrisma();

    // Найти все PM-операции и сгруппировать по itemId для восстановления остатков
    const pmOps = await prisma.consignmentOperation.findMany({
      where: { sourceKey: { startsWith: "pm_sale_" } },
      select: { id: true, itemId: true, quantity: true },
    });

    if (!pmOps.length) {
      return response.json({ ok: true, removedOperations: 0, restoredItems: 0 });
    }

    // Восстановить остатки: для каждого товара суммируем удалённые продажи
    const quantityToRestore = new Map();
    for (const op of pmOps) {
      quantityToRestore.set(op.itemId, (quantityToRestore.get(op.itemId) || 0) + op.quantity);
    }

    const opIds = pmOps.map((op) => op.id);
    let restoredItems = 0;

    await prisma.$transaction(async (tx) => {
      await tx.consignmentOperation.deleteMany({ where: { id: { in: opIds } } });
      for (const [itemId, qty] of quantityToRestore) {
        try {
          const current = await tx.consignmentItem.findUnique({ where: { id: itemId }, select: { quantity: true } });
          if (current) {
            await tx.consignmentItem.update({ where: { id: itemId }, data: { quantity: current.quantity + qty } });
            restoredItems++;
          }
        } catch { /* item might have been deleted, skip */ }
      }
    });

    response.json({ ok: true, removedOperations: pmOps.length, restoredItems });
  } catch (error) {
    next(error);
  }
});
