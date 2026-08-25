// «Насовсем» = 10 лет: инактив без автоснятия; убрать можно вручную в карточке товара
// («Отложить поставщика» → отмена) или вернув строку сборки в статус «к сборке».
const PICKING_PERMANENT_SNOOZE_DAYS = 3650;

let _pickingRedis = null;
function pickingRedis() {
  if (_pickingRedis) return _pickingRedis;
  if (!redisUrl) return null;
  try {
    const Redis = require("ioredis");
    _pickingRedis = new Redis(redisUrl, { maxRetriesPerRequest: 2, enableReadyCheck: false, lazyConnect: false });
    _pickingRedis.on("error", () => {});
    return _pickingRedis;
  } catch { return null; }
}

app.get("/api/supplier-picking-list", requireStaff, async (request, response, next) => {
  try {
    const state = await readSupplierPickingState();
    const status = cleanText(request.query.status || "open").toLowerCase();
    const supplier = cleanText(request.query.supplier).toLowerCase();
    const q = cleanText(request.query.q).toLowerCase();
    const limit = cleanLimit(request.query.limit, 500);
    const showDeferred = cleanText(request.query.deferred || "").toLowerCase() === "1";
    let rows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    if (status && status !== "all") rows = rows.filter((row) => row.status === status);
    // By default hide open rows that are deferred until a future date; pass deferred=1 to show only deferred rows
    if (status === "open" || !status) {
      const now = new Date();
      if (showDeferred) {
        rows = rows.filter((row) => row.deferredUntil && new Date(row.deferredUntil) > now);
      } else {
        rows = rows.filter((row) => !row.deferredUntil || new Date(row.deferredUntil) <= now);
      }
    }
    if (supplier) rows = rows.filter((row) => cleanText(row.supplierName).toLowerCase().includes(supplier));
    if (q) {
      const qWords = q.split(/\s+/).filter(Boolean);
      rows = rows.filter((row) => {
        const text = [row.productName, row.offerId, row.orderId, row.postingNumber, row.supplierName]
          .map((v) => cleanText(v).toLowerCase()).join(" ");
        return qWords.every((w) => text.includes(w));
      });
    }
    rows.sort(compareSupplierPickingRows);
    const allRows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    // Supplier dropdown: only show suppliers that still have rows in the active filtered view
    const supplierSourceRows = (status === "open" || !status)
      ? rows  // use status-filtered rows so fully-assembled suppliers disappear
      : allRows;
    const suppliers = Array.from(new Set(supplierSourceRows.map((row) => row.supplierName).filter(Boolean))).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    const supplierLedgerMap = await supplierLedgerSummaryMapForSuppliers(suppliers.map((name) => ({ id: name, name })));
    const supplierLedger = Object.fromEntries(suppliers.map((name) => [name, supplierLedgerMap.get(name) || supplierLedgerSummaryFromEntries([])]));
    const ratePayload = await getUsdRate().catch(() => null);
    const usdRate = Number(ratePayload?.rate || process.env.DEFAULT_USD_RATE || 95) || 95;
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      rows: rows.slice(0, limit),
      total: rows.length,
      usdRate,
      suppliers,
      supplierLedger,
      summary: {
        open: allRows.filter((row) => row.status === "open" && (!row.deferredUntil || new Date(row.deferredUntil) <= new Date())).length,
        deferred: allRows.filter((row) => row.status === "open" && row.deferredUntil && new Date(row.deferredUntil) > new Date()).length,
        picked: allRows.filter((row) => row.status === "picked").length,
        missing: allRows.filter((row) => row.status === "missing").length,
        reordered: allRows.filter((row) => row.status === "reordered").length,
        suppliers: suppliers.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-picking-list/:key", requireStaff, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key || "");
    const status = cleanText(request.body?.status).toLowerCase();
    const admin = isAdminSession(request.session);
    if (!["open", "picked", "missing", "returned"].includes(status)) {
      return response.status(400).json({ error: "Unsupported picking status.", code: "supplier_picking_status_invalid" });
    }
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    const now = new Date();
    const username = requestUsername(request);
    // Rollback to open: any staff can undo any row (not restricted to own rows)
    // Allow picked→returned (item came back from ПВЗ); all other cross-status transitions blocked
    if (current.status !== "open" && status !== "open" && !(current.status === "picked" && status === "returned")) {
      return response.status(409).json({
        error: "Picking row is already finalized.",
        code: "supplier_picking_finalized",
        row: current,
      });
    }
    // «Не было»: сотрудник выбирает срок инактива поставщика — 1 (завтра появится), 2, 3, 5 дней
    // или насовсем (permanent). Без явного выбора действует прежний срок 7 дней.
    const missingPermanent = status === "missing" && request.body?.permanent === true;
    const requestedSnoozeDays = Math.round(Number(request.body?.snoozeDays || 0) || 0);
    const missingDays = missingPermanent
      ? PICKING_PERMANENT_SNOOZE_DAYS
      : (requestedSnoozeDays >= 1 ? Math.min(60, requestedSnoozeDays) : 7);
    const missingRetryAt = missingPermanent ? null : new Date(now.getTime() + missingDays * 24 * 60 * 60 * 1000).toISOString();
    const requestedPickQty = status === "picked" ? Math.max(1, Math.round(Number(request.body?.pickedQuantity || current.quantity) || current.quantity)) : null;
    const pickedQuantity = requestedPickQty != null ? Math.min(current.quantity, requestedPickQty) : null;
    const pricePaidRub = status === "picked" && request.body?.pricePaidRub != null
      ? (normalizeFinanceMoney(request.body.pricePaidRub, 0) || null)
      : null;
    const nextRow = normalizeSupplierPickingRow({
      ...current,
      status,
      ...(status === "picked" ? { pickedBy: username, pickedAt: now.toISOString(), pickedQuantity, pricePaidRub } : {}),
      ...(status === "missing" ? {
        missingBy: username,
        missingAt: now.toISOString(),
        missingReason: cleanText(request.body?.reason || "employee_missing"),
        missingSnoozeDays: missingPermanent ? 0 : missingDays,
        missingPermanent,
        nextRetryAt: missingRetryAt,
      } : {}),
      ...(status === "open" ? {
        pickedBy: "",
        pickedAt: null,
        pickedQuantity: null,
        pricePaidRub: null,
        missingBy: "",
        missingAt: null,
        missingReason: "",
        missingSnoozeDays: 0,
        missingPermanent: false,
        missingSnoozeLinkId: "",
        nextRetryAt: null,
      } : {}),
      ...(status === "returned" ? { returnedBy: username, returnedAt: now.toISOString() } : {}),
    });

    let linkSnooze = null;
    if (status === "missing") {
      linkSnooze = await snoozeSupplierLinkForPickingRow(current, missingDays).catch((error) => {
        logger.warn("picking missing link snooze failed", { key, detail: error?.message || String(error) });
        return { ok: false, reason: error?.message || String(error) };
      });
      if (linkSnooze?.ok) nextRow.missingSnoozeLinkId = linkSnooze.linkId;
    } else if (status === "open" && current.missingSnoozeLinkId && current.warehouseProductId) {
      // Возврат строки в «к сборке» снимает инактив, поставленный этой же строкой.
      await cancelWarehouseLinkSnooze(current.warehouseProductId, current.missingSnoozeLinkId, request).catch((error) => {
        logger.warn("picking open link snooze cancel failed", { key, detail: error?.message || String(error) });
      });
    }

    state.rows[key] = nextRow;
    await writeSupplierPickingState(state);

    const cartState = await readSupplierCartState();
    const blockKey = supplierBlockKey(current.offerId, current.partnerId);
    if (status === "missing" && current.offerId && current.partnerId) {
      cartState.supplierBlocks[blockKey] = {
        offerId: current.offerId,
        partnerId: current.partnerId,
        supplierName: current.supplierName,
        reason: missingPermanent ? "employee_missing_permanent" : "employee_missing",
        blockedAt: now.toISOString(),
        blockedBy: username,
        expiresAt: nextRow.nextRetryAt,
        sourcePickingKey: current.key,
      };
      const sourceCartKey = current.replacementFor || current.key.replace(/\|retry:.+$/, "");
      if (cartState.processed?.[sourceCartKey]) delete cartState.processed[sourceCartKey];
      await writeSupplierCartState(cartState);
      // Вычитаем из дневного итога: товар «не было» — заказ не состоялся
      try {
        const rowDate = (current.createdAt || now.toISOString()).slice(0, 10);
        await adjustDailyCartTotal(rowDate, -((Number(current.price) || 0) * Math.max(1, Math.round(Number(current.quantity || 1)))), -Math.max(1, Math.round(Number(current.quantity || 1))));
      } catch (e) { logger.warn("daily_cart_total subtract (missing) failed", { key, detail: e?.message || String(e) }); }
      await appendAudit(request, "supplier_cart.supplier_blocked", {
        entityType: "supplier_cart",
        entityId: blockKey,
        newValue: { ...cartState.supplierBlocks[blockKey], linkSnooze },
      });
    } else if (status === "open") {
      if (current.status === "missing") await deactivateSupplierBlockForPickingRow(current, request);
      if (current.requestRowId || current.requestDocId) await restoreSupplierCartProcessedForPickingRow(current, request);
    }

    // Ozon: подтвердить упаковку при физической сборке (и экспресс, и обычные).
    // WB FBS: при сборке создать поставку, если ещё нет (напр. строка добавлена вручную/автокорзиной без поставки)
    let wbShipment = null;
    if (status === "picked" && nextRow.marketplace === "wb" && nextRow.orderId && !nextRow.wbSupplyId) {
      wbShipment = await confirmWbOrdersWithSupply([nextRow]).catch((error) => ({
        ok: false, orderId: nextRow.orderId, error: error?.message || String(error),
      }));
      if (wbShipment?.ok && wbShipment.supplyId) {
        nextRow.wbSupplyId = wbShipment.supplyId;
        state.rows[key] = nextRow;
        await writeSupplierPickingState(state);
      }
    }

    let financeOrder = null;
    let supplierLedgerEntry = null;
    let stockRecovery = null;
    if (status === "picked") {
      financeOrder = await upsertFinanceOrderFromPickingRow(nextRow, request);
      supplierLedgerEntry = await upsertSupplierLedgerDebtFromPickingRow(nextRow, financeOrder, request);
      // Deduct from picker's persistent balance (tracked in supplier native currency, usually USD)
      const pickerDeductAmt = nextRow.price > 0
        ? nextRow.price * Math.max(1, Math.round(Number(nextRow.quantity || 1)))
        : 0;
      if (pickerDeductAmt > 0 && nextRow.pickedBy) {
        try {
          const pickerBal = await loadPickerBalance(nextRow.pickedBy);
          // Use picking key as deduction ID so we can reverse it on rollback
          const debitId = `picking:${key}`;
          if (!pickerBal.credits.some((c) => String(c.id) === debitId)) {
            pickerBal.credits.push({
              id: debitId,
              amount: -pickerDeductAmt,
              note: `Оплата: ${nextRow.productName || nextRow.offerId || key}`,
              createdAt: now.toISOString(),
              createdBy: "system",
            });
            await savePickerBalance(nextRow.pickedBy, pickerBal);
          }
        } catch (balanceError) {
          logger.warn("picker balance deduction failed", { key, detail: balanceError?.message || String(balanceError) });
        }
      }
      // PM MySQL: отметить заказ «Получен» когда все строки документа собраны
      if (nextRow.requestDocId) {
        const pmDocId = cleanText(nextRow.requestDocId);
        const allForDoc = Object.values(state.rows).filter((r) => cleanText(r.requestDocId) === pmDocId);
        const allDone = allForDoc.length > 0 && allForDoc.every((r) => r.status !== "open");
        if (allDone) {
          let pmConn;
          try {
            pmConn = await pool.getConnection();
            await pmConn.query("UPDATE RequestDocs SET Recieved=1 WHERE DocID=?", [Number(pmDocId)]);
            logger.info("PM order marked received", { docId: pmDocId });
          } catch (pmError) {
            logger.warn("PM order received update failed", { docId: pmDocId, detail: pmError?.message || String(pmError) });
          } finally {
            if (pmConn) pmConn.release();
          }
        }
      }
    } else if (current.status === "picked" && status !== "returned") {
      // Keep finance order when item returns from ПВЗ — the sale will be re-attempted
      await removeFinanceOrderForPickingRow(current);
      supplierLedgerEntry = await voidSupplierLedgerDebtForPickingRow(current, request);
      // Restore picker balance on rollback
      if (current.pricePaidRub > 0 && current.pickedBy) {
        try {
          const pickerBal = await loadPickerBalance(current.pickedBy);
          const debitId = `picking:${key}`;
          pickerBal.credits = pickerBal.credits.filter((c) => String(c.id) !== debitId);
          await savePickerBalance(current.pickedBy, pickerBal);
        } catch (balanceError) {
          logger.warn("picker balance rollback failed", { key, detail: balanceError?.message || String(balanceError) });
        }
      }
    }

    await appendAudit(request, `supplier_picking.${status === "picked" ? "picked" : status === "missing" ? "missing" : status === "returned" ? "returned" : "status_update"}`, {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: current,
      newValue: nextRow,
      financeOrderId: financeOrder?.id || null,
      supplierLedgerEntryId: supplierLedgerEntry?.id || null,
      stockRecovery,
      wbShipment,
    });
    response.json({ ok: true, row: nextRow, financeOrder, supplierLedgerEntry, stockRecovery: null, linkSnooze, wbShipment });

    // Marketplace confirmations + stock recovery run in background so "Собрал" responds immediately
    if (status === "picked") {
      // Это резервный вызов — confirmMarketplaceOrdersAfterInsert делает то же при формировании корзины,
      // но только если ozonProductId был известен на тот момент. При сборке он точно есть в nextRow.
      if (nextRow.marketplace === "ozon" && nextRow.postingNumber && nextRow.ozonProductId) {
        setImmediate(() => {
          confirmOzonPostingPackaged(nextRow.postingNumber, [{
            product_id: Number(nextRow.ozonProductId),
            quantity: Math.max(1, Math.round(Number(nextRow.pickedQuantity || nextRow.quantity || 1))),
          }]).catch((error) => {
            logger.warn("ozon posting package confirm failed at picking", {
              key, postingNumber: nextRow.postingNumber, detail: error?.message || String(error),
            });
          });
        });
      }
      // Yandex экспресс: READY_TO_SHIP только после физической сборки.
      if (nextRow.isExpress && nextRow.marketplace === "yandex" && nextRow.orderId) {
        setImmediate(() => {
          confirmYandexOrderReadyToShip(nextRow.orderId, nextRow.campaignId).catch((error) => {
            logger.warn("express yandex order ready-to-ship failed", {
              key, orderId: nextRow.orderId, detail: error?.message || String(error),
            });
          });
        });
      }
    }

    if (status === "picked" && nextRow.warehouseProductId) {
      const bgProductIds = [nextRow.warehouseProductId];
      const bgQuantity = nextRow.quantity;
      setImmediate(async () => {
        try {
          const freshProducts = await buildFreshWarehouseProducts(bgProductIds, { livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: autoPricePmTimeoutMs })
            .catch((bgErr) => { logger.warn("picking stock restore build failed (bg)", { detail: bgErr?.message || String(bgErr), productIds: bgProductIds }); return []; });
          const restoreProducts = freshProducts.map((product) => ({
            ...product,
            targetStock: Math.max(1, Math.round(Number(product.targetStock || 0)), Math.round(Number(bgQuantity || 1) || 1)),
          }));
          if (restoreProducts.length) {
            const bgRecovery = await runSupplierRecoveryAutomation({ products: restoreProducts }, {
              productIds: bgProductIds, force: true, source: "supplier_picking_picked", sourceEvent: "supplier_picking_stock_restore",
            }).catch((bgErr) => { logger.warn("picking stock restore failed (bg)", { detail: bgErr?.message || String(bgErr), productIds: bgProductIds }); return null; });
            if (bgRecovery) logger.info("picking stock restore complete (bg)", { key, productIds: bgProductIds, recovered: bgRecovery.recovered, restoredStocks: bgRecovery.restoredStocks });
          }
        } catch (bgErr) {
          logger.warn("picking stock restore background error", { key, detail: bgErr?.message || String(bgErr) });
        }
      });
    }
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-picking-list/:key/cancel-cart", requireAdmin, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key || "");
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });

    let financeRemoval = null;
    let supplierLedgerEntry = null;
    if (current.status === "picked") {
      financeRemoval = await removeFinanceOrderForPickingRow(current);
      supplierLedgerEntry = await voidSupplierLedgerDebtForPickingRow(current, request);
    }
    if (current.status === "missing") await deactivateSupplierBlockForPickingRow(current, request);

    const priceMaster = await deleteSupplierCartPriceMasterRow(current);
    const sourceCartKey = await removeSupplierCartProcessedForPickingRow(current);
    delete state.rows[key];
    await writeSupplierPickingState(state);
    await deleteSupplierPickingStateRow(key);
    // Вычитаем из дневного итога: отмена заказа
    try {
      const rowDate = (current.createdAt || new Date().toISOString()).slice(0, 10);
      await adjustDailyCartTotal(rowDate, -((Number(current.price) || 0) * Math.max(1, Math.round(Number(current.quantity || 1)))), -Math.max(1, Math.round(Number(current.quantity || 1))));
    } catch (e) { logger.warn("daily_cart_total subtract (cancel-cart) failed", { key, detail: e?.message || String(e) }); }

    await appendAudit(request, "supplier_cart.cancel_committed", {
      entityType: "supplier_cart",
      entityId: sourceCartKey || key,
      oldValue: current,
      newValue: { cancelled: true, priceMaster, financeRemoval, supplierLedgerEntry },
    });
    response.json({ ok: true, cancelled: true, key, sourceCartKey, priceMaster, financeRemoval, supplierLedgerEntry });
  } catch (error) {
    next(error);
  }
});

// Defer a picking row to tomorrow (or clear deferral)
app.post("/api/supplier-picking-list/:key/defer", requireStaff, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key || "");
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    if (current.status !== "open") return response.status(409).json({ error: "Only open rows can be deferred.", code: "picking_not_open" });
    const clear = request.body?.clear === true;
    let deferredUntil = null;
    if (!clear) {
      // Defer to next working day (Mon–Fri) at midnight Moscow (UTC+3); Fri → Mon
      const now = new Date();
      const moscowDow = new Date(now.getTime() + 3 * 60 * 60 * 1000).getUTCDay(); // 0=Sun..6=Sat
      const daysToAdd = moscowDow === 5 ? 3 : moscowDow === 6 ? 2 : 1;
      const target = new Date(now);
      target.setUTCDate(target.getUTCDate() + daysToAdd);
      target.setUTCHours(0 - 3, 0, 0, 0); // midnight Moscow
      deferredUntil = target.toISOString();
    }
    const nextRow = normalizeSupplierPickingRow({ ...current, deferredUntil });
    state.rows[key] = nextRow;
    await writeSupplierPickingState(state);
    await appendAudit(request, clear ? "supplier_picking.defer_cleared" : "supplier_picking.deferred", {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: { deferredUntil: current.deferredUntil },
      newValue: { deferredUntil },
    });
    response.json({ ok: true, row: nextRow, deferredUntil });
  } catch (error) {
    next(error);
  }
});

// ── Picking list viewers (coordination heartbeat) ────────────────────────────
// Stores {username, viewedAt} in Redis with 45s TTL. GET returns all active viewers.
app.put("/api/supplier-picking-list/heartbeat", requireStaff, async (request, response, next) => {
  try {
    const username = requestUsername(request) || "неизвестно";
    const redis = pickingRedis();
    if (!redis) return response.json({ ok: true });
    await redis.set(`picking:viewer:${username}`, JSON.stringify({ username, viewedAt: new Date().toISOString() }), "EX", 45);
    response.json({ ok: true });
  } catch (error) { next(error); }
});

app.get("/api/supplier-picking-list/viewers", requireStaff, async (_request, response, next) => {
  try {
    const redis = pickingRedis();
    if (!redis) return response.json({ ok: true, viewers: [] });
    const keys = await redis.keys("picking:viewer:*");
    if (!keys.length) return response.json({ ok: true, viewers: [] });
    const vals = await redis.mget(keys);
    const viewers = vals.flatMap((v) => {
      try { return v ? [JSON.parse(v)] : []; } catch { return []; }
    });
    response.json({ ok: true, viewers });
  } catch (error) { next(error); }
});

