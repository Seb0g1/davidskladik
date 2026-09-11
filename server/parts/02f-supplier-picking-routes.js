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
    if (status && status !== "all") {
      // "open" view also shows cancelled rows so pickers see what was cancelled
      if (status === "open") {
        rows = rows.filter((row) => row.status === "open" || row.status === "cancelled");
      } else {
        rows = rows.filter((row) => row.status === status);
      }
    }
    // By default hide open rows that are deferred until a future date; pass deferred=1 to show only deferred rows
    if (status === "open" || !status) {
      const now = new Date();
      if (showDeferred) {
        rows = rows.filter((row) => row.status === "open" && row.deferredUntil && new Date(row.deferredUntil) > now);
      } else {
        rows = rows.filter((row) => row.status === "cancelled" || !row.deferredUntil || new Date(row.deferredUntil) <= now);
      }
    }
    // Snapshot rows after status+deferred filter but BEFORE supplier/q filter — used for the
    // supplier dropdown. If we used the post-supplier-filter snapshot, the dropdown would
    // disappear when the user has filtered to a specific supplier and all their items are done.
    const rowsForSupplierDropdown = rows;
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
    // Supplier dropdown: only show suppliers that still have rows in the active filtered view.
    // Use the pre-supplier/q snapshot so the dropdown stays visible when a supplier filter is
    // active — otherwise marking all their items done empties the dropdown and the filter can
    // no longer be changed without a page refresh.
    const supplierSourceRows = (status === "open" || !status)
      ? rowsForSupplierDropdown
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
        cancelled: allRows.filter((row) => row.status === "cancelled").length,
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
    if (!["open", "picked", "missing", "returned", "cancelled"].includes(status)) {
      return response.status(400).json({ error: "Unsupported picking status.", code: "supplier_picking_status_invalid" });
    }
    if (status === "cancelled" && !admin) {
      return response.status(403).json({ error: "Только администратор может отменить строку сборки.", code: "supplier_picking_cancel_admin_only" });
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
      ...(status === "cancelled" ? { cancelledBy: username, cancelledAt: now.toISOString() } : {}),
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
    } else if (status === "cancelled") {
      const sourceCartKey = current.replacementFor || current.key.replace(/\|retry:.+$/, "");
      if (cartState.processed?.[sourceCartKey]) delete cartState.processed[sourceCartKey];
      await writeSupplierCartState(cartState);
      try {
        const rowDate = (current.createdAt || now.toISOString()).slice(0, 10);
        await adjustDailyCartTotal(rowDate, -((Number(current.price) || 0) * Math.max(1, Math.round(Number(current.quantity || 1)))), -Math.max(1, Math.round(Number(current.quantity || 1))));
      } catch (e) { logger.warn("daily_cart_total subtract (cancelled) failed", { key, detail: e?.message || String(e) }); }
      // Notify supplier by email
      if (current.partnerId) {
        try {
          const numericPartnerId = Number(current.partnerId);
          const [emailRows] = await pool.query(
            "SELECT Email FROM Partners WHERE PartnerID = ? LIMIT 1",
            [Number.isFinite(numericPartnerId) && numericPartnerId > 0 ? numericPartnerId : current.partnerId],
          );
          const partnerEmail = cleanText(emailRows?.[0]?.Email || "");
          if (partnerEmail) {
            const escH = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
            const cancelEmailHtml = `<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#333;line-height:1.6">
<p>Здравствуйте!</p>
<p>Отмена товара: <strong>${escH(nextRow.productName || nextRow.offerId || "Неизвестный товар")}</strong>, ${escH(nextRow.quantity || 1)} шт.</p>
<p>Если у вас возникли вопросы, свяжитесь с нами.</p>
<p style="color:#999;font-size:12px">— Magic Vibes Склад</p>
</body></html>`;
            await shopSendEmail({
              to: partnerEmail,
              subject: `Отмена заказа: ${nextRow.productName || nextRow.offerId || ""}`,
              html: cancelEmailHtml,
            });
            nextRow.cancelledNotifiedEmail = partnerEmail;
            state.rows[key] = nextRow;
            await writeSupplierPickingState(state);
            logger.info("manual cancellation email sent", { key, to: partnerEmail, offerId: nextRow.offerId });
          }
        } catch (emailError) {
          logger.warn("manual cancellation email failed", { key, detail: emailError?.message || String(emailError) });
        }
      }
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
    }

    await appendAudit(request, `supplier_picking.${status === "picked" ? "picked" : status === "missing" ? "missing" : status === "returned" ? "returned" : status === "cancelled" ? "cancelled" : "status_update"}`, {
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

// Record that a returned item was physically sent back to the supplier and credit the ledger.
app.post("/api/supplier-picking-list/:key/supplier-return", requireStaff, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key || "");
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    if (current.status !== "returned") {
      return response.status(409).json({ error: "Только строки в статусе «Возврат из ПВЗ» можно вернуть поставщику.", code: "supplier_picking_not_returned", row: current });
    }
    if (current.supplierReturnedAt) {
      return response.status(409).json({ error: "Возврат поставщику уже зафиксирован.", code: "supplier_return_already_exists", row: current });
    }

    const now = new Date();
    const username = requestUsername(request);
    const rawAmount = request.body?.amountRub;
    const amountRub = rawAmount != null ? (normalizeFinanceMoney(rawAmount, 0) || null) : null;
    const note = cleanText(request.body?.note || "Возврат товара поставщику");

    // Create supplier_return ledger entry (credit) if ledger available
    let ledgerEntry = null;
    if (shouldUsePostgresStorage() && current.supplierName) {
      const debtEntry = await getPrisma().supplierLedgerEntry.findFirst({
        where: { pickingKey: key, entryType: "purchase_debt", status: "active" },
      });
      const creditAmount = amountRub != null ? Math.abs(amountRub) : (debtEntry ? Math.abs(Number(debtEntry.amount)) : 0);
      if (creditAmount > 0) {
        const entry = normalizeSupplierLedgerEntry({
          sourceKey: `supplier_return:picking:${key}`,
          entryType: "supplier_return",
          supplierName: current.supplierName,
          partnerId: current.partnerId,
          amount: creditAmount,
          currency: "RUB",
          pickingKey: key,
          financeOrderId: debtEntry?.financeOrderId || null,
          orderId: current.orderId || current.postingNumber || key,
          postingNumber: current.postingNumber,
          offerId: current.offerId,
          productName: current.productName,
          quantity: current.quantity,
          note,
          occurredAt: now.toISOString(),
          createdBy: username,
          raw: { source: "supplier_return_picking", pickingKey: key, debtEntryId: debtEntry?.id || null },
        });
        try {
          const saved = await getPrisma().supplierLedgerEntry.create({
            data: {
              id: entry.id,
              sourceKey: entry.sourceKey,
              entryType: entry.entryType,
              supplierName: entry.supplierName || null,
              partnerId: entry.partnerId || null,
              amount: entry.amount,
              currency: entry.currency,
              pickingKey: entry.pickingKey || null,
              financeOrderId: entry.financeOrderId || null,
              orderId: entry.orderId || null,
              postingNumber: entry.postingNumber || null,
              offerId: entry.offerId || null,
              productName: entry.productName || null,
              quantity: entry.quantity,
              note: entry.note || null,
              status: "active",
              occurredAt: toDateOrNull(entry.occurredAt) || new Date(),
              createdBy: entry.createdBy || null,
              raw: entry.raw,
            },
          });
          suppliersListCache = null;
          ledgerEntry = supplierLedgerEntryFromPostgres(saved);
          await appendAudit(request, "supplier_ledger.supplier_return", {
            entityType: "supplier_ledger",
            entityId: saved.id,
            newValue: ledgerEntry,
          }).catch((e) => logger.warn("supplier return ledger audit failed", { detail: e?.message }));
        } catch (ledgerError) {
          if (ledgerError?.code !== "P2002") throw ledgerError;
          // Already exists — idempotent
          const existing = await getPrisma().supplierLedgerEntry.findUnique({ where: { sourceKey: entry.sourceKey } });
          if (existing) ledgerEntry = supplierLedgerEntryFromPostgres(existing);
        }
      }
    }

    const nextRow = normalizeSupplierPickingRow({
      ...current,
      status: "return_used",
      supplierReturnedBy: username,
      supplierReturnedAt: now.toISOString(),
      supplierReturnAmountRub: amountRub,
    });
    state.rows[key] = nextRow;
    await writeSupplierPickingState(state);

    await appendAudit(request, "supplier_picking.supplier_return", {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: current,
      newValue: nextRow,
      ledgerEntryId: ledgerEntry?.id || null,
    });
    response.json({ ok: true, row: nextRow, ledgerEntry });
  } catch (error) {
    next(error);
  }
});

// ── Employee picking report ──────────────────────────────────────────────────

async function fetchPickerRowsForRange(fromDate, toDate) {
  const prisma = getPrisma();
  if (!prisma) return [];
  const start = new Date(`${fromDate}T00:00:00.000Z`);
  const end = new Date(`${toDate}T23:59:59.999Z`);
  return prisma.supplierPickingRow.findMany({
    where: { status: "picked", pickedAt: { gte: start, lte: end } },
    orderBy: { pickedAt: "asc" },
  });
}

function groupPickerRowsByDate(rows) {
  const byDate = {};
  for (const row of rows) {
    const dateKey = row.pickedAt?.toISOString()?.slice(0, 10) || new Date().toISOString().slice(0, 10);
    if (!byDate[dateKey]) byDate[dateKey] = {};
    const picker = row.pickedBy || "неизвестно";
    if (!byDate[dateKey][picker]) byDate[dateKey][picker] = { username: picker, items: [], totalUsd: 0, count: 0 };
    const price = Number(row.price || 0);
    const qty = Math.max(1, Number(row.quantity || 1));
    const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
    byDate[dateKey][picker].items.push({
      productName: row.productName || "",
      supplierName: row.supplierName || "",
      price,
      priceCurrency: row.priceCurrency || "USD",
      pricePaidRub: raw.pricePaidRub != null ? Number(raw.pricePaidRub) || null : null,
      quantity: qty,
      pickedAt: row.pickedAt?.toISOString() || null,
      marketplace: row.marketplace || "",
      orderId: row.orderId || "",
      postingNumber: row.postingNumber || "",
    });
    byDate[dateKey][picker].totalUsd += price * qty;
    byDate[dateKey][picker].count += qty;
  }
  return Object.entries(byDate)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, pickerMap]) => ({
      date,
      pickers: Object.values(pickerMap)
        .map((p) => ({ ...p, totalUsd: Math.round(p.totalUsd * 100) / 100 }))
        .sort((a, b) => b.count - a.count),
    }));
}

function buildPickerReportSheet(ws, dateStr, pickers, usdRate) {
  const rate = Number(usdRate) || 95;
  const dateLabel = new Date(`${dateStr}T12:00:00Z`).toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" });

  // Items collapse under supplier rows — summary above detail rows
  ws.properties.outlineProperties = { summaryBelow: false };

  const titleRow = ws.addRow([`Отчёт оплат за ${dateLabel}  (курс ${rate} ₽/$)`]);
  titleRow.getCell(1).font = { bold: true, size: 14 };
  const noteRow = ws.addRow(["* — сумма оплаты не введена, указана расчётная цена PM"]);
  noteRow.getCell(1).font = { italic: true, color: { argb: "FF999999" }, size: 10 };
  ws.addRow([]);

  // cols: Поставщик | Товар | Маркетплейс | Заказ | Цена ₽ | Цена $ | Кол-во | Время
  const COL_SUPPLIER = 1;
  const COL_PRODUCT  = 2;
  const COL_MP       = 3;
  const COL_ORDER    = 4;
  const COL_RUB      = 5;
  const COL_USD      = 6;
  const COL_QTY      = 7;
  const COL_TIME     = 8;

  let irinaTotalRub = 0;
  let irinaTotalUsd = 0;
  let irinaCount = 0;
  let irinaFound = false;

  for (const picker of pickers) {
    const pickerHeaderRow = ws.addRow([`Сотрудник: ${picker.username}`]);
    pickerHeaderRow.getCell(1).font = { bold: true, size: 12 };
    pickerHeaderRow.getCell(1).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFD6E4FF" } };
    pickerHeaderRow.outlineLevel = 0;

    const colHeaderRow = ws.addRow(["Поставщик", "Товар", "Маркетплейс", "Заказ / Отправление", "Оплата ₽", "Оплата $", "Кол-во", "Время"]);
    colHeaderRow.eachCell((cell) => {
      cell.font = { bold: true };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFEEF3FF" } };
      cell.border = { bottom: { style: "thin", color: { argb: "FFADC6FF" } } };
    });

    // Group items by supplier — pricePaidRub (actual payment) takes priority over PM price
    const supplierMap = new Map();
    for (const item of picker.items) {
      const sName = item.supplierName || "— поставщик не указан —";
      const currency = String(item.priceCurrency || "USD").toUpperCase();
      if (!supplierMap.has(sName)) supplierMap.set(sName, { name: sName, currency, items: [], totalRub: 0, totalUsd: 0, count: 0 });
      const se = supplierMap.get(sName);
      let priceRub = 0;
      let priceUsd = 0;
      let isPaid = false;
      if (item.pricePaidRub) {
        // Actual payment entered by employee — primary source
        priceRub = item.pricePaidRub;
        priceUsd = item.pricePaidRub / rate;
        isPaid = true;
      } else if (item.price > 0) {
        // Fallback to PM price when no payment was entered
        if (currency === "RUB") { priceRub = item.price; priceUsd = item.price / rate; }
        else { priceUsd = item.price; priceRub = item.price * rate; }
      }
      se.totalRub += priceRub * item.quantity;
      se.totalUsd += priceUsd * item.quantity;
      se.count += item.quantity;
      se.items.push({ ...item, priceRub, priceUsd, isPaid });
    }

    const suppliers = [...supplierMap.values()].sort((a, b) => b.totalRub - a.totalRub);
    let sumRub = 0;
    let sumUsd = 0;

    for (const se of suppliers) {
      sumRub += se.totalRub;
      sumUsd += se.totalUsd;

      // Supplier summary row (visible, outlineLevel=0 — has +/- expand button)
      const supRow = ws.addRow([
        se.name, "", "", "",
        `${Math.round(se.totalRub).toLocaleString("ru-RU")} ₽`,
        `${(Math.round(se.totalUsd * 100) / 100).toLocaleString("ru-RU", { maximumFractionDigits: 2 })} $`,
        se.count, "",
      ]);
      supRow.outlineLevel = 0;
      supRow.getCell(COL_SUPPLIER).font = { bold: true };
      supRow.getCell(COL_RUB).font = { bold: true };
      supRow.getCell(COL_USD).font = { bold: true };
      supRow.eachCell({ includeEmpty: true }, (cell) => {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF0F4FF" } };
      });

      // Item detail rows (outlineLevel=1, hidden=true → collapsed by default)
      for (const item of se.items) {
        const time = item.pickedAt
          ? new Date(item.pickedAt).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })
          : "—";
        const rubStr = item.priceRub ? `${Math.round(item.priceRub).toLocaleString("ru-RU")} ₽${item.isPaid ? "" : " *"}` : "—";
        const usdStr = item.priceUsd ? `${(Math.round(item.priceUsd * 100) / 100).toLocaleString("ru-RU", { maximumFractionDigits: 2 })} $${item.isPaid ? "" : " *"}` : "—";
        const orderRef = item.postingNumber || item.orderId || "—";
        const itemRow = ws.addRow(["", item.productName, item.marketplace.toUpperCase(), orderRef, rubStr, usdStr, item.quantity, time]);
        itemRow.outlineLevel = 1;
        itemRow.hidden = true;
        itemRow.eachCell({ includeEmpty: true }, (cell) => {
          cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: item.isPaid ? "FFFAFBFF" : "FFFFF8F0" } };
          cell.font = { size: 11, italic: !item.isPaid, color: item.isPaid ? undefined : { argb: "FF999999" } };
        });
      }
    }

    const totalRow = ws.addRow([
      "ИТОГО", "", "", "",
      `${Math.round(sumRub).toLocaleString("ru-RU")} ₽`,
      `${(Math.round(sumUsd * 100) / 100).toLocaleString("ru-RU", { maximumFractionDigits: 2 })} $`,
      picker.count, "",
    ]);
    totalRow.outlineLevel = 0;
    totalRow.eachCell((cell, col) => {
      if (col >= 1) cell.font = { bold: true };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFAF0FF" } };
    });
    ws.addRow([]);

    if (picker.username.toLowerCase().includes("ирин")) {
      irinaTotalRub += sumRub;
      irinaTotalUsd += sumUsd;
      irinaCount += picker.count;
      irinaFound = true;
    }
  }

  if (irinaFound) {
    const irinaHeaderRow = ws.addRow([`★ Итого ИРИНА за ${dateLabel}:`]);
    irinaHeaderRow.getCell(1).font = { bold: true, size: 12, color: { argb: "FF1A3A8F" } };
    irinaHeaderRow.getCell(1).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFCFE2FF" } };
    const irinaTotalRow = ws.addRow([
      "ИТОГО", "", "", "",
      `${Math.round(irinaTotalRub).toLocaleString("ru-RU")} ₽`,
      `${(Math.round(irinaTotalUsd * 100) / 100).toLocaleString("ru-RU", { maximumFractionDigits: 2 })} $`,
      irinaCount, "",
    ]);
    irinaTotalRow.eachCell((cell, col) => {
      if (col >= 1) cell.font = { bold: true };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFCFE2FF" } };
    });
  }

  ws.getColumn(COL_SUPPLIER).width = 28;
  ws.getColumn(COL_PRODUCT).width = 40;
  ws.getColumn(COL_MP).width = 14;
  ws.getColumn(COL_ORDER).width = 22;
  ws.getColumn(COL_RUB).width = 14;
  ws.getColumn(COL_USD).width = 12;
  ws.getColumn(COL_QTY).width = 8;
  ws.getColumn(COL_TIME).width = 7;
}

async function buildPickerExcel(dateRows) {
  const ExcelJS = require("exceljs");
  const wb = new ExcelJS.Workbook();
  wb.creator = "DavidSklad";
  wb.created = new Date();

  const ratePayload = await getUsdRate().catch(() => null);
  const usdRate = Number(ratePayload?.rate || process.env.DEFAULT_USD_RATE || 95) || 95;

  for (const { date, pickers } of dateRows) {
    if (!pickers.length) continue;
    // Sheet name: "07.09.26" (8 chars, Excel limit 31)
    const d = new Date(`${date}T12:00:00Z`);
    const sheetName = d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit" });
    const ws = wb.addWorksheet(sheetName);
    buildPickerReportSheet(ws, date, pickers, usdRate);
  }

  if (!wb.worksheets.length) {
    const ws = wb.addWorksheet("Нет данных");
    ws.addRow(["Нет собранных позиций за выбранный период."]);
  }

  return wb;
}

// Single-day export
app.get("/api/picker-report/export", requireAdmin, async (request, response, next) => {
  try {
    const rawDate = cleanText(request.query.date || "");
    const dateStr = /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : new Date().toISOString().slice(0, 10);
    const rows = await fetchPickerRowsForRange(dateStr, dateStr);
    const dateRows = groupPickerRowsByDate(rows);
    const wb = await buildPickerExcel(dateRows.length ? dateRows : [{ date: dateStr, pickers: [] }]);
    const d = new Date(`${dateStr}T12:00:00Z`);
    const dateLabel = d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" }).replace(/\./g, "-");
    response.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    response.setHeader("Content-Disposition", `attachment; filename="picker-report-${dateLabel}.xlsx"`);
    await wb.xlsx.write(response);
    response.end();
  } catch (error) {
    next(error);
  }
});

// Multi-sheet export (one sheet per date)
app.get("/api/picker-report/export/full", requireAdmin, async (request, response, next) => {
  try {
    const rawFrom = cleanText(request.query.from || "");
    const rawTo = cleanText(request.query.to || "");
    const today = new Date().toISOString().slice(0, 10);
    const toDate = /^\d{4}-\d{2}-\d{2}$/.test(rawTo) ? rawTo : today;
    // Default: last 30 days
    const defaultFrom = new Date(Date.now() - 29 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const fromDate = /^\d{4}-\d{2}-\d{2}$/.test(rawFrom) ? rawFrom : defaultFrom;
    const rows = await fetchPickerRowsForRange(fromDate, toDate);
    const dateRows = groupPickerRowsByDate(rows);
    const wb = await buildPickerExcel(dateRows);
    const fromLabel = fromDate.replace(/-/g, "");
    const toLabel = toDate.replace(/-/g, "");
    response.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    response.setHeader("Content-Disposition", `attachment; filename="picker-report-${fromLabel}-${toLabel}.xlsx"`);
    await wb.xlsx.write(response);
    response.end();
  } catch (error) {
    next(error);
  }
});

app.get("/api/picker-report", requireAdmin, async (request, response, next) => {
  try {
    const rawDate = cleanText(request.query.date || "");
    const dateStr = /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? rawDate : new Date().toISOString().slice(0, 10);
    const start = new Date(`${dateStr}T00:00:00.000Z`);
    const end = new Date(`${dateStr}T23:59:59.999Z`);
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, date: dateStr, pickers: [], summary: null });

    const ratePayload = await getUsdRate().catch(() => null);
    const usdRate = Number(ratePayload?.rate || process.env.DEFAULT_USD_RATE || 95) || 95;

    const rows = await prisma.supplierPickingRow.findMany({
      where: { status: "picked", pickedAt: { gte: start, lte: end } },
      orderBy: { pickedAt: "asc" },
    });
    const byPicker = {};
    let totalPickedRub = 0;
    let totalPaidRub = 0;
    let totalUnpaidCount = 0;
    for (const row of rows) {
      const picker = row.pickedBy || "неизвестно";
      if (!byPicker[picker]) byPicker[picker] = { username: picker, items: [], totalUsd: 0, count: 0, paidTotalRub: 0, unpaidCount: 0 };
      const price = Number(row.price || 0);
      const qty = Math.max(1, Number(row.quantity || 1));
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      const pricePaidRub = raw.pricePaidRub != null ? Number(raw.pricePaidRub) || null : null;
      byPicker[picker].items.push({
        key: row.pickingKey,
        productName: row.productName || "",
        supplierName: row.supplierName || "",
        price,
        priceCurrency: row.priceCurrency || "USD",
        pricePaidRub,
        quantity: qty,
        pickedAt: row.pickedAt?.toISOString() || null,
        marketplace: row.marketplace || "",
        orderId: row.orderId || "",
        postingNumber: row.postingNumber || "",
      });
      byPicker[picker].totalUsd += price * qty;
      byPicker[picker].count += qty;
      if (pricePaidRub) {
        totalPickedRub += pricePaidRub * qty;
        totalPaidRub += pricePaidRub * qty;
        byPicker[picker].paidTotalRub += pricePaidRub * qty;
      } else {
        if (price > 0) {
          totalPickedRub += ((row.priceCurrency || "USD") === "RUB" ? price : price * usdRate) * qty;
        }
        totalUnpaidCount += qty;
        byPicker[picker].unpaidCount += qty;
      }
    }
    const pickers = Object.values(byPicker)
      .map((p) => ({ ...p, totalUsd: Math.round(p.totalUsd * 100) / 100, paidTotalRub: Math.round(p.paidTotalRub) }))
      .sort((a, b) => b.count - a.count);

    const totalPickedUsd = pickers.reduce((s, p) => s + p.totalUsd, 0);
    const totalPickedCount = pickers.reduce((s, p) => s + p.count, 0);

    // Supplier returns for the day (supplierReturnedAt stored in raw JSON field)
    let totalReturnedRub = 0;
    let totalReturnedCount = 0;
    try {
      const returnRows = await prisma.$queryRawUnsafe(`
        SELECT COALESCE(CAST(raw->>'supplierReturnAmountRub' AS NUMERIC), 0) AS amount_rub
        FROM supplier_picking_rows
        WHERE status = 'return_used'
          AND raw->>'supplierReturnedAt' IS NOT NULL
          AND raw->>'supplierReturnedAt' >= $1
          AND raw->>'supplierReturnedAt' <= $2
      `, start.toISOString(), end.toISOString());
      for (const r of returnRows) {
        totalReturnedRub += Number(r.amount_rub) || 0;
        totalReturnedCount++;
      }
    } catch {
      // summary is optional — don't fail the whole report
    }

    response.json({
      ok: true, date: dateStr, pickers,
      summary: {
        totalPickedRub: Math.round(totalPickedRub),
        totalPickedUsd: Math.round(totalPickedUsd * 100) / 100,
        totalPickedCount,
        totalPaidRub: Math.round(totalPaidRub),
        totalUnpaidCount,
        totalReturnedRub: Math.round(totalReturnedRub),
        totalReturnedCount,
        usdRate,
      },
    });
  } catch (error) {
    next(error);
  }
});

// ── Picking list viewers (coordination heartbeat) ────────────────────────────
// Stores {username, viewedAt, currentSupplier} in Redis with 45s TTL. GET returns all active viewers.
app.put("/api/supplier-picking-list/heartbeat", requireStaff, async (request, response, next) => {
  try {
    const username = requestUsername(request) || "неизвестно";
    const currentSupplier = cleanText(request.body?.currentSupplier || "");
    const redis = pickingRedis();
    if (!redis) return response.json({ ok: true });
    await redis.set(`picking:viewer:${username}`, JSON.stringify({ username, viewedAt: new Date().toISOString(), currentSupplier: currentSupplier || null }), "EX", 45);
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

