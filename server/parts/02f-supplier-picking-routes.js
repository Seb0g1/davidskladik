// «Насовсем» = 10 лет: инактив без автоснятия; убрать можно вручную в карточке товара
// («Отложить поставщика» → отмена) или вернув строку сборки в статус «к сборке».
const PICKING_PERMANENT_SNOOZE_DAYS = 3650;

app.get("/api/supplier-picking-list", requireStaff, async (request, response, next) => {
  try {
    const state = await readSupplierPickingState();
    const status = cleanText(request.query.status || "open").toLowerCase();
    const supplier = cleanText(request.query.supplier).toLowerCase();
    const q = cleanText(request.query.q).toLowerCase();
    const limit = cleanLimit(request.query.limit, 500);
    let rows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    if (status && status !== "all") rows = rows.filter((row) => row.status === status);
    if (supplier) rows = rows.filter((row) => cleanText(row.supplierName).toLowerCase().includes(supplier));
    if (q) {
      rows = rows.filter((row) => [
        row.productName,
        row.offerId,
        row.orderId,
        row.postingNumber,
        row.supplierName,
      ].some((value) => cleanText(value).toLowerCase().includes(q)));
    }
    rows.sort(compareSupplierPickingRows);
    const allRows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    const suppliers = Array.from(new Set(allRows.map((row) => row.supplierName).filter(Boolean))).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    const supplierLedgerMap = await supplierLedgerSummaryMapForSuppliers(suppliers.map((name) => ({ id: name, name })));
    const supplierLedger = Object.fromEntries(suppliers.map((name) => [name, supplierLedgerMap.get(name) || supplierLedgerSummaryFromEntries([])]));
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      rows: rows.slice(0, limit),
      total: rows.length,
      suppliers,
      supplierLedger,
      summary: {
        open: allRows.filter((row) => row.status === "open").length,
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
    if (!["open", "picked", "missing"].includes(status)) {
      return response.status(400).json({ error: "Unsupported picking status.", code: "supplier_picking_status_invalid" });
    }
    if (status === "open" && !admin) {
      return response.status(403).json({ error: "Only admin can roll back picking rows.", code: "admin_required" });
    }
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    if (current.status !== "open" && status !== "open") {
      return response.status(409).json({
        error: "Picking row is already finalized.",
        code: "supplier_picking_finalized",
        row: current,
      });
    }
    const now = new Date();
    const username = requestUsername(request);
    // «Не было»: сотрудник выбирает срок инактива поставщика — 1 (завтра появится), 2, 3, 5 дней
    // или насовсем (permanent). Без явного выбора действует прежний срок 7 дней.
    const missingPermanent = status === "missing" && request.body?.permanent === true;
    const requestedSnoozeDays = Math.round(Number(request.body?.snoozeDays || 0) || 0);
    const missingDays = missingPermanent
      ? PICKING_PERMANENT_SNOOZE_DAYS
      : (requestedSnoozeDays >= 1 ? Math.min(60, requestedSnoozeDays) : 7);
    const missingRetryAt = missingPermanent ? null : new Date(now.getTime() + missingDays * 24 * 60 * 60 * 1000).toISOString();
    const nextRow = normalizeSupplierPickingRow({
      ...current,
      status,
      ...(status === "picked" ? { pickedBy: username, pickedAt: now.toISOString() } : {}),
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
        missingBy: "",
        missingAt: null,
        missingReason: "",
        missingSnoozeDays: 0,
        missingPermanent: false,
        missingSnoozeLinkId: "",
        nextRetryAt: null,
      } : {}),
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
      await appendAudit(request, "supplier_cart.supplier_blocked", {
        entityType: "supplier_cart",
        entityId: blockKey,
        newValue: { ...cartState.supplierBlocks[blockKey], linkSnooze },
      });
    } else if (status === "open") {
      if (current.status === "missing") await deactivateSupplierBlockForPickingRow(current, request);
      if (current.requestRowId || current.requestDocId) await restoreSupplierCartProcessedForPickingRow(current, request);
    }

    let financeOrder = null;
    let supplierLedgerEntry = null;
    let stockRecovery = null;
    if (status === "picked") {
      financeOrder = await upsertFinanceOrderFromPickingRow(nextRow, request);
      supplierLedgerEntry = await upsertSupplierLedgerDebtFromPickingRow(nextRow, financeOrder, request);
      if (nextRow.warehouseProductId) {
        const productIds = [nextRow.warehouseProductId];
        const freshProducts = await buildFreshWarehouseProducts(productIds, { livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: autoPricePmTimeoutMs })
          .catch((error) => {
            logger.warn("supplier picking stock restore build failed", { detail: error?.message || String(error), productIds });
            return [];
          });
        const restoreProducts = freshProducts.map((product) => ({
          ...product,
          targetStock: Math.max(1, Math.round(Number(product.targetStock || 0)), Math.round(Number(nextRow.quantity || 1) || 1)),
        }));
        if (restoreProducts.length) {
          stockRecovery = await runSupplierRecoveryAutomation({ products: restoreProducts }, {
            productIds,
            force: true,
            source: "supplier_picking_picked",
            sourceEvent: "supplier_picking_stock_restore",
          }).catch((error) => {
            logger.warn("supplier picking stock restore failed", { detail: error?.message || String(error), productIds });
            return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [{ error: error?.message || String(error) }] };
          });
        }
      }
    } else if (current.status === "picked") {
      await removeFinanceOrderForPickingRow(current);
      supplierLedgerEntry = await voidSupplierLedgerDebtForPickingRow(current, request);
    }

    await appendAudit(request, `supplier_picking.${status === "picked" ? "picked" : status === "missing" ? "missing" : "status_update"}`, {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: current,
      newValue: nextRow,
      financeOrderId: financeOrder?.id || null,
      supplierLedgerEntryId: supplierLedgerEntry?.id || null,
      stockRecovery,
    });
    response.json({ ok: true, row: nextRow, financeOrder, supplierLedgerEntry, stockRecovery, linkSnooze });
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

