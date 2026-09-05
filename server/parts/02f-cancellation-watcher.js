// Background watcher: detects marketplace order cancellations and notifies suppliers

async function getPickingRowPartnerEmail(partnerId) {
  if (!partnerId) return null;
  try {
    const numericId = Number(partnerId);
    const [rows] = await pool.query(
      "SELECT Email FROM Partners WHERE PartnerID = ? LIMIT 1",
      [Number.isFinite(numericId) && numericId > 0 ? numericId : partnerId],
    );
    const email = cleanText(rows?.[0]?.Email || "");
    return email || null;
  } catch (e) {
    logger.warn("cancellation watcher: partner email lookup failed", { partnerId, detail: e?.message || String(e) });
    return null;
  }
}

function cancellationEmailHtml(row) {
  const productName = row.productName || row.offerId || "Неизвестный товар";
  const quantity = row.quantity || 1;
  const marketplaceLabel = { ozon: "Ozon", yandex: "Яндекс Маркет", wb: "Wildberries" }[row.marketplace] || row.marketplace || "маркетплейс";
  const orderId = row.postingNumber || row.orderId || "";
  return `
<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#333;line-height:1.6">
<p>Здравствуйте!</p>
<p>Покупатель отменил заказ на маркетплейсе <strong>${marketplaceLabel}</strong>.</p>
<p>Пожалуйста, <strong>не собирайте и не отправляйте</strong> следующую позицию:</p>
<table style="border-collapse:collapse;margin:12px 0">
  <tr><td style="padding:4px 12px 4px 0;color:#666">Товар:</td><td style="padding:4px 0"><strong>${productName}</strong></td></tr>
  <tr><td style="padding:4px 12px 4px 0;color:#666">Количество:</td><td style="padding:4px 0">${quantity} шт.</td></tr>
  ${orderId ? `<tr><td style="padding:4px 12px 4px 0;color:#666">Номер заказа:</td><td style="padding:4px 0">${orderId}</td></tr>` : ""}
</table>
<p>Заявка автоматически аннулирована в системе сборки Magic Vibes.</p>
<p>Если у вас возникли вопросы, свяжитесь с нами.</p>
<p style="color:#999;font-size:12px">— Magic Vibes Склад</p>
</body></html>`.trim();
}

async function markPickingRowCancelledBySystem(key, { cancelledBy = "system", notifiedEmail = "" } = {}) {
  const state = await readSupplierPickingState();
  const row = state.rows[key];
  if (!row || row.status !== "open") return null;
  const now = new Date().toISOString();
  const nextRow = normalizeSupplierPickingRow({
    ...row,
    status: "cancelled",
    cancelledBy,
    cancelledAt: now,
    cancelledNotifiedEmail: notifiedEmail,
  });
  state.rows[key] = nextRow;
  await writeSupplierPickingState(state);

  try {
    const cartState = await readSupplierCartState();
    const sourceCartKey = row.replacementFor || row.key.replace(/\|retry:.+$/, "");
    if (cartState.processed?.[sourceCartKey]) {
      delete cartState.processed[sourceCartKey];
      await writeSupplierCartState(cartState);
    }
  } catch (e) {
    logger.warn("cancellation watcher: cart state cleanup failed", { key, detail: e?.message || String(e) });
  }

  try {
    const rowDate = (row.createdAt || now).slice(0, 10);
    await adjustDailyCartTotal(
      rowDate,
      -((Number(row.price) || 0) * Math.max(1, Math.round(Number(row.quantity || 1)))),
      -Math.max(1, Math.round(Number(row.quantity || 1))),
    );
  } catch (e) {
    logger.warn("cancellation watcher: daily cart total deduction failed", { key, detail: e?.message || String(e) });
  }

  return nextRow;
}

async function checkAndHandleCancelledOrders() {
  const pickingState = await readSupplierPickingState();
  const openRows = Object.values(pickingState.rows).filter((r) => r.status === "open");
  if (!openRows.length) return;

  const byPostingNumber = new Map();
  const byOrderId = new Map();
  for (const row of openRows) {
    if (row.marketplace === "ozon" && row.postingNumber) {
      byPostingNumber.set(row.postingNumber, row);
    } else if (row.orderId) {
      byOrderId.set(`${row.marketplace}:${row.orderId}`, row);
    }
  }

  const cancelledRows = [];
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  const to = new Date(Date.now() + 24 * 60 * 60 * 1000);

  // Ozon: poll cancelled postings
  if (byPostingNumber.size) {
    try {
      const ozonAccounts = getOzonAccounts();
      for (const account of ozonAccounts) {
        let offset = 0;
        while (true) {
          const data = await ozonRequest("/v3/posting/fbs/list", {
            dir: "ASC",
            filter: { since: since.toISOString(), to: to.toISOString(), status: "cancelled" },
            limit: 1000,
            offset,
            with: { analytics_data: false, financial_data: false },
          }, account);
          const postings = Array.isArray(data?.result?.postings) ? data.result.postings : [];
          for (const posting of postings) {
            const pn = cleanText(posting.posting_number || "");
            if (pn && byPostingNumber.has(pn)) cancelledRows.push(byPostingNumber.get(pn));
          }
          if (postings.length < 1000) break;
          offset += postings.length;
        }
      }
    } catch (e) {
      logger.warn("cancellation watcher: Ozon poll failed", { detail: e?.message || String(e) });
    }
  }

  // Yandex Market: poll cancelled orders
  const hasYandexRows = openRows.some((r) => r.marketplace === "yandex" && r.orderId);
  if (hasYandexRows) {
    try {
      const shops = uniqueYandexShopsByBusiness();
      for (const shop of shops) {
        let pageToken = "";
        while (true) {
          const query = new URLSearchParams({ limit: "50" });
          if (pageToken) query.set("pageToken", pageToken);
          const campaignIds = parseYandexCampaignIds(shop.campaignId).map(Number).filter((id) => Number.isFinite(id) && id > 0);
          const data = await yandexRequest(shop, "POST", `/v1/businesses/${shop.businessId}/orders?${query.toString()}`, {
            ...(campaignIds.length ? { campaignIds } : {}),
            statuses: ["CANCELLED"],
            dates: { fromDate: since.toISOString().slice(0, 10), toDate: to.toISOString().slice(0, 10) },
            fake: false,
            sourcePlatforms: ["MARKET"],
          });
          const orders = Array.isArray(data?.orders) ? data.orders : (Array.isArray(data?.result?.orders) ? data.result.orders : []);
          for (const order of orders) {
            const orderId = String(order.id || "");
            const lookupKey = `yandex:${orderId}`;
            if (orderId && byOrderId.has(lookupKey)) cancelledRows.push(byOrderId.get(lookupKey));
          }
          pageToken = cleanText(data?.paging?.nextPageToken || data?.result?.paging?.nextPageToken || "");
          if (!pageToken) break;
        }
      }
    } catch (e) {
      logger.warn("cancellation watcher: Yandex poll failed", { detail: e?.message || String(e) });
    }
  }

  // Wildberries: poll cancelled orders
  const hasWbRows = openRows.some((r) => r.marketplace === "wb" && r.orderId);
  if (hasWbRows) {
    try {
      const wbAccounts = getWbAccounts();
      for (const account of wbAccounts) {
        const data = await wbRequest(account, "marketplace", "GET", "/api/v3/orders/cancelled");
        const orders = Array.isArray(data?.orders) ? data.orders : [];
        for (const order of orders) {
          const orderId = String(order.id || "");
          const lookupKey = `wb:${orderId}`;
          if (orderId && byOrderId.has(lookupKey)) cancelledRows.push(byOrderId.get(lookupKey));
        }
      }
    } catch (e) {
      logger.warn("cancellation watcher: WB poll failed", { detail: e?.message || String(e) });
    }
  }

  // Deduplicate
  const seen = new Set();
  const unique = cancelledRows.filter((r) => { if (seen.has(r.key)) return false; seen.add(r.key); return true; });

  for (const row of unique) {
    try {
      let notifiedEmail = "";
      if (row.partnerId) {
        const partnerEmail = await getPickingRowPartnerEmail(row.partnerId);
        if (partnerEmail) {
          notifiedEmail = partnerEmail;
          await shopSendEmail({
            to: partnerEmail,
            subject: `Отмена заказа: ${row.productName || row.offerId || ""}`,
            html: cancellationEmailHtml(row),
          });
          logger.info("cancellation email sent", { key: row.key, to: partnerEmail, offerId: row.offerId });
        }
      }
      await markPickingRowCancelledBySystem(row.key, { cancelledBy: "system:marketplace_poll", notifiedEmail });
      logger.info("picking row auto-cancelled by marketplace poll", { key: row.key, marketplace: row.marketplace, emailSent: Boolean(notifiedEmail) });
    } catch (e) {
      logger.warn("cancellation watcher: failed to process row cancellation", { key: row.key, detail: e?.message || String(e) });
    }
  }
}

if (process.env.BACKGROUND_JOBS_ENABLED === "true") {
  let _cancelWatcherRunning = false;
  const _cancelWatcherIntervalMs = 15 * 60 * 1000;
  const _cancelWatcherTick = async () => {
    if (!_cancelWatcherRunning) {
      _cancelWatcherRunning = true;
      await checkAndHandleCancelledOrders().catch((e) => {
        logger.warn("cancellation watcher: tick failed", { detail: e?.message || String(e) });
      });
      _cancelWatcherRunning = false;
    }
    setTimeout(_cancelWatcherTick, _cancelWatcherIntervalMs);
  };
  setTimeout(_cancelWatcherTick, 5 * 60 * 1000);
}
