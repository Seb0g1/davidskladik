app.get("/api/supplier-picking-list/invoices", requireStaff, async (request, response, next) => {
  try {
    const state = await readSupplierPickingState();
    const period = cleanText(request.query.period || "30d").toLowerCase();
    const rows = supplierPickingInvoiceRows(state, period);
    const groups = [];
    const bySupplierDate = new Map();
    for (const row of rows) {
      const date = String(row.pickedAt || row.createdAt || "").slice(0, 10) || "unknown";
      const key = `${row.supplierName || "-"}|${date}`;
      if (!bySupplierDate.has(key)) bySupplierDate.set(key, { supplierName: row.supplierName || "-", date, rows: [], totalQuantity: 0, totalValue: 0 });
      const group = bySupplierDate.get(key);
      group.rows.push(row);
      group.totalQuantity += Number(row.quantity || 0);
      group.totalValue += Number(row.quantity || 0) * Number(row.price || 0);
    }
    groups.push(...bySupplierDate.values());
    response.json({
      ok: true,
      period,
      total: rows.length,
      groups,
      rows,
    });
  } catch (error) {
    next(error);
  }
});
