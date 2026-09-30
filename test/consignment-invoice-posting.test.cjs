"use strict";

// Posting / unposting consignment invoices (server/parts/02d-consignment-routes.js) against an
// in-memory stand-in for the Prisma transaction client.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-consignment-routes.js"), "utf8");
const noop = () => {};
const ctx = vm.createContext({
  process: { env: {} },
  console,
  app: { get: noop, post: noop, patch: noop, put: noop, delete: noop },
  requireAdmin: noop,
  cleanText: (value) => String(value ?? "").trim(),
  normalizeFinanceMoney: (value, fallback = 0) => {
    const n = Number(value ?? fallback);
    return Number.isFinite(n) ? Number(n.toFixed(2)) : Number(fallback || 0);
  },
});
vm.runInContext(`${source}
this.api = { postConsignmentInvoice, unpostConsignmentInvoice, consignmentSummaryFromRows, consignmentOperationFromPostgres, consignmentItemFromPostgres };`, ctx);
const { postConsignmentInvoice: post, unpostConsignmentInvoice: unpost, consignmentSummaryFromRows: summarize, consignmentOperationFromPostgres: opRow, consignmentItemFromPostgres: itemRow } = ctx.api;

let seq = 0;
function fakeDb({ invoice, meta, lines, items = [] }) {
  const db = { invoice: { ...invoice }, meta: { ...meta }, lines: lines.map((l) => ({ ...l })), items: items.map((i) => ({ ...i })), ops: [] };
  const matches = (row, where = {}) => Object.entries(where).every(([key, cond]) => {
    if (key === "OR") return cond.some((alt) => matches(row, alt));
    if (key === "raw") return row.raw?.[cond.path[0]] === cond.equals;
    if (cond && typeof cond === "object" && "in" in cond) return cond.in.includes(row[key]);
    if (cond && typeof cond === "object" && "equals" in cond) return String(row[key]).toLowerCase() === String(cond.equals).toLowerCase();
    return row[key] === cond;
  });
  db.tx = {
    consignmentInvoice: { findUnique: async () => ({ ...db.invoice, items: db.lines.map((l) => ({ ...l })) }) },
    consignmentInvoiceItem: { update: async ({ where, data }) => Object.assign(db.lines.find((l) => l.id === where.id), data) },
    consignmentItem: {
      findUnique: async ({ where }) => db.items.find((i) => i.id === where.id) || null,
      findFirst: async ({ where }) => db.items.find((i) => matches(i, where)) || null,
      create: async ({ data }) => { const item = { id: `item${++seq}`, archived: false, ...data }; db.items.push(item); return item; },
      update: async ({ where, data }) => {
        const item = db.items.find((i) => i.id === where.id);
        for (const [key, value] of Object.entries(data)) {
          if (value && typeof value === "object" && "increment" in value) item[key] += value.increment;
          else if (value && typeof value === "object" && "decrement" in value) item[key] -= value.decrement;
          else item[key] = value;
        }
        return item;
      },
    },
    consignmentOperation: {
      create: async ({ data }) => { const op = { id: `op${++seq}`, status: "active", ...data }; db.ops.push(op); return op; },
      findMany: async ({ where }) => db.ops.filter((op) => matches(op, where)),
      deleteMany: async ({ where }) => { db.ops = db.ops.filter((op) => !where.id.in.includes(op.id)); },
    },
    $queryRawUnsafe: async () => [{ id: db.invoice.id, ...db.meta }],
    $executeRawUnsafe: async (sql) => {
      if (/status = 'posted'/.test(sql)) db.meta.status = "posted";
      if (/status = 'draft'/.test(sql)) db.meta.status = "draft";
    },
  };
  return db;
}

const balance = (db) => summarize(db.items.map(itemRow), db.ops.map(opRow)).balance;

test("posting a «с баланса» invoice adds stock and takes the money off the balance; unposting undoes both", async () => {
  const db = fakeDb({
    invoice: { id: "inv1", number: "ПН-011" },
    meta: { status: "draft", fromBalance: true },
    lines: [{ id: "l1", itemId: null, name: "LV Imagination 100 ml", article: "pm:1", quantity: 3, unitPrice: 400 }],
    items: [{ id: "a", name: "LV Imagination 100 ml", article: "pm:1", quantity: 2, purchasePrice: 380, salePrice: 500, archived: false }],
  });
  assert.equal((await post(db.tx, "inv1", "david")).ok, true);
  assert.equal(db.items[0].quantity, 5);
  assert.equal(db.items[0].purchasePrice, 400);
  assert.equal(db.ops.length, 1);
  assert.equal(db.ops[0].type, "purchase");
  assert.equal(db.ops[0].raw.invoiceId, "inv1");
  assert.equal(balance(db), -1200);

  const result = await unpost(db.tx, "inv1");
  assert.equal(result.ok, true);
  assert.equal(db.items[0].quantity, 2);
  assert.equal(db.ops.length, 0);
  assert.equal(balance(db), 0);
  assert.equal(db.meta.status, "draft");
});

test("an invoice whose goods were already sold cannot be unposted", async () => {
  const db = fakeDb({
    invoice: { id: "inv2", number: "ПН-012" },
    meta: { status: "draft", fromBalance: false },
    lines: [{ id: "l1", itemId: null, name: "Dancing Blossom", article: null, quantity: 3, unitPrice: 750 }],
  });
  await post(db.tx, "inv2", "david");
  db.items[0].quantity = 1; // two sold since
  const result = await unpost(db.tx, "inv2");
  assert.equal(result.failure.code, "consignment_invoice_stock_used");
  assert.match(result.failure.error, /Dancing Blossom: на складе 1 шт, по накладной 3 шт/);
  assert.equal(db.ops.length, 1);
  assert.equal(db.items[0].quantity, 1);
});

test("a draft edited after unposting is posted again from its new lines", async () => {
  const db = fakeDb({
    invoice: { id: "inv3", number: "ПН-013" },
    meta: { status: "draft", fromBalance: false },
    lines: [{ id: "l1", itemId: null, name: "Pacific Chill", article: null, quantity: 4, unitPrice: 430 }],
  });
  await post(db.tx, "inv3", "david");
  await unpost(db.tx, "inv3");
  db.lines = [{ id: "l2", itemId: db.items[0].id, name: "Pacific Chill", article: null, quantity: 5, unitPrice: 420 }];
  db.meta.fromBalance = true;
  await post(db.tx, "inv3", "david");
  assert.equal(db.items[0].quantity, 5);
  assert.equal(db.ops.length, 1);
  assert.equal(db.ops[0].type, "purchase");
  assert.equal(balance(db), -2100);
});

test("legacy operations linked only by the note are unposted too", async () => {
  const db = fakeDb({
    invoice: { id: "inv4", number: "ПН-004" },
    meta: { status: "posted", fromBalance: false },
    lines: [],
    items: [{ id: "a", name: "X", quantity: 6, purchasePrice: 1, salePrice: 1, archived: false }],
  });
  db.ops.push({ id: "old", type: "receive", itemId: "a", quantity: 6, note: "Накладная ПН-004", raw: null, balanceDelta: 0, sponsorDelta: 0, myDelta: 0 });
  db.ops.push({ id: "other", type: "receive", itemId: "a", quantity: 1, note: "Накладная ПН-004", raw: { invoiceId: "someone-else" }, balanceDelta: 0, sponsorDelta: 0, myDelta: 0 });
  const result = await unpost(db.tx, "inv4");
  assert.equal(result.removedOperations, 1);
  assert.equal(db.items[0].quantity, 0);
  assert.deepEqual(db.ops.map((op) => op.id), ["other"]);
});

test("posting twice is refused", async () => {
  const db = fakeDb({ invoice: { id: "inv5", number: "ПН-015" }, meta: { status: "posted" }, lines: [{ id: "l", name: "Y", quantity: 1, unitPrice: 1 }] });
  assert.equal((await post(db.tx, "inv5", "david")).failure.code, "consignment_invoice_posted");
});
