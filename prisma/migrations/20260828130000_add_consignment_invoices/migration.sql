CREATE TABLE consignment_invoices (
  id TEXT PRIMARY KEY,
  number TEXT NOT NULL,
  supplier_name TEXT,
  note TEXT,
  total_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_consignment_invoices_created_at ON consignment_invoices(created_at);

CREATE TABLE consignment_invoice_items (
  id TEXT PRIMARY KEY,
  invoice_id TEXT NOT NULL REFERENCES consignment_invoices(id) ON DELETE CASCADE,
  item_id TEXT,
  name TEXT NOT NULL,
  article TEXT,
  quantity INTEGER NOT NULL,
  unit_price DOUBLE PRECISION NOT NULL
);

CREATE INDEX idx_consignment_invoice_items_invoice_id ON consignment_invoice_items(invoice_id);
