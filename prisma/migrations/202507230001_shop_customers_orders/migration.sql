CREATE TABLE IF NOT EXISTS "shop_customers" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "email" TEXT NOT NULL,
  "password" TEXT NOT NULL,
  "first_name" TEXT,
  "last_name" TEXT,
  "phone" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS "shop_customers_email_key" ON "shop_customers"("email");

CREATE TABLE IF NOT EXISTS "shop_orders" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "customer_id" TEXT,
  "status" TEXT NOT NULL DEFAULT 'pending',
  "items" JSONB NOT NULL,
  "delivery" JSONB NOT NULL,
  "total_rub" INTEGER NOT NULL,
  "comment" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "shop_orders_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "shop_customers"("id") ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "shop_orders_customer_id_idx" ON "shop_orders"("customer_id");
CREATE INDEX IF NOT EXISTS "shop_orders_created_at_idx" ON "shop_orders"("created_at");
