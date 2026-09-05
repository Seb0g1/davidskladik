-- CreateTable: shop_push_subscriptions
CREATE TABLE "shop_push_subscriptions" (
    "id" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "p256dh" TEXT NOT NULL,
    "auth" TEXT NOT NULL,
    "customer_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shop_push_subscriptions_pkey" PRIMARY KEY ("id")
);

-- CreateIndex: unique endpoint (one sub per browser)
CREATE UNIQUE INDEX "shop_push_subscriptions_endpoint_key" ON "shop_push_subscriptions"("endpoint");

-- CreateIndex
CREATE INDEX "shop_push_subscriptions_customer_id_idx" ON "shop_push_subscriptions"("customer_id");
