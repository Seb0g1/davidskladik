-- AlterTable: add photo_url to shop_reviews
ALTER TABLE "shop_reviews" ADD COLUMN "photo_url" TEXT;

-- AlterTable: add loyalty_points to shop_customers
ALTER TABLE "shop_customers" ADD COLUMN "loyalty_points" INTEGER NOT NULL DEFAULT 0;

-- CreateTable: shop_point_transactions
CREATE TABLE "shop_point_transactions" (
    "id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "points" INTEGER NOT NULL,
    "reason" TEXT NOT NULL,
    "ref_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shop_point_transactions_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "shop_point_transactions_customer_id_created_at_idx" ON "shop_point_transactions"("customer_id", "created_at");

-- AddForeignKey
ALTER TABLE "shop_point_transactions" ADD CONSTRAINT "shop_point_transactions_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "shop_customers"("id") ON DELETE CASCADE ON UPDATE CASCADE;
