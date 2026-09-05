-- AddColumn referral_code to shop_customers
ALTER TABLE "shop_customers" ADD COLUMN "referral_code" TEXT;
CREATE UNIQUE INDEX "shop_customers_referral_code_key" ON "shop_customers"("referral_code");

-- AddColumn ref_code to shop_orders
ALTER TABLE "shop_orders" ADD COLUMN "ref_code" TEXT;
