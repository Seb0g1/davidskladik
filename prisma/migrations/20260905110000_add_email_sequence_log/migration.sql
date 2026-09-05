-- CreateTable: shop_email_sequence_logs
CREATE TABLE "shop_email_sequence_logs" (
    "id" TEXT NOT NULL,
    "order_id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "step" INTEGER NOT NULL,
    "sent_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shop_email_sequence_logs_pkey" PRIMARY KEY ("id")
);

-- CreateIndex: unique per order+step
CREATE UNIQUE INDEX "shop_email_sequence_logs_order_id_step_key" ON "shop_email_sequence_logs"("order_id", "step");

-- CreateIndex
CREATE INDEX "shop_email_sequence_logs_step_sent_at_idx" ON "shop_email_sequence_logs"("step", "sent_at");
