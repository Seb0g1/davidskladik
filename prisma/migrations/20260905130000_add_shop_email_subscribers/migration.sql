-- CreateTable
CREATE TABLE "shop_email_subscribers" (
    "id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "source" TEXT NOT NULL DEFAULT 'popup',
    "quiz_category" TEXT,
    "promo_sent" BOOLEAN NOT NULL DEFAULT false,
    "unsubscribed" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shop_email_subscribers_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "shop_email_subscribers_email_key" ON "shop_email_subscribers"("email");

-- CreateIndex
CREATE INDEX "shop_email_subscribers_created_at_idx" ON "shop_email_subscribers"("created_at");
