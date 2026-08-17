-- CreateTable
CREATE TABLE "telegram_news_posts" (
    "id" TEXT NOT NULL,
    "message_id" INTEGER NOT NULL,
    "text" TEXT,
    "photo_url" TEXT,
    "published_at" TIMESTAMP(3) NOT NULL,
    "active" BOOLEAN NOT NULL DEFAULT true,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "telegram_news_posts_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "shop_reviews" (
    "id" TEXT NOT NULL,
    "customer_id" TEXT NOT NULL,
    "offer_id" TEXT,
    "product_name" TEXT,
    "product_img" TEXT,
    "rating" INTEGER NOT NULL DEFAULT 5,
    "text" TEXT NOT NULL,
    "approved" BOOLEAN NOT NULL DEFAULT true,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "shop_reviews_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "telegram_news_posts_message_id_key" ON "telegram_news_posts"("message_id");
CREATE INDEX "telegram_news_posts_active_published_at_idx" ON "telegram_news_posts"("active", "published_at");
CREATE INDEX "shop_reviews_approved_created_at_idx" ON "shop_reviews"("approved", "created_at");
CREATE INDEX "shop_reviews_customer_id_idx" ON "shop_reviews"("customer_id");

-- AddForeignKey
ALTER TABLE "shop_reviews" ADD CONSTRAINT "shop_reviews_customer_id_fkey" FOREIGN KEY ("customer_id") REFERENCES "shop_customers"("id") ON DELETE CASCADE ON UPDATE CASCADE;
