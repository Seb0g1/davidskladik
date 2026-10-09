-- Telegram news: videos/albums and premium (custom) emoji positions.
ALTER TABLE "telegram_news_posts" ADD COLUMN IF NOT EXISTS "media" JSONB;
ALTER TABLE "telegram_news_posts" ADD COLUMN IF NOT EXISTS "entities" JSONB;
ALTER TABLE "telegram_news_posts" ADD COLUMN IF NOT EXISTS "media_group_id" TEXT;
CREATE INDEX IF NOT EXISTS "telegram_news_posts_media_group_id_idx" ON "telegram_news_posts"("media_group_id");
