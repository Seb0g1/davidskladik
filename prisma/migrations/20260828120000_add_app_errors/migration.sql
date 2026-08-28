-- CreateTable: app_errors — centralized application error journal
CREATE TABLE IF NOT EXISTS "app_errors" (
    "id" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "message" TEXT NOT NULL,
    "context" JSONB,
    "resolved_at" TIMESTAMPTZ,
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "app_errors_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX IF NOT EXISTS "app_errors_type_created_at_idx" ON "app_errors"("type", "created_at" DESC);
CREATE INDEX IF NOT EXISTS "app_errors_created_at_idx" ON "app_errors"("created_at" DESC);
