-- CreateTable
CREATE TABLE "support_chats" (
    "id" TEXT NOT NULL,
    "session_token" TEXT NOT NULL,
    "visitor_name" TEXT NOT NULL,
    "category" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'open',
    "unread_admin" BOOLEAN NOT NULL DEFAULT true,
    "unread_user" BOOLEAN NOT NULL DEFAULT false,
    "last_message_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "support_chats_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "support_messages" (
    "id" TEXT NOT NULL,
    "chat_id" TEXT NOT NULL,
    "role" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "support_messages_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "support_chats_session_token_key" ON "support_chats"("session_token");
CREATE INDEX "support_chats_status_idx" ON "support_chats"("status");
CREATE INDEX "support_chats_created_at_idx" ON "support_chats"("created_at");
CREATE INDEX "support_messages_chat_id_created_at_idx" ON "support_messages"("chat_id", "created_at");

-- AddForeignKey
ALTER TABLE "support_messages" ADD CONSTRAINT "support_messages_chat_id_fkey" FOREIGN KEY ("chat_id") REFERENCES "support_chats"("id") ON DELETE CASCADE ON UPDATE CASCADE;
