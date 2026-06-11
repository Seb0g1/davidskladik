// Site notifications: orders / chats / reviews / questions from both marketplaces.
//
// Storage is a plain PG table created lazily (no prisma migration needed). The worker
// pollers insert rows (dedup by type+marketplace+external_id); the API serves the list
// and an SSE stream that tails new rows so any open page gets a toast + sound.

let notificationsTableReady = false;
async function ensureNotificationsTable() {
  if (notificationsTableReady) return true;
  const prisma = getPrisma();
  if (!prisma) return false;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS app_notifications (
      id BIGSERIAL PRIMARY KEY,
      type TEXT NOT NULL,
      marketplace TEXT NOT NULL,
      title TEXT NOT NULL,
      body TEXT,
      external_id TEXT,
      url TEXT,
      read_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  await prisma.$executeRawUnsafe(`
    CREATE UNIQUE INDEX IF NOT EXISTS app_notifications_dedup
    ON app_notifications (type, marketplace, COALESCE(external_id, ''))
  `);
  await prisma.$executeRawUnsafe(`
    CREATE INDEX IF NOT EXISTS app_notifications_created ON app_notifications (created_at DESC)
  `);
  notificationsTableReady = true;
  return true;
}

async function insertAppNotification({ type, marketplace, title, body = "", externalId = "", url = "", eventAt = null }) {
  const prisma = getPrisma();
  if (!prisma || !(await ensureNotificationsTable())) return false;
  const eventDate = eventAt ? new Date(eventAt) : null;
  const createdAt = eventDate && Number.isFinite(eventDate.getTime()) ? eventDate.toISOString() : new Date().toISOString();
  const rows = await prisma.$queryRawUnsafe(
    `INSERT INTO app_notifications (type, marketplace, title, body, external_id, url, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7::timestamptz)
     ON CONFLICT (type, marketplace, COALESCE(external_id, '')) DO NOTHING
     RETURNING id`,
    cleanText(type) || "info",
    cleanText(marketplace) || "site",
    cleanText(title).slice(0, 300) || "Событие",
    cleanText(body).slice(0, 1000),
    cleanText(externalId).slice(0, 200),
    cleanText(url).slice(0, 500),
    createdAt,
  );
  return rows.length > 0;
}

app.get("/api/notifications", async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma || !(await ensureNotificationsTable())) return response.json({ ok: true, rows: [], unread: 0 });
    const limit = Math.max(1, Math.min(100, Number(request.query.limit || 30) || 30));
    const rows = await prisma.$queryRawUnsafe(
      `SELECT id::text, type, marketplace, title, body, external_id AS "externalId", url,
              read_at AS "readAt", created_at AS "createdAt"
       FROM app_notifications ORDER BY created_at DESC, id DESC LIMIT ${limit}`,
    );
    const unreadRows = await prisma.$queryRawUnsafe(
      "SELECT COUNT(*)::int AS n FROM app_notifications WHERE read_at IS NULL",
    );
    response.json({ ok: true, rows, unread: unreadRows[0]?.n || 0 });
  } catch (error) {
    next(error);
  }
});

app.post("/api/notifications/read", async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma || !(await ensureNotificationsTable())) return response.json({ ok: true, updated: 0 });
    let updated = 0;
    if (request.body?.all === true) {
      updated = await prisma.$executeRawUnsafe(
        "UPDATE app_notifications SET read_at = now() WHERE read_at IS NULL",
      );
    } else {
      const ids = (Array.isArray(request.body?.ids) ? request.body.ids : [])
        .map((id) => Number(id))
        .filter((id) => Number.isFinite(id) && id > 0);
      if (ids.length) {
        updated = await prisma.$executeRawUnsafe(
          `UPDATE app_notifications SET read_at = now() WHERE read_at IS NULL AND id IN (${ids.join(",")})`,
        );
      }
    }
    response.json({ ok: true, updated });
  } catch (error) {
    next(error);
  }
});

// SSE: tails new notification rows so any open admin page reacts instantly.
app.get("/api/notifications/stream", async (request, response) => {
  const prisma = getPrisma();
  if (!prisma || !(await ensureNotificationsTable().catch(() => false))) {
    return response.status(503).end();
  }
  response.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  response.write(": connected\n\n");

  let cursor = Number(request.headers["last-event-id"] || 0) || 0;
  if (!cursor) {
    const last = await prisma.$queryRawUnsafe("SELECT COALESCE(MAX(id), 0)::bigint AS id FROM app_notifications").catch(() => [{ id: 0 }]);
    cursor = Number(last[0]?.id || 0);
  }
  let closed = false;
  const tick = async () => {
    if (closed) return;
    try {
      const rows = await prisma.$queryRawUnsafe(
        `SELECT id::text, type, marketplace, title, body, url, created_at AS "createdAt"
         FROM app_notifications WHERE id > ${cursor} ORDER BY id ASC LIMIT 50`,
      );
      for (const row of rows) {
        cursor = Math.max(cursor, Number(row.id));
        response.write(`id: ${row.id}\nevent: notification\ndata: ${JSON.stringify(row)}\n\n`);
      }
      if (!rows.length) response.write(": ping\n\n");
    } catch {
      response.write(": ping\n\n");
    }
  };
  const timer = setInterval(tick, 7000);
  void tick();
  request.on("close", () => {
    closed = true;
    clearInterval(timer);
  });
});
