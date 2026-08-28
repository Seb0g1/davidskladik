function createSessionToken(user) {
  const username = typeof user === "string" ? user : user.username;
  const role = typeof user === "string" ? process.env.APP_ROLE || "admin" : user.role || "manager";
  const payload = base64Url(
    JSON.stringify({
      username,
      role,
      expiresAt: Date.now() + sessionTtlMs,
    }),
  );
  return `${payload}.${sign(payload)}`;
}

function readSession(request) {
  const token = parseCookies(request.headers.cookie)[sessionCookieName];
  if (!token || !token.includes(".")) return null;

  const [payload, signature] = token.split(".");
  if (!timingSafeEqual(sign(payload), signature)) return null;

  try {
    const session = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (!session.expiresAt || session.expiresAt < Date.now()) return null;
    return session;
  } catch (_error) {
    return null;
  }
}

const uploadSessionStats = new Map();
const UPLOAD_QUOTA_WINDOW_MS = 24 * 60 * 60 * 1000;

function uploadSessionKey(request) {
  const session = readSession(request);
  if (session?.username) return `user:${session.username}`;
  const token = parseCookies(request.headers.cookie)[sessionCookieName];
  if (token) return `sess:${crypto.createHash("sha256").update(token).digest("hex").slice(0, 24)}`;
  const ip = request.ip || request.socket?.remoteAddress || "unknown";
  return `ip:${ip}`;
}

function consumeUploadQuota(request, fileCount) {
  const max = Math.max(1, Number(process.env.UPLOAD_MAX_FILES_PER_SESSION || 200));
  const key = uploadSessionKey(request);
  const now = Date.now();
  let entry = uploadSessionStats.get(key);
  if (!entry || entry.resetAt < now) {
    entry = { count: 0, resetAt: now + UPLOAD_QUOTA_WINDOW_MS };
  }
  if (entry.count + fileCount > max) {
    const err = new Error(
      `Превышен лимит загрузок: не более ${max} файлов за 24 часа для этой сессии.`,
    );
    err.statusCode = 429;
    throw err;
  }
  entry.count += fileCount;
  uploadSessionStats.set(key, entry);
}

function isAdminSession(session) {
  return cleanText(session?.role).toLowerCase() === "admin";
}

function isAdminPagePath(pathname = "") {
  return ["/settings", "/settings.html", "/pricemaster", "/pricemaster.html"].includes(pathname);
}

async function pruneUploadDirectory() {
  const retentionDays = Number(process.env.UPLOAD_RETENTION_DAYS || 14);
  const maxMb = Number(process.env.UPLOAD_MAX_DISK_MB || 800);
  try {
    await fs.mkdir(uploadImageDir, { recursive: true });
  } catch (_e) {
    return;
  }
  let names;
  try {
    names = await fs.readdir(uploadImageDir);
  } catch (_e) {
    return;
  }
  const cutoff = Date.now() - Math.max(1, retentionDays) * 24 * 60 * 60 * 1000;
  const files = [];
  for (const name of names) {
    const fp = path.join(uploadImageDir, name);
    try {
      const st = await fs.stat(fp);
      if (st.isFile()) files.push({ fp, size: st.size, mtime: st.mtimeMs });
    } catch (_e) {
      /* skip */
    }
  }
  for (const f of files) {
    if (f.mtime < cutoff) await fs.unlink(f.fp).catch(() => {});
  }
  let alive = [];
  try {
    for (const name of await fs.readdir(uploadImageDir)) {
      const fp = path.join(uploadImageDir, name);
      try {
        const st = await fs.stat(fp);
        if (st.isFile()) alive.push({ fp, size: st.size, mtime: st.mtimeMs });
      } catch (_e) {
        /* skip */
      }
    }
  } catch (_e) {
    return;
  }
  let total = alive.reduce((sum, f) => sum + f.size, 0);
  const maxBytes = Math.max(10, maxMb) * 1024 * 1024;
  alive.sort((a, b) => a.mtime - b.mtime);
  while (total > maxBytes && alive.length) {
    const f = alive.shift();
    await fs.unlink(f.fp).catch(() => {});
    total -= f.size;
  }
}
