/**
 * Утилиты обслуживания каталога data/:
 *   - очистка незавершённых *.tmp;
 *   - удаление старых *.backup-* и *.corrupt-*;
 *   - ротация history.jsonl по размеру.
 *
 * Все пороги читаются из ENV, имеют безопасные дефолты и могут вызываться
 * как при старте сервера, так и из CLI (scripts/cleanup-data.js).
 */

const fs = require("fs/promises");
const path = require("path");
const logger = require("./logger");

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;
const MB = 1024 * 1024;

function parseNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

async function statSafe(filePath) {
  try {
    return await fs.stat(filePath);
  } catch (_e) {
    return null;
  }
}

async function unlinkSafe(filePath) {
  try {
    await fs.unlink(filePath);
    return true;
  } catch (_e) {
    return false;
  }
}

/**
 * Удаляет «осиротевшие» *.tmp файлы (упавший atomic-write) и старые backup/corrupt файлы.
 *
 * Возвращает {removed, freedBytes} для логов.
 */
async function cleanupDataDirectory(dataDir, options = {}) {
  const tmpRetentionHours = parseNumber(
    options.tmpRetentionHours ?? process.env.DATA_TMP_RETENTION_HOURS,
    2,
  );
  const backupRetentionDays = parseNumber(
    options.backupRetentionDays ?? process.env.DATA_BACKUP_RETENTION_DAYS,
    30,
  );

  let names;
  try {
    names = await fs.readdir(dataDir);
  } catch (_e) {
    return { removed: 0, freedBytes: 0 };
  }

  const now = Date.now();
  const tmpCutoff = now - tmpRetentionHours * HOUR_MS;
  const backupCutoff = now - backupRetentionDays * DAY_MS;

  let removed = 0;
  let freedBytes = 0;

  for (const name of names) {
    const filePath = path.join(dataDir, name);
    const stat = await statSafe(filePath);
    if (!stat || !stat.isFile()) continue;

    const isTmp = /\.tmp$/i.test(name);
    const isBackup = /\.backup-\d+/i.test(name);
    const isCorrupt = /\.corrupt-\d+/i.test(name);

    let shouldRemove = false;
    if (isTmp && stat.mtimeMs < tmpCutoff) shouldRemove = true;
    if ((isBackup || isCorrupt) && stat.mtimeMs < backupCutoff) shouldRemove = true;

    if (shouldRemove && (await unlinkSafe(filePath))) {
      removed += 1;
      freedBytes += stat.size;
    }
  }

  if (removed > 0) {
    logger.info("data cleanup done", {
      removed,
      freedMb: Math.round((freedBytes / MB) * 100) / 100,
    });
  }
  return { removed, freedBytes };
}

/**
 * Ротация history.jsonl: если размер > maxMb, файл переименовывается в
 * history-YYYYMMDD-HHmmss.jsonl, после чего удаляются старые ротированные
 * файлы старше retentionDays.
 */
async function rotateHistoryFile(historyPath, options = {}) {
  const maxMb = parseNumber(options.maxMb ?? process.env.HISTORY_MAX_MB, 50);
  const retentionDays = parseNumber(
    options.retentionDays ?? process.env.HISTORY_RETENTION_DAYS,
    60,
  );

  if (maxMb <= 0) return { rotated: false };

  const stat = await statSafe(historyPath);
  if (!stat || stat.size < maxMb * MB) return { rotated: false };

  const dir = path.dirname(historyPath);
  const baseName = path.basename(historyPath, ".jsonl");
  const stamp = new Date()
    .toISOString()
    .replace(/[:T]/g, "-")
    .replace(/\..+$/, "");
  const rotatedName = `${baseName}-${stamp}.jsonl`;
  const rotatedPath = path.join(dir, rotatedName);

  try {
    await fs.rename(historyPath, rotatedPath);
    logger.info("history rotated", { to: rotatedName, sizeMb: Math.round(stat.size / MB) });
  } catch (error) {
    logger.warn("history rotate failed", { detail: error.code || error.message });
    return { rotated: false };
  }

  if (retentionDays > 0) {
    const cutoff = Date.now() - retentionDays * DAY_MS;
    let names;
    try {
      names = await fs.readdir(dir);
    } catch (_e) {
      return { rotated: true };
    }
    for (const name of names) {
      if (!new RegExp(`^${baseName}-.+\\.jsonl$`).test(name)) continue;
      if (name === path.basename(historyPath)) continue;
      const fp = path.join(dir, name);
      const st = await statSafe(fp);
      if (st && st.mtimeMs < cutoff) await unlinkSafe(fp);
    }
  }

  return { rotated: true };
}

module.exports = { cleanupDataDirectory, rotateHistoryFile };
