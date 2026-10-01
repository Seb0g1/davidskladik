/**
 * Структурированные логи: в production / при LOG_JSON=true — одна JSON-строка на событие (удобно для journald, Docker, logrotate).
 * Ротацию файла делайте снаружи: pm2, systemd, или logrotate на перенаправленный stdout.
 */

const useJson =
  process.env.LOG_JSON === "true" ||
  process.env.NODE_ENV === "production";

function serialize(level, msg, extra) {
  const safe = { ...extra };
  if (safe.err instanceof Error) safe.err = safe.err.message;
  const record = {
    time: new Date().toISOString(),
    level,
    msg: String(msg || ""),
    ...safe,
  };
  // BigInt (Postgres BIGINT ids) would make JSON.stringify throw and break the caller
  const replacer = (_key, value) => (typeof value === "bigint" ? String(value) : value);
  if (useJson) return JSON.stringify(record, replacer);
  const tail = Object.keys(extra).length ? ` ${JSON.stringify(extra, replacer)}` : "";
  return `[${record.time}] ${level.toUpperCase()} ${record.msg}${tail}`;
}

function info(msg, extra = {}) {
  console.log(serialize("info", msg, extra));
}

// Silent unless LOG_LEVEL=debug (callers such as findWarehouseProductById log cache misses here).
function debug(msg, extra = {}) {
  if (String(process.env.LOG_LEVEL || "").toLowerCase() === "debug") console.log(serialize("debug", msg, extra));
}

function warn(msg, extra = {}) {
  console.warn(serialize("warn", msg, extra));
}

function error(msg, extra = {}) {
  const line = serialize("error", msg, extra);
  if (extra.err && extra.err instanceof Error) {
    console.error(line, extra.err);
    return;
  }
  console.error(line);
}

module.exports = { debug, info, warn, error, serialize };
