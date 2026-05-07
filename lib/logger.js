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
  if (useJson) return JSON.stringify(record);
  const tail = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  return `[${record.time}] ${level.toUpperCase()} ${record.msg}${tail}`;
}

function info(msg, extra = {}) {
  console.log(serialize("info", msg, extra));
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

module.exports = { info, warn, error, serialize };
