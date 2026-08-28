#!/usr/bin/env bash
# PLAN-HARDENING.md 2.4: daily PostgreSQL backup with 14-day rotation.
#
# Reads DATABASE_URL from the app .env, writes a compressed custom-format dump
# (pg_dump -Fc, restorable with pg_restore) to $BACKUP_DIR, prunes dumps older
# than $RETENTION_DAYS, and appends a one-line result to the log. Designed to be
# run from cron; exits non-zero on failure so cron mails the error.
#
# Install on prod (run once):
#   chmod +x scripts/pg-backup.sh
#   ( crontab -l 2>/dev/null; echo "30 3 * * * /var/www/davidsklad/davidskladik/scripts/pg-backup.sh >> /var/backups/davidsklad/backup.log 2>&1" ) | crontab -
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/davidsklad/davidskladik}"
ENV_FILE="${ENV_FILE:-$APP_DIR/.env}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/davidsklad}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"
LOG_PREFIX="$(date -u +%Y-%m-%dT%H:%M:%SZ) pg-backup:"

mkdir -p "$BACKUP_DIR"

if [ -z "${DATABASE_URL:-}" ]; then
  if [ ! -f "$ENV_FILE" ]; then
    echo "$LOG_PREFIX FAIL no DATABASE_URL and no $ENV_FILE" >&2
    exit 1
  fi
  # Take the last DATABASE_URL= line, strip surrounding quotes/whitespace.
  DATABASE_URL="$(grep -E '^DATABASE_URL=' "$ENV_FILE" | tail -1 | sed -E 's/^DATABASE_URL=//; s/^"//; s/"$//; s/^[[:space:]]+//; s/[[:space:]]+$//')"
fi

if [ -z "$DATABASE_URL" ]; then
  echo "$LOG_PREFIX FAIL DATABASE_URL is empty" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT="$BACKUP_DIR/davidsklad-$STAMP.dump"
TMP="$OUT.partial"

# -Fc compressed custom format; --no-owner/--no-privileges keep restores portable.
if ! pg_dump --dbname="$DATABASE_URL" -Fc --no-owner --no-privileges -f "$TMP"; then
  rm -f "$TMP"
  echo "$LOG_PREFIX FAIL pg_dump errored" >&2
  exit 1
fi

# Guard against a silent empty/truncated dump.
MIN_BYTES="${MIN_BACKUP_BYTES:-10240}"
SIZE="$(stat -c %s "$TMP" 2>/dev/null || echo 0)"
if [ "$SIZE" -lt "$MIN_BYTES" ]; then
  rm -f "$TMP"
  echo "$LOG_PREFIX FAIL dump too small ($SIZE bytes < $MIN_BYTES)" >&2
  exit 1
fi

mv "$TMP" "$OUT"

# Rotation: delete dumps older than RETENTION_DAYS.
find "$BACKUP_DIR" -maxdepth 1 -name 'davidsklad-*.dump' -type f -mtime +"$RETENTION_DAYS" -delete || true

REMAINING="$(find "$BACKUP_DIR" -maxdepth 1 -name 'davidsklad-*.dump' -type f | wc -l | tr -d ' ')"
echo "$LOG_PREFIX OK wrote $OUT ($SIZE bytes); $REMAINING dump(s) retained (<= ${RETENTION_DAYS}d)"
