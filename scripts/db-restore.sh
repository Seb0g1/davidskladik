#!/usr/bin/env bash
# Restore a PostgreSQL backup created by pg-backup.sh.
# The backup uses pg_dump -Fc (custom format), so pg_restore is required.
#
# Usage:
#   DATABASE_URL=postgres://... bash scripts/db-restore.sh /var/backups/davidsklad/davidsklad-20260828-030001.dump
#
# WARNING: This DROPS and recreates all application tables. Stop api+worker first.
#   pm2 stop davidsklad-api davidsklad-worker davidsklad-health-watchdog
set -euo pipefail

BACKUP_FILE="${1:-}"
DB_URL="${DATABASE_URL:-}"

# --- validation ---
if [ -z "$BACKUP_FILE" ]; then
  echo "Usage: $0 /path/to/davidsklad-YYYYMMDD-HHMMSS.dump" >&2
  exit 1
fi
if [ ! -f "$BACKUP_FILE" ]; then
  echo "ERROR: backup file not found: $BACKUP_FILE" >&2
  exit 1
fi
if [ -z "$DB_URL" ]; then
  # Try to read from .env
  ENV_FILE="${ENV_FILE:-/var/www/davidsklad/davidskladik/.env}"
  if [ -f "$ENV_FILE" ]; then
    DB_URL="$(grep -E '^DATABASE_URL=' "$ENV_FILE" | tail -1 | sed -E 's/^DATABASE_URL=//; s/^"//; s/"$//; s/^[[:space:]]+//; s/[[:space:]]+$//')"
  fi
fi
if [ -z "$DB_URL" ]; then
  echo "ERROR: DATABASE_URL is not set and .env not found" >&2
  exit 1
fi

SIZE="$(du -sh "$BACKUP_FILE" | cut -f1)"
echo ""
echo "  Backup file : $BACKUP_FILE ($SIZE)"
echo "  Target DB   : $(echo "$DB_URL" | sed 's/:\/\/[^:]*:[^@]*@/:\/\/***:***@/')"
echo ""
echo "  WARNING: All application data will be overwritten."
echo "  Make sure davidsklad-api and davidsklad-worker are stopped:"
echo "    pm2 stop davidsklad-api davidsklad-worker davidsklad-health-watchdog"
echo ""
read -rp "Type YES to proceed with restore: " confirm
if [ "$confirm" != "YES" ]; then
  echo "Aborted."
  exit 0
fi

echo ""
echo "Restoring..."
# --clean drops existing objects before recreating; --if-exists avoids errors on missing objects.
pg_restore --dbname="$DB_URL" --clean --if-exists --no-owner --no-privileges "$BACKUP_FILE"

echo ""
echo "Restore complete. Start services:"
echo "  pm2 start davidsklad-api davidsklad-worker davidsklad-health-watchdog"
