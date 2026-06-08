# BullMQ failed jobs triage (2026-06-08)

## Summary

- **Total failed:** 120
- **By job type:**
  - `auto-price-push`: 105
  - `supplier-recovery-automation`: 11
  - `ozon-unarchive-queue-process`: 4

## Root cause

All sampled jobs: **`job stalled more than allowable limit`**.

Это следствие старых рестартов monolith/worker во время долгих price-push, не актуальные ошибки бизнес-логики.

## Action taken

- Inspect only (no blind `--retry`)
- Удаление stale failed записей: `--remove-failed` (без повторного запуска job)

## Prevention

- api+worker split, api не выполняет jobs inline
- `BULLMQ_LOCK_DURATION_MS=900000` на worker
- post-deploy алерт при `failed > 20`
