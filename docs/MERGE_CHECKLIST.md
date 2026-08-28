# Merge checklist: codex/restore-4dfc0cb → main

**Soak начат:** 2026-06-08 (после split api+worker deploy). Merge не раньше **2026-06-10..11**.

## Перед merge (48–72 ч на проде)

- [ ] `pm2 list` — только `davidsklad-api` + `davidsklad-worker`, без `davidsklad`
- [ ] Login < 2 с (`prod-post-deploy-check.cjs` ok)
- [ ] Каталог `linked=unlinked&grouped=true` — HTTP 200, items > 0
- [ ] Привязка SKU → job в Redis → worker completion в логах
- [ ] Нет emergency recover за период наблюдения
- [ ] Heap api < 80% лимита (3 GB)

## BullMQ failed jobs

На сервере (после стабилизации):

```bash
cd /var/www/davidsklad/davidskladik
node scripts/inspect-bullmq-failed-jobs.cjs --limit=120
# разобрать byName; повторить осмысленные:
node scripts/inspect-bullmq-failed-jobs.cjs --retry --limit=50
```

## PR

```bash
git push -u origin codex/restore-4dfc0cb
gh pr create --base main --head codex/restore-4dfc0cb \
  --title "Stable production: api+worker split on 16 GB" \
  --body "## Summary
- Split HTTP (api) and background work (worker) with BullMQ producer on api
- Memory caps: api 3 GB / worker 5 GB; monolith removed from ecosystem
- Blocking deploy pipeline with npm test, build, post-deploy checks
- Runbook and monitoring scripts

## Test plan
- [x] npm test (216/216)
- [ ] prod post-deploy check after deploy
- [ ] link SKU end-to-end
"
```

## После merge

1. Deploy from `main`: `DEPLOY_PASSWORD=... node scripts/deploy-prod.cjs`
2. `node scripts/setup-prod-monitoring.cjs` (cron + logrotate)
3. Напомнить сотрудникам Ctrl+F5 после deploy
