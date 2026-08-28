#!/usr/bin/env node
"use strict";
// Diagnoses BullMQ failed jobs across all queues.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

const { Queue } = require("bullmq");
const Redis = require("ioredis");

async function main() {
  const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";
  const conn = new Redis(redisUrl, { maxRetriesPerRequest: null, lazyConnect: true });
  await conn.connect();

  // Discover all BullMQ queues by scanning Redis keys
  let cursor = "0";
  const queueNames = new Set();
  do {
    const [nextCursor, keys] = await conn.scan(cursor, "MATCH", "bull:*:meta", "COUNT", 500);
    cursor = nextCursor;
    for (const key of keys) {
      const parts = key.split(":");
      if (parts.length >= 3) queueNames.add(parts.slice(1, -1).join(":"));
    }
  } while (cursor !== "0");

  console.log(`=== BullMQ Queues (${queueNames.size} found) ===\n`);

  const queues = [...queueNames].map((name) => new Queue(name, { connection: conn }));

  let grandTotalFailed = 0;

  for (const queue of queues) {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      queue.getWaitingCount(),
      queue.getActiveCount(),
      queue.getCompletedCount(),
      queue.getFailedCount(),
      queue.getDelayedCount(),
    ]);

    grandTotalFailed += failed;
    const line = `[${queue.name}] waiting=${waiting} active=${active} completed=${completed} failed=${failed} delayed=${delayed}`;
    console.log(failed > 0 ? `⚠️  ${line}` : `   ${line}`);

    if (failed > 0) {
      const failedJobs = await queue.getFailed(0, Math.min(failed - 1, 19));
      const byReason = new Map();
      for (const job of failedJobs) {
        const reason = (job.failedReason || "unknown").slice(0, 120);
        if (!byReason.has(reason)) byReason.set(reason, { count: 0, sample: job });
        byReason.get(reason).count++;
      }
      for (const [reason, { count, sample }] of byReason) {
        const name = sample.name || "(no name)";
        const dataStr = JSON.stringify(sample.data || {}).slice(0, 80);
        console.log(`      reason(${count}x): ${reason}`);
        console.log(`        job.name=${name} data=${dataStr}`);
      }
    }
  }

  console.log(`\n=== Grand total failed: ${grandTotalFailed} ===`);

  await conn.quit();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
