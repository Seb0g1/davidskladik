#!/usr/bin/env node
"use strict";
// Cleans stalled/failed BullMQ jobs from all queues.
// Also promotes stalled active jobs to failed so they get cleaned too.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

const { Queue, Worker } = require("bullmq");
const Redis = require("ioredis");

async function main() {
  const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";
  const conn = new Redis(redisUrl, { maxRetriesPerRequest: null, lazyConnect: true });
  await conn.connect();

  // Discover all BullMQ queues
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

  console.log(`Found ${queueNames.size} queues\n`);

  for (const name of queueNames) {
    const queue = new Queue(name, { connection: conn });

    const failedCount = await queue.getFailedCount();
    const activeCount = await queue.getActiveCount();

    if (failedCount === 0 && activeCount === 0) {
      console.log(`[${name}] — clean, skipping`);
      await queue.close();
      continue;
    }

    console.log(`[${name}] failed=${failedCount} active=${activeCount}`);

    // Check active jobs for stalled ones (no worker lock)
    if (activeCount > 0) {
      const activeJobs = await queue.getActive(0, activeCount - 1);
      let stalledCount = 0;
      for (const job of activeJobs) {
        const lock = await conn.get(`bull:${name}:${job.id}:lock`);
        if (!lock) {
          // No lock — this job is orphaned/stalled
          await job.moveToFailed(new Error("job stalled — orphaned active job, cleaned manually"), "0", true);
          stalledCount++;
        }
      }
      if (stalledCount > 0) console.log(`  Moved ${stalledCount} orphaned active jobs → failed`);
    }

    // Clean all failed jobs (grace=0 means clean all regardless of age)
    const removed = await queue.clean(0, 10000, "failed");
    console.log(`  Removed ${removed.length} failed jobs`);

    await queue.close();
  }

  // Final counts
  console.log("\n=== After cleanup ===");
  for (const name of queueNames) {
    const q = new Queue(name, { connection: conn });
    const [w, a, f, d] = await Promise.all([q.getWaitingCount(), q.getActiveCount(), q.getFailedCount(), q.getDelayedCount()]);
    if (w + a + f + d > 0) console.log(`[${name}] waiting=${w} active=${a} failed=${f} delayed=${d}`);
    await q.close();
  }

  await conn.quit();
  console.log("\nDone.");
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
