#!/usr/bin/env node
"use strict";
// Obliterates legacy BullMQ queues that no longer exist in the codebase.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

const { Queue } = require("bullmq");
const Redis = require("ioredis");

const LEGACY_QUEUES = ["warehouse-operations"];

async function main() {
  const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";
  const conn = new Redis(redisUrl, { maxRetriesPerRequest: null, lazyConnect: true });
  await conn.connect();

  for (const name of LEGACY_QUEUES) {
    const queue = new Queue(name, { connection: conn });
    const [w, a, f, d] = await Promise.all([
      queue.getWaitingCount(), queue.getActiveCount(), queue.getFailedCount(), queue.getDelayedCount(),
    ]);
    console.log(`[${name}] waiting=${w} active=${a} failed=${f} delayed=${d} → obliterating...`);
    await queue.obliterate({ force: true });
    console.log(`  Done.`);
    await queue.close();
  }

  await conn.quit();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
