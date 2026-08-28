#!/usr/bin/env node
"use strict";
// Checks active BullMQ jobs and whether they have valid locks.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

const { Queue } = require("bullmq");
const Redis = require("ioredis");

async function main() {
  const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";
  const conn = new Redis(redisUrl, { maxRetriesPerRequest: null, lazyConnect: true });
  await conn.connect();

  const queue = new Queue("marketplace-tasks", { connection: conn });
  const activeJobs = await queue.getActive(0, 20);

  console.log(`Active jobs in marketplace-tasks: ${activeJobs.length}\n`);

  for (const job of activeJobs) {
    // BullMQ v4+ lock key format
    const lockKey1 = `bull:marketplace-tasks:${job.id}:lock`;
    const lockKey2 = `bull:marketplace-tasks:locks:${job.id}`;
    const lock1 = await conn.get(lockKey1);
    const lock2 = await conn.get(lockKey2);
    const hasLock = !!(lock1 || lock2);
    const ttl1 = lock1 ? await conn.pttl(lockKey1) : -1;
    const age = job.processedOn ? Math.round((Date.now() - job.processedOn) / 1000) : "?";
    const dataStr = JSON.stringify(job.data || {}).slice(0, 100);
    console.log(`job ${job.id} name=${job.name} hasLock=${hasLock} lockTtl=${ttl1}ms age=${age}s`);
    console.log(`  data=${dataStr}`);
    console.log(`  attempts=${job.attemptsMade} failedReason=${job.failedReason || "none"}`);
  }

  // Check locks via SCAN to find all marketplace-tasks locks
  let cursor = "0";
  const locks = [];
  do {
    const [nextCursor, keys] = await conn.scan(cursor, "MATCH", "bull:marketplace-tasks:*:lock", "COUNT", 100);
    cursor = nextCursor;
    locks.push(...keys);
  } while (cursor !== "0");
  console.log(`\nAll lock keys found: ${locks.length}`);
  for (const k of locks) {
    const ttl = await conn.pttl(k);
    console.log(`  ${k} ttl=${ttl}ms`);
  }

  await queue.close();
  await conn.quit();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
