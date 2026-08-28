#!/usr/bin/env node
"use strict";

// Показывает детали упавших BullMQ-джоб на проде.
// Подключается к Redis напрямую через REDIS_URL из .env.

const fs = require("node:fs");
const path = require("node:path");
const { Queue } = require("bullmq");

function readEnv(key) {
  const text = fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8");
  const match = text.match(new RegExp(`^${key}=(.*)$`, "m"));
  return match ? match[1].trim().replace(/^"|"$/g, "") : "";
}

async function main() {
  const redisUrl = readEnv("REDIS_URL");
  if (!redisUrl) throw new Error("REDIS_URL не найден в .env");
  console.log(`redis: ${redisUrl.replace(/:\/\/.*@/, "://*@")}`);

  const queue = new Queue("marketplace-tasks", {
    connection: { url: redisUrl, maxRetriesPerRequest: null },
  });

  try {
    const counts = await queue.getJobCounts("waiting", "active", "delayed", "failed", "paused", "completed");
    console.log("\n=== counts ===");
    console.log(JSON.stringify(counts, null, 2));

    const failed = await queue.getJobs(["failed"], 0, 50);
    console.log(`\n=== failed jobs (${failed.length}) ===`);
    for (const job of failed) {
      console.log(`\njobId=${job.id} name=${job.name}`);
      console.log(`  failedReason: ${job.failedReason}`);
      console.log(`  attemptsMade: ${job.attemptsMade}`);
      console.log(`  processedOn: ${job.processedOn ? new Date(job.processedOn).toISOString() : "n/a"}`);
      const data = job.data || {};
      const keys = Object.keys(data).slice(0, 6);
      if (keys.length) console.log(`  data: ${JSON.stringify(Object.fromEntries(keys.map((k) => [k, data[k]])))}`);
      if (job.stacktrace?.length) console.log(`  stack: ${job.stacktrace[0]?.slice(0, 300)}`);
    }
  } finally {
    await queue.close();
  }
}

main().catch((error) => { console.error(error.message); process.exit(1); });
