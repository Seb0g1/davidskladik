#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { Queue } = require("bullmq");

const redisUrl = String(process.env.REDIS_URL || "").trim();
const shouldRetry = process.argv.includes("--retry");
const shouldRemove = process.argv.includes("--remove-failed");
const limit = Math.max(1, Math.min(500, Number(process.argv.find((arg) => arg.startsWith("--limit="))?.split("=")[1] || 50) || 50));

async function main() {
  if (!redisUrl) {
    console.error("REDIS_URL is required");
    process.exit(1);
  }
  const queue = new Queue("marketplace-tasks", { connection: { url: redisUrl, maxRetriesPerRequest: null } });
  const failed = await queue.getJobs(["failed"], 0, limit - 1, true);
  const summary = {};
  for (const job of failed) {
    const key = job.name || "unknown";
    summary[key] = (summary[key] || 0) + 1;
  }
  console.log(JSON.stringify({
    failedTotal: failed.length,
    byName: summary,
    samples: failed.slice(0, 10).map((job) => ({
      id: job.id,
      name: job.name,
      failedReason: job.failedReason,
      timestamp: job.timestamp,
      dataKeys: job.data ? Object.keys(job.data) : [],
    })),
  }, null, 2));

  if (shouldRemove && failed.length) {
    let removed = 0;
    for (const job of failed) {
      try {
        await job.remove();
        removed += 1;
      } catch (error) {
        console.warn(`remove failed for ${job.id}: ${error.message}`);
      }
    }
    console.log(`Removed ${removed}/${failed.length} failed jobs (no retry)`);
  } else if (shouldRetry && failed.length) {
    let retried = 0;
    for (const job of failed) {
      try {
        await job.retry();
        retried += 1;
      } catch (error) {
        console.warn(`retry failed for ${job.id}: ${error.message}`);
      }
    }
    console.log(`Retried ${retried}/${failed.length} failed jobs`);
  }
  await queue.close();
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
