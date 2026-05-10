#!/usr/bin/env node
/* CLI: ручная очистка data/ и ротация history.jsonl. */

const path = require("path");
require("dotenv").config();

const { cleanupDataDirectory, rotateHistoryFile } = require("../lib/cleanup");

const dataDir = path.join(__dirname, "..", "data");
const historyPath = path.join(dataDir, "history.jsonl");

(async () => {
  const cleanup = await cleanupDataDirectory(dataDir);
  const rotation = await rotateHistoryFile(historyPath);
  console.log(JSON.stringify({ cleanup, rotation }, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
