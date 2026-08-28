#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");

const baseUrl = process.env.APP_BASE_URL || `http://127.0.0.1:${process.env.PORT || 3000}`;

function run(command, args, options = {}) {
  process.stdout.write(`\n$ ${[command, ...args].join(" ")}\n`);
  const result = spawnSync(command, args, {
    shell: process.platform === "win32",
    stdio: options.capture ? "pipe" : "inherit",
    encoding: "utf8",
    env: { ...process.env, DISABLE_BACKGROUND_JOBS: "true", ...(options.env || {}) },
  });
  if (options.capture) {
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
  }
  if (options.required !== false && result.status !== 0) {
    process.stderr.write(`\npostdeploy diagnose failed at: ${[command, ...args].join(" ")}\n`);
    process.exit(result.status || 1);
  }
  return result;
}

async function getJson(url) {
  const response = await fetch(url, { headers: { accept: "application/json" } });
  const text = await response.text();
  let payload = null;
  try {
    payload = JSON.parse(text);
  } catch {
    payload = { raw: text.slice(0, 1000) };
  }
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status} ${response.statusText}`);
    error.payload = payload;
    throw error;
  }
  return payload;
}

async function main() {
  process.stdout.write(`Postdeploy diagnostics for ${baseUrl}\n`);
  const health = await getJson(`${baseUrl.replace(/\/$/, "")}/health?deep=1`);
  process.stdout.write(`${JSON.stringify({
    ok: health.ok !== false,
    version: health.version,
    time: health.time,
    postgres: health.components?.postgres,
    postgresTables: {
      ok: health.components?.postgresTables?.ok,
      missing: health.components?.postgresTables?.missing || [],
    },
    redis: health.components?.redis,
    runtime: health.components?.runtime,
    automation: health.components?.automation,
  }, null, 2)}\n`);

  if (health.ok === false || health.components?.postgresTables?.ok === false || health.components?.redis?.ok === false) {
    process.stderr.write("\nHealth check is not green. Inspect the JSON above before accepting deploy.\n");
    process.exitCode = 1;
  }

  run("npx", ["prisma", "migrate", "status"]);
  run("node", ["scripts/ops-diagnose.cjs", "--json", "--deep", "--log-lines=120"]);
  run("pm2", ["status", "davidsklad"], { required: false });
  run("pm2", ["logs", "davidsklad", "--lines", "80", "--nostream"], { required: false });
}

main().catch((error) => {
  process.stderr.write(`${JSON.stringify({ ok: false, error: error.message, payload: error.payload || null }, null, 2)}\n`);
  process.exit(1);
});
