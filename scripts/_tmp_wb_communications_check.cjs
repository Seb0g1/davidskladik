#!/usr/bin/env node
"use strict";

// Read-only: проверка WB-веток отзывов/вопросов/чатов на проде.

const fs = require("node:fs");
const path = require("node:path");

const BASE = "https://davidsklad.ru";

function readEnvCreds() {
  const text = fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8");
  const get = (key) => {
    const match = text.match(new RegExp(`^${key}=(.*)$`, "m"));
    return match ? match[1].trim().replace(/^"|"$/g, "") : "";
  };
  return { username: get("APP_USER"), password: get("APP_PASSWORD") };
}

let cookie = "";

async function api(method, urlPath, body) {
  const response = await fetch(`${BASE}${urlPath}`, {
    method,
    headers: { "Content-Type": "application/json", ...(cookie ? { Cookie: cookie } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const setCookie = response.headers.get("set-cookie");
  if (setCookie) cookie = setCookie.split(";")[0];
  return { status: response.status, data: await response.json().catch(() => null) };
}

async function main() {
  const login = await api("POST", "/api/login", readEnvCreds());
  if (login.status !== 200) throw new Error(`login failed: ${login.status}`);

  const reviews = await api("GET", "/api/reviews?marketplace=wb&unanswered=false&limit=20");
  console.log(`=== reviews wb (${reviews.status}) rows=${reviews.data?.rows?.length} warnings=${JSON.stringify(reviews.data?.warnings)}`);
  for (const row of (reviews.data?.rows || []).slice(0, 3)) {
    console.log(`  ${row.rating}★ ${row.productName?.slice(0, 40)} | ${row.text?.slice(0, 60)} | needsReply=${row.needsReply}`);
  }

  const questions = await api("GET", "/api/questions?unanswered=false&limit=30");
  const wbQuestions = (questions.data?.rows || []).filter((row) => row.marketplace === "wb");
  console.log(`\n=== questions (${questions.status}) total=${questions.data?.rows?.length} wb=${wbQuestions.length} warnings=${JSON.stringify(questions.data?.warnings)}`);
  for (const row of wbQuestions.slice(0, 3)) {
    console.log(`  ${row.productName?.slice(0, 40)} | ${row.text?.slice(0, 60)} | needsAnswer=${row.needsAnswer}`);
  }

  const chats = await api("GET", "/api/chats?marketplace=wb");
  console.log(`\n=== chats wb (${chats.status}) rows=${chats.data?.rows?.length} warnings=${JSON.stringify(chats.data?.warnings)}`);
  for (const row of (chats.data?.rows || []).slice(0, 3)) {
    console.log(`  ${row.title} | lastMessageAt=${row.lastMessageAt} | replySign=${row.replySign ? "есть" : "НЕТ"}`);
  }
  const firstChat = (chats.data?.rows || [])[0];
  if (firstChat) {
    const history = await api("GET", `/api/chats/history?marketplace=wb&target=${encodeURIComponent(firstChat.target)}&chatId=${encodeURIComponent(firstChat.chatId)}`);
    console.log(`\n=== history "${firstChat.title}" (${history.status}) messages=${history.data?.rows?.length}`);
    for (const message of (history.data?.rows || []).slice(-3)) {
      console.log(`  [${message.createdAt}] ${message.author}: ${message.text?.slice(0, 70)}`);
    }
  }
}

main().catch((error) => { console.error(error.message); process.exit(1); });
