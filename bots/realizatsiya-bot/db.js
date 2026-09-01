'use strict';

const fs = require('fs');
const path = require('path');

const USERS_FILE = path.resolve(process.env.BOT_USERS_FILE || './data/users.json');

function ensureDir() {
  fs.mkdirSync(path.dirname(USERS_FILE), { recursive: true });
}

function loadUsers() {
  ensureDir();
  try { return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')); } catch { return {}; }
}

function saveUsers(users) {
  ensureDir();
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
}

function getUser(chatId) {
  return loadUsers()[String(chatId)] || null;
}

function setUser(chatId, data) {
  const users = loadUsers();
  users[String(chatId)] = { ...users[String(chatId)], ...data, chatId: String(chatId) };
  saveUsers(users);
  return users[String(chatId)];
}

module.exports = { loadUsers, saveUsers, getUser, setUser };
