'use strict';

const fs = require('fs');
const path = require('path');

const DB_PATH = path.resolve('./data/users.json');
const BROADCAST_STATE_PATH = path.resolve('./data/broadcast-state.json');

function loadDB() {
  try {
    return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
  } catch {
    return {};
  }
}

function saveDB(data) {
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2), 'utf8');
}

// User record: { telegramId, email, token, customer, linkedAt, subscribedBrands, state }
function getUser(telegramId) {
  const db = loadDB();
  return db[String(telegramId)] || null;
}

function setUser(telegramId, data) {
  const db = loadDB();
  db[String(telegramId)] = { ...(db[String(telegramId)] || {}), ...data };
  saveDB(db);
  return db[String(telegramId)];
}

function getAllUsers() {
  const db = loadDB();
  return Object.values(db);
}

function deleteUser(telegramId) {
  const db = loadDB();
  delete db[String(telegramId)];
  saveDB(db);
}

// Pending state machine for multi-step flows
function setUserState(telegramId, state, meta = {}) {
  setUser(telegramId, { _state: state, _stateMeta: meta });
}

function clearUserState(telegramId) {
  const db = loadDB();
  const user = db[String(telegramId)];
  if (user) {
    delete user._state;
    delete user._stateMeta;
    saveDB(db);
  }
}

function getUserState(telegramId) {
  const user = getUser(telegramId);
  return user ? { state: user._state || null, meta: user._stateMeta || {} } : { state: null, meta: {} };
}

// Broadcast anti-spam: one broadcast per day
function getLastBroadcastTime() {
  try {
    const d = JSON.parse(fs.readFileSync(BROADCAST_STATE_PATH, 'utf8'));
    return d.lastBroadcast || 0;
  } catch {
    return 0;
  }
}

function setLastBroadcastTime() {
  fs.mkdirSync(path.dirname(BROADCAST_STATE_PATH), { recursive: true });
  fs.writeFileSync(BROADCAST_STATE_PATH, JSON.stringify({ lastBroadcast: Date.now() }), 'utf8');
}

module.exports = {
  getUser,
  setUser,
  getAllUsers,
  deleteUser,
  setUserState,
  clearUserState,
  getUserState,
  getLastBroadcastTime,
  setLastBroadcastTime,
};
