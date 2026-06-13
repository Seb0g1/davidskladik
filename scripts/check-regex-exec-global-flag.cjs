#!/usr/bin/env node
"use strict";

// PLAN-HARDENING.md 2.1: `while (re.exec(text))` without the `g` flag never advances
// `lastIndex`, so the loop spins on the same match forever — this incident froze both
// the api and worker processes (see the writeup and fixed pattern in
// 02a-ozon-yandex-import-cleanup.js). This script flags while-loops whose .exec()
// target can be statically traced to a regex that is missing `g`. Cases it can't trace
// (e.g. a regex returned from a function call) are printed as warnings to review
// manually, not build failures.

const fs = require("fs");
const path = require("path");

const WHILE_EXEC_RE = /while\s*\(\s*(?:\(\s*[\w.$]+\s*=\s*)?(\/(?:\\.|[^/\\\n])+\/[a-z]*|[\w.$]+)\.exec\(/g;
const REGEX_LITERAL_RE = /^\/(?:\\.|[^/\\\n])+\/([a-z]*)$/;

function flagsFromLiteral(literal) {
  const match = REGEX_LITERAL_RE.exec(literal);
  return match ? match[1] : null;
}

// `openParenIndex` points at the "(" of a `new RegExp(...)` call. Looks for the first
// `, "<flags>")` after it without trying to balance nested parens/brackets.
function flagsFromNewRegexpCall(source, openParenIndex) {
  const window = source.slice(openParenIndex, openParenIndex + 400);
  const match = /^\([\s\S]{0,380}?,\s*["'`]([a-z]*)["'`]\s*\)/.exec(window);
  return match ? match[1] : null;
}

function findArrayRegexFlags(source, arrayName) {
  const escaped = arrayName.replace(/[$.]/g, "\\$&");
  const arrayMatch = new RegExp(`\\b${escaped}\\s*=\\s*\\[([\\s\\S]*?)\\];`).exec(source);
  if (!arrayMatch) return null;
  const body = arrayMatch[1];

  const flagsList = [];

  const literalRe = /\/(?:\\.|[^/\\\n])+\/([a-z]*)/g;
  let literalMatch;
  while ((literalMatch = literalRe.exec(body))) flagsList.push(literalMatch[1]);

  const newRegexpRe = /new RegExp\(/g;
  let newRegexpMatch;
  while ((newRegexpMatch = newRegexpRe.exec(body))) {
    const openIdx = newRegexpMatch.index + "new RegExp".length;
    flagsList.push(flagsFromNewRegexpCall(body, openIdx));
  }

  if (!flagsList.length) return null;
  if (flagsList.some((flags) => flags === null)) return null;
  return flagsList.every((flags) => flags.includes("g")) ? "g" : "";
}

// Returns the flags of the regex bound to `identifier`, or null if it can't be traced.
function findIdentifierFlags(source, identifier) {
  const escaped = identifier.replace(/[$.]/g, "\\$&");

  const literalAssign = new RegExp(`\\b${escaped}\\s*=\\s*(\\/(?:\\\\.|[^/\\\\\\n])+\\/[a-z]*)`).exec(source);
  if (literalAssign) return flagsFromLiteral(literalAssign[1]);

  const newRegexpAssign = new RegExp(`\\b${escaped}\\s*=\\s*new RegExp`).exec(source);
  if (newRegexpAssign) {
    const openIdx = newRegexpAssign.index + newRegexpAssign[0].length;
    return flagsFromNewRegexpCall(source, openIdx);
  }

  const forOf = new RegExp(`for\\s*\\(\\s*(?:const|let|var)\\s+${escaped}\\s+of\\s+([\\w.$]+)`).exec(source);
  if (forOf) return findArrayRegexFlags(source, forOf[1]);

  return null;
}

// Scans `source` for `while (...exec(...))` loops and classifies each into a violation
// (the traced regex is missing `g`) or a warning (the regex can't be traced).
function scanSourceForExecLoopViolations(source) {
  const violations = [];
  const warnings = [];

  WHILE_EXEC_RE.lastIndex = 0;
  let match;
  while ((match = WHILE_EXEC_RE.exec(source))) {
    const target = match[1];
    const line = source.slice(0, match.index).split("\n").length;
    const flags = target.startsWith("/") ? flagsFromLiteral(target) : findIdentifierFlags(source, target);
    if (flags === null) {
      warnings.push({ line, target });
    } else if (!flags.includes("g")) {
      violations.push({ line, target, flags });
    }
  }

  return { violations, warnings };
}

function main() {
  const partsDir = path.join(__dirname, "..", "server", "parts");
  const violations = [];
  const warnings = [];

  for (const file of fs.readdirSync(partsDir)) {
    if (!file.endsWith(".js")) continue;
    const source = fs.readFileSync(path.join(partsDir, file), "utf8");
    const result = scanSourceForExecLoopViolations(source);
    for (const violation of result.violations) violations.push({ file, ...violation });
    for (const warning of result.warnings) warnings.push({ file, ...warning });
  }

  for (const warning of warnings) {
    console.warn(`WARN: ${warning.file}:${warning.line} — while(...${warning.target}.exec(...)) — could not confirm the "g" flag, review manually.`);
  }

  if (violations.length) {
    console.error("Found while(...exec(...)) loops whose regex is missing the \"g\" flag:\n");
    for (const violation of violations) {
      console.error(`  ${violation.file}:${violation.line} — ${violation.target} (flags: "${violation.flags}")`);
    }
    console.error(
      "\nPLAN-HARDENING.md 2.1: `while (re.exec(text))` without the \"g\" flag never advances\n" +
      "lastIndex, so the loop never terminates — this is the bug that previously froze the\n" +
      "api and worker processes. Add the \"g\" flag to the regex.",
    );
    process.exit(1);
  }

  console.log("OK: while(...exec(...)) loops use globally-flagged regexes.");
}

if (require.main === module) main();

module.exports = { scanSourceForExecLoopViolations };
