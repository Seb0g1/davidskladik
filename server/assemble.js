"use strict";

const path = require("path");
const Module = require("module");
const { readServerSource } = require("./source");

const projectRoot = path.join(__dirname, "..");
const source = readServerSource();

const assembled = new Module(path.join(projectRoot, "server.js"));
assembled.filename = path.join(projectRoot, "server.js");
assembled.paths = Module._nodeModulePaths(projectRoot);
assembled._compile(source, assembled.filename);

module.exports = assembled.exports;
