"use strict";

const server = require("./server/assemble");

module.exports = server;

if (require.main === module) {
  void server.startServer();
}
