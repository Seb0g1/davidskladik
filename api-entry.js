process.env.SERVER_ROLE = process.env.SERVER_ROLE || "api";
const { startServer } = require("./server.js");
void startServer();
