process.env.SERVER_ROLE = process.env.SERVER_ROLE || "worker";
const { startServer } = require("./server.js");
void startServer();
