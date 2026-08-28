#!/usr/bin/env node
"use strict";
require("dotenv").config();
const path = require("path");
const { Client } = require("ssh2");
const fs = require("fs");

const password = process.env.DEPLOY_PASSWORD;
const remoteRoot = "/var/www/davidsklad/davidskladik";

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    sftp.fastPut(path.join(__dirname, "_avito_sample_check.cjs"), `${remoteRoot}/_avito_sample_check.cjs`, {}, (err2) => {
      if (err2) { console.error(err2); conn.end(); return; }
      conn.exec(`cd ${remoteRoot} && node --env-file .env _avito_sample_check.cjs; rm -f _avito_sample_check.cjs`, (err3, stream) => {
        if (err3) { console.error(err3); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => conn.end());
      });
    });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
