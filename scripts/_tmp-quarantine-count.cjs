const path=require("node:path");
require("dotenv").config({path:path.join("/var/www/davidsklad/davidskladik",".env")});
process.chdir("/var/www/davidsklad/davidskladik");
const fs=require("fs");
const data=JSON.parse(fs.readFileSync("data/price-retry-queue.json","utf8"));
const items=Array.isArray(data.items)?data.items:(Array.isArray(data)?data:[]);
const q=items.filter(r=>/quarantine/i.test(String(r.lastError||r.retryReason||"")));
console.log(JSON.stringify({retryQueueTotal:items.length,quarantineLike:q.length,sample:q.slice(0,3).map(r=>({offerId:r.offerId,retryReason:r.retryReason,lastError:String(r.lastError||"").slice(0,80)}))}));
