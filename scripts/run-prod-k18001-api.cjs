#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
function exec(conn, command, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("exec timeout")), timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => { clearTimeout(timer); resolve(); });
    });
  });
}
async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
  });
  try {
    const auth = Buffer.from("david:CGJ-Ge-90").toString("base64");
    const headers = `-H "Authorization: Basic ${auth}" -H "Content-Type: application/json"`;

    console.log("\n=== K18001 price breakdown from API ===\n");
    await exec(conn, `curl -s ${headers} "http://localhost:3000/api/warehouse/prices/breakdown?search=K18001&limit=10" | node -e "
      let d='';
      process.stdin.on('data',c=>d+=c);
      process.stdin.on('end',()=>{
        const r=JSON.parse(d);
        const items=(r.data||r.items||r||[]).filter(p=>p.offerId==='K18001'||p.offer_id==='K18001');
        items.forEach(p=>{
          console.log('['+p.marketplace+'] current='+p.currentPrice+' next='+p.nextPrice+' usdRate='+p.usdRate+' markup='+p.markupCoefficient);
          if(p.selectedSupplier){const s=p.selectedSupplier;console.log('  supplier:'+s.partnerName+' price='+s.price+' currency='+(s.priceCurrency||s.currency)+' coef='+s.markupCoefficient+' final='+s.effectiveFinalPrice);}
        });
        if(!items.length){console.log('(not found in breakdown)'); console.log(JSON.stringify(r).slice(0,300));}
      });
    "`, 25000);

    console.log("\n=== ЮК345754 price breakdown ===\n");
    await exec(conn, `curl -s ${headers} "http://localhost:3000/api/warehouse/prices/breakdown?search=%D0%AE%D0%9A345754&limit=5" | node -e "
      let d='';
      process.stdin.on('data',c=>d+=c);
      process.stdin.on('end',()=>{
        const r=JSON.parse(d);
        const items=(r.data||r.items||r||[]).filter(p=>p.offerId==='ЮК345754'||p.offer_id==='ЮК345754');
        items.forEach(p=>{
          console.log('['+p.marketplace+'] current='+p.currentPrice+' next='+p.nextPrice+' usdRate='+p.usdRate+' markup='+p.markupCoefficient);
          if(p.selectedSupplier){const s=p.selectedSupplier;console.log('  supplier:'+s.partnerName+' price='+s.price+' currency='+(s.priceCurrency||s.currency)+' coef='+s.markupCoefficient+' final='+s.effectiveFinalPrice);}
        });
        if(!items.length){console.log('(not found)'); console.log(JSON.stringify(r).slice(0,300));}
      });
    "`, 25000);
  } finally { conn.end(); }
}
main().catch((e) => { console.error(e.message); process.exit(1); });
