# -*- coding: utf-8 -*-
import paramiko, sys, json, time
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)
def run(cmd, t=120):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=t)
    return stdout.read().decode('utf-8', errors='replace').strip()

# count ozon set products not on yandex (candidates for import now)
script = r'''
var p = new (require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client").PrismaClient)();
(async function(){
  var ozonSets = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM warehouse_products WHERE marketplace='ozon' AND archived=false AND name ILIKE '%набор%'");
  var yandexSets = await p.$queryRawUnsafe("SELECT COUNT(*)::int AS n FROM warehouse_products WHERE marketplace='yandex' AND name ILIKE '%набор%'");
  console.log("ozon active sets:", ozonSets[0].n, "| yandex sets:", yandexSets[0].n);
  var sample = await p.$queryRawUnsafe("SELECT offer_id, LEFT(name,55) AS name FROM warehouse_products WHERE marketplace='ozon' AND archived=false AND name ILIKE '%набор%' LIMIT 4");
  sample.forEach(function(r){ console.log("  ", r.offer_id, "|", r.name); });
  process.exit(0);
})().catch(function(e){console.error("ERR", e.message.slice(0,200));process.exit(1);});
'''
run("cat > /tmp/sets.js << 'X'\n" + script + "\nX")
print("=== sets in DB ===")
print(run("node /tmp/sets.js 2>&1"))

# trigger auto-import (now sets are eligible); it logs ozon_yandex_auto_import_complete
creds = run("grep -E '^APP_USER=|^APP_PASSWORD=' /var/www/davidsklad/davidskladik/.env")
user = pw = ""
for line in creds.split('\n'):
    if line.startswith('APP_USER='): user = line.split('=',1)[1].strip().strip('"')
    if line.startswith('APP_PASSWORD='): pw = line.split('=',1)[1].strip().strip('"')
run(f'''curl -s -c /tmp/wk.txt --max-time 15 -X POST http://localhost:3001/api/login -H "Content-Type: application/json" -d '{{"username":"{user}","password":"{pw}"}}' ''')
print("\n=== auto-import run (limit 200) ===")
print(run('curl -s -b /tmp/wk.txt --max-time 600 -X POST http://localhost:3001/api/ozon-yandex-import/auto-run -H "Content-Type: application/json" -d \'{"limit":200}\' | head -c 400', 610))
client.close()
