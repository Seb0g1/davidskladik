# -*- coding: utf-8 -*-
import paramiko, sys, json, time
sys.stdout.reconfigure(encoding='utf-8')

def connect():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)
    return c

client = connect()

def run(cmd, t=60, retries=3):
    global client
    for attempt in range(retries+1):
        try:
            stdin, stdout, stderr = client.exec_command(cmd, timeout=t)
            return stdout.read().decode('utf-8', errors='replace').strip()
        except Exception:
            if attempt == retries: raise
            try: client.close()
            except: pass
            time.sleep(5)
            client = connect()

print("=== deploy force-bypass cap fix + restart worker ===")
print(run("cd /var/www/davidsklad/davidskladik && git pull 2>&1 | tail -1 && node server/assemble.js 2>&1 | tail -1", 90))
run("pm2 restart davidsklad-worker > /dev/null 2>&1", 40)
time.sleep(12)
print("worker:", run("curl -s --max-time 10 http://localhost:3001/health | head -c 40") or "(no answer)")

creds = run("grep -E '^APP_USER=|^APP_PASSWORD=' /var/www/davidsklad/davidskladik/.env")
user = pw = ""
for line in creds.split('\n'):
    if line.startswith('APP_USER='): user = line.split('=',1)[1].strip().strip('"')
    if line.startswith('APP_PASSWORD='): pw = line.split('=',1)[1].strip().strip('"')
run(f'''curl -s -c /tmp/dsk_cookies_w.txt --max-time 15 -X POST http://localhost:3001/api/login -H "Content-Type: application/json" -d '{{"username":"{user}","password":"{pw}"}}' ''')

print("\n=== start FULL marketplace import (manual sync) on worker ===")
print(run('''curl -s --max-time 30 -b /tmp/dsk_cookies_w.txt -X POST http://localhost:3001/api/warehouse/sync/run -H "Content-Type: application/json" -d '{}' '''))

print("\n=== force-send prices for DIC products to yandex now ===")
body = json.dumps({"confirmed": True, "productIds": [
  "yandex-2da9362a5611414d526a46a6"
], "force": True, "verify": True, "livePriceMaster": True, "marketplace": "yandex"})
run(f"cat > /tmp/dic_body.json << 'BEOF'\n{body}\nBEOF")
print(run('''curl -s --max-time 200 -b /tmp/dsk_cookies_w.txt -X POST http://localhost:3001/api/warehouse/prices/send -H "Content-Type: application/json" -d @/tmp/dic_body.json | head -c 400''', 210))

client.close()
