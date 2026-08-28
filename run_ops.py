# -*- coding: utf-8 -*-
import paramiko, sys, json, time
sys.stdout.reconfigure(encoding='utf-8')

def connect():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)
    return c

client = connect()

def run(cmd, t=120, retries=3):
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

print("=== deploy ===")
print(run("cd /var/www/davidsklad/davidskladik && git pull 2>&1 | tail -1 && node server/assemble.js 2>&1 | tail -1", 120))
run("pm2 restart davidsklad-api davidsklad-worker > /dev/null 2>&1", 60)
time.sleep(15)
print("api:", run("curl -s -o /dev/null -w '%{http_code}' --max-time 12 http://localhost:3000/health"),
      "| worker:", run("curl -s -o /dev/null -w '%{http_code}' --max-time 12 http://localhost:3001/health"))

creds = run("grep -E '^APP_USER=|^APP_PASSWORD=' /var/www/davidsklad/davidskladik/.env")
user = pw = ""
for line in creds.split('\n'):
    if line.startswith('APP_USER='): user = line.split('=',1)[1].strip().strip('"')
    if line.startswith('APP_PASSWORD='): pw = line.split('=',1)[1].strip().strip('"')
run(f'''curl -s -c /tmp/dsk.txt --max-time 15 -X POST http://localhost:3000/api/login -H "Content-Type: application/json" -d '{{"username":"{user}","password":"{pw}"}}' ''')

print("\n=== candidates (onlyEligible, page1) ===")
raw = run('''curl -s -b /tmp/dsk.txt --max-time 60 "http://localhost:3000/api/ozon-yandex-import/candidates?onlyEligible=true&page=1&pageSize=10" -o /tmp/c.json -w "%{http_code} %{time_total}s"''', 70)
print(raw)
d = json.loads(run("cat /tmp/c.json | head -c 60000"))
print("total eligible:", d.get('total'), "| scanCapped:", d.get('scanCapped'), "| items:", len(d.get('items', [])))
for it in (d.get('items') or [])[:5]:
    print("  ", it.get('offerId'), '| eligible:', it.get('eligible'), '| exists:', it.get('existsOnYandex'), '|', (it.get('name') or '')[:40])

print("\n=== candidates search by article ===")
sample_offer = (d.get('items') or [{}])[0].get('offerId', 'YV')
q = run(f'''curl -s -b /tmp/dsk.txt --max-time 60 "http://localhost:3000/api/ozon-yandex-import/candidates?q={sample_offer}&onlyEligible=false&page=1&pageSize=5"''', 70)
qd = json.loads(q)
print(f"search '{sample_offer}': total={qd.get('total')} items={len(qd.get('items',[]))}")

# pick 2 eligible ids and send
ids = [it['id'] for it in (d.get('items') or []) if it.get('eligible')][:2]
print("\n=== send-selected (2 eligible) ===", ids)
if ids:
    body = json.dumps({"ids": ids})
    run(f"cat > /tmp/sel.json << 'X'\n{body}\nX")
    print(run('curl -s -b /tmp/dsk.txt --max-time 120 -X POST http://localhost:3000/api/ozon-yandex-import/send-selected -H "Content-Type: application/json" -d @/tmp/sel.json | head -c 400', 130))
else:
    print("no eligible items to test send")

client.close()
