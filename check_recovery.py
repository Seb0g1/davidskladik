# -*- coding: utf-8 -*-
import paramiko, sys, json, time
sys.stdout.reconfigure(encoding='utf-8')

def connect():
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)
    return c

def runc(c, cmd, t=60):
    stdin, stdout, stderr = c.exec_command(cmd, timeout=t)
    return stdout.read().decode('utf-8', errors='replace').strip()

for i in range(40):
    time.sleep(90)
    try:
        c = connect()
        t = runc(c, 'date +%T')
        status = runc(c, '''curl -s --max-time 15 -b /tmp/dsk_cookies_w.txt http://localhost:3001/api/warehouse/sync/status 2>/dev/null | head -c 300''')
        if not status or 'running' not in status:
            # fallback: check via run endpoint state in logs
            status = runc(c, 'grep -E "manual warehouse sync|sync complete" /root/.pm2/logs/davidsklad-worker-out-2.log | tail -1 | cut -c1-200')
        counts = runc(c, '''cd /var/www/davidsklad/davidskladik && node -e "
var p=new(require('./node_modules/@prisma/client').PrismaClient)();
Promise.all([p.warehouseProduct.count({where:{marketplace:'ozon'}}),p.warehouseProduct.count({where:{marketplace:'yandex'}}),
p.warehouseProduct.count({where:{offerId:'532623523523'}})])
.then(function(a){console.log('ozon='+a[0]+' yandex='+a[1]+' target_article_found='+a[2]);process.exit(0);})" 2>&1''', 40)
        health = runc(c, 'curl -s -o /dev/null -w "%{http_code}" --max-time 8 http://localhost:3001/health')
        c.close()
        print(f"[{t}] {counts} | worker={health}")
        print(f"  status: {status[:180]}")
        if 'target_article_found=1' in counts:
            print("*** 532623523523 imported! ***")
            break
        if '"running":false' in status or 'finishedAt' in status and 'null' not in status[:200]:
            pass
    except Exception as e:
        print(f"[err] {e}")
