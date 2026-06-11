import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, timeout=60):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    if err and 'warning' not in err.lower(): print('ERR:', err[:400])
    return out

print("=== git pull ===")
print(run('cd /var/www/davidsklad/davidskladik && git pull 2>&1', 45))

print("\n=== assemble ===")
print(run('cd /var/www/davidsklad/davidskladik && node server/assemble.js 2>&1 | tail -2', 30))

print("\n=== pm2 restart worker ===")
print(run('pm2 restart davidsklad-worker 2>&1 | tail -3', 30))

import time
time.sleep(5)

print("\n=== pm2 status ===")
print(run('pm2 list 2>&1 | grep davidsklad', 10))

print("\n=== last worker logs ===")
print(run('tail -10 /root/.pm2/logs/davidsklad-worker-out.log 2>/dev/null || pm2 logs davidsklad-worker --lines 8 --nostream 2>&1 | tail -10', 15))

client.close()
