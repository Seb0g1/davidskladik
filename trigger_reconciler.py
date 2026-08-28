import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, timeout=20):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    if err: print('ERR:', err[:200])
    return out

# Check reconciler endpoints
print("=== Reconciler API state ===")
print(run('curl -s --max-time 5 "http://localhost:3001/api/sync/linked-reconciler/state" 2>&1 | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({k:d[k] for k in [\'cursor\',\'totalLinked\',\'cyclesCompleted\',\'lastRunAt\'] if k in d}, indent=2))" 2>&1', 15))

print("\n=== Routes with reconciler ===")
print(run('grep -r "linked-reconciler" /var/www/davidsklad/davidskladik/server/source.js 2>/dev/null | head -10', 10))

client.close()
