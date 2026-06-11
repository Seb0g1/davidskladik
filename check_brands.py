import paramiko, sys, json
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, timeout=25):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# Check Di Caprio, Gleam, PERROY, Nasamat - are they linked and in what state?
brands = ['Di Caprio', 'Gleam', 'PERROY', 'Nasamat']

print("=== Brand archive status in recent logs ===")
for brand in brands:
    result = run(f'grep -i "{brand}" /root/.pm2/logs/davidsklad-worker-out-2.log | tail -3')
    print(f"\n{brand}:")
    print(result[:500] if result else "(no logs)")

# Check DB for these brands
print("\n=== Checking linked reconciler state ===")
print(run('grep "linked_reconciler_complete" /root/.pm2/logs/davidsklad-worker-out-2.log | tail -5'))

# Check if yandex unarchive queue cleaned up
print("\n=== Yandex unarchive queue after new deploy ===")
print(run('grep "yandex unarchive queue" /root/.pm2/logs/davidsklad-worker-out-2.log | tail -8'))

client.close()
