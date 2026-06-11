import paramiko, sys, time
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, t=30):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=t)
    return stdout.read().decode('utf-8', errors='replace').strip()

print("=== worker CPU state ===")
print(run("ps aux | grep worker-entry | grep -v grep"))

# Capture stack via gdb (works on hung event loop, no inspector needed)
print("\n=== install gdb if missing & capture JS stack ===")
print(run("which gdb || apt-get install -y gdb 2>&1 | tail -1", 60))

# Use kill -USR1 + inspector? simpler: gdb backtrace of main thread
print(run("gdb -p 92954 -batch -ex 'thread 1' -ex 'bt 25' 2>/dev/null | head -40", 60))

client.close()
