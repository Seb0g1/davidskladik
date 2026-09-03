# -*- coding: utf-8 -*-
# Deploy updated realizatsiya-bot to Netherlands server
# Usage: NL_PASSWORD=... python3 scripts/deploy-realizatsiya-bot.py
import sys, os, pathlib
import paramiko

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HOST = "5.129.238.210"
USER = "root"
PASSWORD = os.environ["NL_PASSWORD"]
REMOTE_DIR = "/opt/bots/realizatsiya-bot"
LOCAL_DIR = pathlib.Path(__file__).parent.parent / "bots" / "realizatsiya-bot"

SKIP = {"node_modules", ".git", "__pycache__"}


def run(ssh, cmd):
    print(f"  $ {cmd}")
    _, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    code = stdout.channel.recv_exit_status()
    if out: print(f"    {out[:500]}")
    if err and code != 0: print(f"    ERR: {err[:300]}")
    return code


def upload_dir(sftp, local: pathlib.Path, remote: str):
    try: sftp.stat(remote)
    except FileNotFoundError: sftp.mkdir(remote)
    for item in local.iterdir():
        if item.name in SKIP: continue
        rpath = remote.rstrip('/') + '/' + item.name
        if item.is_dir():
            upload_dir(sftp, item, rpath)
        else:
            sftp.put(str(item), rpath)
            print(f"  upload: {item.name}")


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)
    print(f"Connected to {HOST}")

    sftp = ssh.open_sftp()

    print(f"\n=== Uploading realizatsiya-bot files ===")
    upload_dir(sftp, LOCAL_DIR, REMOTE_DIR)

    print(f"\n=== npm install ===")
    run(ssh, f"cd {REMOTE_DIR} && npm install --production 2>&1 | tail -3")

    print(f"\n=== Restart PM2 ===")
    run(ssh, "pm2 restart realizatsiya-bot --update-env")
    run(ssh, "sleep 3 && pm2 show realizatsiya-bot | grep -E 'status|restarts|uptime'")

    sftp.close()
    ssh.close()
    print("\n[OK] realizatsiya-bot updated and restarted!")


if __name__ == "__main__":
    main()
