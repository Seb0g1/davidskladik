# -*- coding: utf-8 -*-
# Quick ops on main prod server: reset consignment + update .env
# Usage: PROD_PASSWORD=... PROD_API_SECRET=... python3 scripts/prod-quick-ops.py
import sys, os, pathlib
import paramiko

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HOST = "81.17.154.153"
USER = "root"
PASSWORD = os.environ["PROD_PASSWORD"]
REMOTE_ROOT = "/var/www/davidsklad/davidskladik"
DAVIDSKLAD_API_SECRET = os.environ.get("PROD_API_SECRET", "6da1ef4a181f64f5c7d3e2d539afca3289c0811bdb4fe21d300e8bd07bcfaf85")
ALERT_BOT_TOKEN = "8270081253:AAFbNra1X4VqiiGt4ag0cr_DX6Kvov3uPPY"


def run(ssh, cmd):
    print(f"  $ {cmd[:120]}")
    _, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    code = stdout.channel.recv_exit_status()
    if out:
        print(f"    {out[:400]}")
    if err and code != 0:
        print(f"    ERR: {err[:300]}")
    return out, code


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)
    print(f"Connected to {HOST}")

    sftp = ssh.open_sftp()

    # 1. Update .env
    print("\n=== Updating .env ===")
    env_path = f"{REMOTE_ROOT}/.env"
    try:
        with sftp.open(env_path, "r") as f:
            current_env = f.read().decode("utf-8", errors="replace")
    except Exception:
        current_env = ""

    keys_to_set = {
        "DAVIDSKLAD_API_SECRET": DAVIDSKLAD_API_SECRET,
        "ALERT_BOT_TOKEN": ALERT_BOT_TOKEN,
    }
    lines = current_env.splitlines()
    updated = []
    for line in lines:
        key = line.split("=", 1)[0].strip()
        if key in keys_to_set:
            updated.append(f"{key}={keys_to_set.pop(key)}")
        else:
            updated.append(line)
    for k, v in keys_to_set.items():
        updated.append(f"{k}={v}")
        print(f"  + {k}")

    with sftp.open(env_path, "w") as f:
        f.write("\n".join(updated) + "\n")
    print("  .env updated")

    # 2. Upload & run consignment reset
    print("\n=== Resetting consignment data ===")
    local_script = pathlib.Path(__file__).parent / "reset-consignment.cjs"
    remote_script = f"{REMOTE_ROOT}/scripts/reset-consignment.cjs"
    sftp.put(str(local_script), remote_script)
    out, code = run(ssh, f"cd {REMOTE_ROOT} && node scripts/reset-consignment.cjs")
    if code != 0:
        print("  RESET FAILED")
    else:
        print("  Consignment data cleared")

    # 3. Restart to pick up new .env
    print("\n=== Restarting services ===")
    run(ssh, "pm2 restart davidsklad-api davidsklad-worker --update-env")
    run(ssh, "sleep 8")
    run(ssh, "pm2 list | grep davidsklad")

    sftp.close()
    ssh.close()
    print("\n[OK] Done. Bot /balance should now work.")


if __name__ == "__main__":
    main()
