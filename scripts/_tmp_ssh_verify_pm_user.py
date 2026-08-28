#!/usr/bin/env python3
"""Verify PriceMaster_Other user was created and show grants."""
import sys
import paramiko

SSH_HOST = "81.17.154.153"
SSH_USER = "pricemasteddb"
SSH_PASS = "F4_P0F3%SAp4Yu?"

VERIFY_SQL = """
SELECT User, Host FROM mysql.user WHERE User='PriceMaster_Other';
SHOW GRANTS FOR 'PriceMaster_Other'@'%';
SELECT COUNT(*) AS view_exists FROM information_schema.VIEWS WHERE TABLE_SCHEMA='PriceMasterDB' AND TABLE_NAME='v_pm_other_catalog';
"""

def main():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS, timeout=20)
    print("SSH OK")

    cmd_prefix = f"echo '{SSH_PASS}' | sudo -S mysql"
    full = "cat <<'ENDSQL' | " + cmd_prefix + " 2>&1\n" + VERIFY_SQL + "\nENDSQL"
    stdin, stdout, stderr = client.exec_command(full)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    rc = stdout.channel.recv_exit_status()
    print(f"exit={rc}")
    for line in (out + err).splitlines():
        print(line)
    client.close()

if __name__ == "__main__":
    main()
