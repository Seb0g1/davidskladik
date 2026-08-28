#!/usr/bin/env python3
"""SSH and look for MySQL credentials in config files."""
import paramiko

SSH_HOST = "81.17.154.153"
SSH_USER = "pricemasteddb"
SSH_PASS = "F4_P0F3%SAp4Yu?"

CMDS = [
    ("mysql port",          "ss -tlnp 2>/dev/null | grep mysql || netstat -tlnp 2>/dev/null | grep mysql"),
    ("processes",           "ps aux | grep mysql | grep -v grep"),
    ("debian.cnf readable?","cat /etc/mysql/debian.cnf 2>&1 || echo NOT_READABLE"),
    ("root .my.cnf?",       "cat /root/.my.cnf 2>&1 || echo NOT_READABLE"),
    ("user .my.cnf?",       "cat ~/.my.cnf 2>&1 || echo NOT_FOUND"),
    ("mysql config dir",    "ls /etc/mysql/ 2>&1"),
    ("my.cnf content",      "cat /etc/mysql/my.cnf 2>/dev/null || cat /etc/my.cnf 2>/dev/null || echo NOT_FOUND"),
    ("mysql user",          "id $(ps aux | grep mysqld | grep -v grep | awk '{print $1}' | head -1) 2>/dev/null || echo unknown"),
    ("whoami",              "whoami && id"),
    ("sudo list",           "sudo -l 2>&1 | head -20"),
]

def main():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS, timeout=20)
    print(f"SSH OK as {SSH_USER}\n")

    for label, cmd in CMDS:
        stdin, stdout, stderr = client.exec_command(cmd)
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        combined = (out + (" | ERR: " + err if err else "")).strip()
        print(f"=== {label} ===")
        print(combined or "(empty)")
        print()

    client.close()

if __name__ == "__main__":
    main()
