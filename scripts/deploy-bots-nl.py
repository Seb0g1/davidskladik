# -*- coding: utf-8 -*-
# Deploy all 3 Telegram bots to Netherlands server 5.129.238.210
import sys
import os, io, stat, secrets, pathlib, json
import paramiko

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HOST = "5.129.238.210"
USER = "root"
PASSWORD = "kbK-RQyR587EwD"
BOTS_ROOT = "/opt/bots"

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
BOTS_LOCAL = PROJECT_ROOT / "bots"

API_SECRET = secrets.token_hex(32)

BOTS = [
    {
        "name": "realizatsiya-bot",
        "pm2_name": "realizatsiya-bot",
        "local_dir": BOTS_LOCAL / "realizatsiya-bot",
        "env": {
            "TELEGRAM_BOT_TOKEN": "8993485518:AAG0vnkx_QbDiPEiXuWH0Srh6aINOjd64cQ",
            "DAVIDSKLAD_API_BASE": "https://davidsklad.ru",
            "DAVIDSKLAD_API_SECRET": API_SECRET,
            "BOT_USERS_FILE": "./data/users.json",
            "NODE_ENV": "production",
        },
    },
    {
        "name": "errors-bot",
        "pm2_name": "errors-bot",
        "local_dir": BOTS_LOCAL / "errors-bot",
        "env": {
            "TELEGRAM_BOT_TOKEN": "8270081253:AAFbNra1X4VqiiGt4ag0cr_DX6Kvov3uPPY",
            "DAVIDSKLAD_API_BASE": "https://davidsklad.ru",
            "DAVIDSKLAD_API_SECRET": API_SECRET,
            "ADMIN_TELEGRAM_IDS": "",
            "RU_SERVER_HOST": "81.17.154.153",
            "NODE_ENV": "production",
        },
    },
    {
        "name": "magicvibes-bot",
        "pm2_name": "magicvibes-bot",
        "local_dir": BOTS_LOCAL / "magicvibes-bot",
        "env": {
            "TELEGRAM_BOT_TOKEN": "8691183442:AAFoWdQuuNHwYdFUAoJmv3ipvFOVnRkJ83c",
            "MAGICVIBES_API_BASE": "https://magicvibes.ru",
            "ADMIN_TELEGRAM_IDS": "",
            "MAGICVIBES_BOT_USE_WEBHOOK": "false",
            "NODE_ENV": "production",
        },
    },
]

SKIP_DIRS = {"node_modules", ".git", "__pycache__"}
SKIP_EXTS = {".pyc"}


def run(ssh, cmd, check=True):
    print(f"  $ {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    code = stdout.channel.recv_exit_status()
    if out:
        print(f"    {out}")
    if err and code != 0:
        print(f"    ERR: {err}")
    if check and code != 0:
        raise RuntimeError(f"Command failed (exit {code}): {cmd}\n{err}")
    return out, err, code


def sftp_mkdir_p(sftp, remote_path):
    parts = remote_path.split("/")
    cur = ""
    for part in parts:
        if not part:
            cur = "/"
            continue
        cur = cur.rstrip("/") + "/" + part
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            try:
                sftp.mkdir(cur)
            except Exception:
                pass


def upload_dir(sftp, local_dir: pathlib.Path, remote_dir: str):
    sftp_mkdir_p(sftp, remote_dir)
    for item in local_dir.iterdir():
        if item.name in SKIP_DIRS:
            continue
        if item.suffix in SKIP_EXTS:
            continue
        remote_path = remote_dir.rstrip("/") + "/" + item.name
        if item.is_dir():
            upload_dir(sftp, item, remote_path)
        else:
            sftp.put(str(item), remote_path)


def write_env(sftp, remote_dir: str, env: dict):
    content = "\n".join(f"{k}={v}" for k, v in env.items()) + "\n"
    with sftp.open(remote_dir.rstrip("/") + "/.env", "w") as f:
        f.write(content)


def connect():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASSWORD, timeout=15)
    print(f"Connected to {HOST}")
    return ssh


def setup_node(ssh):
    print("\n=== Checking Node.js / npm / PM2 ===")
    _, _, code = run(ssh, "node --version", check=False)
    if code != 0:
        print("  Installing Node.js 20 LTS...")
        run(ssh, "curl -fsSL https://deb.nodesource.com/setup_20.x | bash -")
        run(ssh, "apt-get install -y nodejs")
    else:
        ver, _, _ = run(ssh, "node --version")
        print(f"  Node: {ver}")

    _, _, code = run(ssh, "pm2 --version", check=False)
    if code != 0:
        print("  Installing PM2...")
        run(ssh, "npm install -g pm2")
    else:
        ver, _, _ = run(ssh, "pm2 --version")
        print(f"  PM2: {ver}")


def deploy_bot(ssh, sftp, bot: dict):
    name = bot["name"]
    remote_dir = f"{BOTS_ROOT}/{name}"
    print(f"\n=== Deploying {name} -> {remote_dir} ===")

    run(ssh, f"mkdir -p {remote_dir}/data {remote_dir}/logs")

    print(f"  Uploading files...")
    upload_dir(sftp, bot["local_dir"], remote_dir)

    print(f"  Writing .env...")
    write_env(sftp, remote_dir, bot["env"])

    print(f"  npm install --production...")
    run(ssh, f"cd {remote_dir} && npm install --production 2>&1 | tail -5")

    print(f"  Starting PM2...")
    _, _, code = run(ssh, f"pm2 describe {bot['pm2_name']}", check=False)
    if code == 0:
        run(ssh, f"pm2 restart {bot['pm2_name']}", check=False)
    else:
        run(ssh, f"cd {remote_dir} && pm2 start ecosystem.config.cjs --env production")


def main():
    print(f"Generated DAVIDSKLAD_API_SECRET: {API_SECRET}")
    print("(Add this to .env on the main davidsklad.ru server as DAVIDSKLAD_API_SECRET)\n")

    ssh = connect()
    setup_node(ssh)

    run(ssh, f"mkdir -p {BOTS_ROOT}")
    sftp = ssh.open_sftp()

    for bot in BOTS:
        deploy_bot(ssh, sftp, bot)

    print("\n=== Saving PM2 startup ===")
    run(ssh, "pm2 save")
    run(ssh, "pm2 startup systemd -u root --hp /root 2>&1 | tail -3", check=False)

    print("\n=== Status ===")
    run(ssh, "pm2 list")

    sftp.close()
    ssh.close()

    print("\n[OK] All bots deployed.")
    print(f"\nIMPORTANT - add to main server .env on 81.17.154.153:")
    print(f"  DAVIDSKLAD_API_SECRET={API_SECRET}")
    print(f"  ALERT_BOT_TOKEN=8270081253:AAFbNra1X4VqiiGt4ag0cr_DX6Kvov3uPPY")
    print(f"  ALERT_CHAT_ID=<your Telegram chat ID with @MagicVibeAlert_bot>")
    print(f"\nFor admin commands in bots 2 & 3:")
    print(f"  Send /start to @MagicVibeAlert_bot and @magicvibepafrum_bot to get your chat ID,")
    print(f"  then set ADMIN_TELEGRAM_IDS on the server: pm2 restart errors-bot magicvibes-bot")


if __name__ == "__main__":
    main()
