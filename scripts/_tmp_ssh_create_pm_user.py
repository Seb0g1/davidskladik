#!/usr/bin/env python3
"""SSH into server, upload SQL via SFTP, run as sudo mysql."""
import io, sys
import paramiko

SSH_HOST = "81.17.154.153"
SSH_USER = "pricemasteddb"
SSH_PASS = "F4_P0F3%SAp4Yu?"
MYSQL_PORT = 65123

SQL = """
DROP USER IF EXISTS 'PriceMaster_Other'@'%';
CREATE USER 'PriceMaster_Other'@'%' IDENTIFIED BY '9UyMLHXu.1tJ:Qz';
GRANT SELECT (RowID,DocID,NativeID,NativeName,NativePrice,Active,Ignored)
  ON `PriceMasterDB`.`OfferRows` TO 'PriceMaster_Other'@'%';
GRANT SELECT (DocID,DocDate,PartnerID)
  ON `PriceMasterDB`.`OfferDocs` TO 'PriceMaster_Other'@'%';
GRANT SELECT (PartnerID,PartnerName)
  ON `PriceMasterDB`.`Partners` TO 'PriceMaster_Other'@'%';
CREATE OR REPLACE VIEW `PriceMasterDB`.`v_pm_other_catalog` AS
  SELECT p.PartnerName AS supplier_name, r.NativeName AS product_name,
         r.NativeID AS article_number, r.NativePrice AS price_usd,
         d.DocDate AS last_updated
  FROM `PriceMasterDB`.`OfferRows` r
  JOIN `PriceMasterDB`.`OfferDocs` d ON d.DocID=r.DocID
  JOIN `PriceMasterDB`.`Partners` p ON p.PartnerID=d.PartnerID
  WHERE r.Ignored=0 AND r.Active=1 AND r.NativePrice>0;
GRANT SELECT ON `PriceMasterDB`.`v_pm_other_catalog` TO 'PriceMaster_Other'@'%';
FLUSH PRIVILEGES;
SELECT User, Host FROM mysql.user WHERE User='PriceMaster_Other';
SHOW GRANTS FOR 'PriceMaster_Other'@'%';
"""

VERIFY_ONLY_SQL = """
SELECT User, Host FROM mysql.user WHERE User='PriceMaster_Other';
SHOW GRANTS FOR 'PriceMaster_Other'@'%';
SELECT COUNT(*) AS view_exists FROM information_schema.VIEWS
  WHERE TABLE_SCHEMA='PriceMasterDB' AND TABLE_NAME='v_pm_other_catalog';
"""

def run_sql_via_sftp(client, sql_content, label=""):
    tmp_path = "/tmp/_pm_setup_sql.sql"
    sftp = client.open_sftp()
    sftp.putfo(io.BytesIO(sql_content.encode("utf-8")), tmp_path)
    sftp.close()
    print(f"  SQL uploaded to {tmp_path}")

    cmd = f"echo '{SSH_PASS}' | sudo -S mysql < {tmp_path} 2>&1; rm -f {tmp_path}"
    stdin, stdout, stderr = client.exec_command(cmd)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    rc = stdout.channel.recv_exit_status()
    combined = (out + err).strip()
    print(f"  exit={rc}")
    for line in combined.splitlines():
        print(f"  {line}")
    return rc == 0 and "ERROR" not in combined.upper()

def main():
    print(f"Connecting SSH {SSH_USER}@{SSH_HOST} ...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS, timeout=20)
    print("SSH OK\n")

    print("[SETUP] Uploading and running SQL as sudo mysql...")
    ok = run_sql_via_sftp(client, SQL, "create user")

    if ok:
        print("\n[SUCCESS] User PriceMaster_Other created!")
    else:
        print("\n[PARTIAL] Checking current state...")
        run_sql_via_sftp(client, VERIFY_ONLY_SQL, "verify")

    client.close()

if __name__ == "__main__":
    main()
