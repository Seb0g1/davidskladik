import paramiko, sys, json
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, timeout=25):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# Check these brands in DB via node script
script = r"""
cd /var/www/davidsklad/davidskladik && node --input-type=module << 'NODEOF'
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();

const brands = ['Di Caprio', 'Gleam', 'PERROY', 'Nasamat'];

for (const brand of brands) {
  const rows = await p.warehouseProduct.findMany({
    where: {
      OR: [
        { raw: { path: ['brand'], string_contains: brand } },
        { raw: { path: ['ozon', 'brand'], string_contains: brand } },
        { raw: { path: ['name'], string_contains: brand } },
      ],
      links: { some: {} }
    },
    select: { id: true, marketplace: true, archived: true, offerId: true, raw: true },
    take: 5
  });
  console.log(`\nBrand: ${brand}, linked products: ${rows.length}`);
  rows.forEach(r => {
    const raw = r.raw || {};
    console.log(`  id=${r.id} mp=${r.marketplace} archived=${r.archived} offerId=${r.offerId} brand="${raw.brand || raw.ozon?.brand || ''}" `);
  });
}

await p.$disconnect();
NODEOF
"""

print("=== Brand check in DB ===")
print(run(script, 30))

# Check reconciler state - how many products total, where cursor is
print("\n=== Linked reconciler cursor state ===")
print(run('curl -s "http://localhost:3000/api/sync/linked-reconciler/state" 2>&1'))

client.close()
