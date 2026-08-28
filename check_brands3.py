import paramiko, sys
sys.stdout.reconfigure(encoding='utf-8')
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('81.17.154.153', username='root', password='pm^e7-jVL_gAyM', timeout=30)

def run(cmd, timeout=30):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    if err:
        print('ERR:', err[:200])
    return out

print("=== Sample of Yandex archived+linked products ===")
print(run(r"""cd /var/www/davidsklad/davidskladik && node -e "
const {PrismaClient} = require('./node_modules/@prisma/client');
const p = new PrismaClient();
p.warehouseProduct.findMany({
  where:{marketplace:'yandex',archived:true,links:{some:{}}},
  select:{id:true,offerId:true,raw:true,links:{select:{partnerId:true,keyword:true}}},
  take:10
}).then(rows=>{
  const withSup = rows.filter(r=>(r.raw||{}).selectedSupplier);
  const noSup = rows.filter(r=>!(r.raw||{}).selectedSupplier);
  console.log('with selectedSupplier:', withSup.length, 'without:', noSup.length);
  noSup.slice(0,4).forEach(r=>{
    const raw=r.raw||{};
    console.log('NO SUP id='+r.id+' offerId='+r.offerId+' name='+(raw.name||'').slice(0,40));
    console.log('  links='+JSON.stringify(r.links));
    console.log('  noSupAuto='+JSON.stringify(raw.noSupplierAutomation||'{}'));
  });
  withSup.slice(0,3).forEach(r=>{
    const raw=r.raw||{};
    console.log('HAS SUP id='+r.id+' selSup='+raw.selectedSupplier+' recovery='+JSON.stringify(raw.noSupplierAutomation||'{}'));
  });
  p['\$disconnect']();
}).catch(e=>{console.error(e.message);p['\$disconnect']();})
" 2>&1""", 25))

client.close()
