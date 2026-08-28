// One-time script: add missing TN VED category assignments and re-run apply + Yandex
const https = require('https');

function fetchProd(path, method, body, cookie) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: 'davidsklad.ru', path, method: method || 'GET',
      headers: {
        'Content-Type': 'application/json',
        ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    };
    const req = https.request(opts, res => {
      let d = ''; res.on('data', c => d += c); res.on('end', () => resolve({ status: res.statusCode, body: d, headers: res.headers }));
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const login = await fetchProd('/api/login', 'POST', { username: 'david', password: 'CGJ-Ge-90' });
  const cookie = login.headers['set-cookie'].map(c => c.split(';')[0]).join('; ');
  console.log('Login:', login.status);

  const existing = [
    { descCatId: 17028988, typeId: 93403, tnvedCode: '3303001000' },
    { descCatId: 17028988, typeId: 93405, tnvedCode: '3303001000' },
    { descCatId: 17028988, typeId: 93397, tnvedCode: '3303001000' },
    { descCatId: 17028988, typeId: 93404, tnvedCode: '3303001000' },
    { descCatId: 17028991, typeId: 93883, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93873, tnvedCode: '3304990000' },
    { descCatId: 17028983, typeId: 93466, tnvedCode: '3401300000' },
    { descCatId: 17028992, typeId: 93950, tnvedCode: '3305900000' },
    { descCatId: 17028988, typeId: 93402, tnvedCode: '3303001000' },
    { descCatId: 17028983, typeId: 97704, tnvedCode: '3401300000' },
    { descCatId: 17028988, typeId: 971214875, tnvedCode: '3303001000' },
    { descCatId: 17028983, typeId: 97769, tnvedCode: '3401300000' },
    { descCatId: 17028992, typeId: 93920, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93887, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93913, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93902, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93882, tnvedCode: '3304990000' },
    { descCatId: 17028988, typeId: 970674005, tnvedCode: '3303001000' },
    { descCatId: 17028992, typeId: 93935, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 93934, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93897, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93942, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93915, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93909, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 629618165, tnvedCode: '3304990000' },
    { descCatId: 17028983, typeId: 93482, tnvedCode: '3401300000' },
    { descCatId: 17028991, typeId: 93898, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93945, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 97910, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93876, tnvedCode: '3304990000' },
    { descCatId: 52620370, typeId: 93503, tnvedCode: '3304100000' },
    { descCatId: 78032222, typeId: 93961, tnvedCode: '3304100000' },
    { descCatId: 17028990, typeId: 93413, tnvedCode: '3304100000' },
    { descCatId: 17028990, typeId: 93436, tnvedCode: '3304100000' },
    { descCatId: 17028991, typeId: 970983098, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93443, tnvedCode: '3304100000' },
    { descCatId: 17028990, typeId: 93419, tnvedCode: '3304100000' },
    { descCatId: 17028990, typeId: 93453, tnvedCode: '3304100000' },
    { descCatId: 17028990, typeId: 93442, tnvedCode: '3304100000' },
    { descCatId: 17028992, typeId: 97700, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 93936, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93916, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 708190925, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93904, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93932, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93881, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93939, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93919, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93930, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 93938, tnvedCode: '3305900000' },
    { descCatId: 17028983, typeId: 93483, tnvedCode: '3401300000' },
    { descCatId: 17028992, typeId: 93933, tnvedCode: '3305900000' },
    { descCatId: 17028983, typeId: 93465, tnvedCode: '3401300000' },
    { descCatId: 17028991, typeId: 93918, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93917, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 93929, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 93941, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 93924, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 93885, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 971217759, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 97768, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 97703, tnvedCode: '3304990000' },
    { descCatId: 17028992, typeId: 970951258, tnvedCode: '3305900000' },
    { descCatId: 17028992, typeId: 97764, tnvedCode: '3305900000' },
    { descCatId: 17028991, typeId: 97707, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 971095452, tnvedCode: '3304990000' },
    { descCatId: 17028991, typeId: 93874, tnvedCode: '3304990000' },
  ];

  // Categories that previously had no code — add them
  const added = [
    { descCatId: 17028739, typeId: 95741, tnvedCode: '3406000000' },     // Свечи и подсвечники
    { descCatId: 17028990, typeId: 93440, tnvedCode: '3304990000' },     // Декоративная косметика (remaining typeIds)
    { descCatId: 17028990, typeId: 93412, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93450, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93424, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93431, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93448, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93418, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93411, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93454, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93420, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93439, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93414, tnvedCode: '3304990000' },
    { descCatId: 17028990, typeId: 93427, tnvedCode: '3304990000' },
    { descCatId: 86029514, typeId: 92718, tnvedCode: '3307490000' },     // Ароматы для дома
    { descCatId: 86029514, typeId: 92721, tnvedCode: '3307490000' },
    { descCatId: 77857885, typeId: 93886, tnvedCode: '3304990000' },     // Маска косметическая
    { descCatId: 77857885, typeId: 93895, tnvedCode: '3304990000' },
    { descCatId: 17054869, typeId: 97309, tnvedCode: '3307100000' },     // Средства для бритья
    { descCatId: 17054869, typeId: 97311, tnvedCode: '3307100000' },
    { descCatId: 17054869, typeId: 970593490, tnvedCode: '3307100000' },
    { descCatId: 200001240, typeId: 93488, tnvedCode: '3306100000' },    // Гигиена полости рта
    { descCatId: 200001240, typeId: 93469, tnvedCode: '3306100000' },
    { descCatId: 200001240, typeId: 970593718, tnvedCode: '3306100000' },
    { descCatId: 17027904, typeId: 93337, tnvedCode: '3304990000' },     // Аксессуары
    { descCatId: 87504219, typeId: 352274038, tnvedCode: '9603290000' }, // Щетка для сухого массажа
    { descCatId: 30960284, typeId: 97897, tnvedCode: '9019100000' },     // Массаж
    { descCatId: 78021424, typeId: 93989, tnvedCode: '3401119000' },     // Мочалки и спонжи
    { descCatId: 17028730, typeId: 970865037, tnvedCode: '6302200000' }, // Полотенца и скатерти
    { descCatId: 17027920, typeId: 92695, tnvedCode: '3401209000' },     // Моющие и чистящие
    { descCatId: 200000122, typeId: 971806593, tnvedCode: '9019101000' },// Проф. аппараты косметологии
  ];

  const allAssignments = [...existing, ...added];
  console.log('Total assignments:', allAssignments.length, '(was 67, added', added.length, ')');

  const saveR = await fetchProd('/api/ozon/tnved/assignments', 'PUT', { assignments: allAssignments }, cookie);
  console.log('Save assignments:', saveR.status, saveR.body);

  if (saveR.status !== 200) { console.error('Save failed'); return; }

  // Run apply
  const applyR = await fetchProd('/api/ozon/tnved/apply', 'POST', {}, cookie);
  console.log('Apply launched:', applyR.status, applyR.body);

  // Poll progress
  for (let i = 0; i < 80; i++) {
    await sleep(5000);
    const p = await fetchProd('/api/ozon/tnved/progress', 'GET', null, cookie);
    const prog = JSON.parse(p.body);
    const pct = prog.totalProducts ? Math.round((prog.processed || 0) / prog.totalProducts * 100) : 0;
    process.stdout.write(`[${new Date().toISOString()}] ${prog.phase} ${prog.processed}/${prog.totalProducts} updated:${prog.updated} errors:${prog.errors} (${pct}%)\r`);
    if (!prog.running && prog.completedAt) {
      console.log(`\nOzon apply done: updated=${prog.updated} errors=${prog.errors} errorSamples=${JSON.stringify(prog.errorSamples || [])}`);
      break;
    }
  }

  // Now run Yandex apply
  console.log('\nStarting Yandex TN VED apply...');
  const yR = await fetchProd('/api/yandex/tnved/apply', 'POST', { dryRun: false }, cookie);
  console.log('Yandex apply:', yR.status, yR.body.slice(0, 500));
}

main().catch(console.error);
