// Renders studio shots of the Magic Vibes flacon (lib/flacon.js) into public/brand/shots.
// Usage (from shop-next): node scripts/render-flacon-shots.mjs
// Needs Playwright + Chromium (root repo devDependency).
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
// SHOTS_OUT=<dir> renders elsewhere (for comparisons), SHOTS_ONLY=her,niche limits the set
const OUT = process.env.SHOTS_OUT ? path.resolve(process.env.SHOTS_OUT) : path.join(ROOT, "public", "brand", "shots");
fs.mkdirSync(OUT, { recursive: true });
const require = createRequire(path.join(ROOT, "..", "package.json"));
const { chromium } = require("playwright");

const SHOTS = [
  { name: "her",      family: "floral",   bg: ["#ffd9e7", "#ff9fc3"], pedestal: "#ffb3cf", props: ["petals"],           rot: -0.35, w: 1200, h: 1500 },
  { name: "him",      family: "woody",    bg: ["#2a2622", "#121212"], pedestal: "#3a332c", props: ["sticks", "leaves"], rot: 0.4,   w: 1200, h: 1500 },
  { name: "niche",    family: "niche",    bg: ["#7d71ff", "#3a2be0"], pedestal: "#5a4bff", props: ["sparks"],           rot: -0.5,  w: 1200, h: 1500 },
  { name: "oriental", family: "oriental", bg: ["#ffc07a", "#ff7a1a"], pedestal: "#ff9a45", props: ["orbs"],             rot: 0.3,   w: 1200, h: 1500 },
  { name: "fresh",    family: "fresh",    bg: ["#d6efff", "#7cc9ff"], pedestal: "#a6dbff", props: ["drops", "leaves"],  rot: -0.25, w: 1200, h: 1500 },
  { name: "citrus",   family: "citrus",   bg: ["#f4ff9c", "#d9f84a"], pedestal: "#e8ff6e", props: ["slices", "leaves"], rot: 0.35,  w: 1200, h: 1500 },
  { name: "gift",     family: "gift",     bg: ["#ffe3ec", "#ff6f9f"], pedestal: "#ff8fb4", props: ["sparks", "petals"], rot: -0.2,  w: 1200, h: 1500 },
  // wide hero shot + transparent cut-out (fallback poster for the live 3D hero)
  { name: "hero-wide", family: "floral",  bg: ["#e9ff8a", "#d9f84a"], pedestal: "#c8ec2f", props: ["petals", "sparks"], rot: -0.3, w: 1920, h: 1080 },
  { name: "flacon-cutout", family: "floral", bg: null, pedestal: null, props: ["petals", "sparks"], rot: -0.3, w: 1000, h: 1200 },
];

const MIME = { ".js": "text/javascript", ".html": "text/html", ".mjs": "text/javascript" };
const server = http.createServer((req, res) => {
  const url = decodeURIComponent(req.url.split("?")[0]);
  let file;
  if (url === "/") { res.writeHead(200, { "content-type": "text/html" }); return res.end(PAGE); }
  if (url.startsWith("/three/")) file = path.join(ROOT, "node_modules", "three", url.slice(7));
  else if (url === "/flacon.js") file = path.join(ROOT, "lib", "flacon.js");
  if (!file || !fs.existsSync(file)) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "content-type": MIME[path.extname(file)] || "application/octet-stream" });
  fs.createReadStream(file).pipe(res);
});

const PAGE = `<!doctype html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@700;800&family=Onest:wght@500;700&family=Marck+Script&display=block&subset=cyrillic" rel="stylesheet">
<style>html,body{margin:0;height:100%;overflow:hidden}#stage{position:fixed;inset:0}canvas{position:absolute;inset:0;width:100%;height:100%}
.grain{position:absolute;inset:0;opacity:.07;mix-blend-mode:multiply;background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='160' height='160'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='2'/></filter><rect width='100%' height='100%' filter='url(%23n)'/></svg>")}</style>
<script type="importmap">{"imports":{"three":"/three/build/three.module.js","three/addons/":"/three/examples/jsm/"}}</script>
</head><body><div id="stage"></div>
<script type="module">
import { createFlacon, createNotes, animateNotes, setupStudio, THREE } from "/flacon.js";
window.renderShot = async (o) => {
  await document.fonts.load("800 80px Unbounded"); await document.fonts.load("80px 'Marck Script'"); await document.fonts.load("700 30px Onest");
  const stage = document.getElementById("stage"); stage.innerHTML = "";
  if (o.bg) stage.style.background = "radial-gradient(120% 90% at 50% 30%, " + o.bg[0] + " 0%, " + o.bg[1] + " 100%)"; else stage.style.background = "transparent";
  const canvas = document.createElement("canvas"); stage.appendChild(canvas);
  if (o.bg) { const g = document.createElement("div"); g.className = "grain"; stage.appendChild(g); }
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true, preserveDrawingBuffer: true });
  renderer.setPixelRatio(1); renderer.setSize(o.w, o.h, false); renderer.setClearColor(0x000000, 0);
  renderer.shadowMap.enabled = true; renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  const scene = new THREE.Scene(); setupStudio(renderer, scene);
  const wide = o.w > o.h;
  const cam = new THREE.PerspectiveCamera(wide ? 24 : 30, o.w / o.h, 0.1, 100);
  cam.position.set(0, 0.5, wide ? 9.5 : 6.5); cam.lookAt(0, 0.12, 0);
  const f = createFlacon({ family: o.family }); f.group.rotation.y = o.rot; f.group.rotation.z = -o.rot * 0.12;
  f.group.traverse(m => { if (m.isMesh) m.castShadow = true; });
  const root = new THREE.Group(); root.add(f.group);
  if (wide) root.position.x = 1.7;
  scene.add(root);
  const notes = createNotes(o.props, wide ? 16 : 12, o.name.length * 7 + 3); animateNotes(notes, 1.3); root.add(notes);
  if (o.pedestal) {
    const ped = new THREE.Mesh(new THREE.CylinderGeometry(1.25, 1.3, 0.5, 96), new THREE.MeshStandardMaterial({ color: o.pedestal, roughness: 0.55 }));
    ped.position.y = -0.86; ped.receiveShadow = true; ped.castShadow = true; root.add(ped);
  }
  const sun = new THREE.DirectionalLight(0xffffff, 1.2); sun.position.set(1.5, 5, 3); sun.castShadow = true;
  sun.shadow.mapSize.set(2048, 2048); sun.shadow.radius = 8; sun.shadow.camera.left = -3; sun.shadow.camera.right = 3; sun.shadow.camera.top = 3; sun.shadow.camera.bottom = -3;
  scene.add(sun);
  renderer.render(scene, cam); renderer.render(scene, cam);
  return true;
};
window.ready = true;
</script></body></html>`;

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const browser = await chromium.launch({ channel: process.env.PW_CHANNEL || "chrome", args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"] });
try {
  const only = (process.env.SHOTS_ONLY || "").split(",").filter(Boolean);
  for (const s of SHOTS.filter((x) => !only.length || only.includes(x.name))) {
    const page = await browser.newPage({ viewport: { width: s.w, height: s.h }, deviceScaleFactor: 1 });
    page.on("console", (m) => { if (m.type() === "error") console.error("[page]", m.text()); });
    page.on("pageerror", (e) => console.error("[pageerror]", e.message));
    await page.goto(`http://127.0.0.1:${port}/`);
    await page.waitForFunction(() => window.ready === true);
    await page.evaluate((o) => window.renderShot(o), s);
    await page.waitForTimeout(300);
    const file = path.join(OUT, `${s.name}.${s.bg ? "jpg" : "png"}`);
    if (s.bg) await page.screenshot({ path: file, type: "jpeg", quality: 86 });
    else await page.screenshot({ path: file, type: "png", omitBackground: true });
    console.log("saved", path.relative(ROOT, file), Math.round(fs.statSync(file).size / 1024) + "KB");
    await page.close();
  }
} finally {
  await browser.close();
  server.close();
}
