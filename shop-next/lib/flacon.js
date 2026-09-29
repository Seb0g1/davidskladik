// Magic Vibes signature flacon — shared by the live home-page story (FlaconStory)
// and by scripts/render-flacon-shots (studio "photos" for the landing gallery).
// Plain ESM so the render script can load it in a browser without a build step.
import * as THREE from "three";
import { RoundedBoxGeometry } from "three/addons/geometries/RoundedBoxGeometry.js";
import { RoomEnvironment } from "three/addons/environments/RoomEnvironment.js";

export const FAMILIES = {
  floral:   { liquid: "#ff4f95", label: "ЦВЕТЫ",    script: "букет цветов",    props: "petals"  },
  fresh:    { liquid: "#2fb4ff", label: "СВЕЖЕСТЬ", script: "морской бриз",    props: "drops"   },
  woody:    { liquid: "#b8702f", label: "ДЕРЕВО",   script: "тёплое дерево",   props: "sticks"  },
  oriental: { liquid: "#ff7a0f", label: "ВОСТОК",   script: "амбра и уд",      props: "orbs"    },
  niche:    { liquid: "#7b66ff", label: "НИША",     script: "редкие ноты",     props: "sparks"  },
  citrus:   { liquid: "#ffc800", label: "ЦИТРУС",   script: "игристый цитрус", props: "slices"  },
  gift:     { liquid: "#ff2f72", label: "ПОДАРОК",  script: "с любовью",       props: "sparks"  },
};

const INK = "#121212";

/* ─────────────────────────── label ─────────────────────────── */
function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function fitText(ctx, text, maxW, size, font) {
  let s = size;
  ctx.font = font(s);
  while (ctx.measureText(text).width > maxW && s > 20) { s -= 4; ctx.font = font(s); }
  return s;
}

export function drawLabel(canvas, { title = "MAGIC", script = "eau de parfum", accent = "#ff3d7f" } = {}) {
  const W = canvas.width, H = canvas.height;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, W, H);
  // paper
  roundRect(ctx, 8, 8, W - 16, H - 16, 64);
  ctx.fillStyle = "#fbf8f2";
  ctx.fill();
  ctx.lineWidth = 6;
  ctx.strokeStyle = "rgba(18,18,18,0.85)";
  roundRect(ctx, 34, 34, W - 68, H - 68, 44);
  ctx.stroke();
  // top wordmark
  ctx.fillStyle = INK;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.font = "700 44px Unbounded, Onest, sans-serif";
  ctx.fillText("MAGIC VIBES", W / 2, 118);
  ctx.font = "500 26px Onest, sans-serif";
  ctx.fillStyle = "rgba(18,18,18,0.55)";
  ctx.fillText("✦  PARFUM  ✦", W / 2, 168);
  // title
  ctx.fillStyle = INK;
  const sz = fitText(ctx, title, W - 150, 150, (s) => `800 ${s}px Unbounded, Onest, sans-serif`);
  ctx.font = `800 ${sz}px Unbounded, Onest, sans-serif`;
  ctx.fillText(title, W / 2, H * 0.47);
  // script
  ctx.fillStyle = accent;
  fitText(ctx, script, W - 190, 100, (s) => `${s}px 'Marck Script', cursive`);
  ctx.save();
  ctx.translate(W / 2, H * 0.47 + sz * 0.78);
  ctx.rotate(-0.05);
  ctx.fillText(script, 0, 0);
  ctx.restore();
  // pill
  const pw = 360, ph = 84;
  roundRect(ctx, W / 2 - pw / 2, H - 200, pw, ph, 42);
  ctx.fillStyle = INK;
  ctx.fill();
  ctx.fillStyle = "#d9f84a";
  ctx.font = "700 34px Onest, sans-serif";
  ctx.fillText("✓ ОРИГИНАЛ · 100 ML", W / 2, H - 158);
}

/* ─────────────────────────── bottle ─────────────────────────── */
// Thick cut-glass flacon: bevelled glass shell, heavy glass base, translucent
// tinted juice filled to the shoulder (with a meniscus), paper label on the
// glass, lacquered cap with a gold collar.
export function createFlacon({ family = "floral", lite = false, contactShadow = true } = {}) {
  const fam = FAMILIES[family] || FAMILIES.floral;
  const group = new THREE.Group();
  const segs = lite ? 4 : 8;

  // ── Glass: two shells give the wall a visible thickness (outer front faces + inner back
  // faces), a heavy glass base and a juice volume that fills the inside up to the shoulder.
  // Alpha-blended, not transmission: the canvas is transparent over a CSS background and
  // transmission only refracts the 3D scene, so it rendered milky white.
  const W = 1.0, H = 1.32, D = 0.64, WALL = 0.045, BOTTOM = 0.14;
  // Fresnel: grazing faces get denser; the rim goes to a cool grey-teal (what thick glass
  // looks like against a light backdrop — refraction darkens it), with a bright inner line.
  const fresnelGlass = (mat, edgeAlpha, edgeLight) => {
    mat.onBeforeCompile = (shader) => {
      shader.fragmentShader = shader.fragmentShader.replace("#include <opaque_fragment>", `
        float mvCos = clamp(abs(dot(normalize(normal), normalize(vViewPosition))), 0.0, 1.0);
        float mvFres = pow(1.0 - mvCos, 3.0);
        float mvRim = smoothstep(0.35, 0.95, 1.0 - mvCos);
        diffuseColor.a = clamp(diffuseColor.a + mvFres * ${edgeAlpha.toFixed(2)} + mvRim * 0.22, 0.0, 0.96);
        outgoingLight = mix(outgoingLight, vec3(0.32, 0.42, 0.47), mvRim * 0.7);
        outgoingLight += vec3(pow(1.0 - mvCos, 8.0) * ${edgeLight.toFixed(2)});
        #include <opaque_fragment>`);
    };
    return mat;
  };
  const glassMat = fresnelGlass(new THREE.MeshPhysicalMaterial({ color: 0xf4fbff, roughness: 0.02, metalness: 0, transparent: true, opacity: lite ? 0.1 : 0.07, clearcoat: 1, clearcoatRoughness: 0.01, envMapIntensity: 3, specularIntensity: 1, ior: 1.5, depthWrite: false }), 0.75, 0.45);
  const innerGlassMat = fresnelGlass(new THREE.MeshPhysicalMaterial({ color: 0xe8f6ff, roughness: 0.03, transparent: true, opacity: 0.03, envMapIntensity: 1.6, depthWrite: false, side: THREE.BackSide }), 0.5, 0.25);

  const body = new THREE.Mesh(new RoundedBoxGeometry(W, H, D, segs, 0.1), glassMat);
  body.renderOrder = 5;
  group.add(body);
  const inner = new THREE.Mesh(new RoundedBoxGeometry(W - WALL * 2, H - BOTTOM - WALL * 2, D - WALL * 2, segs, 0.07), innerGlassMat);
  inner.position.y = (BOTTOM - WALL) / 2;
  inner.renderOrder = 2;
  group.add(inner);

  // heavy glass base — slightly denser, catches light like a thick cut bottom
  const baseMat = fresnelGlass(new THREE.MeshPhysicalMaterial({ color: 0xd8eef2, roughness: 0.03, transparent: true, opacity: 0.1, clearcoat: 1, envMapIntensity: 1.8, depthWrite: false }), 0.6, 0.35);
  const base = new THREE.Mesh(new RoundedBoxGeometry(W - WALL * 2, BOTTOM, D - WALL * 2, segs, 0.04), baseMat);
  base.position.y = -H / 2 + WALL + BOTTOM / 2 - 0.02;
  base.renderOrder = 3;
  group.add(base);

  // juice: fills the inside to ~88% (small air gap under the shoulder); darker at the bottom,
  // lighter at the top, thinner at grazing angles — reads as a liquid volume, not a block
  const floorY = -H / 2 + WALL + BOTTOM - 0.02;
  const juiceH = (H / 2 - WALL - floorY) * 0.88;
  const liquidMat = new THREE.MeshPhysicalMaterial({ color: new THREE.Color(fam.liquid), emissive: new THREE.Color(fam.liquid), emissiveIntensity: lite ? 0.08 : 0.1, roughness: 0.12, transparent: true, opacity: lite ? 0.9 : 0.88, clearcoat: 1, clearcoatRoughness: 0.08, envMapIntensity: 0.8, depthWrite: false });
  liquidMat.onBeforeCompile = (shader) => {
    shader.vertexShader = shader.vertexShader
      .replace("#include <common>", "#include <common>\nvarying float vMvH;")
      .replace("#include <begin_vertex>", `#include <begin_vertex>\nvMvH = clamp(position.y / ${juiceH.toFixed(3)} + 0.5, 0.0, 1.0);`);
    shader.fragmentShader = shader.fragmentShader
      .replace("#include <common>", "#include <common>\nvarying float vMvH;")
      .replace("#include <opaque_fragment>", `
        float mvEdge = pow(1.0 - clamp(abs(dot(normalize(normal), normalize(vViewPosition))), 0.0, 1.0), 2.0);
        outgoingLight *= mix(0.62, 1.1, smoothstep(0.0, 1.0, vMvH));
        diffuseColor.a *= mix(1.0, 0.7, mvEdge) * mix(1.04, 0.9, vMvH);
        #include <opaque_fragment>`);
  };
  // small bevel: a big one turns the flat juice surface into a cushion
  const liquid = new THREE.Mesh(new RoundedBoxGeometry(W - WALL * 2 - 0.012, juiceH, D - WALL * 2 - 0.012, segs, 0.022), liquidMat);
  liquid.position.y = floorY + juiceH / 2;
  liquid.renderOrder = 1;
  group.add(liquid);
  // meniscus: a bright thin surface with a soft edge
  const meniscusMat = new THREE.MeshPhysicalMaterial({ color: new THREE.Color(fam.liquid).lerp(new THREE.Color("#ffffff"), 0.5), roughness: 0.04, transparent: true, opacity: 0.6, clearcoat: 1, depthWrite: false });
  const meniscus = new THREE.Mesh(new THREE.PlaneGeometry(W - WALL * 2 - 0.03, D - WALL * 2 - 0.03), meniscusMat);
  meniscus.rotation.x = -Math.PI / 2;
  meniscus.position.y = floorY + juiceH - 0.003;
  meniscus.renderOrder = 1;
  group.add(meniscus);

  // studio softbox reflections: two vertical streaks and a top-edge glint on the front glass
  const streakTex = (() => {
    const c = document.createElement("canvas");
    c.width = 64; c.height = 256;
    const g = c.getContext("2d");
    const h = g.createLinearGradient(0, 0, 64, 0);
    h.addColorStop(0, "rgba(255,255,255,0)"); h.addColorStop(0.5, "rgba(255,255,255,1)"); h.addColorStop(1, "rgba(255,255,255,0)");
    g.fillStyle = h; g.fillRect(0, 0, 64, 256);
    g.globalCompositeOperation = "destination-in";
    const v = g.createLinearGradient(0, 0, 0, 256);
    v.addColorStop(0, "rgba(0,0,0,0)"); v.addColorStop(0.18, "rgba(0,0,0,1)"); v.addColorStop(0.82, "rgba(0,0,0,1)"); v.addColorStop(1, "rgba(0,0,0,0)");
    g.fillStyle = v; g.fillRect(0, 0, 64, 256);
    const t = new THREE.CanvasTexture(c);
    t.colorSpace = THREE.SRGBColorSpace;
    return t;
  })();
  const streakMat = new THREE.MeshBasicMaterial({ map: streakTex, transparent: true, opacity: 0.55, depthWrite: false, blending: THREE.AdditiveBlending, toneMapped: false });
  const streakA = new THREE.Mesh(new THREE.PlaneGeometry(0.075, H * 0.82), streakMat);
  streakA.position.set(-W / 2 + 0.11, 0.02, D / 2 + 0.004);
  streakA.renderOrder = 6;
  group.add(streakA);
  const streakB = new THREE.Mesh(new THREE.PlaneGeometry(0.035, H * 0.7), streakMat.clone());
  streakB.material.opacity = 0.35;
  streakB.position.set(W / 2 - 0.07, 0.04, D / 2 + 0.004);
  streakB.renderOrder = 6;
  group.add(streakB);
  const glint = new THREE.Mesh(new THREE.PlaneGeometry(0.035, W * 0.7), streakMat.clone());
  glint.material.opacity = 0.3;
  glint.rotation.z = Math.PI / 2;
  glint.position.set(0, H / 2 - 0.05, D / 2 + 0.004);
  glint.renderOrder = 6;
  group.add(glint);

  // soft contact shadow so the bottle stands on the surface instead of floating
  const shadowTex = (() => {
    const c = document.createElement("canvas");
    c.width = c.height = 128;
    const g = c.getContext("2d");
    const r = g.createRadialGradient(64, 64, 4, 64, 64, 64);
    r.addColorStop(0, "rgba(0,0,0,0.55)"); r.addColorStop(0.55, "rgba(0,0,0,0.18)"); r.addColorStop(1, "rgba(0,0,0,0)");
    g.fillStyle = r; g.fillRect(0, 0, 128, 128);
    return new THREE.CanvasTexture(c);
  })();
  const contact = new THREE.Mesh(new THREE.PlaneGeometry(W * 1.7, D * 1.9), new THREE.MeshBasicMaterial({ map: shadowTex, transparent: true, depthWrite: false, toneMapped: false }));
  contact.rotation.x = -Math.PI / 2;
  contact.position.y = -H / 2 - 0.002;
  contact.renderOrder = 0;
  // off for the floating / tilting hero bottle: a shadow glued to it would tilt along
  if (contactShadow) group.add(contact);

  // label glued to the front of the glass
  const labelCanvas = document.createElement("canvas");
  labelCanvas.width = 768; labelCanvas.height = 896;
  drawLabel(labelCanvas, { title: fam.label, script: fam.script });
  const labelTex = new THREE.CanvasTexture(labelCanvas);
  labelTex.colorSpace = THREE.SRGBColorSpace;
  labelTex.anisotropy = 8;
  const label = new THREE.Mesh(
    new THREE.PlaneGeometry(0.6, 0.7),
    new THREE.MeshPhysicalMaterial({ map: labelTex, alphaTest: 0.5, roughness: 0.62, metalness: 0, sheen: 0.3, sheenColor: new THREE.Color("#ffffff") })
  );
  label.position.set(0, -0.03, D / 2 + 0.002);
  label.renderOrder = 7;
  group.add(label);

  // glass neck, gold collar, lacquered faceted cap
  const neck = new THREE.Mesh(new THREE.CylinderGeometry(0.13, 0.15, 0.12, 48), glassMat);
  neck.position.y = H / 2 + 0.05;
  group.add(neck);
  const gold = new THREE.MeshPhysicalMaterial({ color: 0xe9c77b, metalness: 1, roughness: 0.16, clearcoat: 0.6, envMapIntensity: 1.6 });
  const collar = new THREE.Mesh(new THREE.CylinderGeometry(0.21, 0.21, 0.08, 64), gold);
  collar.position.y = 0.8;
  group.add(collar);
  const lip = new THREE.Mesh(new THREE.TorusGeometry(0.21, 0.016, 16, 64), gold);
  lip.rotation.x = Math.PI / 2;
  lip.position.y = 0.84;
  group.add(lip);

  const capMat = new THREE.MeshPhysicalMaterial({ color: new THREE.Color(INK), roughness: 0.14, metalness: 0.1, clearcoat: 1, clearcoatRoughness: 0.03, iridescence: 0.35, iridescenceIOR: 1.4, iridescenceThicknessRange: [200, 500], envMapIntensity: 1.6, flatShading: true });
  const capBody = new THREE.Mesh(new THREE.CylinderGeometry(0.3, 0.28, 0.42, 12, 1), capMat);
  capBody.position.y = 1.06;
  group.add(capBody);
  const capTop = new THREE.Mesh(new THREE.SphereGeometry(0.3, 12, 8, 0, Math.PI * 2, 0, Math.PI / 2), capMat);
  capTop.scale.y = 0.35;
  capTop.position.y = 1.27;
  group.add(capTop);
  const capBand = new THREE.Mesh(new THREE.CylinderGeometry(0.305, 0.305, 0.03, 64), gold);
  capBand.position.y = 0.88;
  group.add(capBand);

  const tmp = new THREE.Color();
  const tmpLight = new THREE.Color();
  const white = new THREE.Color("#ffffff");
  return {
    group,
    family,
    /** smoothly move liquid colour toward target (call every frame) */
    lerpLiquid(hex, k = 0.08) {
      tmp.set(hex);
      liquidMat.color.lerp(tmp, k);
      liquidMat.emissive.lerp(tmp, k);
      if (liquidMat.attenuationColor) liquidMat.attenuationColor.lerp(tmp, k);
      tmpLight.copy(tmp).lerp(white, 0.45);
      meniscusMat.color.lerp(tmpLight, k);
    },
    /** jump straight to a colour (e.g. when the bottle pops in for a new section) */
    setLiquid(hex) {
      tmp.set(hex);
      liquidMat.color.copy(tmp);
      liquidMat.emissive.copy(tmp);
      meniscusMat.color.copy(tmp).lerp(white, 0.45);
    },
    setFamily(key) {
      const f = FAMILIES[key];
      if (!f || key === this.family) return;
      this.family = key;
      drawLabel(labelCanvas, { title: f.label, script: f.script });
      labelTex.needsUpdate = true;
    },
    redrawLabel() {
      const f = FAMILIES[this.family];
      drawLabel(labelCanvas, { title: f.label, script: f.script });
      labelTex.needsUpdate = true;
    },
    dispose() {
      group.traverse((o) => { if (o.geometry) o.geometry.dispose(); if (o.material) o.material.dispose(); });
      labelTex.dispose();
      streakTex.dispose();
      shadowTex.dispose();
    },
  };
}

/* ─────────────────────────── scent-note props ─────────────────────────── */
function citrusTexture(color = "#ffd21f", rind = "#fff6d6") {
  const c = document.createElement("canvas");
  c.width = c.height = 256;
  const g = c.getContext("2d");
  g.fillStyle = rind; g.beginPath(); g.arc(128, 128, 126, 0, Math.PI * 2); g.fill();
  g.fillStyle = "#fffaf0"; g.beginPath(); g.arc(128, 128, 118, 0, Math.PI * 2); g.fill();
  const rg = g.createRadialGradient(128, 128, 10, 128, 128, 112);
  rg.addColorStop(0, "#fffbe8"); rg.addColorStop(0.35, color); rg.addColorStop(1, color);
  g.fillStyle = rg; g.beginPath(); g.arc(128, 128, 112, 0, Math.PI * 2); g.fill();
  g.strokeStyle = "#fff6d6"; g.lineWidth = 7;
  for (let i = 0; i < 10; i++) { const a = (i / 10) * Math.PI * 2; g.beginPath(); g.moveTo(128, 128); g.lineTo(128 + Math.cos(a) * 112, 128 + Math.sin(a) * 112); g.stroke(); }
  g.fillStyle = "#fff6d6"; g.beginPath(); g.arc(128, 128, 14, 0, Math.PI * 2); g.fill();
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t;
}

function citrusSlice(flesh, rind) {
  const tex = citrusTexture(flesh, rind);
  const side = new THREE.MeshStandardMaterial({ color: rind, roughness: 0.5 });
  const face = new THREE.MeshPhysicalMaterial({ map: tex, roughness: 0.35, clearcoat: 1, clearcoatRoughness: 0.15 });
  return new THREE.Mesh(new THREE.CylinderGeometry(0.22, 0.22, 0.06, 48), [side, face, face]);
}

// Curved petal / leaf blade: a flat outline bent into a shallow cup.
function bladeGeometry(len, wid, cup, tipSharp = 1) {
  const sh = new THREE.Shape();
  sh.moveTo(0, -len / 2);
  sh.bezierCurveTo(wid * 0.9, -len * 0.3, wid * 0.75 * tipSharp, len * 0.35, 0, len / 2);
  sh.bezierCurveTo(-wid * 0.75 * tipSharp, len * 0.35, -wid * 0.9, -len * 0.3, 0, -len / 2);
  const g = new THREE.ShapeGeometry(sh, 16);
  const pos = g.attributes.position;
  for (let i = 0; i < pos.count; i++) {
    const x = pos.getX(i), y = pos.getY(i);
    pos.setZ(i, cup * ((x / wid) ** 2) * 1.2 + cup * 0.5 * ((y / len) ** 2));
  }
  g.computeVertexNormals();
  return g;
}

function gradientTexture(stops, vertical = true) {
  const c = document.createElement("canvas");
  c.width = vertical ? 4 : 128; c.height = vertical ? 128 : 4;
  const g = c.getContext("2d");
  const gr = vertical ? g.createLinearGradient(0, 128, 0, 0) : g.createLinearGradient(0, 0, 128, 0);
  stops.forEach(([o, col]) => gr.addColorStop(o, col));
  g.fillStyle = gr; g.fillRect(0, 0, c.width, c.height);
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t;
}

let _petalTex, _petalWhiteTex, _leafTex;
function petal(color) {
  const geo = bladeGeometry(0.34, 0.2, 0.06);
  // ShapeGeometry UVs are in shape units: remap so the gradient runs base → tip
  const uv = geo.attributes.uv, pos = geo.attributes.position;
  for (let i = 0; i < uv.count; i++) uv.setXY(i, 0.5, (pos.getY(i) + 0.17) / 0.34);
  const map = color === "white"
    ? (_petalWhiteTex ||= gradientTexture([[0, "#ffe0ec"], [0.6, "#fff9fb"], [1, "#fff0f5"]]))
    : (_petalTex ||= gradientTexture([[0, "#ff2f7a"], [0.55, "#ff6fa6"], [1, "#ffc2da"]]));
  return new THREE.Mesh(geo, new THREE.MeshPhysicalMaterial({ map, roughness: 0.5, sheen: 1, sheenRoughness: 0.4, sheenColor: new THREE.Color("#ffd6e6"), side: THREE.DoubleSide }));
}

const PROP_KINDS = {
  petals: () => petal("pink"),
  drops: () => {
    // teardrop: lathe profile, sharp tip up
    const pts = [];
    for (let i = 0; i <= 24; i++) { const t = i / 24, a = t * Math.PI; pts.push(new THREE.Vector2(Math.sin(a) * 0.1 * (1 - t * 0.55) ** 1.4, -Math.cos(a) * 0.1 + t * t * 0.1)); }
    return new THREE.Mesh(new THREE.LatheGeometry(pts, 40), new THREE.MeshPhysicalMaterial({ color: "#eaf7ff", roughness: 0, transmission: 1, thickness: 0.25, ior: 1.33, clearcoat: 1, envMapIntensity: 1.6 }));
  },
  sticks: () => {
    // cinnamon quill: rolled bark (open tube) with a lighter inner curl
    const g = new THREE.Group();
    const tube = new THREE.Mesh(new THREE.CylinderGeometry(0.045, 0.045, 0.7, 20, 1, true, 0.4, Math.PI * 1.75), new THREE.MeshStandardMaterial({ color: "#7a4424", roughness: 0.85, side: THREE.DoubleSide }));
    const inner = new THREE.Mesh(new THREE.CylinderGeometry(0.03, 0.03, 0.7, 16, 1, true, 2.2, Math.PI * 1.6), new THREE.MeshStandardMaterial({ color: "#9a5a30", roughness: 0.9, side: THREE.DoubleSide }));
    g.add(tube, inner);
    return g;
  },
  orbs: () => {
    // amber resin droplet: warm translucent, not a gold ball
    const m = new THREE.Mesh(new THREE.IcosahedronGeometry(0.1, 3), new THREE.MeshPhysicalMaterial({ color: "#ff9a2e", roughness: 0.12, transmission: 0.6, thickness: 0.2, ior: 1.54, attenuationColor: new THREE.Color("#c2530a"), attenuationDistance: 0.2, clearcoat: 1, envMapIntensity: 1.3 }));
    m.scale.set(1, 0.82, 0.92);
    return m;
  },
  sparks: () => {
    // four-point star, the brand "✦"
    const sh = new THREE.Shape();
    for (let i = 0; i < 8; i++) { const a = (i / 8) * Math.PI * 2, r = i % 2 ? 0.035 : 0.12; const x = Math.sin(a) * r, y = Math.cos(a) * r; if (i) sh.lineTo(x, y); else sh.moveTo(x, y); }
    const geo = new THREE.ExtrudeGeometry(sh, { depth: 0.03, bevelEnabled: true, bevelThickness: 0.015, bevelSize: 0.01, bevelSegments: 3 });
    geo.center();
    return new THREE.Mesh(geo, new THREE.MeshPhysicalMaterial({ color: "#d9f84a", roughness: 0.15, metalness: 0.2, clearcoat: 1, iridescence: 0.6, envMapIntensity: 1.3 }));
  },
  slices: () => citrusSlice("#ffc21a", "#ffb000"),
  orange: () => citrusSlice("#ff8a1c", "#ff7a00"),
  lemon: () => citrusSlice("#ffe23a", "#f5cf00"),
  lime: () => citrusSlice("#9bdc3c", "#4caf2a"),
  fruit: () => {
    const colors = ["#ff8a1c", "#ffd21f", "#8fd13a"];
    const c = colors[Math.floor(Math.random() * colors.length)];
    const m = new THREE.Mesh(new THREE.SphereGeometry(0.2, 40, 30), new THREE.MeshPhysicalMaterial({ color: c, roughness: 0.55, clearcoat: 0.6, clearcoatRoughness: 0.4 }));
    m.scale.set(1, 0.94, 1);
    return m;
  },
  vanilla: () => new THREE.Mesh(new THREE.CapsuleGeometry(0.025, 0.7, 6, 12), new THREE.MeshStandardMaterial({ color: "#3b2414", roughness: 0.7 })),
  cotton: () => {
    const g = new THREE.Group();
    const mat = new THREE.MeshPhysicalMaterial({ color: "#ffffff", roughness: 0.9, sheen: 1, sheenColor: new THREE.Color("#f3eaff") });
    for (let i = 0; i < 5; i++) {
      const b = new THREE.Mesh(new THREE.SphereGeometry(0.09 + Math.random() * 0.04, 20, 14), mat);
      b.position.set((Math.random() - 0.5) * 0.18, (Math.random() - 0.5) * 0.12, (Math.random() - 0.5) * 0.18);
      g.add(b);
    }
    return g;
  },
  fern: () => {
    const g = new THREE.Group();
    const mat = new THREE.MeshStandardMaterial({ color: "#4f9e3a", roughness: 0.6 });
    for (let i = 0; i < 7; i++) {
      const l = new THREE.Mesh(new THREE.SphereGeometry(0.07, 12, 8), mat);
      l.scale.set(0.5, 0.08, 1.2);
      l.position.set(0, i * 0.08 - 0.25, 0);
      l.rotation.y = i % 2 ? 0.6 : -0.6;
      g.add(l);
    }
    return g;
  },
  petalsWhite: () => petal("white"),
  leaves: () => {
    const geo = bladeGeometry(0.42, 0.13, 0.035, 0.7);
    const uv = geo.attributes.uv, pos = geo.attributes.position;
    for (let i = 0; i < uv.count; i++) uv.setXY(i, (pos.getX(i) + 0.13) / 0.26, 0.5);
    // horizontal gradient with a pale midrib down the centre
    const map = (_leafTex ||= gradientTexture([[0, "#3f8f35"], [0.46, "#6cc25a"], [0.5, "#c9efb4"], [0.54, "#6cc25a"], [1, "#3f8f35"]], false));
    return new THREE.Mesh(geo, new THREE.MeshPhysicalMaterial({ map, roughness: 0.45, clearcoat: 0.5, clearcoatRoughness: 0.3, side: THREE.DoubleSide }));
  },
};

/** Scatter scent props around the bottle; each prop floats in animateNotes(). */
export { PROP_KINDS };

export function createNotes(kinds = ["petals", "sparks"], count = 10, seed = 1) {
  let s = seed;
  const rnd = () => ((s = (s * 16807) % 2147483647) / 2147483647);
  const group = new THREE.Group();
  for (let i = 0; i < count; i++) {
    const kind = kinds[i % kinds.length];
    const m = PROP_KINDS[kind]();
    const a = rnd() * Math.PI * 2;
    const r = 0.95 + rnd() * 0.9;
    m.position.set(Math.cos(a) * r * 1.1, (rnd() - 0.45) * 2.2, Math.sin(a) * r * 0.55 - 0.1);
    m.rotation.set(rnd() * Math.PI, rnd() * Math.PI, rnd() * Math.PI);
    const k = 0.7 + rnd() * 0.6;
    m.scale.multiplyScalar(k);
    m.userData = { base: m.position.clone(), phase: rnd() * Math.PI * 2, speed: 0.4 + rnd() * 0.6, spin: (rnd() - 0.5) * 0.8 };
    group.add(m);
  }
  return group;
}

export function animateNotes(group, t) {
  group.children.forEach((m) => {
    const u = m.userData;
    m.position.y = u.base.y + Math.sin(t * u.speed + u.phase) * 0.12;
    m.position.x = u.base.x + Math.cos(t * u.speed * 0.7 + u.phase) * 0.05;
    m.rotation.y += u.spin * 0.01;
    m.rotation.x += u.spin * 0.006;
  });
}

/* ─────────────────────────── studio ─────────────────────────── */
export function setupStudio(renderer, scene) {
  renderer.toneMapping = THREE.ACESFilmicToneMapping;
  renderer.toneMappingExposure = 1.05;
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  const pmrem = new THREE.PMREMGenerator(renderer);
  const env = pmrem.fromScene(new RoomEnvironment(), 0.02).texture;
  scene.environment = env;
  pmrem.dispose();
  scene.add(new THREE.AmbientLight(0xffffff, 0.35));
  const key = new THREE.DirectionalLight(0xffffff, 2.2);
  key.position.set(2.5, 3.5, 4);
  scene.add(key);
  const rim = new THREE.DirectionalLight(0xffe6f2, 1.4);
  rim.position.set(-3, 1.5, -2);
  scene.add(rim);
  return env;
}

export { THREE };
