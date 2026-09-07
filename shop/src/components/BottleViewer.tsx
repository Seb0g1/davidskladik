import { useEffect, useRef } from "react";
import * as THREE from "three";
import { RoomEnvironment } from "three/addons/environments/RoomEnvironment.js";

// Profile curve for a generic cylindrical perfume bottle (units in arbitrary 3D space)
const BOTTLE_PROFILE: [number, number][] = [
  [0.00, 0.00], [0.00, 0.00], // seam start
  [1.80, 0.00], [2.10, 0.15], [2.30, 0.50], // base spread
  [2.50, 1.20], [2.55, 2.00],               // lower body
  [2.60, 3.50], [2.58, 5.00],               // mid body
  [2.55, 6.50], [2.45, 8.00],               // upper body
  [2.20, 9.20], [1.90, 9.80], [1.50, 10.20], // shoulder
  [1.10, 10.60], [0.90, 11.00],             // neck
  [0.90, 11.80],                             // neck top
];

const CAP_PROFILE: [number, number][] = [
  [0.00, 11.80],
  [0.95, 11.80], [1.15, 11.90],
  [1.20, 12.20], [1.20, 13.20],
  [1.10, 13.60], [0.90, 13.80],
  [0.00, 13.80],
];

function toVector2Array(pairs: [number, number][]): THREE.Vector2[] {
  return pairs.map(([x, y]) => new THREE.Vector2(x * 0.1, y * 0.1));
}

export default function BottleViewer({ tint = "#C9A96E" }: { tint?: string }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    // Renderer
    const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.2;

    // Scene
    const scene = new THREE.Scene();

    // Camera
    const w = canvas.offsetWidth;
    const h = canvas.offsetHeight;
    renderer.setSize(w, h, false);
    const camera = new THREE.PerspectiveCamera(35, w / h, 0.01, 100);
    camera.position.set(0, 0.7, 3.6);
    camera.lookAt(0, 0.65, 0);

    // Environment — simple IBL via PMREMGenerator
    const pmrem = new THREE.PMREMGenerator(renderer);
    const envTexture = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
    scene.environment = envTexture;

    // Lights
    const ambient = new THREE.AmbientLight(0xfff8f0, 0.4);
    scene.add(ambient);

    const key = new THREE.DirectionalLight(0xfff4e0, 2.5);
    key.position.set(2, 4, 3);
    key.castShadow = true;
    scene.add(key);

    const fill = new THREE.PointLight(0xd0c0ff, 1.2, 12);
    fill.position.set(-2, 2, -1);
    scene.add(fill);

    const rim = new THREE.PointLight(0xffe8c0, 1.5, 10);
    rim.position.set(0, 3, -2.5);
    scene.add(rim);

    // Glass material
    const tintColor = new THREE.Color(tint).multiplyScalar(0.7);
    const glassMat = new THREE.MeshPhysicalMaterial({
      color: tintColor,
      transparent: true,
      opacity: 0.82,
      transmission: 0.88,
      roughness: 0.04,
      metalness: 0.0,
      ior: 1.52,
      thickness: 0.5,
      reflectivity: 0.6,
      envMapIntensity: 1.8,
      side: THREE.FrontSide,
    });

    // Gold cap material
    const goldMat = new THREE.MeshPhysicalMaterial({
      color: new THREE.Color("#C9A25E"),
      metalness: 0.95,
      roughness: 0.12,
      envMapIntensity: 2.2,
    });

    // Build bottle body
    const bodyPoints = toVector2Array(BOTTLE_PROFILE);
    const bodyGeo = new THREE.LatheGeometry(bodyPoints, 64, 0, Math.PI * 2);
    bodyGeo.computeVertexNormals();
    const bodyMesh = new THREE.Mesh(bodyGeo, glassMat);
    bodyMesh.castShadow = true;

    // Build cap
    const capPoints = toVector2Array(CAP_PROFILE);
    const capGeo = new THREE.LatheGeometry(capPoints, 64, 0, Math.PI * 2);
    capGeo.computeVertexNormals();
    const capMesh = new THREE.Mesh(capGeo, goldMat);
    capMesh.castShadow = true;

    // Group + center
    const group = new THREE.Group();
    group.add(bodyMesh);
    group.add(capMesh);
    // Shift so bottle base sits at y=0, then center on its mid-height
    group.position.y = -0.72;
    scene.add(group);

    // Shadow plane
    const shadowGeo = new THREE.PlaneGeometry(4, 4);
    const shadowMat = new THREE.ShadowMaterial({ opacity: 0.18 });
    const shadowPlane = new THREE.Mesh(shadowGeo, shadowMat);
    shadowPlane.rotation.x = -Math.PI / 2;
    shadowPlane.position.y = -0.72;
    shadowPlane.receiveShadow = true;
    scene.add(shadowPlane);

    // Mouse interaction
    let mouseX = 0;
    let mouseY = 0;
    let isHovering = false;

    const onMouseMove = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      mouseX = ((e.clientX - rect.left) / rect.width - 0.5) * 2;
      mouseY = ((e.clientY - rect.top) / rect.height - 0.5) * 2;
    };
    const onEnter = () => { isHovering = true; };
    const onLeave = () => { isHovering = false; mouseX = 0; mouseY = 0; };
    canvas.addEventListener("mousemove", onMouseMove);
    canvas.addEventListener("mouseenter", onEnter);
    canvas.addEventListener("mouseleave", onLeave);

    // Resize observer
    const ro = new ResizeObserver(() => {
      const nw = canvas.offsetWidth;
      const nh = canvas.offsetHeight;
      renderer.setSize(nw, nh, false);
      camera.aspect = nw / nh;
      camera.updateProjectionMatrix();
    });
    ro.observe(canvas);

    // Animation
    const clock = new THREE.Clock();
    let rotY = 0;
    let raf = 0;

    function animate() {
      raf = requestAnimationFrame(animate);
      const dt = clock.getDelta();

      if (!isHovering && !reduced) {
        rotY += dt * 0.5;
      }

      // Smooth tilt toward mouse on hover
      const targetTiltX = isHovering ? mouseY * 0.25 : 0;
      const targetTiltZ = isHovering ? -mouseX * 0.15 : 0;
      group.rotation.x += (targetTiltX - group.rotation.x) * 0.08;
      group.rotation.z += (targetTiltZ - group.rotation.z) * 0.08;
      group.rotation.y = rotY;

      renderer.render(scene, camera);
    }

    animate();

    return () => {
      cancelAnimationFrame(raf);
      canvas.removeEventListener("mousemove", onMouseMove);
      canvas.removeEventListener("mouseenter", onEnter);
      canvas.removeEventListener("mouseleave", onLeave);
      ro.disconnect();
      renderer.dispose();
      glassMat.dispose();
      goldMat.dispose();
      bodyGeo.dispose();
      capGeo.dispose();
      shadowGeo.dispose();
      envTexture.dispose();
      pmrem.dispose();
    };
  }, [tint]);

  return (
    <canvas
      ref={canvasRef}
      style={{ width: "100%", height: "100%", display: "block", cursor: "grab" }}
    />
  );
}
