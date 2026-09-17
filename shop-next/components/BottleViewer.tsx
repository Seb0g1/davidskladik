"use client";
import { useEffect, useRef } from "react";
import * as THREE from "three";
import { RoomEnvironment } from "three/addons/environments/RoomEnvironment.js";

const BOTTLE_PROFILE: [number, number][] = [
  [0.00, 0.00], [0.00, 0.00],
  [1.80, 0.00], [2.10, 0.15], [2.30, 0.50],
  [2.50, 1.20], [2.55, 2.00],
  [2.60, 3.50], [2.58, 5.00],
  [2.55, 6.50], [2.45, 8.00],
  [2.20, 9.20], [1.90, 9.80], [1.50, 10.20],
  [1.10, 10.60], [0.90, 11.00],
  [0.90, 11.80],
];

const CAP_PROFILE: [number, number][] = [
  [0.00, 11.80],
  [0.95, 11.80], [1.15, 11.90],
  [1.20, 12.20], [1.20, 13.20],
  [1.10, 13.60], [0.90, 13.80],
  [0.00, 13.80],
];

function toV2(pairs: [number, number][]): THREE.Vector2[] {
  return pairs.map(([x, y]) => new THREE.Vector2(x * 0.1, y * 0.1));
}

export default function BottleViewer({ tint = "#C9A96E" }: { tint?: string }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.2;

    const scene = new THREE.Scene();
    const w = canvas.offsetWidth, h = canvas.offsetHeight;
    renderer.setSize(w, h, false);
    const camera = new THREE.PerspectiveCamera(35, w / h, 0.01, 100);
    camera.position.set(0, 0.7, 3.6);
    camera.lookAt(0, 0.65, 0);

    const pmrem = new THREE.PMREMGenerator(renderer);
    const envTexture = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
    scene.environment = envTexture;

    scene.add(new THREE.AmbientLight(0xfff8f0, 0.4));
    const key = new THREE.DirectionalLight(0xfff4e0, 2.5);
    key.position.set(2, 4, 3); key.castShadow = true; scene.add(key);
    const fill = new THREE.PointLight(0xd0c0ff, 1.2, 12);
    fill.position.set(-2, 2, -1); scene.add(fill);
    const rim = new THREE.PointLight(0xffe8c0, 1.5, 10);
    rim.position.set(0, 3, -2.5); scene.add(rim);

    const glassMat = new THREE.MeshPhysicalMaterial({
      color: new THREE.Color(tint).multiplyScalar(0.7),
      transparent: true, opacity: 0.82, transmission: 0.88,
      roughness: 0.04, metalness: 0.0, ior: 1.52, thickness: 0.5,
      reflectivity: 0.6, envMapIntensity: 1.8, side: THREE.FrontSide,
    });
    const goldMat = new THREE.MeshPhysicalMaterial({
      color: new THREE.Color("#C9A25E"), metalness: 0.95, roughness: 0.12, envMapIntensity: 2.2,
    });

    const bodyGeo = new THREE.LatheGeometry(toV2(BOTTLE_PROFILE), 64, 0, Math.PI * 2);
    bodyGeo.computeVertexNormals();
    const bodyMesh = new THREE.Mesh(bodyGeo, glassMat); bodyMesh.castShadow = true;

    const capGeo = new THREE.LatheGeometry(toV2(CAP_PROFILE), 64, 0, Math.PI * 2);
    capGeo.computeVertexNormals();
    const capMesh = new THREE.Mesh(capGeo, goldMat); capMesh.castShadow = true;

    const group = new THREE.Group();
    group.add(bodyMesh, capMesh);
    group.position.y = -0.72;
    scene.add(group);

    const shadowPlane = new THREE.Mesh(new THREE.PlaneGeometry(4, 4), new THREE.ShadowMaterial({ opacity: 0.18 }));
    shadowPlane.rotation.x = -Math.PI / 2;
    shadowPlane.position.y = -0.72;
    shadowPlane.receiveShadow = true;
    scene.add(shadowPlane);

    let mouseX = 0, mouseY = 0, isHovering = false;
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

    const ro = new ResizeObserver(() => {
      const nw = canvas.offsetWidth, nh = canvas.offsetHeight;
      renderer.setSize(nw, nh, false);
      camera.aspect = nw / nh;
      camera.updateProjectionMatrix();
    });
    ro.observe(canvas);

    const clock = new THREE.Clock();
    let rotY = 0, raf = 0;

    function animate() {
      raf = requestAnimationFrame(animate);
      const dt = clock.getDelta();
      if (!isHovering && !reduced) rotY += dt * 0.5;
      group.rotation.x += ((isHovering ? mouseY * 0.25 : 0) - group.rotation.x) * 0.08;
      group.rotation.z += ((isHovering ? -mouseX * 0.15 : 0) - group.rotation.z) * 0.08;
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
      glassMat.dispose(); goldMat.dispose();
      bodyGeo.dispose(); capGeo.dispose();
      shadowPlane.geometry.dispose();
      envTexture.dispose(); pmrem.dispose();
    };
  }, [tint]);

  return <canvas ref={canvasRef} style={{ width: "100%", height: "100%", display: "block", cursor: "grab" }} />;
}
