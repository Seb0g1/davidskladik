import { useEffect, useRef } from "react";

const VERT_SRC = /* glsl */ `#version 300 es
in vec2 a_pos;
void main() { gl_Position = vec4(a_pos, 0.0, 1.0); }
`;

const FRAG_SRC = /* glsl */ `#version 300 es
precision mediump float;

uniform float u_time;
uniform vec2  u_res;
out vec4 fragColor;

float hash(vec2 p) {
  return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}
float noise(vec2 p) {
  vec2 i = floor(p); vec2 f = fract(p);
  f = f * f * (3.0 - 2.0 * f);
  return mix(mix(hash(i), hash(i+vec2(1,0)), f.x),
             mix(hash(i+vec2(0,1)), hash(i+vec2(1,1)), f.x), f.y);
}
// Ridge noise: flips valleys into bright crests — the core of the metallic sheen
float ridge(vec2 p) { return 1.0 - abs(noise(p) * 2.0 - 1.0); }

float fbm(vec2 p) {
  float v = 0.0; float a = 0.5;
  for (int i = 0; i < 5; i++) {
    v += a * noise(p);
    p = p * 2.1 + vec2(cos(u_time*0.07+float(i)), sin(u_time*0.05+float(i)*1.3)) * 0.6;
    a *= 0.52;
  }
  return v;
}
// Ridge-fBm: layered ridge octaves for flowing specular streaks
float rfbm(vec2 p) {
  float v = 0.0; float a = 0.5;
  for (int i = 0; i < 4; i++) {
    v += a * ridge(p);
    p = p * 2.3 - vec2(sin(u_time*0.09+float(i)*0.71), cos(u_time*0.06+float(i)*1.13)) * 0.45;
    a *= 0.46;
  }
  return v;
}
void main() {
  vec2 uv = gl_FragCoord.xy / u_res;
  float t  = u_time * 0.14;
  vec2 p   = (uv - 0.5) * 3.0;

  // Two-level domain warp (Quilez technique) — gives the "molten pour" motion
  vec2 q = vec2(fbm(p),
                fbm(p + vec2(5.2, 1.3)));
  vec2 r = vec2(fbm(p + 4.0*q + vec2(1.7, 9.2) + t*0.20),
                fbm(p + 4.0*q + vec2(8.3, 2.8) + t*0.16));
  float n = fbm(p + 4.0*r);

  // Metallic specular layer driven by ridge noise
  float m   = rfbm(p * 0.75 + q * 1.8 + vec2(t * 0.28));
  float spec = pow(m, 2.8) * smoothstep(0.48, 0.82, n + m * 0.28);

  // 5-stop palette: void → charcoal gold → warm gold → bright gold → white-hot spec
  vec3 c0 = vec3(0.04, 0.028, 0.007);
  vec3 c1 = vec3(0.32, 0.23, 0.09);
  vec3 c2 = vec3(0.62, 0.49, 0.22);
  vec3 c3 = vec3(0.88, 0.74, 0.42);
  vec3 c4 = vec3(1.00, 0.96, 0.80);
  vec3 col = c0;
  col = mix(col, c1, smoothstep(0.18, 0.42, n));
  col = mix(col, c2, smoothstep(0.40, 0.60, n));
  col = mix(col, c3, smoothstep(0.58, 0.78, n) * 0.88);
  col = mix(col, c4, spec * 0.82);

  float edge  = smoothstep(0.0, 0.36, uv.x) * smoothstep(1.0, 0.64, uv.x)
              * smoothstep(0.0, 0.52, uv.y) * smoothstep(1.0, 0.40, uv.y);
  float alpha = clamp((n * 0.54 + spec * 0.22) * edge, 0.0, 1.0);
  fragColor   = vec4(col, alpha);
}
`;

function compileShader(gl: WebGL2RenderingContext, type: number, src: string): WebGLShader | null {
  const shader = gl.createShader(type);
  if (!shader) return null;
  gl.shaderSource(shader, src);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    console.error("[HeroShader] shader compile error:", gl.getShaderInfoLog(shader));
    gl.deleteShader(shader);
    return null;
  }
  return shader;
}

export default function HeroShader() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const gl = canvas.getContext("webgl2", { alpha: true, premultipliedAlpha: false });
    if (!gl) return; // WebGL2 not available — render nothing

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    // Compile shaders
    const vert = compileShader(gl, gl.VERTEX_SHADER, VERT_SRC);
    const frag = compileShader(gl, gl.FRAGMENT_SHADER, FRAG_SRC);
    if (!vert || !frag) return;

    // Link program
    const prog = gl.createProgram();
    if (!prog) return;
    gl.attachShader(prog, vert);
    gl.attachShader(prog, frag);
    gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
      console.error("[HeroShader] program link error:", gl.getProgramInfoLog(prog));
      return;
    }
    gl.useProgram(prog);

    // Full-screen quad: 2 triangles, 6 vertices
    const vertices = new Float32Array([-1, -1, 1, -1, -1, 1, -1, 1, 1, -1, 1, 1]);
    const buf = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.bufferData(gl.ARRAY_BUFFER, vertices, gl.STATIC_DRAW);

    const aPos = gl.getAttribLocation(prog, "a_pos");
    gl.enableVertexAttribArray(aPos);
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);

    // Uniforms
    const uTime = gl.getUniformLocation(prog, "u_time");
    const uRes  = gl.getUniformLocation(prog, "u_res");

    // Blending for premultiplied-alpha canvas
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);
    gl.clearColor(0, 0, 0, 0);

    // Resize handler
    const ro = new ResizeObserver(() => {
      const w = canvas.offsetWidth;
      const h = canvas.offsetHeight;
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width  = w;
        canvas.height = h;
        gl.viewport(0, 0, w, h);
      }
    });
    ro.observe(canvas);

    const start = performance.now();
    let raf = 0;

    function render() {
      // Sync size
      const w = canvas!.offsetWidth;
      const h = canvas!.offsetHeight;
      if (canvas!.width !== w || canvas!.height !== h) {
        canvas!.width  = w;
        canvas!.height = h;
        gl!.viewport(0, 0, w, h);
      }

      const t = (performance.now() - start) / 1000;
      gl!.clear(gl!.COLOR_BUFFER_BIT);
      gl!.uniform1f(uTime, t);
      gl!.uniform2f(uRes, canvas!.width, canvas!.height);
      gl!.drawArrays(gl!.TRIANGLES, 0, 6);

      if (!reduced) {
        raf = requestAnimationFrame(render);
      }
    }

    raf = requestAnimationFrame(render);

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
      // Lose context to free GPU memory
      const ext = gl.getExtension("WEBGL_lose_context");
      if (ext) ext.loseContext();
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{
        position: "absolute",
        inset: 0,
        width: "100%",
        height: "100%",
        pointerEvents: "none",
        zIndex: 0,
      }}
    />
  );
}
