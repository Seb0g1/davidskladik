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
float fbm(vec2 p) {
  float v = 0.0; float a = 0.5;
  for (int i = 0; i < 5; i++) {
    v += a * noise(p);
    p = p * 2.1 + vec2(cos(u_time*0.07+float(i)), sin(u_time*0.05+float(i)*1.3)) * 0.6;
    a *= 0.52;
  }
  return v;
}
void main() {
  vec2 uv = gl_FragCoord.xy / u_res;
  float t = u_time * 0.22;
  vec2 p = (uv - 0.5) * 2.8;
  p += vec2(sin(t*0.6)*0.4, cos(t*0.45)*0.3);
  float n = fbm(p + fbm(p + fbm(p)));
  vec3 dark   = vec3(0.05, 0.035, 0.01);
  vec3 mid    = vec3(0.49, 0.39, 0.22);
  vec3 bright = vec3(0.81, 0.69, 0.43);
  vec3 col    = mix(dark, mid, smoothstep(0.3, 0.6, n));
  col         = mix(col, bright, smoothstep(0.6, 0.85, n) * 0.7);
  float edge  = smoothstep(0.0, 0.38, uv.x) * smoothstep(1.0, 0.62, uv.x)
              * smoothstep(0.0, 0.55, uv.y) * smoothstep(1.0, 0.38, uv.y);
  float alpha = clamp(n * 0.52 * edge, 0.0, 1.0);
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
