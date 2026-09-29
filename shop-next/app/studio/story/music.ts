// Generative, royalty-free soundtrack for the video stories, rendered offline (OfflineAudioContext)
// to an AudioBuffer of exactly the video length. Scene cuts land on bar lines (see timeline.ts), and
// optional transition sounds (whoosh + impact) are placed on every cut.

export interface Mood {
  id: string; label: string; bpm: number; swing: number;
  root: number;                 // MIDI note of the key
  chords: number[][];           // semitone offsets from the root, one chord per bar
  kick: string; snare: string; hat: string; bass: string; // 16-step patterns: x = hit, o = soft, . = rest
  lead: "pluck" | "bell" | "keys" | "none";
  pad: boolean; vinyl?: boolean;
}

export const MOODS: Mood[] = [
  { id: "pop",    label: "Поп-бит · 118",    bpm: 118, swing: 0,    root: 57, chords: [[0, 3, 7], [-4, 0, 3], [3, 7, 10], [-2, 2, 5]],
    kick: "x...x...x...x...", snare: "....x.......x...", hat: "..x...x...x...xo", bass: "x..x..x.x..x..x.", lead: "pluck", pad: true },
  { id: "lux",    label: "Люкс · эмбиент 72", bpm: 72,  swing: 0,    root: 53, chords: [[0, 4, 7, 11], [-1, 2, 7, 11], [-3, 0, 5, 9], [-5, -1, 2, 7]],
    kick: "x.........x.....", snare: "........x.......", hat: "..o...o...o...o.", bass: "x.......x.......", lead: "bell", pad: true },
  { id: "lofi",   label: "Lo-fi · 84",        bpm: 84,  swing: 0.18, root: 50, chords: [[0, 3, 7, 10, 14], [5, 9, 12, 15], [-2, 2, 5, 9], [-5, -2, 2, 5]],
    kick: "x......x..x.....", snare: "....x.......x...", hat: "x.x.x.x.x.x.x.xo", bass: "x.....x...x.....", lead: "keys", pad: false, vinyl: true },
  { id: "energy", label: "Энергия · 128",     bpm: 128, swing: 0,    root: 57, chords: [[0, 3, 7], [0, 3, 7], [-4, 0, 3], [-2, 2, 5]],
    kick: "x...x...x...x...", snare: "....x.......x...", hat: "..x...x...x...x.", bass: ".xx..xx..xx..xx.", lead: "pluck", pad: true },
  { id: "chic",   label: "Шик · хаус 122",    bpm: 122, swing: 0.08, root: 55, chords: [[0, 4, 7, 11], [-3, 0, 4, 7], [2, 5, 9, 12], [-5, -1, 2, 5]],
    kick: "x...x...x...x...", snare: "....x.......x...", hat: "..x.o.x...x.o.x.", bass: "x..x...x.x..x...", lead: "keys", pad: true },
];

const mtof = (m: number) => 440 * Math.pow(2, (m - 69) / 12);

function noiseBuffer(ctx: BaseAudioContext, sec = 1) {
  const b = ctx.createBuffer(1, Math.ceil(ctx.sampleRate * sec), ctx.sampleRate);
  const d = b.getChannelData(0);
  let seed = 1234567;
  for (let i = 0; i < d.length; i++) { seed = (seed * 16807) % 2147483647; d[i] = (seed / 2147483647) * 2 - 1; }
  return b;
}

export interface MusicOpts { mood: Mood; duration: number; cuts: number[]; sfx: boolean; intensity?: number }

export async function renderMusic({ mood, duration, cuts, sfx, intensity = 1 }: MusicOpts): Promise<AudioBuffer> {
  const sr = 44100;
  const ctx = new OfflineAudioContext(2, Math.ceil(sr * (duration + 0.5)), sr);
  const beat = 60 / mood.bpm, step = beat / 4, bar = beat * 4;
  const noise = noiseBuffer(ctx, 2);

  const master = ctx.createGain();
  master.gain.setValueAtTime(0.0001, 0);
  master.gain.exponentialRampToValueAtTime(0.9, 0.4);
  master.gain.setValueAtTime(0.9, Math.max(0.5, duration - 1.6));
  master.gain.linearRampToValueAtTime(0.0001, duration);
  const comp = ctx.createDynamicsCompressor();
  comp.threshold.value = -14; comp.ratio.value = 4; comp.attack.value = 0.004; comp.release.value = 0.2;
  master.connect(comp).connect(ctx.destination);
  // sidechain feel: pad/bass bus ducks on every kick
  const bus = ctx.createGain(); bus.connect(master);

  const kick = (t: number, v = 1) => {
    const o = ctx.createOscillator(), g = ctx.createGain();
    o.frequency.setValueAtTime(150, t); o.frequency.exponentialRampToValueAtTime(42, t + 0.12);
    g.gain.setValueAtTime(0.95 * v * intensity, t); g.gain.exponentialRampToValueAtTime(0.001, t + 0.42);
    o.connect(g).connect(master); o.start(t); o.stop(t + 0.45);
    bus.gain.setValueAtTime(0.35, t); bus.gain.linearRampToValueAtTime(1, t + beat * 0.6);
  };
  const noiseHit = (t: number, dur: number, type: BiquadFilterType, freq: number, q: number, v: number, dest: AudioNode = master) => {
    const s = ctx.createBufferSource(); s.buffer = noise; s.playbackRate.value = 1 + Math.random() * 0.02;
    const f = ctx.createBiquadFilter(); f.type = type; f.frequency.value = freq; f.Q.value = q;
    const g = ctx.createGain(); g.gain.setValueAtTime(v, t); g.gain.exponentialRampToValueAtTime(0.001, t + dur);
    s.connect(f).connect(g).connect(dest); s.start(t, Math.random() * 1.5); s.stop(t + dur + 0.02);
  };
  const snare = (t: number, v = 1) => {
    noiseHit(t, 0.22, "bandpass", 1900, 0.8, 0.5 * v * intensity);
    const o = ctx.createOscillator(), g = ctx.createGain(); o.type = "triangle"; o.frequency.setValueAtTime(220, t);
    g.gain.setValueAtTime(0.25 * v, t); g.gain.exponentialRampToValueAtTime(0.001, t + 0.12); o.connect(g).connect(master); o.start(t); o.stop(t + 0.14);
  };
  const hat = (t: number, v = 1) => noiseHit(t, 0.05, "highpass", 8000, 0.7, 0.16 * v * intensity);
  const synth = (t: number, midi: number, dur: number, type: OscillatorType, v: number, cutoff: number, attack = 0.005, dest: AudioNode = bus, detune = 0) => {
    const o = ctx.createOscillator(), f = ctx.createBiquadFilter(), g = ctx.createGain();
    o.type = type; o.frequency.value = mtof(midi); o.detune.value = detune;
    f.type = "lowpass"; f.frequency.setValueAtTime(cutoff, t); f.frequency.exponentialRampToValueAtTime(Math.max(200, cutoff * 0.35), t + dur);
    g.gain.setValueAtTime(0.0001, t); g.gain.exponentialRampToValueAtTime(v, t + attack); g.gain.exponentialRampToValueAtTime(0.0008, t + dur);
    o.connect(f).connect(g).connect(dest); o.start(t); o.stop(t + dur + 0.05);
  };
  const bell = (t: number, midi: number, v: number) => {
    for (const [ratio, amp, dec] of [[1, 1, 1.6], [2.76, 0.35, 0.7], [5.4, 0.12, 0.35]] as const) {
      const o = ctx.createOscillator(), g = ctx.createGain(); o.type = "sine"; o.frequency.value = mtof(midi) * ratio;
      g.gain.setValueAtTime(v * amp, t); g.gain.exponentialRampToValueAtTime(0.0005, t + dec); o.connect(g).connect(master); o.start(t); o.stop(t + dec + 0.05);
    }
  };

  const bars = Math.ceil(duration / bar);
  for (let b = 0; b < bars; b++) {
    const t0 = b * bar;
    const chord = mood.chords[b % mood.chords.length];
    const root = mood.root + chord[0];
    const lastBar = t0 + bar > duration - 0.01;
    const firstBar = b === 0;
    // pad
    if (mood.pad) for (const n of chord) for (const d of [-7, 7]) synth(t0, mood.root + 12 + n, bar * 1.02, "sawtooth", 0.028, 1600, bar * 0.25, bus, d);
    for (let s = 0; s < 16; s++) {
      const sw = s % 2 ? mood.swing * step : 0;
      const t = t0 + s * step + sw;
      if (t >= duration - 0.05) break;
      const k = mood.kick[s], sn = mood.snare[s], h = mood.hat[s], bs = mood.bass[s];
      if (!firstBar || s >= 8) {
        if (k !== ".") kick(t, k === "o" ? 0.6 : 1);
        if (sn !== ".") snare(t, sn === "o" ? 0.5 : 1);
      }
      if (h !== ".") hat(t, h === "o" ? 0.5 : 1);
      if (bs !== "." && !firstBar) synth(t, root - 12, step * 1.8, "sawtooth", 0.22, 900, 0.005, bus);
      // lead line: chord tones, a little melody over the bar
      if (mood.lead !== "none" && !lastBar && (s % 4 === 2 || (mood.lead === "pluck" && s % 2 === 0 && s > 7))) {
        const tone = chord[(s / 2 + b) % chord.length] + 24 + (s > 11 ? 12 : 0);
        if (mood.lead === "bell") { if (s % 8 === 2) bell(t, mood.root + tone - 12, 0.12); }
        else if (mood.lead === "keys") synth(t, mood.root + tone, step * 3, "triangle", 0.09, 2600, 0.01, master);
        else synth(t, mood.root + tone, step * 1.5, "square", 0.05, 3200, 0.003, master);
      }
    }
    if (mood.vinyl) for (let i = 0; i < 18; i++) noiseHit(t0 + Math.random() * bar, 0.012, "highpass", 3000, 0.5, 0.05);
  }

  // transitions: a whoosh rising into every cut and an impact on it
  if (sfx) {
    for (const c of cuts) {
      if (c <= 0.2 || c >= duration - 0.2) continue;
      const s = ctx.createBufferSource(); s.buffer = noise;
      const f = ctx.createBiquadFilter(); f.type = "bandpass"; f.Q.value = 1.4;
      f.frequency.setValueAtTime(400, c - 0.45); f.frequency.exponentialRampToValueAtTime(6000, c);
      const g = ctx.createGain(); g.gain.setValueAtTime(0.0001, c - 0.45); g.gain.exponentialRampToValueAtTime(0.28, c - 0.03); g.gain.exponentialRampToValueAtTime(0.0001, c + 0.12);
      s.connect(f).connect(g).connect(master); s.start(c - 0.45); s.stop(c + 0.15);
      const o = ctx.createOscillator(), og = ctx.createGain(); o.frequency.setValueAtTime(90, c); o.frequency.exponentialRampToValueAtTime(35, c + 0.35);
      og.gain.setValueAtTime(0.6, c); og.gain.exponentialRampToValueAtTime(0.001, c + 0.5); o.connect(og).connect(master); o.start(c); o.stop(c + 0.55);
    }
  }
  return ctx.startRendering();
}
