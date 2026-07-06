// recon-symphonym.js
// Main-thread API for the in-browser Symphonym phonetic generator (Phase 7). Lazily spawns the worker
// (which loads the ~21 MB ONNX + wasm once, browser-cached) and offers phonetic clustering of the
// user's own place names — grouping variant spellings / near-duplicates before any network call.

let worker = null;
let seq = 0;
const pending = new Map(); // id → { resolve, reject, onProgress }

function ensureWorker() {
  if (worker) return worker;
  worker = new Worker(new URL('./recon-symphonym.worker.js', import.meta.url), { type: 'module' });
  worker.onmessage = (e) => {
    const m = e.data || {};
    if (m.type === 'progress') { pending.forEach((p) => p.onProgress && p.onProgress(m.done, m.total)); return; }
    const p = pending.get(m.id);
    if (!p) return;
    if (m.type === 'error') { pending.delete(m.id); p.reject(new Error(m.error)); return; }
    if (m.type === 'clusters') { pending.delete(m.id); p.resolve(m.groups); return; }
    if (m.type === 'embeddings') { pending.delete(m.id); p.resolve(m.embs); return; }
  };
  worker.onerror = (err) => { pending.forEach((p) => p.reject(err)); pending.clear(); };
  return worker;
}

function call(payload, onProgress) {
  const w = ensureWorker();
  const id = ++seq;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject, onProgress });
    w.postMessage({ ...payload, id });
  });
}

// Group phonetically-similar / near-duplicate names. Returns arrays of indices into `names`
// (only groups of ≥2). threshold ~0.9 (cosine) is a sensible default; higher = stricter.
export function clusterNames(names, opts = {}) {
  return call({ type: 'cluster', names, threshold: opts.threshold, lang: opts.lang }, opts.onProgress);
}

// Raw embeddings (Float32Array N*128) if a caller wants to do its own similarity work.
export function embedNames(names, opts = {}) {
  return call({ type: 'embed', names, lang: opts.lang }, opts.onProgress);
}

export function terminate() {
  if (worker) { worker.terminate(); worker = null; }
  pending.clear();
}
