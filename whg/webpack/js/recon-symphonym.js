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

// Int8Array(N*128) of language-conditioned Symphonym embeddings, quantised to match the gateway's
// KNN vectors. Sent per row on reconcile so the server ranks by phonetic (vector) similarity.
export function embedNames(names, opts = {}) {
  return call({ type: 'embed', names, lang: opts.lang }, opts.onProgress);
}

export function terminate() {
  if (worker) { worker.terminate(); worker = null; }
  pending.clear();
}
