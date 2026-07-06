// recon-symphonym.worker.js
// In-browser Symphonym phonetic encoder (Phase 7). Runs the int8 ONNX Student encoder via
// onnxruntime-web (WASM backend, single-threaded — no SharedArrayBuffer / cross-origin isolation
// needed) to embed toponyms, then clusters near-duplicate / phonetically-similar names by cosine
// similarity. All local; nothing leaves the browser. Assets are self-hosted under /static/webpack/symphonym/.

import * as ort from 'onnxruntime-web/wasm';
import { tokenise } from './recon-symphonym-preprocess.js';

const BASE = '/static/webpack/symphonym/';
// The ESM loader is shipped with a .js extension: Django static serves .mjs as
// application/octet-stream, which browsers refuse to import() as a module. .js is served as
// application/javascript. The .wasm is served correctly (application/wasm).
ort.env.wasm.wasmPaths = {
  wasm: BASE + 'ort-wasm-simd-threaded.wasm',
  mjs: BASE + 'ort-wasm-simd-threaded.mjs.js',
};
ort.env.wasm.numThreads = 1;        // single-threaded: no SAB, works without COOP/COEP headers

let session = null;
let vocabs = null;
let ready = null;

async function init() {
  if (ready) return ready;
  ready = (async () => {
    const [cv, sv, lv] = await Promise.all([
      fetch(BASE + 'char_vocab.json').then((r) => r.json()),
      fetch(BASE + 'script_vocab.json').then((r) => r.json()),
      fetch(BASE + 'lang_vocab.json').then((r) => r.json()),
    ]);
    vocabs = {
      charToId: cv.char_to_id || cv,
      scriptToId: sv.script_to_id || sv,
      langToId: lv.lang_to_id || lv,
    };
    session = await ort.InferenceSession.create(BASE + 'symphonym.onnx', { executionProviders: ['wasm'] });
  })();
  return ready;
}

const unkChar = () => (vocabs.charToId['<UNK>'] != null ? vocabs.charToId['<UNK>'] : 1);

// Embed one (text, lang) → Float32Array(128), L2-normalised (batch=1; the LSTM export is batch-1).
async function embedOne(text, lang) {
  const t = tokenise(String(text || ''), lang || 'und', vocabs);
  const ids = t.length ? t.charIds : new Int32Array([unkChar()]); // never feed a zero-length sequence
  const len = ids.length;
  const charIds = BigInt64Array.from(ids, (v) => BigInt(v));
  const feeds = {
    char_ids: new ort.Tensor('int64', charIds, [1, len]),
    script_id: new ort.Tensor('int64', BigInt64Array.from([BigInt(t.scriptId)]), [1]),
    lang_id: new ort.Tensor('int64', BigInt64Array.from([BigInt(t.langId)]), [1]),
    length: new ort.Tensor('int64', BigInt64Array.from([BigInt(len)]), [1]),
  };
  const out = await session.run(feeds);
  return out.embedding.data; // Float32Array(128)
}

// Embed a list of names, reporting progress. Returns Float32Array(N*128).
async function embedAll(names, lang) {
  const N = names.length;
  const embs = new Float32Array(N * 128);
  for (let i = 0; i < N; i++) {
    const e = await embedOne(names[i], lang);
    embs.set(e, i * 128);
    if ((i & 31) === 0 || i === N - 1) self.postMessage({ type: 'progress', done: i + 1, total: N });
  }
  return embs;
}

function cosine(a, ai, b, bi) {
  let dot = 0;
  for (let k = 0; k < 128; k++) dot += a[ai * 128 + k] * b[bi * 128 + k]; // both L2-normalised → dot = cosine
  return dot;
}

// Single-link clustering by cosine ≥ threshold (union-find). Returns groups of size ≥ 2.
function cluster(embs, N, threshold) {
  const parent = new Int32Array(N).map((_, i) => i);
  const find = (x) => { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
  const union = (a, b) => { const ra = find(a), rb = find(b); if (ra !== rb) parent[ra] = rb; };
  for (let i = 0; i < N; i++) {
    for (let j = i + 1; j < N; j++) {
      if (cosine(embs, i, embs, j) >= threshold) union(i, j);
    }
  }
  const groups = new Map();
  for (let i = 0; i < N; i++) { const r = find(i); if (!groups.has(r)) groups.set(r, []); groups.get(r).push(i); }
  return [...groups.values()].filter((g) => g.length > 1);
}

self.onmessage = async (e) => {
  const msg = e.data || {};
  try {
    await init();
    if (msg.type === 'cluster') {
      const names = msg.names || [];
      const embs = await embedAll(names, msg.lang);
      const groups = cluster(embs, names.length, msg.threshold != null ? msg.threshold : 0.9);
      self.postMessage({ type: 'clusters', id: msg.id, groups });
    } else if (msg.type === 'embed') {
      const embs = await embedAll(msg.names || [], msg.lang);
      self.postMessage({ type: 'embeddings', id: msg.id, embs }, [embs.buffer]);
    }
  } catch (err) {
    self.postMessage({ type: 'error', id: msg.id, error: String((err && err.message) || err) });
  }
};
