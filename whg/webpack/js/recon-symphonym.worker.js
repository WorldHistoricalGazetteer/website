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

// Quantise an fp32 L2-normalised embedding to int8 EXACTLY as the gateway does
// (symphonym.quantize_to_byte): round(v*127) clipped to [-128,127]. The stored toponym vectors and
// the server-side query vectors use this, so the client vector must match for KNN to be comparable.
function quantiseByte(emb, out, off) {
  for (let k = 0; k < 128; k++) {
    let q = Math.round(emb[k] * 127);
    if (q > 127) q = 127; else if (q < -128) q = -128;
    out[off + k] = q;
  }
}

// Embed a list of names → Int8Array(N*128), reporting progress.
async function embedAll(names, lang) {
  const N = names.length;
  const out = new Int8Array(N * 128);
  for (let i = 0; i < N; i++) {
    const e = await embedOne(names[i], lang);
    quantiseByte(e, out, i * 128);
    if ((i & 31) === 0 || i === N - 1) self.postMessage({ type: 'progress', done: i + 1, total: N });
  }
  return out;
}

self.onmessage = async (e) => {
  const msg = e.data || {};
  try {
    await init();
    if (msg.type === 'embed') {
      const embs = await embedAll(msg.names || [], msg.lang);
      self.postMessage({ type: 'embeddings', id: msg.id, embs }, [embs.buffer]);
    }
  } catch (err) {
    self.postMessage({ type: 'error', id: msg.id, error: String((err && err.message) || err) });
  }
};
