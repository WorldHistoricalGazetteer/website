// recon-symphonym.worker.js
// In-browser Symphonym phonetic encoder (Phase 7). Runs the int8 ONNX Student encoder via
// onnxruntime-web (WASM backend, single-threaded — no SharedArrayBuffer / cross-origin isolation
// needed) to embed toponyms, then clusters near-duplicate / phonetically-similar names by cosine
// similarity. All local; nothing leaves the browser. Assets are self-hosted under /static/webpack/symphonym/.

import * as ort from 'onnxruntime-web/wasm';
import { tokenise } from './recon-symphonym-preprocess.js';
import { quantiseByte } from './recon-symphonym-quantise.js';
import { assetUrl, fetchVerified } from './symphonym-fetch.js';

const decoder = new TextDecoder();
const asJson = (buf) => JSON.parse(decoder.decode(buf));

// The ESM loader is shipped with a .js extension: Django static serves .mjs as
// application/octet-stream, which browsers refuse to import() as a module. .js is served as
// application/javascript. The .wasm is served correctly (application/wasm).
// Versioned but NOT integrity-checked: ORT fetches these itself, so their bytes never reach us.
ort.env.wasm.wasmPaths = {
  wasm: assetUrl('ort-wasm-simd-threaded.wasm'),
  mjs: assetUrl('ort-wasm-simd-threaded.mjs.js'),
};
ort.env.wasm.numThreads = 1;        // single-threaded: no SAB, works without COOP/COEP headers

let session = null;
let vocabs = null;
let ready = null;

async function init() {
  if (ready) return ready;
  ready = (async () => {
    const [cvBuf, svBuf, lvBuf, onnxBuf] = await Promise.all([
      fetchVerified('char_vocab.json'),
      fetchVerified('script_vocab.json'),
      fetchVerified('lang_vocab.json'),
      fetchVerified('symphonym.onnx'),
    ]);
    const cv = asJson(cvBuf), sv = asJson(svBuf), lv = asJson(lvBuf);
    vocabs = {
      charToId: cv.char_to_id || cv,
      scriptToId: sv.script_to_id || sv,
      langToId: lv.lang_to_id || lv,
    };
    // Created from the VERIFIED bytes, not re-fetched by URL — so the graph that runs is provably
    // the one that was hashed.
    session = await ort.InferenceSession.create(new Uint8Array(onnxBuf), { executionProviders: ['wasm'] });
  })();
  // A failed init must not be memoised as a permanent poisoned promise: a transient network error
  // would otherwise disable phonetic matching for the life of the page.
  ready.catch(() => { ready = null; });
  return ready;
}

// Embed one (text, lang) → Float32Array(128), L2-normalised (batch=1; the LSTM export is batch-1).
// `tokenise` guarantees at least one id (it emits [<UNK>] for whitespace-only or empty input), so
// there is no zero-length sequence to guard against here — the guard now lives where the canonical
// Python puts it, which is the only place it can also be tested against the golden fixture.
async function embedOne(text, lang) {
  const t = tokenise(String(text || ''), lang || 'und', vocabs);
  const ids = t.charIds;
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
