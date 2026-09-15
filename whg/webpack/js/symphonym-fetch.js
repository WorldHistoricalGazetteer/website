// symphonym-fetch.js
// Versioned, integrity-checked fetching of the Symphonym assets (place#283).
//
// Extracted from the worker so it can be tested against the REAL function rather than a copy of
// it. A guard whose whole job is to fire rarely is exactly the one that must be driven through its
// failure modes, and a worker that imports onnxruntime-web cannot be loaded in node to do that.
//
// WHY IT EXISTS. These are raw committed statics, not webpack output, so they carry neither a
// content hash nor the ?v=<git-hash> that bundles get. A deploy that changes them can serve a NEW
// symphonym.onnx to a browser still holding a CACHED char_vocab.json, and pairing a model with the
// wrong vocabulary does not raise — it embeds plausible nonsense.
//
// It fails NON-UNIFORMLY, which is what makes it dangerous rather than merely bad. Across v7/v8,
// of 113,269 shared characters only 98 keep their id (0.1%); by script LATIN 52/1,128, CJK
// 0/93,316, CYRILLIC 0/375, ARABIC 0/1,102, GREEK 0/365. The stable 52 are the frequent ASCII
// letters — enough for "London" and "Bury St. Edmunds" to tokenise IDENTICALLY under a mispairing
// while every non-Latin script silently gets ids from a different region of the table. The failure
// PRESENTS AS A WORKING DEPLOY and an English smoke test passes it.
//
// Two mechanisms, and the second is the necessary one: the sha256 in the URL means changed bytes
// are a changed URL (no stale cache); re-hashing what arrives catches every other route in —
// wrong build, partial deploy, two of three files copied.

import { SYMPHONYM_ASSETS } from './symphonym-assets.js';

export const ASSET_BASE = '/static/webpack/symphonym/';

export function assetUrl(name, base = ASSET_BASE) {
  const a = SYMPHONYM_ASSETS[name];
  if (!a) throw new Error(`Symphonym asset not in the manifest: ${name}`);
  return `${base}${name}?v=${a.sha256.slice(0, 16)}`;
}

export async function sha256Hex(buf) {
  const subtle = globalThis.crypto && globalThis.crypto.subtle;
  // Fail CLOSED when we cannot verify. "I could not check it" must not quietly become "proceed":
  // refusing costs only that the gateway embeds server-side, which is correct and merely slower,
  // so there is never a reason to run unverified.
  if (!subtle) {
    throw new Error('Symphonym: SubtleCrypto unavailable, cannot verify asset integrity — '
      + 'refusing to run rather than risk a mispaired model. The gateway will embed server-side.');
  }
  const digest = await subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, '0')).join('');
}

export async function fetchVerified(name, { base = ASSET_BASE, fetchImpl = globalThis.fetch } = {}) {
  const a = SYMPHONYM_ASSETS[name];
  if (!a) throw new Error(`Symphonym asset not in the manifest: ${name}`);
  const res = await fetchImpl(assetUrl(name, base));
  if (!res.ok) throw new Error(`Symphonym asset ${name} failed to load (HTTP ${res.status})`);
  const buf = await res.arrayBuffer();
  const got = await sha256Hex(buf);
  if (got !== a.sha256) {
    throw new Error(`Symphonym asset ${name} does not match the manifest `
      + `(expected ${a.sha256.slice(0, 16)}…, got ${got.slice(0, 16)}…, ${buf.byteLength} bytes). `
      + 'Model and vocabularies are mispaired or corrupt; refusing to encode.');
  }
  return buf;
}
