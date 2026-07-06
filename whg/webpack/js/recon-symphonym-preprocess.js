// recon-symphonym-preprocess.js
// Exact JS port of the Symphonym tokeniser/preprocessing from indexing/hf/inference.py — the input
// side of the in-browser phonetic embedding (Phase 7). Parity with the reference is essential: the
// ONNX encoder was trained on these ids, so any drift silently degrades embeddings. Verified against
// inference.py._tokenise for a multi-script sample set (see the node parity check in developer notes).
//
// Produces the four ONNX inputs for a (text, lang) pair:
//   char_ids : Int32Array  (per-codepoint ids from char_vocab; <UNK> fallback)
//   script_id: dominant Unicode script id (script_vocab; OTHER→…; default 0)
//   lang_id  : language id (lang_vocab; <UNK>/0 fallback)
//   length   : codepoint count

// Unicode script ranges used during training — order matters (first matching range wins per char).
const SCRIPT_RANGES = [
  ['LATIN', [[0x0041, 0x007A], [0x00C0, 0x024F], [0x1E00, 0x1EFF]]],
  ['CYRILLIC', [[0x0400, 0x04FF], [0x0500, 0x052F]]],
  ['ARABIC', [[0x0600, 0x06FF], [0x0750, 0x077F], [0xFB50, 0xFDFF], [0xFE70, 0xFEFF]]],
  ['CJK', [[0x4E00, 0x9FFF], [0x3400, 0x4DBF], [0x20000, 0x2A6DF], [0xF900, 0xFAFF]]],
  ['HANGUL', [[0xAC00, 0xD7AF], [0x1100, 0x11FF], [0x3130, 0x318F]]],
  ['HIRAGANA', [[0x3041, 0x3096]]],
  ['KATAKANA', [[0x30A1, 0x30FA], [0x31F0, 0x31FF]]],
  ['DEVANAGARI', [[0x0900, 0x097F]]],
  ['BENGALI', [[0x0980, 0x09FF]]],
  ['GUJARATI', [[0x0A80, 0x0AFF]]],
  ['GURMUKHI', [[0x0A00, 0x0A7F]]],
  ['TAMIL', [[0x0B80, 0x0BFF]]],
  ['TELUGU', [[0x0C00, 0x0C7F]]],
  ['KANNADA', [[0x0C80, 0x0CFF]]],
  ['MALAYALAM', [[0x0D00, 0x0D7F]]],
  ['THAI', [[0x0E00, 0x0E7F]]],
  ['GEORGIAN', [[0x10A0, 0x10FF]]],
  ['ARMENIAN', [[0x0530, 0x058F]]],
  ['HEBREW', [[0x0590, 0x05FF], [0xFB1D, 0xFB4F]]],
  ['GREEK', [[0x0370, 0x03FF], [0x1F00, 0x1FFF]]],
];

// Dominant script by codepoint count; first matching range wins; unmatched → OTHER; empty → OTHER.
export function detectScript(text) {
  const counts = Object.create(null);
  for (const ch of text) {
    const cp = ch.codePointAt(0);
    let matched = false;
    for (const [name, ranges] of SCRIPT_RANGES) {
      for (const [lo, hi] of ranges) {
        if (cp >= lo && cp <= hi) { counts[name] = (counts[name] || 0) + 1; matched = true; break; }
      }
      if (matched) break;
    }
    if (!matched) counts.OTHER = (counts.OTHER || 0) + 1;
  }
  let best = null, bestN = -1;
  for (const k in counts) if (counts[k] > bestN) { best = k; bestN = counts[k]; }
  return best == null ? 'OTHER' : best;
}

// vocabs: { charToId, scriptToId, langToId } — the *_to_id maps from the JSON vocab files.
export function tokenise(text, lang, vocabs) {
  const charToId = vocabs.charToId, scriptToId = vocabs.scriptToId, langToId = vocabs.langToId;
  const unkChar = charToId['<UNK>'] != null ? charToId['<UNK>'] : 1;
  const unkLang = langToId['<UNK>'] != null ? langToId['<UNK>'] : 0;
  const chars = Array.from(text); // iterate by codepoint, matching Python str iteration
  const charIds = new Int32Array(chars.length);
  for (let i = 0; i < chars.length; i++) {
    const id = charToId[chars[i]];
    charIds[i] = id != null ? id : unkChar;
  }
  const scriptId = scriptToId[detectScript(text)];
  const langId = langToId[lang];
  return {
    charIds,
    scriptId: scriptId != null ? scriptId : 0,
    langId: langId != null ? langId : unkLang,
    length: chars.length,
  };
}
